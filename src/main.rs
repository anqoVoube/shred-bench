//! Shred-stream provider benchmark — Pulse Raiden vs Shreder.
//!
//! Subscribes to both providers concurrently with the bot's account-include
//! filter (PumpFun pAMM + Raydium AMM v4 + Raydium CPMM), stamps every tx's
//! arrival, and reports race timing + coverage + throughput at the end of a
//! fixed window.
//!
//! Usage:
//!     cargo run --release -- [--duration 60] [--grace 5]
//!                            [--raiden  http://fra.pulse.raiden.wtf:16000]
//!                            [--shreder http://fra.binary.shreder.xyz:9991]

mod shreder_binary {
    tonic::include_proto!("shreder_binary");
}

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::{channel::mpsc::unbounded as fut_unbounded, sink::SinkExt};
use shreder_binary::{
    shreder_binary_service_client::ShrederBinaryServiceClient,
    SubscribeBinaryTransactionsRequest, SubscribeRequestFilterBinaryTransactions,
};
use tokio::sync::mpsc;

// Bot's shred-path subscription set (`bot::spawn_shred_stream` in client.rs):
// 3 direct AMM programs + 4 aggregator outer-programs the dispatchers handle.
const PUMP_FUN: &str     = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const RAYDIUM_LPV4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const RAYDIUM_CPMM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
const JUPITER: &str      = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
const OKX: &str          = "proVF4pMXVaYqmy4NjniPh4pqKNfMmsihgd4wdkCX3u";
const DFLOW: &str        = "DF1ow4tspfHX9JwWJsAb9epbkA8hmpSEAtxXy1V27QBH";
const AXIOM: &str        = "FLASHX8DrLbgeR8FcfNV1F5krxYcYMUdBkrP1EPBtxB9";

const DEFAULT_RAIDEN: &str = "http://fra.pulse.raiden.wtf:16000";
const DEFAULT_SHREDER: &str = "http://fra.binary.shreder.xyz:9991";
const DEFAULT_DURATION_SECS: u64 = 60;
const DEFAULT_GRACE_SECS: u64 = 5;
const ROLLING_TICK: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Provider {
    Raiden,
    Shreder,
}

impl Provider {
    fn label(self) -> &'static str {
        match self {
            Provider::Raiden => "raiden",
            Provider::Shreder => "shreder",
        }
    }
}

struct StreamEvent {
    provider: Provider,
    sig: [u8; 64],
    /// Monotonic clock at message receipt — used for the wall-clock race
    /// (cross-provider per-sig deltas).
    arrival: Instant,
    /// Wall-clock time at message receipt — used together with `created_at`
    /// to compute one-way latency `wall_arrival - created_at`.
    wall_arrival: SystemTime,
    /// Provider-stamped send time from the response envelope. `None` if
    /// the provider didn't populate `SubscribeBinaryTransactionsResponse.
    /// created_at` (shouldn't happen in practice but be defensive).
    created_at: Option<SystemTime>,
}

#[derive(Default)]
struct SigRec {
    raiden: Option<Instant>,
    shreder: Option<Instant>,
    raiden_created_at: Option<SystemTime>,
    shreder_created_at: Option<SystemTime>,
}

fn ts_to_system_time(ts: &prost_types::Timestamp) -> SystemTime {
    let secs = ts.seconds.max(0) as u64;
    let nanos = ts.nanos.max(0) as u32;
    UNIX_EPOCH + Duration::new(secs, nanos)
}

/// Signed `a - b` in microseconds. Negative if `b > a`. Saturates at
/// `i64::MAX` / `i64::MIN` if a duration somehow exceeds 290k years.
fn signed_micros_diff(a: SystemTime, b: SystemTime) -> i64 {
    match a.duration_since(b) {
        Ok(d) => i64::try_from(d.as_micros()).unwrap_or(i64::MAX),
        Err(e) => -i64::try_from(e.duration().as_micros()).unwrap_or(i64::MAX),
    }
}

/// How to express the program filter on the wire. Both providers ship the
/// same proto (3 filter fields: `account_include`, `account_exclude`,
/// `account_required`), but `account_include` isn't implemented identically
/// across providers — the shreder example for instance uses
/// `account_required`. This flag lets you A/B without recompiling.
#[derive(Clone, Copy, Debug)]
enum FilterMode {
    /// `account_include = [PumpFun, RaydiumAmm, RaydiumCpmm, Jupiter, OKX,
    /// DFlow, Axiom]` — exactly mirrors `bot::spawn_shred_stream` in
    /// supra-stop-loss/client.rs.
    Include,
    /// `account_required = [PumpFun]` — matches the shreder reference example
    /// (`SubscribeRequestFilterBinaryTransactions { account_required: [...] }`).
    /// Single program only because `account_required` semantics are AND across
    /// the list (every named account must appear in the tx).
    RequiredPumpfun,
    /// All three filter vecs empty — provider should stream everything that
    /// matches the filter group. Useful as a "is the stream alive at all?"
    /// probe when both targeted modes return nothing.
    None,
}

impl FilterMode {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "include" => Some(Self::Include),
            "required" | "required-pumpfun" => Some(Self::RequiredPumpfun),
            "none" => Some(Self::None),
            _ => None,
        }
    }

    fn build(self) -> SubscribeRequestFilterBinaryTransactions {
        match self {
            Self::Include => SubscribeRequestFilterBinaryTransactions {
                account_include: vec![
                    // Direct AMM dispatch targets.
                    PUMP_FUN.to_string(),
                    RAYDIUM_LPV4.to_string(),
                    RAYDIUM_CPMM.to_string(),
                    // Aggregator outer-program ids — Jupiter / OKX / DFlow /
                    // Axiom routes show up here even when the underlying AMM
                    // tx wouldn't trigger the AMM-only filter.
                    JUPITER.to_string(),
                    OKX.to_string(),
                    DFLOW.to_string(),
                    AXIOM.to_string(),
                ],
                account_exclude: vec![],
                account_required: vec![],
            },
            Self::RequiredPumpfun => SubscribeRequestFilterBinaryTransactions {
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![PUMP_FUN.to_string()],
            },
            Self::None => SubscribeRequestFilterBinaryTransactions {
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![],
            },
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Include => {
                "account_include = [PumpFun pAMM, Raydium AMM v4, Raydium CPMM, Jupiter, OKX, DFlow, Axiom]"
            }
            Self::RequiredPumpfun => "account_required = [PumpFun]",
            Self::None => "(empty — match-all)",
        }
    }
}

struct Cli {
    raiden_url: String,
    shreder_url: String,
    duration: Duration,
    grace: Duration,
    filter: FilterMode,
}

fn parse_cli() -> Cli {
    let mut raiden_url = DEFAULT_RAIDEN.to_string();
    let mut shreder_url = DEFAULT_SHREDER.to_string();
    let mut duration_secs = DEFAULT_DURATION_SECS;
    let mut grace_secs = DEFAULT_GRACE_SECS;
    let mut filter = FilterMode::Include;

    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--raiden" => {
                raiden_url = args.get(i + 1).expect("--raiden needs a URL").clone();
                i += 2;
            }
            "--shreder" => {
                shreder_url = args.get(i + 1).expect("--shreder needs a URL").clone();
                i += 2;
            }
            "--duration" => {
                duration_secs = args
                    .get(i + 1)
                    .expect("--duration needs seconds")
                    .parse()
                    .expect("--duration must be a number");
                i += 2;
            }
            "--grace" => {
                grace_secs = args
                    .get(i + 1)
                    .expect("--grace needs seconds")
                    .parse()
                    .expect("--grace must be a number");
                i += 2;
            }
            "--filter" | "--filter-mode" => {
                let v = args.get(i + 1).expect("--filter needs a value");
                filter = FilterMode::parse(v).unwrap_or_else(|| {
                    eprintln!("bad --filter value '{v}' (use: include | required | none)");
                    std::process::exit(2);
                });
                i += 2;
            }
            "--help" | "-h" => {
                println!(
                    "usage: shred-bench [--duration SECS] [--grace SECS] \
                     [--filter include|required|none] \
                     [--raiden URL] [--shreder URL]"
                );
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown arg: {other}");
                std::process::exit(2);
            }
        }
    }
    Cli {
        raiden_url,
        shreder_url,
        duration: Duration::from_secs(duration_secs),
        grace: Duration::from_secs(grace_secs),
        filter,
    }
}

/// Connect, subscribe with the bot's filter, forward every tx into `tx`.
/// Stops when the parent task drops the receiver, or on stream error.
async fn run_provider(
    provider: Provider,
    url: String,
    filter: FilterMode,
    tx: mpsc::UnboundedSender<StreamEvent>,
) {
    let label = provider.label();
    const STAGE_TIMEOUT: Duration = Duration::from_secs(10);

    eprintln!("[{label}] connecting to {url}...");
    let connect_fut = ShrederBinaryServiceClient::connect(url.clone());
    let mut client = match tokio::time::timeout(STAGE_TIMEOUT, connect_fut).await {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => {
            eprintln!("[{label}] connect failed: {e}");
            return;
        }
        Err(_) => {
            eprintln!("[{label}] connect timed out after {:?}", STAGE_TIMEOUT);
            return;
        }
    };
    eprintln!("[{label}] connected");

    let request = SubscribeBinaryTransactionsRequest {
        transactions: maplit::hashmap! {
            "pools".to_owned() => filter.build()
        },
    };
    let (mut sub_tx, sub_rx) = fut_unbounded();

    // Push the request *before* awaiting the server's response. Some servers
    // hold the bidi stream open without producing a header until they see the
    // first client message; doing it in the original order works on Shreder
    // but appeared to deadlock against Raiden.
    if let Err(e) = sub_tx.send(request).await {
        eprintln!("[{label}] send subscribe req failed: {e}");
        return;
    }
    eprintln!("[{label}] subscribe request queued; awaiting server stream...");

    let subscribe_fut = client.subscribe_binary_transactions(sub_rx);
    let response = match tokio::time::timeout(STAGE_TIMEOUT, subscribe_fut).await {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            eprintln!("[{label}] subscribe failed: {e}");
            return;
        }
        Err(_) => {
            eprintln!(
                "[{label}] subscribe timed out after {:?} — server accepted the connection but never opened the response stream",
                STAGE_TIMEOUT
            );
            return;
        }
    };
    let mut stream = response.into_inner();
    eprintln!("[{label}] subscribed: {url}");

    let mut got_first = false;
    loop {
        match stream.message().await {
            Ok(Some(resp)) => {
                let arrival = Instant::now();
                let wall_arrival = SystemTime::now();
                if !got_first {
                    got_first = true;
                    eprintln!("[{label}] first message arrived");
                }
                let created_at = resp.created_at.as_ref().map(ts_to_system_time);
                let Some(update) = resp.transaction else {
                    continue;
                };
                let Some(btx) = update.transaction else {
                    continue;
                };
                let Some(first_sig) = btx.signatures.first() else {
                    continue;
                };
                if first_sig.len() != 64 {
                    continue;
                }
                let mut sig = [0u8; 64];
                sig.copy_from_slice(first_sig);
                if tx
                    .send(StreamEvent {
                        provider,
                        sig,
                        arrival,
                        wall_arrival,
                        created_at,
                    })
                    .is_err()
                {
                    break;
                }
            }
            Ok(None) => {
                eprintln!("[{label}] stream ended");
                break;
            }
            Err(e) => {
                eprintln!("[{label}] stream error: {e}");
                break;
            }
        }
    }
}

/// Aggregator state.
struct Agg {
    sigs: HashMap<[u8; 64], SigRec>,
    raiden_msgs: u64,
    shreder_msgs: u64,
    // -- wall-clock race (monotonic Instant deltas) --
    /// Per-sig race deltas in microseconds (raiden_arrival − shreder_arrival).
    /// Negative = shreder first, positive = raiden first.
    deltas_us: Vec<i64>,
    raiden_first: u64,
    shreder_first: u64,
    ties: u64,
    // -- one-way wire latency: our_arrival − provider.created_at --
    raiden_one_way_us: Vec<i64>,
    shreder_one_way_us: Vec<i64>,
    // -- provider-stamp race: raiden.created_at − shreder.created_at --
    /// Per-sig provider-internal race deltas in microseconds. Subject to
    /// host-clock skew between Raiden's and Shreder's servers — a constant
    /// offset across all entries is the skew, variance is real processing
    /// timing differences.
    created_at_deltas_us: Vec<i64>,
    created_at_raiden_first: u64,
    created_at_shreder_first: u64,
    created_at_ties: u64,
}

impl Agg {
    fn new() -> Self {
        Self {
            sigs: HashMap::new(),
            raiden_msgs: 0,
            shreder_msgs: 0,
            deltas_us: Vec::new(),
            raiden_first: 0,
            shreder_first: 0,
            ties: 0,
            raiden_one_way_us: Vec::new(),
            shreder_one_way_us: Vec::new(),
            created_at_deltas_us: Vec::new(),
            created_at_raiden_first: 0,
            created_at_shreder_first: 0,
            created_at_ties: 0,
        }
    }

    fn ingest(&mut self, ev: StreamEvent) {
        match ev.provider {
            Provider::Raiden => self.raiden_msgs += 1,
            Provider::Shreder => self.shreder_msgs += 1,
        }

        // One-way latency: how long after the provider stamped the message
        // did we see it? Sample only when the provider populated created_at.
        if let Some(stamp) = ev.created_at {
            let one_way = signed_micros_diff(ev.wall_arrival, stamp);
            match ev.provider {
                Provider::Raiden => self.raiden_one_way_us.push(one_way),
                Provider::Shreder => self.shreder_one_way_us.push(one_way),
            }
        }

        let rec = self.sigs.entry(ev.sig).or_default();
        match ev.provider {
            Provider::Raiden => {
                if rec.raiden.is_none() {
                    rec.raiden = Some(ev.arrival);
                    rec.raiden_created_at = ev.created_at;
                }
            }
            Provider::Shreder => {
                if rec.shreder.is_none() {
                    rec.shreder = Some(ev.arrival);
                    rec.shreder_created_at = ev.created_at;
                }
            }
        }

        // Wall-clock race close: only on the second arrival, so each shared
        // sig contributes exactly one delta.
        if let (Some(r), Some(s)) = (rec.raiden, rec.shreder) {
            let just_closed = match ev.provider {
                Provider::Raiden => rec.shreder.is_some() && rec.raiden == Some(ev.arrival),
                Provider::Shreder => rec.raiden.is_some() && rec.shreder == Some(ev.arrival),
            };
            if just_closed {
                let delta_us = (r.saturating_duration_since(s).as_micros() as i64)
                    - (s.saturating_duration_since(r).as_micros() as i64);
                self.deltas_us.push(delta_us);
                if delta_us < 0 {
                    self.shreder_first += 1;
                } else if delta_us > 0 {
                    self.raiden_first += 1;
                } else {
                    self.ties += 1;
                }
                // Provider-stamp race close (only if both providers
                // populated created_at on their respective messages).
                if let (Some(rc), Some(sc)) = (rec.raiden_created_at, rec.shreder_created_at) {
                    let cdelta_us = signed_micros_diff(rc, sc);
                    self.created_at_deltas_us.push(cdelta_us);
                    if cdelta_us < 0 {
                        self.created_at_shreder_first += 1;
                    } else if cdelta_us > 0 {
                        self.created_at_raiden_first += 1;
                    } else {
                        self.created_at_ties += 1;
                    }
                }
            }
        }
    }

    fn coverage(&self) -> (u64, u64, u64) {
        let mut both = 0u64;
        let mut r_only = 0u64;
        let mut s_only = 0u64;
        for rec in self.sigs.values() {
            match (rec.raiden.is_some(), rec.shreder.is_some()) {
                (true, true) => both += 1,
                (true, false) => r_only += 1,
                (false, true) => s_only += 1,
                _ => {}
            }
        }
        (both, r_only, s_only)
    }
}

fn pct(numer: u64, denom: u64) -> f64 {
    if denom == 0 {
        0.0
    } else {
        100.0 * numer as f64 / denom as f64
    }
}

fn percentile(sorted: &[i64], p: f64) -> Option<i64> {
    if sorted.is_empty() {
        return None;
    }
    let idx = ((sorted.len() - 1) as f64 * p / 100.0).round() as usize;
    Some(sorted[idx.min(sorted.len() - 1)])
}

fn fmt_us(us: i64) -> String {
    if us.abs() >= 1000 {
        format!("{:>+8.2}ms", us as f64 / 1000.0)
    } else {
        format!("{:>+8}µs", us)
    }
}

fn running_median(v: &[i64]) -> Option<i64> {
    if v.is_empty() {
        return None;
    }
    let mut s = v.to_vec();
    s.sort_unstable();
    Some(s[s.len() / 2])
}

/// Print min / p1 / p5 / p25 / p50 / p75 / p95 / p99 / max / mean for a
/// pre-sorted slice. No-op (with a one-line note) if empty.
fn print_distribution(label: &str, sorted: &[i64]) {
    if sorted.is_empty() {
        println!("  {label}: (no samples)");
        return;
    }
    let n = sorted.len() as i64;
    let mean: i64 = sorted.iter().sum::<i64>() / n.max(1);
    println!("  {label}");
    println!("    n   : {}", sorted.len());
    println!("    min : {}", fmt_us(*sorted.first().unwrap()));
    println!("    p1  : {}", fmt_us(percentile(sorted, 1.0).unwrap()));
    println!("    p5  : {}", fmt_us(percentile(sorted, 5.0).unwrap()));
    println!("    p25 : {}", fmt_us(percentile(sorted, 25.0).unwrap()));
    println!("    p50 : {}", fmt_us(percentile(sorted, 50.0).unwrap()));
    println!("    p75 : {}", fmt_us(percentile(sorted, 75.0).unwrap()));
    println!("    p95 : {}", fmt_us(percentile(sorted, 95.0).unwrap()));
    println!("    p99 : {}", fmt_us(percentile(sorted, 99.0).unwrap()));
    println!("    max : {}", fmt_us(*sorted.last().unwrap()));
    println!("    mean: {}", fmt_us(mean));
}

fn print_rolling(agg: &Agg, elapsed: Duration) {
    let (both, r_only, s_only) = agg.coverage();
    let total = both + r_only + s_only;
    let secs = elapsed.as_secs_f64().max(0.001);
    let r_one_way = running_median(&agg.raiden_one_way_us)
        .map(fmt_us)
        .unwrap_or_else(|| "    n/a".to_string());
    let s_one_way = running_median(&agg.shreder_one_way_us)
        .map(fmt_us)
        .unwrap_or_else(|| "    n/a".to_string());
    println!(
        "[{:>4.0}s] msgs r={:>6} ({:>6.1}/s) s={:>6} ({:>6.1}/s) | sigs total={:>5} both={:>5} r_only={:>4} s_only={:>4} | races r_first={:>5} s_first={:>5} ties={} | one_way r_p50={} s_p50={}",
        elapsed.as_secs_f64(),
        agg.raiden_msgs, agg.raiden_msgs as f64 / secs,
        agg.shreder_msgs, agg.shreder_msgs as f64 / secs,
        total, both, r_only, s_only,
        agg.raiden_first, agg.shreder_first, agg.ties,
        r_one_way, s_one_way,
    );
}

fn print_summary(agg: &Agg, window: Duration, total_elapsed: Duration) {
    let (both, r_only, s_only) = agg.coverage();
    let total = both + r_only + s_only;
    let secs = window.as_secs_f64().max(0.001);

    println!();
    println!("==============================================================");
    println!("  shred-bench summary  (window {:.1}s, +grace included)", window.as_secs_f64());
    println!("==============================================================");
    println!();
    println!("Throughput");
    println!("  raiden  : {:>7} msgs   ({:>7.1} /s)", agg.raiden_msgs, agg.raiden_msgs as f64 / secs);
    println!("  shreder : {:>7} msgs   ({:>7.1} /s)", agg.shreder_msgs, agg.shreder_msgs as f64 / secs);
    println!();
    println!("Coverage  (unique signatures)");
    println!("  total seen          : {:>5}", total);
    println!("  both providers      : {:>5}   ({:>5.1}%)", both,    pct(both, total));
    println!("  raiden  only        : {:>5}   ({:>5.1}%)", r_only,  pct(r_only, total));
    println!("  shreder only        : {:>5}   ({:>5.1}%)", s_only,  pct(s_only, total));
    println!();

    // Wall-clock race
    if agg.deltas_us.is_empty() {
        println!("Wall-clock race  : no shared signatures observed");
    } else {
        let mut sorted = agg.deltas_us.clone();
        sorted.sort_unstable();
        println!("Wall-clock race  (raiden_arrival − shreder_arrival)");
        println!("  Negative = shreder arrived first; positive = raiden arrived first.");
        println!("  pairs        : {:>6}", agg.deltas_us.len());
        println!("  raiden first : {:>6}   ({:>5.1}%)", agg.raiden_first, pct(agg.raiden_first, both));
        println!("  shreder first: {:>6}   ({:>5.1}%)", agg.shreder_first, pct(agg.shreder_first, both));
        println!("  ties         : {:>6}   ({:>5.1}%)", agg.ties, pct(agg.ties, both));
        println!();
        print_distribution("delta distribution:", &sorted);
    }
    println!();

    // One-way wire latency, per provider
    println!("One-way wire latency  (our_arrival − provider.created_at)");
    let mut r_one = agg.raiden_one_way_us.clone();
    r_one.sort_unstable();
    print_distribution("raiden:", &r_one);
    let mut s_one = agg.shreder_one_way_us.clone();
    s_one.sort_unstable();
    print_distribution("shreder:", &s_one);
    println!();

    // Provider-stamp race (caveat: cross-host clock skew baked in)
    if agg.created_at_deltas_us.is_empty() {
        println!("Provider-stamp race  : no shared sigs with both `created_at` populated");
    } else {
        let mut sorted = agg.created_at_deltas_us.clone();
        sorted.sort_unstable();
        let cab_pairs = agg.created_at_deltas_us.len() as u64;
        println!("Provider-stamp race  (raiden.created_at − shreder.created_at)");
        println!("  Negative = shreder stamped first; positive = raiden stamped first.");
        println!("  Caveat: includes any clock-skew between the two provider hosts.");
        println!("  pairs        : {:>6}", agg.created_at_deltas_us.len());
        println!("  raiden first : {:>6}   ({:>5.1}%)", agg.created_at_raiden_first, pct(agg.created_at_raiden_first, cab_pairs));
        println!("  shreder first: {:>6}   ({:>5.1}%)", agg.created_at_shreder_first, pct(agg.created_at_shreder_first, cab_pairs));
        println!("  ties         : {:>6}   ({:>5.1}%)", agg.created_at_ties, pct(agg.created_at_ties, cab_pairs));
        println!();
        print_distribution("delta distribution:", &sorted);
    }

    println!();
    println!("Verdict");
    let r_first = agg.raiden_first;
    let s_first = agg.shreder_first;
    let race_winner = if both == 0 {
        "n/a (no shared sigs)"
    } else if s_first > r_first {
        "shreder (faster on shared sigs)"
    } else if r_first > s_first {
        "raiden (faster on shared sigs)"
    } else {
        "tie on race count"
    };
    let coverage_winner = match (r_only, s_only) {
        (a, b) if a > b => "raiden (more unique sigs)",
        (a, b) if b > a => "shreder (more unique sigs)",
        _ => "tie",
    };
    let throughput_winner = match (agg.raiden_msgs, agg.shreder_msgs) {
        (a, b) if a > b => "raiden",
        (a, b) if b > a => "shreder",
        _ => "tie",
    };
    println!("  race timing : {}", race_winner);
    println!("  coverage    : {}", coverage_winner);
    println!("  throughput  : {}", throughput_winner);
    println!();
    println!("(total wall time including grace: {:.1}s)", total_elapsed.as_secs_f64());
}

#[tokio::main]
async fn main() {
    let cli = parse_cli();
    println!("shred-bench");
    println!("  raiden  : {}", cli.raiden_url);
    println!("  shreder : {}", cli.shreder_url);
    println!("  duration: {:?}  grace: {:?}", cli.duration, cli.grace);
    println!("  filter  : {}", cli.filter.label());
    println!();

    let (tx, mut rx) = mpsc::unbounded_channel::<StreamEvent>();

    let raiden_handle = tokio::spawn(run_provider(
        Provider::Raiden,
        cli.raiden_url.clone(),
        cli.filter,
        tx.clone(),
    ));
    let shreder_handle = tokio::spawn(run_provider(
        Provider::Shreder,
        cli.shreder_url.clone(),
        cli.filter,
        tx.clone(),
    ));
    drop(tx); // aggregator owns the receiving end; drops happen via task abort

    let start = Instant::now();
    let window_end = start + cli.duration;
    let final_end = window_end + cli.grace;
    let mut next_tick = start + ROLLING_TICK;

    let mut agg = Agg::new();

    // Phase 1 — main window: ingest + periodic rolling stats.
    loop {
        let now = Instant::now();
        if now >= window_end {
            break;
        }
        let until_tick = next_tick.saturating_duration_since(now);
        let until_end = window_end.saturating_duration_since(now);
        let timeout = until_tick.min(until_end);

        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(ev)) => agg.ingest(ev),
            Ok(None) => break,
            Err(_) => {} // timeout, fall through to tick check
        }

        if Instant::now() >= next_tick {
            print_rolling(&agg, Instant::now() - start);
            next_tick += ROLLING_TICK;
        }
    }

    // Stop both subscriptions — closing the channel sends EOF up the pipe.
    raiden_handle.abort();
    shreder_handle.abort();

    // Phase 2 — grace: drain in-flight messages already in transit.
    eprintln!("[bench] window done; draining grace period...");
    loop {
        let now = Instant::now();
        if now >= final_end {
            break;
        }
        let until_end = final_end - now;
        match tokio::time::timeout(until_end, rx.recv()).await {
            Ok(Some(ev)) => agg.ingest(ev),
            Ok(None) => break,
            Err(_) => break,
        }
    }

    print_summary(&agg, cli.duration, Instant::now() - start);
}
