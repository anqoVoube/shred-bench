# shred-bench

Side-by-side benchmark of two shred-stream providers — **Pulse Raiden**
(`http://fra.pulse.raiden.wtf:16000`) and **Shreder**
(`http://fra.binary.shreder.xyz:9991`). Both speak the same `shreder_binary`
gRPC service (`SubscribeBinaryTransactions`); the proto is copied verbatim
from `~/Work/shreder-rust-example`.

The benchmark subscribes to both streams concurrently with the bot's account-
include filter (PumpFun pAMM + Raydium AMM v4 + Raydium CPMM), stamps every
tx's arrival, and reports race timing + coverage + throughput at the end of
a fixed window.

## Run

```bash
cargo build --release
./target/release/shred-bench                      # default 60s + 5s grace
./target/release/shred-bench --duration 300       # 5-minute run
./target/release/shred-bench --raiden http://...  # override an endpoint
```

## What the output means

```
Throughput
  raiden  : 12,034 msgs   ( 200.6 /s)
  shreder : 11,892 msgs   ( 198.2 /s)

Coverage  (unique signatures)
  total seen          : 11,123
  both providers      : 10,950   (98.4%)
  raiden  only        :    87    ( 0.8%)
  shreder only        :    86    ( 0.8%)

Race timing  (raiden_arrival − shreder_arrival)
  Negative = shreder arrived first; positive = raiden arrived first.
  pairs        : 10950
  raiden first :  3120  (28.5%)
  shreder first:  7820  (71.4%)
  ties         :    10  ( 0.1%)

  delta distribution:
    p50: +12.4ms     ← median: shreder beat raiden by ~12ms
    p95: +47.8ms     ← 95% of the time shreder won by ≤48ms
    ...
```

- **Race timing** is the primary metric for the trading edge — it shows how
  many ms earlier each provider delivers the same signature. Negative
  deltas mean Shreder won; positive mean Raiden won.
- **Coverage** catches a provider that's fast but lossy (or vice versa). On
  a healthy run, both providers should overlap on ≥95% of sigs.
- **Throughput** is a sanity check on connection health — large drift
  between the two usually means one stream stalled.

## Notes

- Race timestamps come from `Instant::now()` immediately after
  `stream.message().await` returns. Both streams run in the same Tokio
  runtime so scheduling jitter affects them equally.
- After the main window closes, a short grace period (default 5s) drains
  in-flight messages so deliveries that crossed the boundary still count.
- The shared-sig pair set is built from the **first** signature in each
  delivered tx (proto field `BinaryTransaction.signatures[0]`).
- Filter is hard-coded to the bot's three programs. Edit `PUMP_FUN`,
  `RAYDIUM_LPV4`, `RAYDIUM_CPMM` constants in `src/main.rs` to broaden.
