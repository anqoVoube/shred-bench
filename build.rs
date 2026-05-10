fn main() {
    if std::env::var("PROTOC").is_err() {
        std::env::set_var("PROTOC", protobuf_src::protoc());
    }
    tonic_build::configure()
        .compile(&["proto/shreder_binary.proto"], &["proto"])
        .unwrap();
}
