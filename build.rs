fn main() {
    prost_build::compile_protos(&["proto/mq.proto"], &["proto/"]).unwrap();
}
