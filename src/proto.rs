pub mod mq {
    include!(concat!(env!("OUT_DIR"), "/mq.rs"));
}

// pub fn test() {
//     let _ = mq::Command::default();
// }
