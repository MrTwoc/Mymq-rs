use anyhow::Ok;
use rcgen::{CertificateParams, KeyPair, SanType};
use std::fs;

fn main() -> anyhow::Result<()> {
    let key_pair = KeyPair::generate()?;
    let mut params = CertificateParams::default();

    params.subject_alt_names = vec![
        //
        SanType::DnsName("localhost".try_into()?),
        SanType::IpAddress("127.0.0.1".parse()?),
    ];

    let cert = params.self_signed(&key_pair)?;

    fs::write("cert.der", cert.der())?;
    fs::write("key.der", key_pair.serialize_der())?;

    println!("证书已生成：cert.der / key.der");
    Ok(())
}
