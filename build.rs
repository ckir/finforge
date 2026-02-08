fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=src/proto/yahoo_pricing.proto");
    prost_build::compile_protos(
        &["src/proto/yahoo_pricing.proto"],
        &["src/proto/"],
    )?;
    Ok(())
}