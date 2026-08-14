// SPDX-License-Identifier: Apache-2.0

//! Build script: compiles the buf-managed contracts under `proto/` into Rust
//! server and client stubs with tonic-prost-build.
//!
//! Generation happens on every build rather than being committed, so the
//! stubs cannot drift from the schema that `buf lint` gates. Clients are
//! generated too, so the integration tests can drive a real server over a
//! real socket. `buf generate` (see `buf.gen.yaml`) produces the same stubs
//! outside cargo and is not part of the build.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = "proto";
    let protos = [
        // Vendored byte-identical from the gRParse repository: the Document
        // plane this collector projects into. Never edited here.
        "proto/ai/pipestream/document/v1/document.proto",
        "proto/ai/pipestream/xml/v1/xml.proto",
        "proto/ai/pipestream/xml/v1/xml_service.proto",
    ];
    for proto in &protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    let descriptor = std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("descriptor.bin");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(descriptor)
        .compile_protos(&protos, &[proto_root])?;
    Ok(())
}
