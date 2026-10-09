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
    // Vendored byte-identical from the gRParse repository beside
    // document.proto, which imports them for `Document.analyses`.
    let opennlp = [
        "proto/org/apache/opennlp/grpc/v1/opennlp_document.proto",
        "proto/org/apache/opennlp/grpc/v1/opennlp_annotations.proto",
    ];
    for proto in protos.iter().chain(&opennlp) {
        println!("cargo:rerun-if-changed={proto}");
    }
    // The OpenNLP package is generated on its own and included at
    // `crate::opennlp::v1` (see lib.rs); the main pass below names it by
    // `extern_path`, because prost's relative path from the document module
    // would climb past the crate root.
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        .compile_protos(&opennlp, &[proto_root])?;
    let descriptor = std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("descriptor.bin");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(descriptor)
        .extern_path(".org.apache.opennlp.grpc.v1", "crate::opennlp::v1")
        .compile_protos(&protos, &[proto_root])?;
    Ok(())
}
