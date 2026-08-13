// SPDX-License-Identifier: Apache-2.0

//! Server binary: binds the gRPC endpoint and serves `XmlParseService`, the
//! standard health service and server reflection until SIGINT or SIGTERM.
//!
//! Every knob is an optional environment variable, because the image is
//! read-only and has no config file to mount:
//!
//! - `GRPC_XML_ADDR` — listen address (default `0.0.0.0:50051`).
//! - `GRPC_XML_WORKERS` — tokio worker threads (default: CPU count).
//! - `GRPC_XML_BLOCKING_THREADS` — cap on the blocking pool that runs the
//!   parsers (default 512, tokio's own default).
//! - `GRPC_XML_MAX_CONCURRENT_PARSES` — parses admitted at once (default 64).
//!   Past the cap a request is refused with `RESOURCE_EXHAUSTED` rather than
//!   queued.
//! - `GRPC_XML_MAX_DOCUMENT_MIB` — byte cap applied when a request asks for 0
//!   (default 256).
//! - `GRPC_XML_MAX_DOCUMENT_MIB_CEILING` — hard cap a request cannot exceed
//!   (default 1024).
//! - `GRPC_XML_METRICS_INTERVAL_SECS` — seconds between metrics lines
//!   (default 60; 0 disables them).
//! - `GRPC_XML_WINDOW_BYTES` — HTTP/2 initial stream and connection window
//!   (default 16 MiB).

use std::time::Duration;

use grpc_xml::metrics::{self, Metrics};
use grpc_xml::proto;
use grpc_xml::proto::v1::xml_parse_service_server::XmlParseServiceServer;
use grpc_xml::service::{
    CEILING_MAX_DOCUMENT_MIB, DEFAULT_MAX_CONCURRENT_PARSES, DEFAULT_MAX_DOCUMENT_MIB, XmlGrpc,
};
use tonic::transport::Server;

/// Default listen address when `GRPC_XML_ADDR` is not set.
const DEFAULT_ADDR: &str = "0.0.0.0:50051";

/// Default HTTP/2 initial window, for both the stream and the connection.
///
/// hyper defaults both to 1 MiB, which paces a bulk upload at one window per
/// round trip as soon as there is real latency in the path. Documents here
/// are tens of megabytes, so the window is widened; it is not made enormous,
/// because the whole point of the parse being streamed is that the server
/// never needs the document resident.
const DEFAULT_WINDOW_BYTES: u32 = 16 * 1024 * 1024;

/// Default seconds between metrics lines.
const DEFAULT_METRICS_INTERVAL_SECS: u64 = 60;

/// Read a numeric environment variable, falling back to `default`.
fn env_num<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workers = env_num(
        "GRPC_XML_WORKERS",
        std::thread::available_parallelism().map_or(4, usize::from),
    );
    let blocking = env_num("GRPC_XML_BLOCKING_THREADS", 512usize);

    // Parsing is CPU-bound and blocking; it runs in the blocking pool so it
    // never stalls the async workers moving bytes on and off the sockets.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(workers)
        .max_blocking_threads(blocking)
        .build()?;
    runtime.block_on(serve())
}

async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("GRPC_XML_ADDR")
        .unwrap_or_else(|_| DEFAULT_ADDR.to_owned())
        .parse()?;

    let metrics = Metrics::new();
    metrics::spawn_reporter(
        std::sync::Arc::clone(&metrics),
        Duration::from_secs(env_num(
            "GRPC_XML_METRICS_INTERVAL_SECS",
            DEFAULT_METRICS_INTERVAL_SECS,
        )),
    );

    let service = XmlGrpc::with_metrics(metrics)
        .with_default_max_document_mib(env_num(
            "GRPC_XML_MAX_DOCUMENT_MIB",
            DEFAULT_MAX_DOCUMENT_MIB,
        ))
        .with_ceiling_max_document_mib(env_num(
            "GRPC_XML_MAX_DOCUMENT_MIB_CEILING",
            CEILING_MAX_DOCUMENT_MIB,
        ))
        .with_max_concurrent_parses(env_num(
            "GRPC_XML_MAX_CONCURRENT_PARSES",
            DEFAULT_MAX_CONCURRENT_PARSES,
        ))
        .into_service();

    // Health lets an orchestrator gate traffic; reflection lets grpcurl and
    // the fleet's tooling discover the contract without a local copy of the
    // protos.
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<XmlParseServiceServer<XmlGrpc>>()
        .await;
    // The health contract is registered for reflection too, so `grpcurl list`
    // shows an operator everything the server answers rather than only the
    // half this repository owns.
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    let window: u32 = env_num("GRPC_XML_WINDOW_BYTES", DEFAULT_WINDOW_BYTES);
    println!("grpc-xml listening on {addr} (http2 window {window} bytes)");
    Server::builder()
        .tcp_nodelay(true)
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .http2_keepalive_interval(Some(Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)))
        .initial_stream_window_size(window)
        .initial_connection_window_size(window)
        .add_service(health_service)
        .add_service(reflection)
        .add_service(service)
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;
    println!("grpc-xml shut down");
    Ok(())
}

/// Resolve on SIGINT (Ctrl-C) or SIGTERM, the signal a container runtime
/// sends on stop, so open streams drain instead of being cut mid-item.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");
    tokio::select! {
        _ = ctrl_c => {}
        _ = sigterm.recv() => {}
    }
}
