// SPDX-License-Identifier: Apache-2.0

//! The `ai.pipestream.xml.v1.XmlParseService` implementation.
//!
//! The shape is the one the fleet's Rust collectors converge on: request
//! chunks are forwarded into a bounded channel, a blocking task pulls them
//! through the parser as a `Read`, and parse events go back over a second
//! bounded channel that the client drains. Both channels are small, which is
//! what makes backpressure real — a client that stops reading stops the
//! parse rather than filling the server's heap with a document it is not
//! collecting.
//!
//! Nothing here holds a complete copy of the document. The only bytes
//! resident are the chunks in flight between the two channels, which is what
//! "diskless" means in practice: there is no spill path because there is
//! nothing large enough to want one.

use std::io::{self, Read};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::metrics::Metrics;
use crate::parse::{self, CAP_MARKER, InputStats, ParseConfig, ParseError};
use crate::proto::v1 as pb;
use crate::proto::v1::xml_parse_service_server::{XmlParseService, XmlParseServiceServer};
use crate::sniff::Dialect;
use crate::{PARSER, VERSION};

/// Default document byte cap when a request asks for 0: 256 MiB.
pub const DEFAULT_MAX_DOCUMENT_MIB: u32 = 256;

/// Hard ceiling on the byte cap, whatever a request asks for: 1 GiB.
///
/// The cap protects the server's memory even though the parse itself is
/// streaming, because a document is only bounded by what the client sends
/// and the item currently being captured grows with it.
pub const CEILING_MAX_DOCUMENT_MIB: u32 = 1024;

/// Default number of parses admitted at once.
pub const DEFAULT_MAX_CONCURRENT_PARSES: usize = 64;

/// Bound of the chunk channel from the request stream into the parser.
const CHUNK_CHANNEL_BOUND: usize = 8;

/// Bound of the event channel from the parser back to the client.
const EVENT_CHANNEL_BOUND: usize = 32;

/// How long the parser may wait on a client that is not draining before the
/// parse is abandoned.
///
/// Without it a client that opens streams and never reads them pins one
/// blocking thread each, and enough of those take the whole pool. A consumer
/// that has taken nothing in this long is not slow, it is gone.
const CONSUMER_STALL: Duration = Duration::from_secs(30);

/// gRPC implementation of `ai.pipestream.xml.v1.XmlParseService`.
pub struct XmlGrpc {
    default_max_document_bytes: u64,
    ceiling_max_document_bytes: u64,
    max_concurrent_parses: usize,
    parse_slots: Arc<tokio::sync::Semaphore>,
    metrics: Arc<Metrics>,
}

impl XmlGrpc {
    /// A service with the fleet defaults.
    #[must_use]
    pub fn new() -> Self {
        Self::with_metrics(Metrics::new())
    }

    /// A service reporting into an existing counter set, so the binary can
    /// print the same counters the service updates.
    #[must_use]
    pub fn with_metrics(metrics: Arc<Metrics>) -> Self {
        Self {
            default_max_document_bytes: mib(DEFAULT_MAX_DOCUMENT_MIB),
            ceiling_max_document_bytes: mib(CEILING_MAX_DOCUMENT_MIB),
            max_concurrent_parses: DEFAULT_MAX_CONCURRENT_PARSES,
            parse_slots: Arc::new(tokio::sync::Semaphore::new(DEFAULT_MAX_CONCURRENT_PARSES)),
            metrics,
        }
    }

    /// Override the cap applied when a request asks for 0.
    #[must_use]
    pub fn with_default_max_document_mib(mut self, mib_value: u32) -> Self {
        self.default_max_document_bytes = mib(mib_value);
        self
    }

    /// Override the hard ceiling a request cannot exceed.
    #[must_use]
    pub fn with_ceiling_max_document_mib(mut self, mib_value: u32) -> Self {
        self.ceiling_max_document_bytes = mib(mib_value);
        self
    }

    /// Override how many parses may run at once.
    #[must_use]
    pub fn with_max_concurrent_parses(mut self, max: usize) -> Self {
        self.max_concurrent_parses = max;
        self.parse_slots = Arc::new(tokio::sync::Semaphore::new(max));
        self
    }

    /// Wrap the service in its generated tonic server.
    #[must_use]
    pub fn into_service(self) -> XmlParseServiceServer<Self> {
        XmlParseServiceServer::new(self)
    }

    /// The counters this service updates.
    #[must_use]
    pub fn metrics(&self) -> Arc<Metrics> {
        Arc::clone(&self.metrics)
    }

    /// Resolve the byte cap for one request: 0 means the default, and
    /// anything above the ceiling is clamped rather than refused, because a
    /// client asking for more memory than the server has is asking, not
    /// attacking.
    fn resolve_cap(&self, requested_mib: u32) -> u64 {
        if requested_mib == 0 {
            self.default_max_document_bytes
        } else {
            mib(requested_mib).min(self.ceiling_max_document_bytes)
        }
    }

    /// Take a parse slot, or refuse.
    fn admit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, Status> {
        Arc::clone(&self.parse_slots)
            .try_acquire_owned()
            .map_err(|_| {
                self.metrics.parses_refused.fetch_add(1, Ordering::Relaxed);
                Status::resource_exhausted(
                    "too many concurrent parses; retry shortly or raise \
                     GRPC_XML_MAX_CONCURRENT_PARSES",
                )
            })
    }
}

impl Default for XmlGrpc {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl XmlParseService for XmlGrpc {
    type ParseXmlStream = ReceiverStream<Result<pb::ParseXmlResponse, Status>>;

    async fn parse_xml(
        &self,
        request: Request<Streaming<pb::ParseXmlRequest>>,
    ) -> Result<Response<Self::ParseXmlStream>, Status> {
        let permit = self.admit()?;
        let mut requests = request.into_inner();

        let options = match requests.message().await {
            Ok(Some(message)) => match message.payload {
                Some(pb::parse_xml_request::Payload::Options(options)) => options,
                Some(pb::parse_xml_request::Payload::Chunk(_)) | None => {
                    return Err(Status::invalid_argument(
                        "the first ParseXml request message must set `options`",
                    ));
                }
            },
            Ok(None) => {
                return Err(Status::invalid_argument(
                    "empty ParseXml request stream; the first message must set `options`",
                ));
            }
            Err(status) => return Err(status),
        };

        let dialect = pb::XmlDialect::try_from(options.dialect).map_err(|_| {
            Status::invalid_argument(format!("unknown dialect {}", options.dialect))
        })?;
        let config = ParseConfig {
            dialect: Dialect::from_proto(dialect),
            emit_html_islands: options.emit_html_islands,
            include_attributes: options.include_attributes,
            taxonomy_supplied: !options.taxonomy.is_empty(),
        };
        let limit = self.resolve_cap(options.max_document_mib);
        let stats = InputStats::with_limit(limit);

        self.metrics.parses_started.fetch_add(1, Ordering::Relaxed);

        let (chunk_tx, chunk_rx) = mpsc::channel::<Vec<u8>>(CHUNK_CHANNEL_BOUND);
        let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_BOUND);
        let forward_tx = event_tx.clone();
        let panic_tx = event_tx.clone();

        // Forward document bytes into the parser. Dropping `chunk_tx` on the
        // way out is what signals EOF, including when the client aborts.
        tokio::spawn(async move {
            loop {
                match requests.message().await {
                    Ok(Some(message)) => match message.payload {
                        Some(pb::parse_xml_request::Payload::Chunk(chunk)) => {
                            if chunk.is_empty() {
                                continue;
                            }
                            if chunk_tx.send(chunk).await.is_err() {
                                break;
                            }
                        }
                        Some(pb::parse_xml_request::Payload::Options(_)) => {
                            let _ = forward_tx
                                .send(Err(Status::invalid_argument(
                                    "`options` may only be set on the first request message",
                                )))
                                .await;
                            break;
                        }
                        None => {
                            let _ = forward_tx
                                .send(Err(Status::invalid_argument(
                                    "every ParseXml request message must set `options` or \
                                     `chunk`",
                                )))
                                .await;
                            break;
                        }
                    },
                    Ok(None) => break,
                    Err(transport) => {
                        let _ = forward_tx.send(Err(transport)).await;
                        break;
                    }
                }
            }
        });

        let metrics = Arc::clone(&self.metrics);
        let handle = tokio::runtime::Handle::current();
        tokio::spawn(async move {
            // The permit lives as long as the parse, not as long as this
            // async wrapper, so it is moved into the blocking closure.
            let joined = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                run_parse(&handle, chunk_rx, &event_tx, &config, &stats, &metrics);
            })
            .await;
            match joined {
                Ok(()) => {}
                Err(e) if e.is_panic() => {
                    // A panic in the parser is this server's fault, not the
                    // document's, so it is INTERNAL and not INVALID_ARGUMENT.
                    let _ = panic_tx
                        .send(Err(Status::internal("the XML parser task panicked")))
                        .await;
                }
                Err(_) => {
                    let _ = panic_tx
                        .send(Err(Status::cancelled("the XML parser task was cancelled")))
                        .await;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(event_rx)))
    }

    async fn get_service_info(
        &self,
        _request: Request<pb::GetServiceInfoRequest>,
    ) -> Result<Response<pb::GetServiceInfoResponse>, Status> {
        Ok(Response::new(pb::GetServiceInfoResponse {
            service: "grpc-xml".to_owned(),
            version: VERSION.to_owned(),
            parser: PARSER.to_owned(),
            dialects: Dialect::all().iter().map(|d| d.to_proto() as i32).collect(),
            default_max_document_mib: to_mib(self.default_max_document_bytes),
            ceiling_max_document_mib: to_mib(self.ceiling_max_document_bytes),
            max_concurrent_parses: u32::try_from(self.max_concurrent_parses).unwrap_or(u32::MAX),
            // Compiled in, not configured: see `crate::security`.
            entity_expansion_disabled: true,
        }))
    }
}

/// The blocking half of one parse.
fn run_parse(
    handle: &tokio::runtime::Handle,
    chunk_rx: mpsc::Receiver<Vec<u8>>,
    event_tx: &mpsc::Sender<Result<pb::ParseXmlResponse, Status>>,
    config: &ParseConfig,
    stats: &InputStats,
    metrics: &Metrics,
) {
    let reader =
        io::BufReader::with_capacity(64 * 1024, ChannelReader::new(chunk_rx, stats.clone()));
    let mut events = 0u64;
    let mut emit = |event: pb::ParseXmlResponse| {
        // A bounded send with a deadline: real backpressure for a client that
        // is merely slow, and an exit for one that has stopped reading
        // without closing the stream.
        let sent = handle.block_on(async {
            tokio::time::timeout(CONSUMER_STALL, event_tx.send(Ok(event)))
                .await
                .is_ok_and(|r| r.is_ok())
        });
        if sent {
            events += 1;
        }
        sent
    };

    match parse::parse(reader, config, stats, &mut emit) {
        Ok(dialect) => metrics.record_success(dialect, stats.bytes(), events),
        Err(ParseError::ConsumerGone) => {
            metrics.parses_failed.fetch_add(1, Ordering::Relaxed);
        }
        Err(error) => {
            metrics.parses_failed.fetch_add(1, Ordering::Relaxed);
            if matches!(error, ParseError::TooLarge { .. }) {
                metrics.parses_capped.fetch_add(1, Ordering::Relaxed);
            }
            let failure = status_for(&error);
            let _ = handle.block_on(async {
                tokio::time::timeout(CONSUMER_STALL, event_tx.send(Err(failure))).await
            });
        }
    }
}

/// The fleet's error taxonomy, in one place.
fn status_for(error: &ParseError) -> Status {
    let message = error.to_string();
    match error {
        ParseError::Malformed(_)
        | ParseError::Truncated(_)
        | ParseError::Refused(_)
        | ParseError::Ambiguous(_) => Status::invalid_argument(message),
        ParseError::Unsupported(_) => Status::unimplemented(message),
        ParseError::TooLarge { .. } => Status::resource_exhausted(message),
        ParseError::Io(_) => Status::internal(message),
        ParseError::ConsumerGone => Status::cancelled(message),
    }
}

/// A `Read` over the request stream that enforces the byte cap.
///
/// The cap lives here rather than in the driver so it fires on the chunk that
/// crosses the line, while the client is still uploading, instead of after
/// the upload completes. quick-xml can only see an `io::Error`, so the
/// crossing is also recorded in [`InputStats::capped`] for the driver to read
/// back.
struct ChannelReader {
    rx: mpsc::Receiver<Vec<u8>>,
    current: Option<(Vec<u8>, usize)>,
    received: u64,
    stats: InputStats,
}

impl ChannelReader {
    fn new(rx: mpsc::Receiver<Vec<u8>>, stats: InputStats) -> Self {
        Self {
            rx,
            current: None,
            received: 0,
            stats,
        }
    }
}

impl Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            if let Some((chunk, consumed)) = &mut self.current {
                if *consumed < chunk.len() {
                    let n = (chunk.len() - *consumed).min(buf.len());
                    buf[..n].copy_from_slice(&chunk[*consumed..*consumed + n]);
                    *consumed += n;
                    self.stats.consumed.fetch_add(n as u64, Ordering::Relaxed);
                    return Ok(n);
                }
                self.current = None;
            }
            match self.rx.blocking_recv() {
                Some(chunk) if chunk.is_empty() => {}
                Some(chunk) => {
                    self.received += chunk.len() as u64;
                    if self.received > self.stats.limit_bytes {
                        self.stats.capped.store(true, Ordering::Relaxed);
                        return Err(io::Error::other(CAP_MARKER));
                    }
                    self.current = Some((chunk, 0));
                }
                None => return Ok(0),
            }
        }
    }
}

/// Mebibytes as bytes.
fn mib(value: u32) -> u64 {
    u64::from(value) * 1024 * 1024
}

/// Bytes as whole mebibytes, for reporting.
fn to_mib(value: u64) -> u32 {
    u32::try_from(value / (1024 * 1024)).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_selects_the_default_and_large_requests_clamp() {
        let service = XmlGrpc::new()
            .with_default_max_document_mib(8)
            .with_ceiling_max_document_mib(64);
        assert_eq!(service.resolve_cap(0), mib(8));
        assert_eq!(service.resolve_cap(16), mib(16));
        assert_eq!(service.resolve_cap(4096), mib(64));
    }

    #[test]
    fn every_parse_error_maps_to_its_documented_code() {
        use tonic::Code;
        let cases = [
            (ParseError::Malformed("x".into()), Code::InvalidArgument),
            (ParseError::Truncated("x".into()), Code::InvalidArgument),
            (ParseError::Refused("x".into()), Code::InvalidArgument),
            (ParseError::Ambiguous("x".into()), Code::InvalidArgument),
            (ParseError::Unsupported("x".into()), Code::Unimplemented),
            (
                ParseError::TooLarge { limit_bytes: 1 },
                Code::ResourceExhausted,
            ),
            (ParseError::Io("x".into()), Code::Internal),
        ];
        for (error, want) in cases {
            assert_eq!(status_for(&error).code(), want, "{error}");
        }
    }
}
