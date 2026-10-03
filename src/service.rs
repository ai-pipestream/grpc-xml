// SPDX-License-Identifier: Apache-2.0

//! The `ai.pipestream.xml.v1.XmlParseService` implementation.
//!
//! The shape is the one the fleet's Rust collectors converge on: request
//! chunks are forwarded into a bounded channel, a blocking task pulls them
//! through the parser as a `Read`, and parse events go back over a queue
//! bounded in bytes that the client drains. Both bounds are small, which is
//! what makes backpressure real: a client that stops reading stops the
//! parse rather than filling the server's heap with a document it is not
//! collecting. The event queue is bounded in bytes rather than in events so
//! that a client which uploads the whole document before it reads anything
//! still completes whenever the events fit, instead of stalling on the
//! thirty-third small event.
//!
//! Nothing here holds a complete copy of the document. The only bytes
//! resident are the chunks in flight and the events waiting in the queue,
//! both bounded, which is what
//! "diskless" means in practice: there is no spill path because there is
//! nothing large enough to want one.

use std::io::{self, Read};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use prost::Message as _;
use tokio::sync::{Semaphore, mpsc};
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use crate::document_fold::DocumentFold;
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

/// Default bound, in encoded bytes, of the events queued for one client.
///
/// Small events are most of a stream, so a bound in events made a client
/// that uploads everything before reading stall after a few dozen of them.
/// A bound in bytes holds the events of a typical document whole while
/// still capping what one parse can make the server hold for a client that
/// is not reading: 64 parses at 4 MiB each is 256 MiB at worst.
pub const DEFAULT_EVENT_QUEUE_BYTES: usize = 4 * 1024 * 1024;

/// Default for how long the parser waits on a client that is not draining
/// before the parse is abandoned.
///
/// Without it a client that opens streams and never reads them pins one
/// blocking thread each, and enough of those take the whole pool. A consumer
/// that has taken nothing in this long is not slow, it is gone.
pub const DEFAULT_CONSUMER_STALL: Duration = Duration::from_secs(30);

/// Default for how long the server waits for the next request message.
///
/// A parse holds a slot from its options until its upload ends, so a client
/// that opens a stream and then sends nothing would hold one for as long as
/// HTTP/2 keepalive keeps the connection up.
pub const DEFAULT_INPUT_IDLE: Duration = Duration::from_secs(30);

/// gRPC implementation of `ai.pipestream.xml.v1.XmlParseService`.
pub struct XmlGrpc {
    default_max_document_bytes: u64,
    ceiling_max_document_bytes: u64,
    max_concurrent_parses: usize,
    parse_slots: Arc<tokio::sync::Semaphore>,
    metrics: Arc<Metrics>,
    timeouts: Timeouts,
}

/// The waits one parse is allowed, copied into each parse.
#[derive(Debug, Clone, Copy)]
struct Timeouts {
    consumer_stall: Duration,
    input_idle: Duration,
    event_queue_bytes: usize,
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
            timeouts: Timeouts {
                consumer_stall: DEFAULT_CONSUMER_STALL,
                input_idle: DEFAULT_INPUT_IDLE,
                event_queue_bytes: DEFAULT_EVENT_QUEUE_BYTES,
            },
        }
    }

    /// Override how long a parse waits on a client that has stopped reading.
    #[must_use]
    pub fn with_consumer_stall(mut self, stall: Duration) -> Self {
        self.timeouts.consumer_stall = stall;
        self
    }

    /// Override how long the server waits for the next request message.
    #[must_use]
    pub fn with_input_idle_timeout(mut self, idle: Duration) -> Self {
        self.timeouts.input_idle = idle;
        self
    }

    /// Override how many encoded bytes of events may wait for one client.
    #[must_use]
    pub fn with_event_queue_bytes(mut self, bytes: usize) -> Self {
        self.timeouts.event_queue_bytes = bytes.clamp(1, Semaphore::MAX_PERMITS);
        self
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
    type ParseXmlStream = ParseStream;

    async fn parse_xml(
        &self,
        request: Request<Streaming<pb::ParseXmlRequest>>,
    ) -> Result<Response<Self::ParseXmlStream>, Status> {
        let deadline = request_deadline(request.metadata());
        let mut requests = request.into_inner();
        let timeouts = self.timeouts;

        // The slot is taken once the options are in hand, so a client that
        // opens a stream and sends nothing holds no slot while it waits.
        let first = tokio::time::timeout(timeouts.input_idle, requests.message())
            .await
            .map_err(|_| idle_status(timeouts.input_idle))?;
        let options = match first {
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
        let permit = self.admit()?;

        let dialect = pb::XmlDialect::try_from(options.dialect).map_err(|_| {
            Status::invalid_argument(format!("unknown dialect {}", options.dialect))
        })?;
        let config = ParseConfig {
            dialect: Dialect::from_proto(dialect),
            emit_html_islands: options.emit_html_islands,
            emit_inline_spans: options.emit_inline_spans,
            emit_source_metadata: options.emit_source_metadata,
            include_attributes: options.include_attributes,
            taxonomy_supplied: !options.taxonomy.is_empty(),
        };
        let limit = self.resolve_cap(options.max_document_mib);
        let mut stats = InputStats::with_limit(limit);
        stats.deadline = deadline;
        let emit_document = options.emit_document;

        self.metrics.parses_started.fetch_add(1, Ordering::Relaxed);

        let (chunk_tx, chunk_rx) = mpsc::channel::<Vec<u8>>(CHUNK_CHANNEL_BOUND);
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let queue = EventQueue {
            tx: event_tx,
            bytes: Arc::new(Semaphore::new(timeouts.event_queue_bytes)),
            capacity: timeouts.event_queue_bytes,
            stall: timeouts.consumer_stall,
        };
        let stream = ParseStream {
            rx: event_rx,
            bytes: Arc::clone(&queue.bytes),
            stats: stats.clone(),
            finished: false,
        };
        let forward_tx = queue.tx.clone();
        let panic_tx = queue.tx.clone();

        forward_chunks(
            requests,
            chunk_tx,
            forward_tx,
            timeouts.input_idle,
            deadline,
            limit,
        );

        let metrics = Arc::clone(&self.metrics);
        let handle = tokio::runtime::Handle::current();
        tokio::spawn(async move {
            // The permit lives as long as the parse, not as long as this
            // async wrapper, so it is moved into the blocking closure.
            let joined = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                run_parse(
                    &handle,
                    chunk_rx,
                    &queue,
                    &config,
                    &stats,
                    &metrics,
                    emit_document,
                );
            })
            .await;
            match joined {
                Ok(()) => {}
                Err(e) if e.is_panic() => {
                    // A panic in the parser is this server's fault, not the
                    // document's, so it is INTERNAL and not INVALID_ARGUMENT.
                    let _ = panic_tx.send(Queued::failure(Status::internal(
                        "the XML parser task panicked",
                    )));
                }
                Err(_) => {
                    let _ = panic_tx.send(Queued::failure(Status::cancelled(
                        "the XML parser task was cancelled",
                    )));
                }
            }
        });

        Ok(Response::new(stream))
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
            // Frontend advertisement for the shared demo shell; the values
            // are part of the contract, not configuration.
            ui: Some(pb::UiInfo {
                title: "XML".to_owned(),
                path: "/ui/xml".to_owned(),
                description: "Declarative XML to the gRParse Document data plane".to_owned(),
            }),
        }))
    }
}

/// The blocking half of one parse.
///
/// `emit_document` adds the Document fold to the same event path: every event
/// the driver produces is folded on its way out, and the folded Document is
/// sent as its own event just before the trailer. With the flag off no fold
/// exists and the path is byte for byte what it was.
fn run_parse(
    handle: &tokio::runtime::Handle,
    chunk_rx: mpsc::Receiver<Vec<u8>>,
    queue: &EventQueue,
    config: &ParseConfig,
    stats: &InputStats,
    metrics: &Metrics,
    emit_document: bool,
) {
    let reader =
        io::BufReader::with_capacity(64 * 1024, ChannelReader::new(chunk_rx, stats.clone()));
    let mut events = 0u64;
    let mut stalled = false;
    let mut fold = emit_document.then(DocumentFold::new);
    let mut deliver = |event: pb::ParseXmlResponse| match queue.deliver(handle, event) {
        Delivery::Sent => true,
        Delivery::Stalled => {
            stalled = true;
            false
        }
        Delivery::Gone => false,
    };
    let mut emit = |event: pb::ParseXmlResponse| {
        if let Some(fold) = fold.as_mut() {
            fold.consume(&event);
            // The trailer is the fold's cue that the stream is complete: it
            // has now seen every event, so the Document goes out here, ahead
            // of the trailer that is still the last event of the stream.
            if matches!(event.event, Some(pb::parse_xml_response::Event::Status(_))) {
                let document = pb::ParseXmlResponse {
                    event: Some(pb::parse_xml_response::Event::Document(fold.take())),
                };
                if !deliver(document) {
                    return false;
                }
                events += 1;
            }
        }
        let sent = deliver(event);
        if sent {
            events += 1;
        }
        sent
    };

    let result = parse::parse(reader, config, stats, &mut emit);
    match result {
        Ok(dialect) => metrics.record_success(dialect, stats.bytes(), events),
        Err(ParseError::ConsumerGone) if stalled => {
            metrics.parses_failed.fetch_add(1, Ordering::Relaxed);
            // The client is still connected and has not read for the whole
            // stall window. Ending the stream without a status would close
            // the call as OK with no trailer, so the reason goes on the
            // queue, where it is not bounded and waits behind the events.
            let message = format!(
                "client stopped reading: no event taken for {} s with the {}-byte event \
                 queue full; read the response while uploading",
                queue.stall.as_secs(),
                queue.capacity
            );
            let _ = queue
                .tx
                .send(Queued::failure(Status::deadline_exceeded(message)));
        }
        Err(ParseError::ConsumerGone) => {
            metrics.parses_failed.fetch_add(1, Ordering::Relaxed);
        }
        Err(error) => {
            metrics.parses_failed.fetch_add(1, Ordering::Relaxed);
            if matches!(error, ParseError::TooLarge { .. }) {
                metrics.parses_capped.fetch_add(1, Ordering::Relaxed);
            }
            let _ = queue.tx.send(Queued::failure(status_for(&error)));
        }
    }
}

/// Forward document bytes from the request stream into the parser.
///
/// Dropping `chunk_tx` on the way out is what signals EOF, including when the
/// client aborts. A frame that breaks the request contract ends the parse
/// with `INVALID_ARGUMENT` on the event channel rather than being ignored.
///
/// The wait for each message is bounded by the input idle timeout and by the
/// caller's deadline, so a client that goes quiet holds no slot. Once the
/// parse has ended, the rest of the upload is read and discarded rather than
/// left unread: a client that finishes its upload before it reads anything
/// would otherwise sit on a flow-control window that never reopens, and
/// never reach the status that says why the parse ended.
fn forward_chunks(
    mut requests: Streaming<pb::ParseXmlRequest>,
    chunk_tx: mpsc::Sender<Vec<u8>>,
    forward_tx: mpsc::UnboundedSender<Queued>,
    input_idle: Duration,
    deadline: Option<Instant>,
    limit_bytes: u64,
) {
    tokio::spawn(async move {
        loop {
            let deadline_passed = async {
                match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                    None => std::future::pending().await,
                }
            };
            let message = tokio::select! {
                message = tokio::time::timeout(input_idle, requests.message()) => message,
                () = chunk_tx.closed() => {
                    discard_upload(&mut requests, input_idle, limit_bytes).await;
                    break;
                }
                () = deadline_passed => {
                    let _ = forward_tx.send(Queued::failure(Status::deadline_exceeded(
                        "the request deadline passed while the upload was in progress",
                    )));
                    break;
                }
            };
            let Ok(message) = message else {
                let _ = forward_tx.send(Queued::failure(idle_status(input_idle)));
                break;
            };
            match message {
                Ok(Some(message)) => match message.payload {
                    Some(pb::parse_xml_request::Payload::Chunk(chunk)) => {
                        if chunk.is_empty() {
                            continue;
                        }
                        if chunk_tx.send(chunk).await.is_err() {
                            discard_upload(&mut requests, input_idle, limit_bytes).await;
                            break;
                        }
                    }
                    Some(pb::parse_xml_request::Payload::Options(_)) => {
                        let _ = forward_tx.send(Queued::failure(Status::invalid_argument(
                            "`options` may only be set on the first request message",
                        )));
                        break;
                    }
                    None => {
                        let _ = forward_tx.send(Queued::failure(Status::invalid_argument(
                            "every ParseXml request message must set `options` or `chunk`",
                        )));
                        break;
                    }
                },
                Ok(None) => break,
                Err(transport) => {
                    let _ = forward_tx.send(Queued::failure(transport));
                    break;
                }
            }
        }
    });
}

/// Read the rest of an upload the parse no longer wants and drop it.
///
/// Bounded like the upload itself: by the idle timeout between messages and
/// by the request's byte cap, past which the client is not finishing an
/// upload, it is streaming at a server that has already answered.
async fn discard_upload(
    requests: &mut Streaming<pb::ParseXmlRequest>,
    input_idle: Duration,
    limit_bytes: u64,
) {
    let mut discarded = 0u64;
    while let Ok(Ok(Some(message))) = tokio::time::timeout(input_idle, requests.message()).await {
        if let Some(pb::parse_xml_request::Payload::Chunk(chunk)) = message.payload {
            discarded += chunk.len() as u64;
            if discarded > limit_bytes {
                break;
            }
        }
    }
}

/// The status for a client that sent no request message for `idle`.
fn idle_status(idle: Duration) -> Status {
    Status::deadline_exceeded(format!(
        "no request message arrived for {} s; the upload is abandoned",
        idle.as_secs()
    ))
}

/// The caller's deadline, from the `grpc-timeout` header, as an instant.
///
/// tonic bounds only the wait for response headers by it, and this call
/// returns those at once, so the parse has to watch the deadline itself.
fn request_deadline(metadata: &tonic::metadata::MetadataMap) -> Option<Instant> {
    let value = metadata.get("grpc-timeout")?.to_str().ok()?;
    let timeout = parse_grpc_timeout(value)?;
    Instant::now().checked_add(timeout)
}

/// Parse a `grpc-timeout` value: at most eight digits and a unit.
fn parse_grpc_timeout(value: &str) -> Option<Duration> {
    if value.len() < 2 || value.len() > 9 {
        return None;
    }
    let (digits, unit) = value.split_at(value.len() - 1);
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let amount: u64 = digits.parse().ok()?;
    Some(match unit {
        "H" => Duration::from_secs(amount.checked_mul(3600)?),
        "M" => Duration::from_secs(amount.checked_mul(60)?),
        "S" => Duration::from_secs(amount),
        "m" => Duration::from_millis(amount),
        "u" => Duration::from_micros(amount),
        "n" => Duration::from_nanos(amount),
        _ => return None,
    })
}

/// One item on the way to the client, with the queue bytes it holds.
struct Queued {
    cost: u32,
    item: Result<pb::ParseXmlResponse, Status>,
}

impl Queued {
    /// A status ending the stream. It holds no queue bytes, so it can always
    /// be queued, behind whatever events are already waiting.
    fn failure(status: Status) -> Self {
        Self {
            cost: 0,
            item: Err(status),
        }
    }
}

/// The parse's end of the event queue.
struct EventQueue {
    tx: mpsc::UnboundedSender<Queued>,
    /// Queue bytes not yet taken by a waiting event.
    bytes: Arc<Semaphore>,
    capacity: usize,
    stall: Duration,
}

/// What became of one event the parse tried to send.
enum Delivery {
    Sent,
    /// The client is still connected and took nothing for the stall window.
    Stalled,
    /// The client is gone.
    Gone,
}

impl EventQueue {
    /// Send one event to the client.
    ///
    /// The event waits for its encoded size in queue bytes, which is real
    /// backpressure for a client that is merely slow, and the wait is bounded
    /// by the stall window, which is an exit for one that has stopped reading
    /// without closing the stream. An event larger than the whole queue waits
    /// for the queue to empty and then goes alone.
    fn deliver(&self, handle: &tokio::runtime::Handle, event: pb::ParseXmlResponse) -> Delivery {
        let cost = u32::try_from(event.encoded_len().clamp(1, self.capacity)).unwrap_or(u32::MAX);
        let permit = handle.block_on(async {
            tokio::time::timeout(self.stall, self.bytes.acquire_many(cost)).await
        });
        match permit {
            Err(_) => Delivery::Stalled,
            // The stream closes the semaphore when the client goes away.
            Ok(Err(_)) => Delivery::Gone,
            Ok(Ok(permit)) => {
                permit.forget();
                match self.tx.send(Queued {
                    cost,
                    item: Ok(event),
                }) {
                    Ok(()) => Delivery::Sent,
                    Err(_) => Delivery::Gone,
                }
            }
        }
    }
}

/// The response stream of one parse.
///
/// It returns each event's queue bytes as the transport takes the event, and
/// it guarantees the call never ends as a bare OK: a stream that runs out
/// before its `ParseStatus` trailer or an error status ends with an error
/// status instead. Dropping it, which tonic does when the client cancels or
/// disconnects, tells the parse to stop.
pub struct ParseStream {
    rx: mpsc::UnboundedReceiver<Queued>,
    bytes: Arc<Semaphore>,
    stats: InputStats,
    finished: bool,
}

impl Stream for ParseStream {
    type Item = Result<pb::ParseXmlResponse, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        match self.rx.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Queued { cost, item })) => {
                if cost > 0 {
                    self.bytes.add_permits(cost as usize);
                }
                let terminal = match &item {
                    Err(_) => true,
                    Ok(event) => {
                        matches!(event.event, Some(pb::parse_xml_response::Event::Status(_)))
                    }
                };
                self.finished = terminal;
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => {
                self.finished = true;
                Poll::Ready(Some(Err(Status::internal(
                    "the parse ended without a ParseStatus trailer",
                ))))
            }
        }
    }
}

impl Drop for ParseStream {
    fn drop(&mut self) {
        if !self.finished {
            self.stats.cancel();
        }
        self.bytes.close();
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
        ParseError::DeadlineExceeded(_) => Status::deadline_exceeded(message),
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
    fn grpc_timeout_values_parse_and_malformed_ones_do_not() {
        assert_eq!(parse_grpc_timeout("5S"), Some(Duration::from_secs(5)));
        assert_eq!(parse_grpc_timeout("250m"), Some(Duration::from_millis(250)));
        assert_eq!(parse_grpc_timeout("2H"), Some(Duration::from_secs(7200)));
        assert_eq!(
            parse_grpc_timeout("99999999n"),
            Some(Duration::from_nanos(99_999_999))
        );
        assert_eq!(parse_grpc_timeout("S"), None);
        assert_eq!(parse_grpc_timeout("123456789S"), None, "nine digits");
        assert_eq!(parse_grpc_timeout("5s"), None, "units are case sensitive");
        assert_eq!(parse_grpc_timeout("-5S"), None);
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
            (
                ParseError::DeadlineExceeded("x".into()),
                Code::DeadlineExceeded,
            ),
        ];
        for (error, want) in cases {
            assert_eq!(status_for(&error).code(), want, "{error}");
        }
    }
}
