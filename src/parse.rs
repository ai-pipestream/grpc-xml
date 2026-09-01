// SPDX-License-Identifier: Apache-2.0

//! The streaming parse driver: XML events in, protobuf events out.
//!
//! This is where "live stream is the product" is actually implemented. The
//! driver is a single forward pass over a pull parser reading from a
//! [`BufRead`] that is still being filled by the request stream. It holds
//! exactly one item at a time — the text of the element it is currently
//! inside — and hands each finished item to `emit` the moment its end tag is
//! read. There is no document, no tree, and nothing that could be flushed at
//! the end, which is what makes the batch-regression test in
//! `tests/live_stream.rs` mechanical rather than aspirational.
//!
//! The other half of the design is that the driver knows no dialects. Every
//! family-specific decision is a call into [`crate::dialect`], and the two
//! shapes that are not element mappings at all — XBRL instances and XHTML
//! islands — are explicit branches here rather than mapping rules.

use std::collections::{BTreeMap, HashMap};
use std::io::{self, BufRead, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use quick_xml::Writer;
use quick_xml::events::attributes::Attribute as XmlAttribute;
use quick_xml::events::{BytesEnd, BytesStart, BytesText, Event};
use quick_xml::name::ResolveResult;
use quick_xml::reader::NsReader;

use crate::dialect::{
    self, Action, Attrs, CELL_ELEMENTS, ElementCtx, HEADER_CELL_ELEMENTS, HEADER_SECTION_ELEMENTS,
    ROW_ELEMENTS,
};
use crate::proto::v1 as pb;
use crate::security;
use crate::sniff::{self, Dialect, NS_XBRL_INSTANCE, NS_XHTML, Signals, SniffError};
use crate::{COLLECTOR, VERSION};

/// Upper bound on distinct aggregated warnings kept for one parse.
///
/// Warnings aggregate by (code, message), so a normal document produces a
/// handful. A hostile one could mint a distinct message per element, which is
/// unbounded memory on the trailer; past this many the driver stops adding
/// new kinds and keeps counting the ones it has. The archive driver applies
/// the same bound to its own warning map.
pub(crate) const MAX_WARNING_KINDS: usize = 64;

/// Consumer of parse events; returns `false` when the client is gone and the
/// parse should stop.
pub type EmitFn<'a> = &'a mut dyn FnMut(pb::ParseXmlResponse) -> bool;

/// Settings for one parse.
#[derive(Debug, Clone, Default)]
pub struct ParseConfig {
    /// Dialect the caller asked for, or `None` to sniff.
    pub dialect: Option<Dialect>,
    /// Emit XHTML subtrees as islands instead of flattening them.
    pub emit_html_islands: bool,
    /// Attach unconsumed source attributes to every item.
    pub include_attributes: bool,
    /// True when the caller sent taxonomy bytes, which v1 does not use.
    pub taxonomy_supplied: bool,
}

/// Shared view of how much input the parse has taken and whether the byte cap
/// tripped.
///
/// The cap is enforced by the reader rather than by the driver, so it fires
/// on the byte that crosses the line instead of after the upload finishes.
/// The driver only needs to tell a capped read apart from a real I/O failure
/// when quick-xml hands it back an `Error::Io`, and to report the byte count
/// on the trailer.
#[derive(Debug, Default, Clone)]
pub struct InputStats {
    /// Bytes handed to the parser so far.
    pub consumed: Arc<AtomicU64>,
    /// Set by the reader when the byte cap was exceeded.
    pub capped: Arc<AtomicBool>,
    /// The cap that was in force, in bytes.
    pub limit_bytes: u64,
}

impl InputStats {
    /// A meter for a parse with the given cap.
    #[must_use]
    pub fn with_limit(limit_bytes: u64) -> Self {
        Self {
            consumed: Arc::new(AtomicU64::new(0)),
            capped: Arc::new(AtomicBool::new(false)),
            limit_bytes,
        }
    }

    /// Bytes handed to the parser so far.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.consumed.load(Ordering::Relaxed)
    }
}

/// Why a parse ended early.
///
/// The variants are the gRPC status codes the fleet rules fix, named for the
/// cause rather than the code so the mapping in [`crate::service`] is the
/// only place that knows about gRPC.
#[derive(Debug, Clone)]
pub enum ParseError {
    /// The bytes are not well-formed XML. `INVALID_ARGUMENT`.
    Malformed(String),
    /// The document ended inside an open element. `INVALID_ARGUMENT`.
    Truncated(String),
    /// The document asks for something the security policy refuses.
    /// `INVALID_ARGUMENT`.
    Refused(String),
    /// Sniffing found two signals that disagree. `INVALID_ARGUMENT`.
    Ambiguous(String),
    /// The document is not one of the mapped families. `UNIMPLEMENTED`.
    Unsupported(String),
    /// The document exceeded the byte cap. `RESOURCE_EXHAUSTED`.
    TooLarge {
        /// The cap that was exceeded, in bytes.
        limit_bytes: u64,
    },
    /// The input stream failed for a reason that is not the caller's fault.
    /// `INTERNAL`.
    Io(String),
    /// The client stopped reading. No status is sent.
    ConsumerGone,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Malformed(m) => write!(f, "malformed XML: {m}"),
            Self::Truncated(m) => write!(f, "truncated XML: {m}"),
            Self::Refused(m) => write!(f, "refused: {m}"),
            Self::Ambiguous(m) | Self::Unsupported(m) => f.write_str(m),
            Self::TooLarge { limit_bytes } => {
                write!(
                    f,
                    "document exceeds the {limit_bytes} byte cap for this request"
                )
            }
            Self::Io(m) => write!(f, "input stream failed: {m}"),
            Self::ConsumerGone => f.write_str("client stopped reading"),
        }
    }
}

/// Parse one document, emitting events as they are produced.
///
/// The payload's first bytes decide the path before any XML is read: ZIP or
/// gzip magic routes to the archive drivers in [`crate::archive`], anything
/// else to the XML driver. An explicit request still wins — it selects the
/// path, and a payload whose magic contradicts it fails closed instead of
/// being re-sniffed.
///
/// # Errors
///
/// Any [`ParseError`]. A parse that returns `Ok` has already emitted its
/// `ParseStatus` trailer; a parse that returns `Err` has emitted none.
pub fn parse<R: BufRead>(
    reader: R,
    config: &ParseConfig,
    input: &InputStats,
    emit: EmitFn<'_>,
) -> Result<Dialect, ParseError> {
    let mut reader = reader;
    let head = peek_head(&mut reader, input)?;
    let magic = crate::archive::sniff_magic(&head);
    let reader = io::Cursor::new(head).chain(reader);
    match magic {
        Some(archive) => match config.dialect {
            None => crate::archive::parse(archive, reader, config, input, emit, false),
            Some(requested) if requested == archive.dialect() => {
                crate::archive::parse(archive, reader, config, input, emit, true)
            }
            Some(requested) => Err(ParseError::Ambiguous(format!(
                "the request states dialect {} but the payload begins with {} magic, which \
                 means {}; omit the dialect or state the one the payload is",
                requested.model(),
                archive.magic_name(),
                archive.dialect().model()
            ))),
        },
        None => match config.dialect {
            Some(requested) if requested.is_archive() => Err(ParseError::Malformed(format!(
                "dialect {} names an archive format and the payload does not begin with its \
                 magic bytes; it is not one",
                requested.model()
            ))),
            _ => parse_xml(reader, config, input, emit, None),
        },
    }
}

/// Read up to four bytes from the front of the stream, for magic sniffing.
///
/// Fewer than four come back only when the stream itself is shorter; the
/// caller chains what was taken back in front of the reader, so the parse
/// still sees every byte.
fn peek_head<R: BufRead>(reader: &mut R, input: &InputStats) -> Result<Vec<u8>, ParseError> {
    let mut head = [0u8; 4];
    let mut filled = 0;
    while filled < head.len() {
        match reader.read(&mut head[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => {
                if input.capped.load(Ordering::Relaxed) || e.to_string().contains(CAP_MARKER) {
                    return Err(ParseError::TooLarge {
                        limit_bytes: input.limit_bytes,
                    });
                }
                return Err(ParseError::Io(e.to_string()));
            }
        }
    }
    Ok(head[..filled].to_vec())
}

/// Parse one XML document with the streaming driver.
///
/// `forced` carries a resolution the caller already made — an archive driver
/// knows the dialect from the archive, not from the document inside it — and
/// skips the sniff entirely; `None` sniffs as [`crate::sniff`] specifies.
pub(crate) fn parse_xml<R: BufRead>(
    reader: R,
    config: &ParseConfig,
    input: &InputStats,
    emit: EmitFn<'_>,
    forced: Option<(Dialect, sniff::Evidence)>,
) -> Result<Dialect, ParseError> {
    let started = std::time::Instant::now();
    let mut xml = NsReader::from_reader(reader);
    // Empty elements are expanded into a Start/End pair so the driver has one
    // shape to reason about; every depth comparison in it depends on that.
    xml.config_mut().expand_empty_elements = true;
    // Mismatched and unmatched end tags are errors rather than warnings: a
    // collector that silently repairs structure produces a Document nobody
    // can trace back to the source.
    xml.config_mut().check_end_names = true;
    xml.config_mut().allow_unmatched_ends = false;

    let mut driver = Driver {
        xml,
        buf: Vec::with_capacity(8 * 1024),
        config,
        input,
        emit,
        started,
        forced,
        dialect: Dialect::Jats,
        source: pb::CollectorSource::default(),
        index: 0,
        counts: pb::ParseCounts::default(),
        warnings: BTreeMap::new(),
        stack: Vec::new(),
        capture: None,
        table: None,
        island: None,
        pending_caption: None,
        fact: None,
        contexts: HashMap::new(),
        units: HashMap::new(),
    };
    driver.run()
}

/// One element of the open-element stack.
struct Frame {
    /// Local name, what the mapping rules match on.
    local: String,
    /// Name as written, what the positional path shows.
    qname: String,
    /// Position among preceding siblings with the same name, 1-based.
    position: usize,
    /// How many children of each name have been seen, for their positions.
    children: HashMap<String, usize>,
}

/// A text capture in progress.
struct Capture {
    /// Stack length at which the capture started; it closes when the end tag
    /// at that depth arrives.
    depth: usize,
    spec: dialect::Capture,
    text: String,
    path: String,
    element_id: Option<String>,
    attributes: Vec<pb::Attribute>,
    /// True when the capture feeds `pending_caption` instead of the stream.
    is_caption: bool,
    /// True when the previous child event was an element end, which is where
    /// a word boundary between two sibling elements belongs.
    after_child: bool,
}

/// A caption seen before its table.
struct PendingCaption {
    text: String,
    path: String,
    element_id: Option<String>,
    /// Stack length of the wrapper element; an unconsumed caption is emitted
    /// on its own when that wrapper closes.
    wrapper_depth: usize,
}

/// A table being streamed.
struct Table {
    depth: usize,
    /// Identifier the row and end events carry, matching `TableStart`.
    reference: String,
    row_index: u32,
    column_count: u32,
    header_sections: usize,
    row: Option<Row>,
    cell: Option<Cell>,
}

/// A table row being assembled.
struct Row {
    is_header: bool,
    cells: Vec<pb::TableCell>,
    next_column: u32,
}

/// A table cell being assembled.
struct Cell {
    depth: usize,
    text: String,
    column_index: u32,
    column_span: u32,
    row_span: u32,
    is_header: bool,
}

/// An XHTML subtree being re-serialized for the HTML collector.
struct Island {
    depth: usize,
    writer: Writer<Vec<u8>>,
    path: String,
    element_id: Option<String>,
    namespace: String,
}

/// An XBRL fact whose value is still being read.
struct PendingFact {
    depth: usize,
    fact: pb::Fact,
    text: String,
}

/// One materialized XML event.
///
/// quick-xml borrows both the reader and the scratch buffer for the lifetime
/// of an `Event`, so nothing else on the driver can be touched while one is
/// alive. Copying each event into owned data first costs allocations on a
/// hot path but buys a driver that reads like the state machine it is; the
/// alternative is threading two borrows through every branch below.
enum Step {
    Start {
        namespace: String,
        local: String,
        qname: String,
        attrs: Attrs,
    },
    End,
    Text(String),
    GeneralRef {
        /// The reference as written, without `&` and `;`.
        name: String,
        /// The character it resolves to, for character references and the
        /// five predefined entities. `None` for everything else, which is
        /// never expanded.
        resolved: Option<String>,
    },
    Declaration {
        version: Option<String>,
        encoding: Option<String>,
    },
    DocType(String),
    /// A comment or processing instruction: consumed, never mapped.
    Ignorable,
    Eof,
}

struct Driver<'a, R: BufRead> {
    xml: NsReader<R>,
    buf: Vec<u8>,
    config: &'a ParseConfig,
    input: &'a InputStats,
    emit: EmitFn<'a>,
    started: std::time::Instant,
    /// A resolution made before the XML was read, from an archive's magic;
    /// it replaces the sniff, not merely the request.
    forced: Option<(Dialect, sniff::Evidence)>,
    dialect: Dialect,
    source: pb::CollectorSource,
    index: u64,
    counts: pb::ParseCounts,
    warnings: BTreeMap<(i32, String), u64>,
    stack: Vec<Frame>,
    capture: Option<Capture>,
    table: Option<Table>,
    island: Option<Island>,
    pending_caption: Option<PendingCaption>,
    fact: Option<PendingFact>,
    contexts: HashMap<String, pb::XbrlContext>,
    units: HashMap<String, pb::XbrlUnit>,
}

impl<R: BufRead> Driver<'_, R> {
    /// Prolog, then content, then the trailer. Returns the dialect the
    /// document was mapped with, for the process counters.
    fn run(&mut self) -> Result<Dialect, ParseError> {
        self.read_prolog()?;
        self.content_loop()?;
        self.emit_status()?;
        Ok(self.dialect)
    }

    // ---------------------------------------------------------------- prolog

    /// Read up to and including the root start tag, resolve the dialect, and
    /// emit `XmlInfo`.
    fn read_prolog(&mut self) -> Result<(), ParseError> {
        let mut version = None;
        let mut encoding = None;
        let mut doctype = security::Doctype::default();
        loop {
            match self.next_step()? {
                Step::Declaration {
                    version: v,
                    encoding: e,
                } => {
                    version = v;
                    encoding = e;
                }
                Step::DocType(body) => {
                    doctype = security::parse_doctype(&body);
                    match security::check_doctype(&doctype) {
                        Ok(()) => {}
                        // Both refusals are the caller's document asking for
                        // something the policy does not do, and both carry
                        // their own explanation.
                        Err(refusal) => return Err(ParseError::Refused(refusal.to_string())),
                    }
                    if doctype.system_id.is_some() || doctype.public_id.is_some() {
                        self.warn(
                            pb::WarningCode::ExternalIdIgnored,
                            "DOCTYPE external identifier recorded and not dereferenced",
                        );
                    }
                }
                Step::Text(text) if text.trim().is_empty() => {}
                Step::Text(_) => {
                    return Err(ParseError::Malformed(
                        "character data before the root element".to_owned(),
                    ));
                }
                Step::GeneralRef { name, .. } => {
                    return Err(ParseError::Malformed(format!(
                        "entity reference &{name}; before the root element"
                    )));
                }
                Step::Ignorable => {}
                Step::End => {
                    return Err(ParseError::Malformed(
                        "end tag before the root element".to_owned(),
                    ));
                }
                Step::Eof => {
                    return Err(ParseError::Truncated(
                        "the document has no root element".to_owned(),
                    ));
                }
                Step::Start {
                    namespace,
                    local,
                    qname,
                    attrs: _,
                } => {
                    let signals = Signals {
                        root_namespace: namespace.clone(),
                        root_local_name: local.clone(),
                        public_id: doctype.public_id.clone(),
                    };
                    let (dialect, evidence) = self.resolve_dialect(&signals)?;
                    self.dialect = dialect;
                    self.source = pb::CollectorSource {
                        collector: COLLECTOR.to_owned(),
                        model: Some(dialect.model().to_owned()),
                        version: Some(VERSION.to_owned()),
                        confidence: None,
                    };
                    if self.config.taxonomy_supplied {
                        self.warn(
                            pb::WarningCode::TaxonomyIgnored,
                            "taxonomy bytes accepted but unused: label linkbase resolution is \
                             not implemented, labels are concept local names",
                        );
                    }
                    self.push_frame(&local, &qname);
                    self.counts.elements_visited += 1;
                    let info = pb::XmlInfo {
                        dialect: dialect.to_proto() as i32,
                        evidence: evidence.to_proto() as i32,
                        root_namespace: namespace,
                        root_local_name: local,
                        doctype_name: doctype.name.clone(),
                        public_id: doctype.public_id.clone(),
                        system_id: doctype.system_id.clone(),
                        // The title is a body event; XmlInfo goes out before
                        // the parser has reached it, which is the point.
                        title: None,
                        encoding,
                        xml_version: version,
                    };
                    self.send(pb::parse_xml_response::Event::Info(info))?;
                    return Ok(());
                }
            }
        }
    }

    /// Resolve the dialect for this document: the resolution the archive
    /// driver already made when there is one, the sniff otherwise.
    fn resolve_dialect(&self, signals: &Signals) -> Result<(Dialect, sniff::Evidence), ParseError> {
        if let Some(resolution) = self.forced {
            return Ok(resolution);
        }
        sniff::resolve(self.config.dialect, signals).map_err(|e| match e {
            SniffError::Conflict { .. } => ParseError::Ambiguous(e.to_string()),
            SniffError::Unrecognized { .. } => ParseError::Unsupported(e.to_string()),
        })
    }

    // ---------------------------------------------------------------- content

    /// The forward pass over the document body.
    fn content_loop(&mut self) -> Result<(), ParseError> {
        loop {
            match self.next_step()? {
                Step::Start {
                    namespace,
                    local,
                    qname,
                    attrs,
                } => self.on_start(&namespace, &local, &qname, &attrs)?,
                Step::End => {
                    if self.on_end()? {
                        return Ok(());
                    }
                }
                Step::Text(text) => self.on_text(&text),
                Step::GeneralRef { name, resolved } => {
                    self.on_general_ref(&name, resolved.as_deref());
                }
                Step::Declaration { .. } => {
                    return Err(ParseError::Malformed(
                        "an XML declaration may only appear before the root element".to_owned(),
                    ));
                }
                Step::DocType(_) => {
                    return Err(ParseError::Malformed(
                        "a DOCTYPE may only appear before the root element".to_owned(),
                    ));
                }
                Step::Ignorable => {}
                Step::Eof => {
                    let open = self
                        .stack
                        .iter()
                        .map(|f| f.qname.as_str())
                        .collect::<Vec<_>>()
                        .join("/");
                    return Err(ParseError::Truncated(format!(
                        "input ended with {} element(s) still open: {open}",
                        self.stack.len()
                    )));
                }
            }
        }
    }

    fn on_start(
        &mut self,
        namespace: &str,
        local: &str,
        qname: &str,
        attrs: &Attrs,
    ) -> Result<(), ParseError> {
        self.counts.elements_visited += 1;

        // An island swallows its whole subtree verbatim.
        if self.island.is_some() {
            self.write_island_start(qname, attrs);
            self.push_frame(local, qname);
            return Ok(());
        }
        // A capture flattens everything under it; nested rules do not fire.
        if let Some(capture) = self.capture.as_mut() {
            if capture.after_child
                && !capture.text.is_empty()
                && !capture.text.ends_with(char::is_whitespace)
            {
                // Two adjacent sibling elements are two words, not one.
                capture.text.push(' ');
            }
            capture.after_child = false;
            self.push_frame(local, qname);
            return Ok(());
        }
        if self.fact.is_some() {
            // XBRL facts hold simple content; any markup inside one is
            // flattened the same way a capture flattens inline markup.
            self.push_frame(local, qname);
            return Ok(());
        }
        if self.table.is_some() {
            self.table_start(local, qname, attrs);
            return Ok(());
        }
        if self.dialect == Dialect::Xbrl {
            return self.xbrl_start(namespace, local, qname, attrs);
        }
        if self.config.emit_html_islands && namespace == NS_XHTML {
            self.begin_island(namespace, local, qname, attrs);
            return Ok(());
        }

        let ancestors: Vec<String> = self.stack.iter().map(|f| f.local.clone()).collect();
        let ctx = ElementCtx {
            namespace,
            local,
            ancestors: &ancestors,
            attrs,
        };
        match dialect::action(self.dialect, &ctx) {
            Action::Descend => self.push_frame(local, qname),
            Action::Skip => {
                self.count_child(local, qname);
                self.consume_subtree()?;
            }
            Action::Table => {
                self.push_frame(local, qname);
                self.begin_table(attrs)?;
            }
            Action::Caption => {
                self.push_frame(local, qname);
                self.begin_capture(
                    dialect::Capture::new(pb::XmlItemLabel::Caption, ""),
                    attrs,
                    true,
                );
            }
            Action::Capture(spec) => {
                self.push_frame(local, qname);
                self.begin_capture(spec, attrs, false);
            }
            Action::AttrText(spec) => {
                self.push_frame(local, qname);
                if let Some(value) = attrs.get(spec.attr) {
                    let text = collapse(value);
                    if !text.is_empty() {
                        let item = self.text_item(spec.label, &spec.role, text, None, None, attrs);
                        self.send(pb::parse_xml_response::Event::TextItem(item))?;
                        self.counts.text_items += 1;
                    }
                }
            }
        }
        Ok(())
    }

    /// Handle an end tag. Returns true when the root element closed.
    fn on_end(&mut self) -> Result<bool, ParseError> {
        let depth = self.stack.len();

        if let Some(island) = self.island.as_mut() {
            if depth == island.depth {
                self.finish_island()?;
            } else {
                self.write_island_end();
            }
            self.stack.pop();
            return Ok(self.stack.is_empty());
        }
        if let Some(capture) = self.capture.as_mut() {
            if depth == capture.depth {
                self.finish_capture()?;
            } else {
                capture.after_child = true;
            }
            self.stack.pop();
            return Ok(self.stack.is_empty());
        }
        if let Some(fact) = self.fact.as_mut() {
            if depth == fact.depth {
                self.finish_fact()?;
            }
            self.stack.pop();
            return Ok(self.stack.is_empty());
        }
        if self.table.is_some() {
            self.table_end()?;
            self.stack.pop();
            return Ok(self.stack.is_empty());
        }
        if let Some(pending) = self.pending_caption.as_ref()
            && depth == pending.wrapper_depth
        {
            self.flush_pending_caption()?;
        }
        self.stack.pop();
        Ok(self.stack.is_empty())
    }

    fn on_text(&mut self, text: &str) {
        if let Some(island) = self.island.as_mut() {
            let _ = island.writer.write_event(Event::Text(BytesText::new(text)));
            return;
        }
        if let Some(capture) = self.capture.as_mut() {
            capture.text.push_str(text);
            capture.after_child = false;
            return;
        }
        if let Some(fact) = self.fact.as_mut() {
            fact.text.push_str(text);
            return;
        }
        if let Some(table) = self.table.as_mut()
            && let Some(cell) = table.cell.as_mut()
        {
            cell.text.push_str(text);
            return;
        }
        if !text.trim().is_empty() {
            let element = self.stack.last().map_or("document", |f| f.qname.as_str());
            self.warn(
                pb::WarningCode::UnmappedElement,
                &format!("character data in <{element}> has no mapping and was dropped"),
            );
        }
    }

    /// A general entity reference in content.
    ///
    /// `resolved` is `Some` only for character references and the five
    /// predefined entities. Everything else is a reference to a definition
    /// this parser refuses to have, so it is preserved exactly as written —
    /// dropping it would lose text, and expanding it is the vulnerability.
    fn on_general_ref(&mut self, name: &str, resolved: Option<&str>) {
        let text = if let Some(text) = resolved {
            text.to_owned()
        } else {
            self.warn(
                pb::WarningCode::UnexpandedEntity,
                &format!(
                    "entity reference &{name}; was preserved verbatim; this parser declares \
                     and expands no entities"
                ),
            );
            format!("&{name};")
        };
        if let Some(island) = self.island.as_mut() {
            let _ = island
                .writer
                .write_event(Event::Text(BytesText::from_escaped(text)));
            return;
        }
        if let Some(capture) = self.capture.as_mut() {
            capture.text.push_str(&text);
            capture.after_child = false;
            return;
        }
        if let Some(fact) = self.fact.as_mut() {
            fact.text.push_str(&text);
            return;
        }
        if let Some(table) = self.table.as_mut()
            && let Some(cell) = table.cell.as_mut()
        {
            cell.text.push_str(&text);
        }
    }

    // --------------------------------------------------------------- captures

    fn begin_capture(&mut self, spec: dialect::Capture, attrs: &Attrs, is_caption: bool) {
        let level = spec.level.or_else(|| {
            (spec.label == pb::XmlItemLabel::SectionHeader).then(|| self.section_level())
        });
        self.capture = Some(Capture {
            depth: self.stack.len(),
            spec: dialect::Capture { level, ..spec },
            text: String::new(),
            path: self.path(),
            element_id: attrs.get("id").map(str::to_owned),
            attributes: self.reportable_attributes(attrs),
            is_caption,
            after_child: false,
        });
    }

    fn finish_capture(&mut self) -> Result<(), ParseError> {
        let Some(capture) = self.capture.take() else {
            return Ok(());
        };
        let text = collapse(&capture.text);
        if text.is_empty() {
            return Ok(());
        }
        if capture.is_caption {
            // The caption belongs to the table that follows it inside the
            // same wrapper; `wrapper_depth` is where it gives up waiting.
            self.pending_caption = Some(PendingCaption {
                text,
                path: capture.path,
                element_id: capture.element_id,
                wrapper_depth: capture.depth.saturating_sub(1),
            });
            return Ok(());
        }
        let item = pb::TextItem {
            index: self.next_index(),
            label: capture.spec.label as i32,
            role: capture.spec.role,
            text,
            level: capture.spec.level,
            ordinal: capture.spec.ordinal,
            path: capture.path,
            element_id: capture.element_id,
            attributes: capture.attributes,
            source: Some(self.source.clone()),
        };
        self.counts.text_items += 1;
        self.send(pb::parse_xml_response::Event::TextItem(item))
    }

    fn flush_pending_caption(&mut self) -> Result<(), ParseError> {
        let Some(pending) = self.pending_caption.take() else {
            return Ok(());
        };
        let item = pb::TextItem {
            index: self.next_index(),
            label: pb::XmlItemLabel::Caption as i32,
            role: String::new(),
            text: pending.text,
            level: None,
            ordinal: None,
            path: pending.path,
            element_id: pending.element_id,
            attributes: Vec::new(),
            source: Some(self.source.clone()),
        };
        self.counts.text_items += 1;
        self.send(pb::parse_xml_response::Event::TextItem(item))
    }

    // ----------------------------------------------------------------- tables

    fn begin_table(&mut self, attrs: &Attrs) -> Result<(), ParseError> {
        let table_ref = format!("t{}", self.counts.tables + 1);
        let caption = self.pending_caption.take().map(|c| c.text);
        let start = pb::TableStart {
            index: self.next_index(),
            table_ref: table_ref.clone(),
            caption,
            path: self.path(),
            element_id: attrs.get("id").map(str::to_owned),
            source: Some(self.source.clone()),
        };
        self.table = Some(Table {
            depth: self.stack.len(),
            reference: table_ref,
            row_index: 0,
            column_count: 0,
            header_sections: 0,
            row: None,
            cell: None,
        });
        self.counts.tables += 1;
        self.send(pb::parse_xml_response::Event::TableStart(start))
    }

    fn table_start(&mut self, local: &str, qname: &str, attrs: &Attrs) {
        let Some(table) = self.table.as_mut() else {
            return;
        };
        if table.cell.is_some() {
            // Markup inside a cell is flattened into the cell's text.
            self.push_frame(local, qname);
            return;
        }
        if HEADER_SECTION_ELEMENTS.contains(&local) {
            table.header_sections += 1;
        } else if ROW_ELEMENTS.contains(&local) {
            table.row = Some(Row {
                is_header: table.header_sections > 0,
                cells: Vec::new(),
                next_column: 0,
            });
        } else if CELL_ELEMENTS.contains(&local) {
            let column_span = attrs
                .get("colspan")
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(1)
                .max(1);
            // CALS spells a vertical span as `morerows`, counting the extra
            // rows rather than the total.
            let row_span = attrs
                .get("rowspan")
                .and_then(|v| v.parse::<u32>().ok())
                .or_else(|| {
                    attrs
                        .get("morerows")
                        .and_then(|v| v.parse::<u32>().ok().map(|m| m + 1))
                })
                .unwrap_or(1)
                .max(1);
            let is_header = HEADER_CELL_ELEMENTS.contains(&local) || table.header_sections > 0;
            let column_index = table.row.as_ref().map_or(0, |r| r.next_column);
            table.cell = Some(Cell {
                // The frame for this cell is pushed at the end of this
                // function, and `on_end` compares against the stack as it
                // stands before the pop, so the cell's depth is one deeper
                // than the stack is right now.
                depth: self.stack.len() + 1,
                text: String::new(),
                column_index,
                column_span,
                row_span,
                is_header,
            });
        }
        self.push_frame(local, qname);
    }

    fn table_end(&mut self) -> Result<(), ParseError> {
        let depth = self.stack.len();
        let Some(table) = self.table.as_mut() else {
            return Ok(());
        };
        if let Some(cell) = table.cell.as_ref()
            && depth == cell.depth
        {
            let cell = table.cell.take().expect("checked just above");
            if let Some(row) = table.row.as_mut() {
                row.next_column = cell.column_index + cell.column_span;
                row.cells.push(pb::TableCell {
                    column_index: cell.column_index,
                    text: collapse(&cell.text),
                    column_span: cell.column_span,
                    row_span: cell.row_span,
                    is_header: cell.is_header,
                });
            }
            return Ok(());
        }
        // Which structural element closed is decided by what is open, since
        // the stack frame is still the one that is about to be popped.
        let closing = self
            .stack
            .last()
            .map_or("", |f| f.local.as_str())
            .to_owned();
        if HEADER_SECTION_ELEMENTS.contains(&closing.as_str()) {
            table.header_sections = table.header_sections.saturating_sub(1);
            return Ok(());
        }
        if ROW_ELEMENTS.contains(&closing.as_str())
            && let Some(row) = table.row.take()
        {
            if row.cells.is_empty() {
                return Ok(());
            }
            let width = row
                .cells
                .iter()
                .map(|c| c.column_index + c.column_span)
                .max()
                .unwrap_or(0);
            table.column_count = table.column_count.max(width);
            let is_header = row.is_header || row.cells.iter().all(|c| c.is_header);
            let event = pb::TableRow {
                table_ref: table.reference.clone(),
                row_index: table.row_index,
                is_header,
                cells: row.cells,
            };
            table.row_index += 1;
            self.counts.table_rows += 1;
            return self.send(pb::parse_xml_response::Event::TableRow(event));
        }
        if depth == table.depth {
            let end = pb::TableEnd {
                table_ref: table.reference.clone(),
                row_count: table.row_index,
                column_count: table.column_count,
            };
            self.table = None;
            return self.send(pb::parse_xml_response::Event::TableEnd(end));
        }
        Ok(())
    }

    // ---------------------------------------------------------------- islands

    fn begin_island(&mut self, namespace: &str, local: &str, qname: &str, attrs: &Attrs) {
        let mut writer = Writer::new(Vec::new());
        let mut start = BytesStart::new(qname);
        for (key, value) in &attrs.0 {
            start.push_attribute(XmlAttribute::from((key.as_str(), value.as_str())));
        }
        let _ = writer.write_event(Event::Start(start));
        self.push_frame(local, qname);
        self.island = Some(Island {
            depth: self.stack.len(),
            writer,
            path: self.path(),
            element_id: attrs.get("id").map(str::to_owned),
            namespace: namespace.to_owned(),
        });
    }

    fn write_island_start(&mut self, qname: &str, attrs: &Attrs) {
        let Some(island) = self.island.as_mut() else {
            return;
        };
        let mut start = BytesStart::new(qname);
        for (key, value) in &attrs.0 {
            start.push_attribute(XmlAttribute::from((key.as_str(), value.as_str())));
        }
        let _ = island.writer.write_event(Event::Start(start));
    }

    fn write_island_end(&mut self) {
        let Some(qname) = self.stack.last().map(|f| f.qname.clone()) else {
            return;
        };
        if let Some(island) = self.island.as_mut() {
            let _ = island.writer.write_event(Event::End(BytesEnd::new(qname)));
        }
    }

    fn finish_island(&mut self) -> Result<(), ParseError> {
        self.write_island_end();
        let Some(island) = self.island.take() else {
            return Ok(());
        };
        let event = pb::HtmlIsland {
            index: self.next_index(),
            path: island.path,
            element_id: island.element_id,
            namespace: island.namespace,
            html: island.writer.into_inner(),
            source: Some(self.source.clone()),
        };
        self.counts.html_islands += 1;
        self.send(pb::parse_xml_response::Event::HtmlIsland(event))
    }

    // ------------------------------------------------------------------- XBRL

    /// XBRL instances are not a document tree: they are a flat list of
    /// contexts, units and facts under the root. Contexts and units are read
    /// whole because a fact is meaningless without them, and they always
    /// precede the facts that use them in a conformant instance; facts stream
    /// one event each.
    fn xbrl_start(
        &mut self,
        namespace: &str,
        local: &str,
        qname: &str,
        attrs: &Attrs,
    ) -> Result<(), ParseError> {
        if namespace == NS_XBRL_INSTANCE && local == "context" {
            self.count_child(local, qname);
            let id = attrs.get("id").unwrap_or_default().to_owned();
            let context = self.read_context(id)?;
            self.counts.contexts += 1;
            self.contexts.insert(context.id.clone(), context);
            return Ok(());
        }
        if namespace == NS_XBRL_INSTANCE && local == "unit" {
            self.count_child(local, qname);
            let id = attrs.get("id").unwrap_or_default().to_owned();
            let unit = self.read_unit(id)?;
            self.counts.units += 1;
            self.units.insert(unit.id.clone(), unit);
            return Ok(());
        }
        if let Some(context_ref) = attrs.get("contextRef") {
            self.push_frame(local, qname);
            self.begin_fact(namespace, local, qname, context_ref, attrs);
            return Ok(());
        }
        if dialect::is_xbrl_infrastructure(namespace) {
            if local == "schemaRef" {
                self.warn(
                    pb::WarningCode::ExternalIdIgnored,
                    "linkbase schemaRef recorded and not dereferenced",
                );
            }
            self.count_child(local, qname);
            self.consume_subtree()?;
            return Ok(());
        }
        self.push_frame(local, qname);
        Ok(())
    }

    fn begin_fact(
        &mut self,
        namespace: &str,
        local: &str,
        qname: &str,
        context_ref: &str,
        attrs: &Attrs,
    ) {
        let prefix = qname.split_once(':').map_or("", |(p, _)| p).to_owned();
        let unit_ref = attrs.get("unitRef").map(str::to_owned);
        let context = self.contexts.get(context_ref).cloned();
        let unit = unit_ref
            .as_deref()
            .and_then(|id| self.units.get(id))
            .cloned();
        if context.is_none() {
            self.warn(
                pb::WarningCode::DanglingReference,
                &format!("fact references undeclared context {context_ref:?}"),
            );
        }
        if unit_ref.is_some() && unit.is_none() {
            self.warn(
                pb::WarningCode::DanglingReference,
                "fact references a unit that was not declared before it",
            );
        }
        let fact = pb::Fact {
            index: self.next_index(),
            concept_namespace: namespace.to_owned(),
            concept_local_name: local.to_owned(),
            concept_prefix: prefix,
            // v1 has no linkbase resolution, so the label is the local name.
            // `WARNING_CODE_TAXONOMY_IGNORED` says so on the trailer whenever
            // a caller supplied a taxonomy expecting otherwise.
            label: local.to_owned(),
            context_ref: context_ref.to_owned(),
            context,
            unit_ref,
            unit,
            value: String::new(),
            decimals: attrs.get("decimals").map(str::to_owned),
            precision: attrs.get("precision").map(str::to_owned),
            is_nil: attrs.get("nil").is_some_and(|v| v == "true" || v == "1"),
            sign: attrs.get("sign").map(str::to_owned),
            path: self.path(),
            source: Some(self.source.clone()),
        };
        self.fact = Some(PendingFact {
            depth: self.stack.len(),
            fact,
            text: String::new(),
        });
    }

    fn finish_fact(&mut self) -> Result<(), ParseError> {
        let Some(mut pending) = self.fact.take() else {
            return Ok(());
        };
        pending.fact.value = collapse(&pending.text);
        self.counts.facts += 1;
        self.send(pb::parse_xml_response::Event::Fact(pending.fact))
    }

    /// Read a `context` element whole, from just after its start tag to its
    /// end tag.
    fn read_context(&mut self, id: String) -> Result<pb::XbrlContext, ParseError> {
        let mut context = pb::XbrlContext {
            id,
            period: Some(pb::XbrlPeriod::default()),
            ..pb::XbrlContext::default()
        };
        let mut path: Vec<String> = Vec::new();
        let mut text = String::new();
        let mut dimension: Option<pb::XbrlDimension> = None;
        let mut depth = 1usize;
        loop {
            match self.next_step()? {
                Step::Start { local, attrs, .. } => {
                    depth += 1;
                    text.clear();
                    match local.as_str() {
                        "identifier" => {
                            context.entity_scheme = attrs.get("scheme").map(str::to_owned);
                        }
                        "explicitMember" | "typedMember" => {
                            dimension = Some(pb::XbrlDimension {
                                dimension: attrs.get("dimension").unwrap_or_default().to_owned(),
                                member: None,
                                typed_value: None,
                                is_scenario: path.iter().any(|p| p == "scenario"),
                            });
                        }
                        _ => {}
                    }
                    path.push(local);
                }
                Step::End => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(context);
                    }
                    let closing = path.pop().unwrap_or_default();
                    let value = collapse(&text);
                    text.clear();
                    match closing.as_str() {
                        "identifier" => context.entity_identifier = Some(value),
                        "instant" => {
                            context.period.get_or_insert_default().instant = Some(value);
                        }
                        "startDate" => {
                            context.period.get_or_insert_default().start_date = Some(value);
                        }
                        "endDate" => {
                            context.period.get_or_insert_default().end_date = Some(value);
                        }
                        "forever" => context.period.get_or_insert_default().forever = true,
                        "explicitMember" => {
                            if let Some(mut d) = dimension.take() {
                                d.member = Some(value);
                                context.dimensions.push(d);
                            }
                        }
                        "typedMember" => {
                            if let Some(mut d) = dimension.take() {
                                d.typed_value = Some(value);
                                context.dimensions.push(d);
                            }
                        }
                        _ => {}
                    }
                }
                Step::Text(chunk) => text.push_str(&chunk),
                Step::GeneralRef { resolved, .. } => {
                    if let Some(resolved) = resolved {
                        text.push_str(&resolved);
                    }
                }
                Step::Ignorable => {}
                Step::Declaration { .. } | Step::DocType(_) => {
                    return Err(ParseError::Malformed(
                        "a declaration inside an element".to_owned(),
                    ));
                }
                Step::Eof => {
                    return Err(ParseError::Truncated(
                        "input ended inside an XBRL context".to_owned(),
                    ));
                }
            }
        }
    }

    /// Read a `unit` element whole.
    fn read_unit(&mut self, id: String) -> Result<pb::XbrlUnit, ParseError> {
        let mut unit = pb::XbrlUnit {
            id,
            ..pb::XbrlUnit::default()
        };
        let mut path: Vec<String> = Vec::new();
        let mut text = String::new();
        let mut depth = 1usize;
        loop {
            match self.next_step()? {
                Step::Start { local, .. } => {
                    depth += 1;
                    text.clear();
                    path.push(local);
                }
                Step::End => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(unit);
                    }
                    let closing = path.pop().unwrap_or_default();
                    let value = collapse(&text);
                    text.clear();
                    if closing == "measure" {
                        if path.iter().any(|p| p == "unitNumerator") {
                            unit.numerator_measures.push(value);
                        } else if path.iter().any(|p| p == "unitDenominator") {
                            unit.denominator_measures.push(value);
                        } else {
                            unit.measures.push(value);
                        }
                    }
                }
                Step::Text(chunk) => text.push_str(&chunk),
                Step::GeneralRef { resolved, .. } => {
                    if let Some(resolved) = resolved {
                        text.push_str(&resolved);
                    }
                }
                Step::Ignorable => {}
                Step::Declaration { .. } | Step::DocType(_) => {
                    return Err(ParseError::Malformed(
                        "a declaration inside an element".to_owned(),
                    ));
                }
                Step::Eof => {
                    return Err(ParseError::Truncated(
                        "input ended inside an XBRL unit".to_owned(),
                    ));
                }
            }
        }
    }

    // -------------------------------------------------------------- machinery

    /// Consume the current element's subtree, from just after its start tag
    /// through its matching end tag.
    fn consume_subtree(&mut self) -> Result<(), ParseError> {
        let mut depth = 1usize;
        loop {
            match self.next_step()? {
                Step::Start { .. } => {
                    depth += 1;
                    self.counts.elements_visited += 1;
                }
                Step::End => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(());
                    }
                }
                Step::Eof => {
                    return Err(ParseError::Truncated(
                        "input ended inside a skipped element".to_owned(),
                    ));
                }
                _ => {}
            }
        }
    }

    /// Read one XML event and copy it into owned data.
    fn next_step(&mut self) -> Result<Step, ParseError> {
        self.buf.clear();
        let (resolved, event) = self
            .xml
            .read_resolved_event_into(&mut self.buf)
            .map_err(|e| convert_error(&e, self.input))?;
        let step = match event {
            Event::Start(start) => {
                let qname = start.name().as_ref().to_owned();
                let local = start.local_name().as_ref().to_owned();
                let namespace = match resolved {
                    ResolveResult::Bound(ns) => ns.as_ref().to_owned(),
                    ResolveResult::Unbound | ResolveResult::Unknown(_) => String::new(),
                };
                let mut attrs = Vec::new();
                for attribute in start.attributes() {
                    let attribute = attribute.map_err(|e| ParseError::Malformed(e.to_string()))?;
                    let key = attribute.key.as_ref().to_owned();
                    let value = attribute_value(&attribute);
                    attrs.push((key, value));
                }
                Step::Start {
                    namespace,
                    local,
                    qname,
                    attrs: Attrs(attrs),
                }
            }
            // `expand_empty_elements` turns `<a/>` into Start + End, so an
            // Empty event never reaches here.
            Event::Empty(_) => unreachable!("empty elements are expanded"),
            Event::End(_) => Step::End,
            Event::Text(text) => Step::Text(text.xml10_content().into_owned()),
            Event::CData(cdata) => Step::Text(cdata.into_inner().into_owned()),
            Event::GeneralRef(reference) => {
                let name = reference.into_inner().into_owned();
                let resolved = resolve_reference(&name);
                Step::GeneralRef { name, resolved }
            }
            Event::Decl(decl) => {
                let version = decl.version().ok().map(std::borrow::Cow::into_owned);
                let encoding = decl
                    .encoding()
                    .and_then(Result::ok)
                    .map(std::borrow::Cow::into_owned);
                Step::Declaration { version, encoding }
            }
            Event::DocType(doctype) => Step::DocType(doctype.into_inner().into_owned()),
            Event::Comment(_) | Event::PI(_) => Step::Ignorable,
            Event::Eof => Step::Eof,
        };
        Ok(step)
    }

    fn push_frame(&mut self, local: &str, qname: &str) {
        let position = self.count_child(local, qname);
        self.stack.push(Frame {
            local: local.to_owned(),
            qname: qname.to_owned(),
            position,
            children: HashMap::new(),
        });
    }

    /// Record that a child with this name was seen and return its 1-based
    /// position among same-named siblings.
    ///
    /// Called for skipped subtrees too, so a later sibling's positional path
    /// stays correct even when what came before it produced no events.
    fn count_child(&mut self, _local: &str, qname: &str) -> usize {
        let Some(parent) = self.stack.last_mut() else {
            return 1;
        };
        let counter = parent.children.entry(qname.to_owned()).or_insert(0);
        *counter += 1;
        *counter
    }

    /// Positional path of the element on top of the stack.
    fn path(&self) -> String {
        let mut path = String::new();
        for frame in &self.stack {
            path.push('/');
            path.push_str(&frame.qname);
            if frame.position > 1 {
                path.push('[');
                path.push_str(&frame.position.to_string());
                path.push(']');
            }
        }
        path
    }

    /// Heading depth implied by how many section containers are open.
    fn section_level(&self) -> u32 {
        let containers = dialect::section_containers(self.dialect);
        let depth = self
            .stack
            .iter()
            .filter(|f| containers.contains(&f.local.as_str()))
            .count();
        u32::try_from(depth).unwrap_or(u32::MAX).max(1)
    }

    /// Attributes to report on an item, if the caller asked for them.
    ///
    /// Namespace declarations are dropped: they are not content, they are on
    /// every root element of every document, and the resolved namespace is
    /// already reported where it matters.
    fn reportable_attributes(&self, attrs: &Attrs) -> Vec<pb::Attribute> {
        if !self.config.include_attributes {
            return Vec::new();
        }
        attrs
            .0
            .iter()
            .filter(|(key, _)| key != "xmlns" && !key.starts_with("xmlns:"))
            .map(|(name, value)| pb::Attribute {
                name: name.clone(),
                value: value.clone(),
            })
            .collect()
    }

    fn text_item(
        &mut self,
        label: pb::XmlItemLabel,
        role: &str,
        text: String,
        level: Option<u32>,
        ordinal: Option<u64>,
        attrs: &Attrs,
    ) -> pb::TextItem {
        pb::TextItem {
            index: self.next_index(),
            label: label as i32,
            role: role.to_owned(),
            text,
            level,
            ordinal,
            path: self.path(),
            element_id: attrs.get("id").map(str::to_owned),
            attributes: self.reportable_attributes(attrs),
            source: Some(self.source.clone()),
        }
    }

    fn next_index(&mut self) -> u64 {
        let index = self.index;
        self.index += 1;
        index
    }

    fn warn(&mut self, code: pb::WarningCode, message: &str) {
        let key = (code as i32, message.to_owned());
        if let Some(count) = self.warnings.get_mut(&key) {
            *count += 1;
        } else if self.warnings.len() < MAX_WARNING_KINDS {
            self.warnings.insert(key, 1);
        }
    }

    fn send(&mut self, event: pb::parse_xml_response::Event) -> Result<(), ParseError> {
        if (self.emit)(pb::ParseXmlResponse { event: Some(event) }) {
            Ok(())
        } else {
            Err(ParseError::ConsumerGone)
        }
    }

    fn emit_status(&mut self) -> Result<(), ParseError> {
        let warnings = self
            .warnings
            .iter()
            .map(|((code, message), count)| pb::ParseWarning {
                code: *code,
                message: message.clone(),
                count: *count,
            })
            .collect();
        let status = pb::ParseStatus {
            dialect: self.dialect.to_proto() as i32,
            counts: Some(self.counts),
            warnings,
            bytes_consumed: self.input.bytes(),
            elapsed_millis: u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX),
        };
        self.send(pb::parse_xml_response::Event::Status(status))
    }
}

/// Marker an `io::Error` carries when the byte cap tripped.
///
/// The cap is enforced inside the reader, which can only fail an
/// `io::Read`; quick-xml wraps that in `Error::Io`, and this is how the
/// driver tells it apart from a genuine transport failure.
pub const CAP_MARKER: &str = "grpc-xml: document byte cap exceeded";

/// Turn a quick-xml failure into the fleet's error taxonomy.
fn convert_error(error: &quick_xml::Error, input: &InputStats) -> ParseError {
    if input.capped.load(Ordering::Relaxed) {
        return ParseError::TooLarge {
            limit_bytes: input.limit_bytes,
        };
    }
    match error {
        quick_xml::Error::Io(io) => {
            if io.to_string().contains(CAP_MARKER) {
                ParseError::TooLarge {
                    limit_bytes: input.limit_bytes,
                }
            } else {
                ParseError::Io(io.to_string())
            }
        }
        // Everything else quick-xml reports is a statement about the bytes:
        // bad syntax, an unbalanced tree, a bad attribute, an undecodable
        // sequence. All of them are the caller's input being wrong.
        other => ParseError::Malformed(other.to_string()),
    }
}

/// Resolve a reference that is safe to resolve.
///
/// Character references are arithmetic and the five predefined entities are
/// fixed by the XML specification, so both are resolved. A named reference to
/// anything else needs a declaration, this parser has none, and manufacturing
/// one is precisely the entity-expansion vulnerability — so it returns
/// `None` and the caller preserves the reference verbatim.
pub(crate) fn resolve_reference(name: &str) -> Option<String> {
    if name.starts_with('#') {
        return quick_xml::events::BytesRef::new(name.to_owned())
            .resolve_char_ref()
            .ok()
            .flatten()
            .map(|c| c.to_string());
    }
    quick_xml::escape::resolve_predefined_entity(name).map(str::to_owned)
}

/// Attribute value with predefined and character references resolved.
///
/// A value naming an undeclared entity does not fail: normalization errors
/// out on it, and the raw text is kept instead, matching what happens to an
/// unexpandable reference in content.
fn attribute_value(attribute: &XmlAttribute<'_>) -> String {
    attribute
        .normalized_value(quick_xml::XmlVersion::Implicit1_0)
        .map_or_else(
            |_| attribute.value.as_ref().to_owned(),
            std::borrow::Cow::into_owned,
        )
}

/// Collapse XML whitespace: every run becomes one space, ends are trimmed.
///
/// XML has no opinion about how much whitespace an element's content carries,
/// and pretty-printed source carries a great deal of it. The Document plane
/// wants the words.
#[must_use]
pub fn collapse(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut pending_space = false;
    for ch in text.chars() {
        if ch.is_whitespace() {
            pending_space = !out.is_empty();
        } else {
            if pending_space {
                out.push(' ');
                pending_space = false;
            }
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collapse_normalizes_pretty_printed_source() {
        assert_eq!(collapse("  a \n\t b  "), "a b");
        assert_eq!(collapse("\n   \n"), "");
        assert_eq!(collapse("single"), "single");
    }

    #[test]
    fn predefined_and_character_references_resolve_but_named_ones_do_not() {
        assert_eq!(resolve_reference("amp").as_deref(), Some("&"));
        assert_eq!(resolve_reference("lt").as_deref(), Some("<"));
        assert_eq!(resolve_reference("#x41").as_deref(), Some("A"));
        assert_eq!(resolve_reference("#65").as_deref(), Some("A"));
        // The whole point: no lookup table, so nothing to blow up.
        assert_eq!(resolve_reference("lol9"), None);
        assert_eq!(resolve_reference("xxe"), None);
    }
}
