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

use quick_xml::events::attributes::Attribute as XmlAttribute;
use quick_xml::reader::NsReader;

use crate::dialect::{self, Attrs};
use crate::proto::v1 as pb;
use crate::sniff::{self, Dialect};

mod driver;
mod encoding;
mod island;
mod meta;
mod table;
mod xbrl;

pub(crate) use driver::{namespace_bindings, root_attributes, schema_locations};
pub(crate) use encoding::decoding_reader;

use island::Island;
use table::Table;
use xbrl::PendingFact;

/// Upper bound on distinct aggregated warnings kept for one parse.
///
/// Warnings aggregate by (code, message), so a normal document produces a
/// handful. A hostile one could mint a distinct message per element, which is
/// unbounded memory on the trailer; past this many the driver stops adding
/// new kinds and keeps counting the ones it has. The archive driver applies
/// the same bound to its own warning map.
pub(crate) const MAX_WARNING_KINDS: usize = 64;

/// Upper bound on inline runs recorded inside one captured element.
///
/// The runs are recorded while the capture is open, so a paragraph made of a
/// million empty `<b/>` elements would otherwise be unbounded memory for one
/// item. Real prose is orders of magnitude below this; past it the driver
/// keeps flattening and stops recording.
pub(crate) const MAX_INLINE_SPANS: usize = 512;

/// Consumer of parse events; returns `false` when the client is gone and the
/// parse should stop.
pub type EmitFn<'a> = &'a mut dyn FnMut(pb::ParseXmlResponse) -> bool;

/// Settings for one parse.
///
/// The switches are independent request options that mirror `ParseOptions`
/// field for field, so they stay separate booleans rather than collapsing
/// into a mode enum: a caller may set any subset, and the proto is where
/// their meaning is documented.
#[derive(Debug, Clone, Default)]
#[allow(clippy::struct_excessive_bools)]
pub struct ParseConfig {
    /// Dialect the caller asked for, or `None` to sniff.
    pub dialect: Option<Dialect>,
    /// Emit XHTML subtrees as islands instead of flattening them.
    pub emit_html_islands: bool,
    /// Record the inline markup inside captured elements as spans.
    pub emit_inline_spans: bool,
    /// Decode the structured metadata subtrees the item mapping skips.
    pub emit_source_metadata: bool,
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
    let head = peek_bytes(&mut reader, input, 4)?;
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

/// Read up to `want` bytes from the front of the stream, for sniffing.
///
/// Fewer come back only when the stream itself is shorter; the caller
/// chains what was taken back in front of the reader, so the parse still
/// sees every byte.
fn peek_bytes<R: BufRead>(
    reader: &mut R,
    input: &InputStats,
    want: usize,
) -> Result<Vec<u8>, ParseError> {
    let mut head = vec![0u8; want];
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
    head.truncate(filled);
    Ok(head)
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
    let mut xml = NsReader::from_reader(decoding_reader(reader, input)?);
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
        event_start: 0,
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
    /// `Some(ordered)` when this element opens a list. Counting these gives
    /// a list item its nesting depth, and the innermost one says whether its
    /// list is numbered.
    list: Option<bool>,
}

/// A text capture in progress.
struct Capture {
    /// Stack length at which the capture started; it closes when the end tag
    /// at that depth arrives.
    depth: usize,
    spec: dialect::Capture,
    text: String,
    /// Code points appended to `text` so far, kept alongside it so a span
    /// boundary costs a read rather than a walk of the whole string.
    chars: usize,
    path: String,
    element_id: Option<String>,
    /// Qualified name and resolved namespace of the element being captured.
    element_name: String,
    namespace: String,
    /// Offset of the first byte of the element's start tag in the stream the
    /// parser read.
    byte_start: u64,
    /// True once any part of `text` has come from a CDATA section.
    from_cdata: bool,
    attributes: Vec<pb::Attribute>,
    /// Inline runs recognized inside this capture, in the order they opened.
    spans: Vec<SpanBuild>,
    /// The list this item belongs to, read off the open-element stack when
    /// the capture opens; only a list item ever has one.
    list: Option<ListPlacement>,
    /// True when the capture feeds `pending_caption` instead of the stream.
    is_caption: bool,
    /// True when the previous child event was an element end, which is where
    /// a word boundary between two sibling elements belongs.
    after_child: bool,
}

/// Where a list item sits: the nesting depth of its list and whether that
/// list is ordered. Depth starts at 1 for a list that is not inside another.
#[derive(Debug, Clone, Copy)]
struct ListPlacement {
    depth: u32,
    ordered: bool,
}

/// One inline run being measured against the capture's growing text.
///
/// Offsets are into the *raw* captured string, because that is the one the
/// driver is appending to; [`finish_capture`](Driver::finish_capture)
/// translates them onto the collapsed text the item actually carries.
struct SpanBuild {
    /// Stack length of the element that opened the run, so its own end tag
    /// closes it and a descendant's does not.
    depth: usize,
    start: usize,
    end: Option<usize>,
    inline: dialect::Inline,
    element_name: String,
    namespace: String,
    attributes: Vec<pb::Attribute>,
}

/// A caption seen before its table.
struct PendingCaption {
    text: String,
    path: String,
    element_id: Option<String>,
    element_name: String,
    namespace: String,
    byte_start: u64,
    byte_end: u64,
    from_cdata: bool,
    spans: Vec<pb::InlineSpan>,
    /// Stack length of the wrapper element; an unconsumed caption is emitted
    /// on its own when that wrapper closes.
    wrapper_depth: usize,
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
    /// Character data the source wrapped in a CDATA section. The text is
    /// ordinary text; only the source's exemption from markup differs, and
    /// only the item mapping records it.
    CData(String),
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
    /// A comment: consumed, never mapped, and not worth a warning either.
    /// A comment is an author's note to another author.
    Ignorable,
    /// A processing instruction, carrying its target and the rest verbatim.
    /// An instruction is addressed to an application this service is not,
    /// so it is not acted on, but it is content the source put there and
    /// dropping it silently is what `WARNING_CODE_UNMAPPED_ELEMENT` exists
    /// to stop.
    ProcessingInstruction(String),
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
    /// Offset of the first byte of the event [`Driver::next_step`] most
    /// recently read. The reader reports where it has got *to*, so the
    /// position is taken before the read to get where an event starts.
    event_start: u64,
    capture: Option<Capture>,
    table: Option<Table>,
    island: Option<Island>,
    pending_caption: Option<PendingCaption>,
    fact: Option<PendingFact>,
    contexts: HashMap<String, pb::XbrlContext>,
    units: HashMap<String, pb::XbrlUnit>,
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
        return quick_xml::events::BytesRef::new(name)
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

/// Collapse, and record where each source code point landed.
///
/// The returned map has one entry per code point of `text`: the position it
/// occupies in the collapsed string, or `None` when it was whitespace that
/// collapsed away. It is the bridge between the offsets an inline run was
/// measured at — into the raw captured string — and the offsets a consumer
/// can use against the `text` it receives.
pub(crate) fn collapse_positions(text: &str) -> (String, Vec<Option<u32>>) {
    let mut out = String::with_capacity(text.len());
    let mut map = Vec::new();
    let mut pending_space = false;
    let mut count = 0u32;
    for ch in text.chars() {
        if ch.is_whitespace() {
            pending_space = count > 0;
            map.push(None);
        } else {
            if pending_space {
                out.push(' ');
                count += 1;
                pending_space = false;
            }
            out.push(ch);
            map.push(Some(count));
            count += 1;
        }
    }
    (out, map)
}

/// Translate a raw code-point range onto the collapsed text.
///
/// The run shrinks to the characters that survived: it starts at the first
/// surviving code point at or after `start` and ends after the last one
/// before `end`. A run that was nothing but whitespace has no position in
/// the collapsed string and returns `None`.
pub(crate) fn collapsed_range(
    map: &[Option<u32>],
    start: usize,
    end: usize,
) -> Option<pb::IntRange> {
    let end = end.min(map.len());
    if start >= end {
        return None;
    }
    let mut kept = map[start..end].iter().flatten().copied();
    let first = kept.next()?;
    let last = kept.next_back().unwrap_or(first);
    Some(pb::IntRange {
        start: first,
        end: last + 1,
    })
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

    #[test]
    fn the_position_map_agrees_with_the_collapse_it_mirrors() {
        // Two implementations of one rule is a drift risk; this is the check
        // that keeps them honest.
        for sample in [
            "  a \n\t b  ",
            "\n   \n",
            "single",
            "",
            "a  b  c",
            "\u{e9}l\u{e8}ve  na\u{ef}f",
        ] {
            let (collapsed, map) = collapse_positions(sample);
            assert_eq!(collapsed, collapse(sample), "sample {sample:?}");
            assert_eq!(map.len(), sample.chars().count(), "sample {sample:?}");
            for (raw, position) in map.iter().enumerate() {
                let Some(position) = position else { continue };
                let source = sample.chars().nth(raw).expect("in range");
                let landed = collapsed
                    .chars()
                    .nth(*position as usize)
                    .expect("the map points inside the collapsed text");
                assert_eq!(source, landed, "sample {sample:?} at {raw}");
            }
        }
    }

    #[test]
    fn a_run_maps_onto_the_characters_that_survived_collapsing() {
        let raw = "  bold  and  plain ";
        let (collapsed, map) = collapse_positions(raw);
        assert_eq!(collapsed, "bold and plain");
        // The leading whitespace is not part of the run the source marked.
        let range = collapsed_range(&map, 0, 8).expect("the run has text");
        assert_eq!((range.start, range.end), (0, 4));
        let word: String = collapsed
            .chars()
            .skip(range.start as usize)
            .take((range.end - range.start) as usize)
            .collect();
        assert_eq!(word, "bold");
        // A run that is only whitespace has no position of its own.
        assert_eq!(collapsed_range(&map, 6, 8), None);
        assert_eq!(collapsed_range(&map, 3, 3), None);
    }
}
