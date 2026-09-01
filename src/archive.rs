// SPDX-License-Identifier: Apache-2.0

//! The archive drivers: `DocLang` archives (`.dclx`) and Google Books METS
//! exports (`.tar.gz`).
//!
//! Neither payload is XML at byte 0, so [`crate::parse`] routes here on the
//! archive's magic bytes before the XML sniff ever runs. Each driver unpacks
//! in memory, never on disk, and then hands the XML it finds to the same
//! machinery the plain dialects use: a `DocLang` archive's `document.xml`
//! goes through the streaming XML driver with the `DocLang` mapping, and a
//! METS export's manifest and hOCR pages are walked with the same pull
//! parser and the same security policy.
//!
//! **Member layouts are mirrored from docling, not invented.** A `.dclx` is
//! the OPC ZIP that docling-core's `save_as_doclang_archive` writes: the
//! document is the root member named exactly `document.xml`, images live
//! under `assets/` and `pages/`, and `[Content_Types].xml` / `_rels/.rels`
//! carry the OPC furniture; `load_from_doclang_archive` reads `document.xml`
//! back by that name and fails when it is missing, and so does this driver.
//! A METS export is what docling's `MetsGbsDocumentBackend` reads: a gzipped
//! tar holding one `mets` manifest with `PROFILE="gbs"`, whose `fileGrp`
//! elements (`USE` of `image`, `OCR`, `coordOCR`) name the per-page files and
//! whose `div TYPE="page" ORDER="N"` elements order them. Text comes from the
//! `coordOCR` hOCR files, one item per `ocr_line` span, in page order.
//!
//! **The byte cap counts inflated bytes.** A compressed archive is small by
//! construction, so the request cap on uploaded bytes is no bomb guard at
//! all. Every byte inflated out of an archive is counted against the same
//! cap while it is being inflated — the sizes a member header claims are
//! never trusted — and the first byte past the cap ends the parse with the
//! same `RESOURCE_EXHAUSTED` an oversized plain document gets.

use std::collections::{BTreeMap, HashMap};
use std::io::{self, BufRead, Read};

use flate2::read::GzDecoder;
use quick_xml::events::Event;
use quick_xml::name::ResolveResult;
use quick_xml::reader::NsReader;

use crate::parse::{self, EmitFn, InputStats, ParseConfig, ParseError, collapse};
use crate::proto::v1 as pb;
use crate::security;
use crate::sniff::{Dialect, Evidence};
use crate::{COLLECTOR, VERSION};

/// Magic bytes of a ZIP local file header, the shape every `.dclx` starts
/// with.
pub const ZIP_MAGIC: &[u8] = b"PK\x03\x04";

/// Magic bytes of a gzip stream, the shape every `.tar.gz` starts with.
pub const GZIP_MAGIC: &[u8] = &[0x1f, 0x8b];

/// Namespace URI of the METS schema, the manifest vocabulary of a Google
/// Books export.
pub const NS_METS: &str = "http://www.loc.gov/METS/";

/// Upper bound on archive members processed in one parse, mirroring the
/// member-count limit docling's METS backend enforces. A tar minting members
/// costs the server work per member even when every one is empty.
const MAX_ARCHIVE_MEMBERS: usize = 1000;

/// The archive family a payload's magic bytes name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveKind {
    /// ZIP: a `DocLang` archive.
    Zip,
    /// gzip: a Google Books METS export.
    Gzip,
}

impl ArchiveKind {
    /// The dialect this archive family carries.
    #[must_use]
    pub const fn dialect(self) -> Dialect {
        match self {
            Self::Zip => Dialect::Dclx,
            Self::Gzip => Dialect::MetsGbs,
        }
    }

    /// The magic's name, for error messages.
    #[must_use]
    pub const fn magic_name(self) -> &'static str {
        match self {
            Self::Zip => "ZIP archive",
            Self::Gzip => "gzip",
        }
    }
}

/// Match the payload's first bytes against the archive magics.
#[must_use]
pub fn sniff_magic(head: &[u8]) -> Option<ArchiveKind> {
    if head.starts_with(ZIP_MAGIC) {
        return Some(ArchiveKind::Zip);
    }
    if head.starts_with(GZIP_MAGIC) {
        return Some(ArchiveKind::Gzip);
    }
    None
}

/// Parse one archive payload, emitting events as they are produced.
///
/// `requested` records whether the caller stated the dialect: content that
/// contradicts a stated dialect is the caller's error (`INVALID_ARGUMENT`),
/// while a sniffed archive that turns out to be some other kind of ZIP or
/// tar is merely unsupported (`UNIMPLEMENTED`), matching how an unrecognized
/// XML root is refused.
///
/// # Errors
///
/// Any [`ParseError`], under the same contract as [`crate::parse::parse`].
pub fn parse<R: BufRead>(
    kind: ArchiveKind,
    reader: R,
    config: &ParseConfig,
    input: &InputStats,
    emit: EmitFn<'_>,
    requested: bool,
) -> Result<Dialect, ParseError> {
    let evidence = if requested {
        Evidence::Requested
    } else {
        Evidence::ArchiveMagic
    };
    match kind {
        ArchiveKind::Zip => dclx(reader, config, input, emit, evidence, requested),
        ArchiveKind::Gzip => mets_gbs(reader, config, input, emit, evidence, requested),
    }
}

// ------------------------------------------------------------------- budget

/// The decompressed-byte budget of one parse.
///
/// A zero request limit means no cap was configured — the service always
/// configures one, so this arises only when the driver is called directly —
/// and disables the budget rather than refusing every archive.
fn budget_for(input: &InputStats) -> u64 {
    if input.limit_bytes == 0 {
        u64::MAX
    } else {
        input.limit_bytes
    }
}

/// Read a stream to its end, charging every byte against `budget` as it
/// arrives.
///
/// This is the whole zip-bomb guard: the size a member header claims is
/// never consulted, only the bytes that actually inflate, and the byte that
/// crosses the budget ends the parse before the next one is produced. When
/// `keep` is false the bytes are counted and dropped, so an unmapped member
/// still spends the budget its inflation cost.
fn read_inflated<R: Read>(
    mut reader: R,
    budget: &mut u64,
    input: &InputStats,
    keep: bool,
) -> Result<Vec<u8>, ParseError> {
    let mut out = Vec::new();
    let mut buf = [0u8; 16 * 1024];
    loop {
        let n = match reader.read(&mut buf) {
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(stream_error(&e, input)),
        };
        if n == 0 {
            return Ok(out);
        }
        let n64 = n as u64;
        if n64 > *budget {
            return Err(ParseError::TooLarge {
                limit_bytes: input.limit_bytes,
            });
        }
        *budget -= n64;
        if keep {
            out.extend_from_slice(&buf[..n]);
        }
    }
}

/// Turn an I/O failure from an archive read into the fleet taxonomy.
///
/// The request-stream reader signals a tripped byte cap through an
/// `io::Error`, and that must stay `RESOURCE_EXHAUSTED` even when it
/// surfaces through a decompressor; everything else at this layer is the
/// archive's bytes being wrong.
fn stream_error(error: &io::Error, input: &InputStats) -> ParseError {
    if input.capped.load(std::sync::atomic::Ordering::Relaxed)
        || error.to_string().contains(parse::CAP_MARKER)
    {
        return ParseError::TooLarge {
            limit_bytes: input.limit_bytes,
        };
    }
    ParseError::Malformed(format!("archive data cannot be read: {error}"))
}

// --------------------------------------------------------------------- dclx

/// Parse a `DocLang` archive: locate `document.xml`, inflate it under the
/// budget, and run the streaming XML driver over it with the `DocLang`
/// mapping and the resolution this module already made.
///
/// The other members — `assets/`, `pages/`, the OPC furniture — are left
/// compressed and unread: the document references its images by relative
/// `uri`, which the `DocLang` mapping already lifts into picture placeholder
/// items, and this service never decodes pixels.
fn dclx<R: BufRead>(
    reader: R,
    config: &ParseConfig,
    input: &InputStats,
    emit: EmitFn<'_>,
    evidence: Evidence,
    requested: bool,
) -> Result<Dialect, ParseError> {
    // ZIP needs random access to its central directory, so the compressed
    // archive is buffered whole; the request cap on uploaded bytes bounds it.
    let mut compressed = Vec::new();
    if let Err(e) = { reader }.read_to_end(&mut compressed) {
        return Err(stream_error(&e, input));
    }
    let mut budget = budget_for(input);
    let mut archive = zip::ZipArchive::new(io::Cursor::new(compressed))
        .map_err(|e| ParseError::Malformed(format!("not a readable ZIP archive: {e}")))?;
    let member = match archive.by_name("document.xml") {
        Ok(member) => member,
        Err(zip::result::ZipError::FileNotFound) => {
            return Err(if requested {
                ParseError::Malformed(
                    "the ZIP archive has no document.xml member; a DocLang archive (.dclx) \
                     carries its document there"
                        .to_owned(),
                )
            } else {
                ParseError::Unsupported(
                    "the ZIP archive has no document.xml member, so it is not a DocLang \
                     archive (.dclx); this service does not map arbitrary archives"
                        .to_owned(),
                )
            });
        }
        Err(e) => {
            return Err(ParseError::Malformed(format!(
                "unreadable ZIP archive: {e}"
            )));
        }
    };
    let document = read_inflated(member, &mut budget, input, true)?;
    parse::parse_xml(
        io::Cursor::new(document),
        config,
        input,
        emit,
        Some((Dialect::Dclx, evidence)),
    )
}

// ----------------------------------------------------------------- mets-gbs

/// What a METS `fileGrp` uses its files for. Only the three uses docling's
/// backend reads are modelled; a group with any other `USE` is ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileUse {
    /// `USE="image"`: the page scan. Never decoded here.
    Image,
    /// `USE="OCR"`: plain OCR text. Unused, as upstream leaves it unused.
    Ocr,
    /// `USE="coordOCR"`: hOCR with coordinates, the text source.
    CoordOcr,
}

impl FileUse {
    fn from_attr(value: &str) -> Option<Self> {
        match value {
            "image" => Some(Self::Image),
            "OCR" => Some(Self::Ocr),
            "coordOCR" => Some(Self::CoordOcr),
            _ => None,
        }
    }
}

/// One `mets:file` entry: where it is in the tar and what it is for.
#[derive(Debug, Clone)]
struct FileInfo {
    path: String,
    use_type: FileUse,
}

/// Parse a Google Books METS export.
///
/// One pass inflates the tar under the budget, keeping only the members that
/// can carry text (`.xml`, `.html`, `.htm`); a second pass over the kept
/// manifest builds the page map; then each page's hOCR is walked in manifest
/// order. Events stream per line as each page is walked — nothing waits for
/// the last page.
fn mets_gbs<R: BufRead>(
    reader: R,
    config: &ParseConfig,
    input: &InputStats,
    emit: EmitFn<'_>,
    evidence: Evidence,
    requested: bool,
) -> Result<Dialect, ParseError> {
    let mut budget = budget_for(input);
    let members = read_tar_members(reader, input, &mut budget, requested)?;

    let Some((manifest_name, manifest)) = find_mets_manifest(&members) else {
        return Err(if requested {
            ParseError::Malformed(
                "the tar.gz archive has no METS manifest with PROFILE=\"gbs\"; a Google Books \
                 export carries one *.mets.xml member"
                    .to_owned(),
            )
        } else {
            ParseError::Unsupported(
                "the tar.gz archive has no METS manifest with PROFILE=\"gbs\", so it is not a \
                 Google Books export; this service does not map arbitrary archives"
                    .to_owned(),
            )
        });
    };

    let mut driver = MetsDriver {
        config,
        input,
        emit,
        started: std::time::Instant::now(),
        source: pb::CollectorSource {
            collector: COLLECTOR.to_owned(),
            model: Some(Dialect::MetsGbs.model().to_owned()),
            version: Some(VERSION.to_owned()),
            confidence: None,
        },
        index: 0,
        counts: pb::ParseCounts::default(),
        warnings: BTreeMap::new(),
    };
    driver.run(&members, manifest_name, manifest, evidence)?;
    Ok(Dialect::MetsGbs)
}

/// Inflate the tar, keeping the members that can carry text.
///
/// Every member's bytes are inflated through the budget whether kept or not,
/// so an image-shaped bomb is caught exactly like a text-shaped one.
fn read_tar_members<R: BufRead>(
    reader: R,
    input: &InputStats,
    budget: &mut u64,
    requested: bool,
) -> Result<BTreeMap<String, Vec<u8>>, ParseError> {
    let mut archive = tar::Archive::new(GzDecoder::new(reader));
    let mut members = BTreeMap::new();
    let entries = match archive.entries() {
        Ok(entries) => entries,
        Err(e) => return Err(not_a_tar(&e, input, requested)),
    };
    let mut member_count = 0usize;
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) => return Err(not_a_tar(&e, input, requested)),
        };
        if !entry.header().entry_type().is_file() {
            continue;
        }
        member_count += 1;
        if member_count > MAX_ARCHIVE_MEMBERS {
            return Err(ParseError::Malformed(format!(
                "the archive has more than {MAX_ARCHIVE_MEMBERS} members"
            )));
        }
        let name = entry
            .path()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default();
        let keep = wants_member(&name);
        let bytes = read_inflated(entry, budget, input, keep)?;
        if keep {
            members.insert(name, bytes);
        }
    }
    Ok(members)
}

/// True for member names whose bytes this driver may need: the manifest and
/// the hOCR pages. Everything else — scans, plain OCR text, checksums — is
/// inflated for the budget and dropped.
fn wants_member(name: &str) -> bool {
    std::path::Path::new(name).extension().is_some_and(|ext| {
        ext.eq_ignore_ascii_case("xml")
            || ext.eq_ignore_ascii_case("html")
            || ext.eq_ignore_ascii_case("htm")
    })
}

/// Why a gzip payload failed to read as a tar.
///
/// A stated dialect makes wrong content the caller's error; a sniffed gzip
/// that is not a tar at all is merely a payload this service does not map.
fn not_a_tar(error: &io::Error, input: &InputStats, requested: bool) -> ParseError {
    let converted = stream_error(error, input);
    if !requested && matches!(converted, ParseError::Malformed(_)) {
        return ParseError::Unsupported(format!(
            "the gzip payload is not a readable tar archive ({error}); this service maps \
             gzip only as a Google Books METS export"
        ));
    }
    converted
}

/// Find the METS manifest among the kept members, exactly as docling does:
/// try every `.xml` member and accept the first whose root element is
/// `{{{NS_METS}}}mets` with `PROFILE="gbs"`.
fn find_mets_manifest(members: &BTreeMap<String, Vec<u8>>) -> Option<(&str, &[u8])> {
    members
        .iter()
        .filter(|(name, _)| {
            std::path::Path::new(name)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("xml"))
        })
        .find(|(_, bytes)| is_gbs_mets(bytes))
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
}

/// True when the document's root element is a METS `mets` with
/// `PROFILE="gbs"`. Reads only as far as the root start tag.
fn is_gbs_mets(bytes: &[u8]) -> bool {
    let mut xml = NsReader::from_reader(bytes);
    xml.config_mut().expand_empty_elements = true;
    let mut buf = Vec::new();
    loop {
        match xml.read_resolved_event_into(&mut buf) {
            Ok((resolved, Event::Start(start))) => {
                let ns_matches =
                    matches!(&resolved, ResolveResult::Bound(ns) if ns.as_ref() == NS_METS);
                let local_matches = start.local_name().as_ref() == "mets";
                let profile = start.try_get_attribute("PROFILE").ok().flatten();
                return ns_matches
                    && local_matches
                    && profile.is_some_and(|a| a.value.as_ref() == "gbs");
            }
            Ok((
                _,
                Event::Decl(_)
                | Event::Comment(_)
                | Event::PI(_)
                | Event::DocType(_)
                | Event::Text(_),
            )) => {}
            _ => return false,
        }
        buf.clear();
    }
}

/// The page map a manifest describes: for each 1-based `ORDER`, the files
/// its `fptr` elements reference, by use.
#[derive(Debug, Default)]
struct PageFiles {
    image: Option<FileInfo>,
    ocr: Option<FileInfo>,
    coord_ocr: Option<FileInfo>,
}

/// What one manifest walk produces.
#[derive(Debug, Default)]
struct Manifest {
    xml_version: Option<String>,
    encoding: Option<String>,
    root_namespace: String,
    root_local_name: String,
    pages: BTreeMap<u32, PageFiles>,
    elements_visited: u64,
}

/// The element-tracking state of one manifest walk.
#[derive(Debug, Default)]
struct ManifestWalk {
    files_by_id: HashMap<String, FileInfo>,
    /// One entry per open `fileGrp`, so nested groups inherit or override
    /// the `USE` of the group around them.
    grp_uses: Vec<Option<FileUse>>,
    depth: usize,
    /// The page `div` currently open, as (its depth, its `ORDER`).
    open_page: Option<(usize, u32)>,
    /// The `ID` of the `file` element currently open.
    open_file: Option<String>,
}

impl ManifestWalk {
    fn on_start(&mut self, start: &quick_xml::events::BytesStart<'_>, manifest: &mut Manifest) {
        self.depth += 1;
        manifest.elements_visited += 1;
        let local = start.local_name().as_ref().to_owned();
        if manifest.root_local_name.is_empty() {
            manifest.root_local_name.clone_from(&local);
            NS_METS.clone_into(&mut manifest.root_namespace);
        }
        match local.as_str() {
            "fileGrp" => {
                let use_type = attr(start, "USE").and_then(|v| FileUse::from_attr(&v));
                self.grp_uses.push(use_type);
            }
            "file" => {
                self.open_file = attr(start, "ID");
            }
            "FLocat" => {
                if let (Some(id), Some(href)) = (self.open_file.clone(), attr(start, "href"))
                    && let Some(use_type) = self.grp_uses.iter().rev().find_map(|u| *u)
                {
                    self.files_by_id.insert(
                        id,
                        FileInfo {
                            path: href,
                            use_type,
                        },
                    );
                }
            }
            "div" => {
                if attr(start, "TYPE").as_deref() == Some("page")
                    && let Some(order) = attr(start, "ORDER").and_then(|v| v.parse::<u32>().ok())
                {
                    manifest.pages.entry(order).or_default();
                    self.open_page = Some((self.depth, order));
                }
            }
            "fptr" => {
                if let Some((_, order)) = self.open_page
                    && let Some(info) =
                        attr(start, "FILEID").and_then(|id| self.files_by_id.get(&id))
                    && let Some(page) = manifest.pages.get_mut(&order)
                {
                    let slot = match info.use_type {
                        FileUse::Image => &mut page.image,
                        FileUse::Ocr => &mut page.ocr,
                        FileUse::CoordOcr => &mut page.coord_ocr,
                    };
                    *slot = Some(info.clone());
                }
            }
            _ => {}
        }
    }

    fn on_end(&mut self, end: &quick_xml::events::BytesEnd<'_>) {
        if end.local_name().as_ref() == "fileGrp" {
            self.grp_uses.pop();
        }
        if end.local_name().as_ref() == "file" {
            self.open_file = None;
        }
        if let Some((page_depth, _)) = self.open_page
            && self.depth == page_depth
        {
            self.open_page = None;
        }
        self.depth = self.depth.saturating_sub(1);
    }
}

/// One OCR line lifted from an hOCR page.
struct OcrLine {
    text: String,
    element_id: Option<String>,
    confidence: f64,
    attributes: Vec<pb::Attribute>,
}

/// An `ocr_line` span being captured, with the nesting count that finds its
/// matching end tag through the `ocrx_word` spans inside it.
struct OcrCapture {
    text: String,
    span_depth: usize,
    element_id: Option<String>,
    confidence: f64,
    has_bbox: bool,
    attributes: Vec<pb::Attribute>,
}

/// The METS driver: the same emit machinery [`crate::parse`]'s driver has,
/// for a parse whose input is many small documents instead of one stream.
struct MetsDriver<'a> {
    config: &'a ParseConfig,
    input: &'a InputStats,
    emit: EmitFn<'a>,
    started: std::time::Instant,
    source: pb::CollectorSource,
    index: u64,
    counts: pb::ParseCounts,
    warnings: BTreeMap<(i32, String), u64>,
}

impl MetsDriver<'_> {
    /// Manifest, then pages in order, then the trailer.
    fn run(
        &mut self,
        members: &BTreeMap<String, Vec<u8>>,
        manifest_name: &str,
        manifest_bytes: &[u8],
        evidence: Evidence,
    ) -> Result<(), ParseError> {
        let manifest = self.read_manifest(manifest_name, manifest_bytes)?;
        self.counts.elements_visited += manifest.elements_visited;

        let info = pb::XmlInfo {
            dialect: Dialect::MetsGbs.to_proto() as i32,
            evidence: evidence.to_proto() as i32,
            root_namespace: manifest.root_namespace.clone(),
            root_local_name: manifest.root_local_name.clone(),
            doctype_name: None,
            public_id: None,
            system_id: None,
            title: None,
            encoding: manifest.encoding.clone(),
            xml_version: manifest.xml_version.clone(),
        };
        self.send(pb::parse_xml_response::Event::Info(info))?;

        for (order, files) in &manifest.pages {
            self.counts.pages += 1;
            let Some(coord) = files.coord_ocr.as_ref() else {
                self.warn(
                    pb::WarningCode::ArchiveMemberIgnored,
                    "a manifest page has no coordOCR file and was skipped",
                );
                continue;
            };
            let Some(bytes) = member_by_href(members, &coord.path) else {
                self.warn(
                    pb::WarningCode::ArchiveMemberIgnored,
                    "the manifest references a coordOCR member the archive does not contain",
                );
                continue;
            };
            self.emit_page(*order, &coord.path, bytes)?;
            if files.image.is_some() {
                self.warn(
                    pb::WarningCode::ArchiveMemberIgnored,
                    "page image members are not decoded",
                );
            }
            if files.ocr.is_some() {
                self.warn(
                    pb::WarningCode::ArchiveMemberIgnored,
                    "plain OCR text members are not mapped; text comes from coordOCR",
                );
            }
        }

        self.emit_status()
    }

    /// Walk the manifest into a page map.
    ///
    /// The walk accepts `fileGrp`, `file` and `FLocat` at any depth rather
    /// than requiring docling's exact `XPath` shape, because the manifest is
    /// already validated as a GBS METS and looser matching maps the same
    /// documents.
    fn read_manifest(&mut self, name: &str, bytes: &[u8]) -> Result<Manifest, ParseError> {
        let mut manifest = Manifest::default();
        let mut walk = ManifestWalk::default();
        let mut xml = NsReader::from_reader(bytes);
        xml.config_mut().expand_empty_elements = true;
        let mut buf = Vec::new();
        loop {
            let event = xml
                .read_event_into(&mut buf)
                .map_err(|e| ParseError::Malformed(format!("METS manifest {name}: {e}")))?;
            match event {
                Event::Start(start) => walk.on_start(&start, &mut manifest),
                Event::End(end) => walk.on_end(&end),
                Event::Decl(decl) => {
                    manifest.xml_version = decl.version().ok().map(std::borrow::Cow::into_owned);
                    manifest.encoding = decl
                        .encoding()
                        .and_then(Result::ok)
                        .map(std::borrow::Cow::into_owned);
                }
                Event::DocType(doctype) => self.check_doctype(&doctype, name)?,
                Event::Eof => break,
                _ => {}
            }
            buf.clear();
        }
        Ok(manifest)
    }

    /// Walk one hOCR page and emit a text item per `ocr_line` span, exactly
    /// the spans docling reads. A line without a `bbox` in its `title` is
    /// dropped as upstream drops it; the box itself is not carried — this
    /// plane has no prov — but the `x_wconf` confidence is, on the item's
    /// source.
    fn emit_page(&mut self, order: u32, member: &str, bytes: &[u8]) -> Result<(), ParseError> {
        let lines = self.read_hocr(member, bytes)?;
        for (n, line) in lines.into_iter().enumerate() {
            let mut source = self.source.clone();
            source.confidence = Some(line.confidence);
            let item = pb::TextItem {
                index: self.next_index(),
                label: pb::XmlItemLabel::Text as i32,
                role: "ocr-line".to_owned(),
                text: line.text,
                level: None,
                ordinal: None,
                // One parse spans many member documents, so the path is
                // synthetic: the page's manifest ORDER and the line's
                // position on it, not an element path into one file.
                path: format!("/page[{order}]/line[{}]", n + 1),
                element_id: line.element_id,
                attributes: line.attributes,
                source: Some(source),
            };
            self.counts.text_items += 1;
            self.send(pb::parse_xml_response::Event::TextItem(item))?;
        }
        Ok(())
    }

    /// Pull the `ocr_line` spans out of one hOCR document.
    ///
    /// hOCR is HTML: the reader keeps quick-xml's empty-element expansion
    /// but drops end-name checking, and elements other than `span` are not
    /// tracked at all, so an HTML void element that never closes cannot
    /// derail the line capture the way it would derail a depth count.
    fn read_hocr(&mut self, member: &str, bytes: &[u8]) -> Result<Vec<OcrLine>, ParseError> {
        let mut xml = NsReader::from_reader(bytes);
        xml.config_mut().expand_empty_elements = true;
        xml.config_mut().check_end_names = false;
        xml.config_mut().allow_unmatched_ends = true;

        let mut lines = Vec::new();
        let mut capture: Option<OcrCapture> = None;
        let mut buf = Vec::new();
        loop {
            let event = xml
                .read_event_into(&mut buf)
                .map_err(|e| ParseError::Malformed(format!("hOCR member {member}: {e}")))?;
            match event {
                Event::Start(start) => {
                    self.counts.elements_visited += 1;
                    if start.local_name().as_ref() != "span" {
                        buf.clear();
                        continue;
                    }
                    if let Some(capture) = capture.as_mut() {
                        capture.span_depth += 1;
                    } else if attr(&start, "class")
                        .is_some_and(|c| c.split_whitespace().any(|t| t == "ocr_line"))
                    {
                        let title = attr(&start, "title").unwrap_or_default();
                        let attributes = if self.config.include_attributes {
                            attributes_except(&start, &["class", "title", "id"])
                        } else {
                            Vec::new()
                        };
                        capture = Some(OcrCapture {
                            text: String::new(),
                            span_depth: 0,
                            element_id: attr(&start, "id"),
                            confidence: extract_confidence(&title),
                            has_bbox: has_bbox(&title),
                            attributes,
                        });
                    }
                }
                Event::End(end) => {
                    if end.local_name().as_ref() != "span" {
                        buf.clear();
                        continue;
                    }
                    if let Some(open) = capture.as_mut() {
                        if open.span_depth > 0 {
                            open.span_depth -= 1;
                        } else if let Some(done) = capture.take() {
                            let text = collapse(&done.text);
                            if done.has_bbox && !text.is_empty() {
                                lines.push(OcrLine {
                                    text,
                                    element_id: done.element_id,
                                    confidence: done.confidence,
                                    attributes: done.attributes,
                                });
                            }
                        }
                    }
                }
                Event::Text(text) => {
                    if let Some(capture) = capture.as_mut() {
                        capture.text.push_str(&text.xml10_content());
                    }
                }
                Event::CData(cdata) => {
                    if let Some(capture) = capture.as_mut() {
                        capture.text.push_str(&cdata.into_inner());
                    }
                }
                Event::GeneralRef(reference) => {
                    let name = reference.into_inner().into_owned();
                    self.push_reference(capture.as_mut(), &name);
                }
                Event::DocType(doctype) => self.check_doctype(&doctype, member)?,
                Event::Eof => break,
                _ => {}
            }
            buf.clear();
        }
        Ok(lines)
    }

    /// Fold one general entity reference into the open capture, resolving
    /// only what the XML specification fixes — character references and the
    /// five predefined entities — and preserving anything else verbatim with
    /// a warning, exactly as the XML driver treats content references.
    fn push_reference(&mut self, capture: Option<&mut OcrCapture>, name: &str) {
        let resolved = parse::resolve_reference(name);
        if resolved.is_none() {
            self.warn(
                pb::WarningCode::UnexpandedEntity,
                &format!(
                    "entity reference &{name}; was preserved verbatim; this parser declares \
                     and expands no entities"
                ),
            );
        }
        if let Some(capture) = capture {
            if let Some(text) = resolved {
                capture.text.push_str(&text);
            } else {
                capture.text.push('&');
                capture.text.push_str(name);
                capture.text.push(';');
            }
        }
    }

    /// Apply the security policy to a DOCTYPE found inside an archive
    /// member, exactly as the XML driver applies it to a plain document.
    fn check_doctype(
        &mut self,
        doctype: &quick_xml::events::BytesText<'_>,
        member: &str,
    ) -> Result<(), ParseError> {
        let parsed = security::parse_doctype(doctype);
        security::check_doctype(&parsed).map_err(|refusal| {
            ParseError::Refused(format!("archive member {member}: {refusal}"))
        })?;
        if parsed.system_id.is_some() || parsed.public_id.is_some() {
            self.warn(
                pb::WarningCode::ExternalIdIgnored,
                "DOCTYPE external identifier recorded and not dereferenced",
            );
        }
        Ok(())
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
        } else if self.warnings.len() < parse::MAX_WARNING_KINDS {
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
            dialect: Dialect::MetsGbs.to_proto() as i32,
            counts: Some(self.counts),
            warnings,
            bytes_consumed: self.input.bytes(),
            elapsed_millis: u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX),
        };
        self.send(pb::parse_xml_response::Event::Status(status))
    }
}

/// Look a manifest `href` up among the tar members. Exact name first, then
/// with a leading `./` stripped or added, then as a suffix under any single
/// leading directory — real exports nest their members under a barcode
/// directory the hrefs do not repeat.
fn member_by_href<'a>(members: &'a BTreeMap<String, Vec<u8>>, href: &str) -> Option<&'a [u8]> {
    if let Some(bytes) = members.get(href) {
        return Some(bytes);
    }
    let bare = href.strip_prefix("./").unwrap_or(href);
    if let Some(bytes) = members.get(bare) {
        return Some(bytes);
    }
    let suffix = format!("/{bare}");
    members
        .iter()
        .find(|(name, _)| name.ends_with(&suffix))
        .map(|(_, bytes)| bytes.as_slice())
}

/// An attribute's value by local name. hOCR and METS both
/// prefix attributes (`xlink:href`), and the prefix never matters here.
fn attr(start: &quick_xml::events::BytesStart<'_>, local: &str) -> Option<String> {
    start.attributes().filter_map(Result::ok).find_map(|a| {
        let key = a.key.as_ref();
        let key_local = key.rsplit(':').next().unwrap_or(key);
        (key_local == local).then(|| a.value.as_ref().to_owned())
    })
}

/// The attributes of a start tag except the ones the mapping consumed.
fn attributes_except(
    start: &quick_xml::events::BytesStart<'_>,
    consumed: &[&str],
) -> Vec<pb::Attribute> {
    start
        .attributes()
        .filter_map(Result::ok)
        .filter_map(|a| {
            let name = a.key.as_ref().to_owned();
            (!consumed.contains(&name.as_str()) && name != "xmlns" && !name.starts_with("xmlns:"))
                .then(|| pb::Attribute {
                    name,
                    value: a.value.as_ref().to_owned(),
                })
        })
        .collect()
}

/// True when an hOCR `title` attribute carries a `bbox` clause.
fn has_bbox(title: &str) -> bool {
    title.split(';').any(|part| {
        let part = part.trim();
        part.strip_prefix("bbox ").is_some_and(|coords| {
            let mut n = 0;
            for token in coords.split_whitespace() {
                if token.parse::<i64>().is_err() {
                    return false;
                }
                n += 1;
            }
            n == 4
        })
    })
}

/// The `x_wconf` OCR confidence of an hOCR `title` attribute, scaled to
/// 0.0..=1.0. Defaults to 1.0 when absent or unparsable, as upstream does.
fn extract_confidence(title: &str) -> f64 {
    for part in title.split(';') {
        let part = part.trim();
        if let Some(value) = part.strip_prefix("x_wconf") {
            if let Ok(parsed) = value.trim().parse::<f64>() {
                return parsed / 100.0;
            }
            return 1.0;
        }
    }
    1.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn magic_bytes_route_and_short_heads_do_not() {
        assert_eq!(sniff_magic(b"PK\x03\x04rest"), Some(ArchiveKind::Zip));
        assert_eq!(sniff_magic(&[0x1f, 0x8b, 0x08]), Some(ArchiveKind::Gzip));
        assert_eq!(sniff_magic(b"<?xm"), None);
        assert_eq!(sniff_magic(b"P"), None);
        assert_eq!(sniff_magic(&[0x1f]), None);
        assert_eq!(sniff_magic(b""), None);
    }

    #[test]
    fn hocr_title_clauses_parse_the_way_docling_reads_them() {
        assert!(has_bbox("bbox 279 177 306 214; x_wconf 97"));
        assert!(!has_bbox("x_wconf 97"));
        assert!(!has_bbox("bbox 1 2 3"));
        assert!(!has_bbox("bbox one two three four"));
        let confidence = extract_confidence("bbox 1 2 3 4; x_wconf 97");
        assert!((confidence - 0.97).abs() < f64::EPSILON);
        let confidence = extract_confidence("bbox 1 2 3 4");
        assert!((confidence - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn hrefs_resolve_exact_dotted_and_nested_member_names() {
        let mut members = BTreeMap::new();
        members.insert("plain.html".to_owned(), b"a".to_vec());
        members.insert("./dotted.html".to_owned(), b"b".to_vec());
        members.insert("barcode/nested.html".to_owned(), b"c".to_vec());
        assert_eq!(member_by_href(&members, "plain.html"), Some(&b"a"[..]));
        assert_eq!(member_by_href(&members, "dotted.html"), Some(&b"b"[..]));
        assert_eq!(member_by_href(&members, "nested.html"), Some(&b"c"[..]));
        assert_eq!(member_by_href(&members, "absent.html"), None);
    }
}
