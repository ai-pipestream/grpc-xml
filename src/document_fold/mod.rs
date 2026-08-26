// SPDX-License-Identifier: Apache-2.0

//! The collector-side fold from this service's event stream into one
//! `ai.pipestream.document.v1.Document`.
//!
//! The typed event stream is the primary, lossless wire. This module is the
//! lossy structural projection of it, offered because a coordinator that only
//! wants the Document plane should not have to reimplement the mapping — the
//! canonical fold lives next to the collector that knows what the events mean.
//!
//! Three properties shape the code:
//!
//! - **Single pass, one event at a time.** [`DocumentFold::consume`] takes the
//!   same [`ParseXmlResponse`](pb::ParseXmlResponse) the service is about to
//!   write to the socket and does a bounded amount of work with it. The only
//!   state that outlives an event is the table currently being streamed, which
//!   is the one shape the wire splits across events.
//! - **A self-contained fragment.** Refs are dense and local
//!   (`#/texts/0`, `#/tables/1`), every item's `parent` is the section header
//!   it sits under (or `#/body`) and every parent lists the item in its
//!   `children`, so the coordinator's additive merge can renumber the whole
//!   fragment mechanically. [`integrity_errors`] is the check for that, and the
//!   tests assert it is empty.
//! - **Heading-as-parent nesting.** A section header of level N is parented
//!   to the nearest open header of a lower level, and the content after it
//!   hangs off that header rather than off the body. That heading ladder is
//!   the whole nesting model, and it is why this fold builds no section
//!   `GroupItem`s: filler groups exist to patch skipped heading levels, and
//!   our levels come straight from the parser.
//! - **Provenance honesty.** An item carries `prov` when the source states
//!   coordinates, which among these dialects means the hOCR pages of a
//!   Google Books export and nothing else. The single-document dialects have
//!   no pages and no boxes, so their items carry none. Source locators — the
//!   positional path, the element id, the source's own role vocabulary — go
//!   in per-item `meta.custom_fields` under `xml.` keys, where they are data
//!   rather than a fabricated coordinate.
//!
//! What is deliberately not mapped: `html_island` events (an XHTML fragment is
//! the HTML collector's job, and re-parsing it here would produce a worse
//! result than that collector gets) and the unconsumed source attributes of
//! `include_attributes` (they are an inspection aid for the typed stream, not
//! document structure). The count of islands the fold skipped is recorded on
//! the body's meta so the omission is visible rather than silent.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use prost_types::Value;
use prost_types::value::Kind;

mod integrity;

pub use integrity::integrity_errors;

use crate::document::v1 as doc;
use crate::proto::v1 as pb;
use crate::{COLLECTOR, VERSION};

/// Value of `Document.schema_name`: the schema version identifier of the
/// Document plane.
pub const SCHEMA_NAME: &str = "docling_document_v2";

/// Value of `DocumentOrigin.mimetype` for the single-document dialects. The
/// archive dialects override it in `mimetype` with the type of the archive
/// itself; the dialect is on the body meta and on every item's
/// `CollectorSource.model` either way.
pub const MIMETYPE: &str = "application/xml";

/// Self ref of the body group: the parent of everything this fold makes that
/// is not under a section header.
const BODY_REF: &str = "#/body";

/// Self ref of the furniture group. Nothing is put in it: XML dialects have
/// no page chrome to put there.
const FURNITURE_REF: &str = "#/furniture";

/// `SummaryMetaField.created_by` for a summary the document wrote itself.
/// Nothing here generates a summary; an abstract is quoted, not produced.
const SUMMARY_CREATED_BY: &str = "source";

/// Columns of the XBRL fact table, in order: heading and the type the
/// column's cells declare.
///
/// The wire model for an XBRL fact is the richest in this service, and the
/// projection used to be six strings. The typed columns say what each one
/// is, and the cells that have a machine value carry it in `CellValue`
/// alongside the display text.
const FACT_COLUMNS: [(&str, &str); 11] = [
    ("concept", "qname"),
    ("entity_scheme", "uri"),
    ("entity", "identifier"),
    ("context", "id"),
    ("period", "period"),
    ("unit", "unit"),
    ("value", "decimal"),
    ("decimals", "decimal"),
    ("precision", "decimal"),
    ("sign", "boolean"),
    ("nil", "boolean"),
];

/// A fold of one parse's events into one Document.
///
/// Feed it every event of one `ParseXml` response stream in order, then call
/// [`take`](Self::take). Events from two different parses must not be mixed
/// into one fold.
pub struct DocumentFold {
    document: doc::Document,
    /// Attribution for items whose event carried no source of its own, and
    /// for the rows of the fact table.
    fallback_source: doc::CollectorSource,
    /// The table opened by a `table_start` and not yet closed.
    table: Option<PendingTable>,
    /// Arena index of the fact table, created when the first fact arrives.
    facts_table: Option<usize>,
    /// The open section headers, outermost first: the level the wire gave each
    /// one and the self ref content under it names as its parent. Empty means
    /// the body is the parent.
    headings: Vec<(u32, String)>,
    /// `html_island` events seen and not mapped.
    islands_skipped: u64,
    /// Every source identifier this fold has given an item a home for, in
    /// first-writer-wins order. It becomes `Document.anchors`, and it is what
    /// turns a cross-reference into a resolved `FineRef`.
    anchors: BTreeMap<String, String>,
    /// What the document says about itself, gathered as the items that carry
    /// it stream past. Published as `Document.source_meta`.
    source_meta: doc::DocumentMeta,
    /// Abstract paragraphs in document order, for the body's summary.
    summary: Vec<String>,
}

/// A table being assembled from `table_start` / `table_row` / `table_end`.
struct PendingTable {
    /// The wire identifier rows and the end event carry.
    reference: String,
    /// Everything about the item except its position in the arena, which is
    /// only known when it is appended at `table_end`.
    item: doc::TableItem,
    grid: Vec<doc::TableRow>,
    cells: Vec<doc::TableCell>,
    /// Grid slots already taken by a cell that spans into them, so a row
    /// under a `rowspan` starts at the first free column.
    occupied: BTreeSet<(i32, i32)>,
    num_cols: i32,
    /// The column geometry the source declared, which arrives on the closing
    /// event and so is empty until then.
    columns: Vec<doc::TableColumnSchema>,
}

impl Default for DocumentFold {
    fn default() -> Self {
        Self::new()
    }
}

impl DocumentFold {
    /// An empty fold, with the two root groups already in place.
    #[must_use]
    pub fn new() -> Self {
        Self {
            document: doc::Document {
                schema_name: Some(SCHEMA_NAME.to_owned()),
                origin: Some(doc::DocumentOrigin {
                    mimetype: MIMETYPE.to_owned(),
                    ..doc::DocumentOrigin::default()
                }),
                body: Some(group(BODY_REF, doc::ContentLayer::Body)),
                furniture: Some(group(FURNITURE_REF, doc::ContentLayer::Furniture)),
                ..doc::Document::default()
            },
            fallback_source: doc::CollectorSource {
                collector: COLLECTOR.to_owned(),
                model: None,
                version: Some(VERSION.to_owned()),
                confidence: None,
                ..doc::CollectorSource::default()
            },
            table: None,
            facts_table: None,
            headings: Vec::new(),
            islands_skipped: 0,
            anchors: BTreeMap::new(),
            source_meta: doc::DocumentMeta::default(),
            summary: Vec::new(),
        }
    }

    /// Fold one response event.
    ///
    /// Unrecognized and unmapped events are ignored rather than refused: the
    /// fold is a projection, and an event it has no slot for is a gap in the
    /// projection, not a parse failure.
    pub fn consume(&mut self, event: &pb::ParseXmlResponse) {
        use pb::parse_xml_response::Event;
        match event.event.as_ref() {
            Some(Event::Info(info)) => self.on_info(info),
            Some(Event::TextItem(item)) => {
                if pb::XmlItemLabel::try_from(item.label) == Ok(pb::XmlItemLabel::Picture) {
                    self.push_picture(item);
                } else {
                    self.push_text(item);
                }
            }
            Some(Event::TableStart(start)) => self.on_table_start(start),
            Some(Event::TableRow(row)) => self.on_table_row(row),
            Some(Event::TableEnd(end)) => self.on_table_end(end),
            Some(Event::Fact(fact)) => self.on_fact(fact),
            Some(Event::MetaItem(item)) => self.on_meta_item(item),
            Some(Event::Page(page)) => self.on_page(page),
            Some(Event::XbrlNote(note)) => self.on_xbrl_note(note),
            Some(Event::HtmlIsland(_)) => self.islands_skipped += 1,
            // The trailer is counts and warnings, both of which describe the
            // typed stream rather than the document; the fold sees it only so
            // it knows the stream is over.
            // A `document` event is this fold's own output; folding one back
            // in would double the fragment.
            Some(Event::Status(_) | Event::Document(_)) | None => {}
        }
    }

    /// Finish the fragment and take it. The fold is empty afterwards.
    pub fn take(&mut self) -> doc::Document {
        // A table whose `table_end` never arrived still has rows worth
        // keeping; the parse that produced it failed, but the fold does not
        // get to decide that.
        if let Some(pending) = self.table.take() {
            self.append_table(pending);
        }
        if self.islands_skipped > 0 {
            let count = self.islands_skipped;
            if let Some(body) = self.document.body.as_mut() {
                body.meta
                    .get_or_insert_default()
                    .custom_fields
                    .insert("xml.html_islands".to_owned(), number(count));
            }
        }
        self.publish_anchors();
        self.publish_source_meta();
        self.islands_skipped = 0;
        self.facts_table = None;
        self.headings.clear();
        self.anchors.clear();
        self.source_meta = doc::DocumentMeta::default();
        self.summary.clear();
        std::mem::replace(&mut self.document, Self::new().document)
    }

    // ----------------------------------------------------------- source meta

    /// Publish what the document declared about itself.
    ///
    /// `DocumentMeta` is only attached when the source actually said
    /// something: an all-default message would claim the document declared
    /// an empty title and no language, which is not what an absent
    /// declaration means.
    fn publish_source_meta(&mut self) {
        let meta = std::mem::take(&mut self.source_meta);
        if meta != doc::DocumentMeta::default() {
            self.document.source_meta = Some(meta);
        }
        if self.summary.is_empty() {
            return;
        }
        let text = self.summary.join(" ");
        if let Some(body) = self.document.body.as_mut() {
            body.meta.get_or_insert_default().summary = Some(doc::SummaryMetaField {
                text,
                // The abstract is the document's own summary, quoted rather
                // than generated, so there is no confidence to claim and the
                // creator is the source.
                confidence: None,
                created_by: Some(SUMMARY_CREATED_BY.to_owned()),
                ..doc::SummaryMetaField::default()
            });
        }
    }

    /// Fold one decoded metadata record.
    ///
    /// Every shape has a typed home now: dates become the document's
    /// creation and modification declarations, a cited-reference entry
    /// becomes a `REFERENCE` item like any other bibliography entry, and the
    /// rest land on `DocumentMeta` field for field. Nothing here is
    /// flattened into a string or a map.
    fn on_meta_item(&mut self, item: &pb::MetaItem) {
        match item.value.as_ref() {
            Some(pb::meta_item::Value::Date(date)) => self.on_meta_date(date),
            Some(pb::meta_item::Value::Citation(citation)) => {
                if citation.text.is_empty() {
                    return;
                }
                self.push_text(&pb::TextItem {
                    label: pb::XmlItemLabel::Reference as i32,
                    text: citation.text.clone(),
                    ordinal: citation.ordinal,
                    element_id: citation.element_id.clone(),
                    path: item.path.clone(),
                    source: item.source.clone(),
                    ..pb::TextItem::default()
                });
            }
            Some(pb::meta_item::Value::Identifier(identifier)) => {
                if identifier.value.is_empty() {
                    return;
                }
                self.source_meta.identifiers.push(doc::Identifier {
                    kind: identifier.kind.clone(),
                    value: identifier.value.clone(),
                    scope: identifier.scope.clone(),
                });
            }
            Some(pb::meta_item::Value::Classification(classification)) => {
                if classification.code.is_empty() {
                    return;
                }
                self.source_meta.classifications.push(doc::Classification {
                    scheme: classification.scheme.clone(),
                    code: classification.code.clone(),
                    edition: classification.edition.clone(),
                    office: classification.office.clone(),
                });
            }
            Some(pb::meta_item::Value::License(license)) => {
                // A document states its terms once; a second permissions
                // block would be the same terms restated, and the first is
                // the one the parser reached in document order.
                if self.source_meta.license.is_some() {
                    return;
                }
                self.source_meta.license = Some(doc::LicenseMeta {
                    type_uri: license.type_uri.clone(),
                    statement: license.statement.clone(),
                    copyright_statement: license.copyright_statement.clone(),
                    // The schema wants the year as a number; a source that
                    // writes something else there states no year rather than
                    // a zero one.
                    copyright_year: license
                        .copyright_year
                        .as_deref()
                        .and_then(|year| year.trim().parse().ok()),
                    copyright_holder: license.copyright_holder.clone(),
                });
            }
            Some(pb::meta_item::Value::Funding(funding)) => {
                self.source_meta.funding.push(doc::FundingAward {
                    funder: funding.funder.clone(),
                    award_id: funding.award_id.clone(),
                    statement: funding.statement.clone(),
                });
            }
            None => {}
        }
    }

    /// A publication date becomes the document's creation declaration, a
    /// revision date its modification declaration.
    ///
    /// Three fields say three different things about one date, and which of
    /// them the source supports depends on what it wrote. The civil twin is
    /// always set, because an XML publication date is a wall-clock date with
    /// no time zone in it; the `Timestamp` is set only for a whole calendar
    /// date, read as midnight UTC; the `_raw` twin keeps the source's own
    /// spelling as far as it goes. A `pub-date` naming a year and a month is
    /// a civil year and month, never a fabricated first of January.
    fn on_meta_date(&mut self, date: &pb::MetaDate) {
        let Some(written) = iso_date(date) else {
            return;
        };
        let civil = civil_of(date);
        let instant = timestamp_of(date);
        match date.kind.as_str() {
            "" | "pub" | "epub" | "ppub" | "collection" | "epub-ppub" => {
                if self.source_meta.created_raw.is_none() {
                    self.source_meta.created_raw = Some(written);
                    self.source_meta.created_civil = civil;
                    self.source_meta.created = instant;
                }
            }
            "rev-recd" | "revised" | "corrected" | "updated" => {
                self.source_meta.modified_raw = Some(written);
                self.source_meta.modified_civil = civil;
                self.source_meta.modified = instant;
            }
            _ => {}
        }
    }

    /// Route an item that carries document-level metadata into the typed
    /// slots the Document plane already has for it.
    ///
    /// The role vocabulary is the source's own, so the match is on what the
    /// dialects actually emit rather than on a normalized set.
    fn collect_source_meta(&mut self, item: &pb::TextItem, label: pb::XmlItemLabel) {
        if label == pb::XmlItemLabel::Title && self.source_meta.title.is_none() {
            self.source_meta.title = Some(item.text.clone());
        }
        match item.role.as_str() {
            "keyword" => self.source_meta.keywords.push(item.text.clone()),
            // JATS writes the contributor's kind into `contrib-type`, which
            // becomes the role verbatim; USPTO names the role in the element.
            "author" | "contributor" | "inventor" => {
                self.source_meta.authors.push(item.text.clone());
            }
            "abstract" => self.summary.push(item.text.clone()),
            _ => {}
        }
    }

    // --------------------------------------------------------------- anchors

    /// Publish the source identifiers as `Document.anchors`, then upgrade
    /// every cross-reference that names one to point at the item itself.
    ///
    /// Resolution has to wait for the end of the stream because references
    /// run both ways: a citation names an entry of the reference list, which
    /// arrives long after the sentence citing it. Until then a span's target
    /// is the source name written as `#<id>`, which stays as it is for an
    /// identifier the document never defines. Item refs begin `#/`, which no
    /// XML name can, so the two never collide.
    ///
    /// Every run reaches this pass, wherever it sits: a `xref` inside a table
    /// cell points at the same reference list a `xref` in a paragraph does,
    /// and resolving only the paragraph's would make a cell's citation a
    /// second-class one for no reason the source states.
    fn publish_anchors(&mut self) {
        self.document.anchors = self
            .anchors
            .iter()
            .map(|(name, self_ref)| doc::NamedAnchor {
                name: name.clone(),
                target: Some(fine(self_ref)),
            })
            .collect();
        for item in &mut self.document.texts {
            let Some(base) = base_of(item) else { continue };
            resolve_targets(&self.anchors, &mut base.spans);
        }
        for table in &mut self.document.tables {
            let Some(data) = table.data.as_mut() else {
                continue;
            };
            // `grid` and `table_cells` hold clones of the same cells, so both
            // are walked: a consumer reading either one sees the same graph.
            for cell in &mut data.table_cells {
                resolve_targets(&self.anchors, &mut cell.spans);
            }
            for row in &mut data.grid {
                for cell in &mut row.cells {
                    resolve_targets(&self.anchors, &mut cell.spans);
                }
            }
        }
    }

    /// Remember that an item answers to a source identifier. The first item
    /// to claim a name keeps it, matching how a conformant document declares
    /// each identifier once.
    fn claim_anchor(&mut self, element_id: Option<&String>, self_ref: &str) {
        let Some(id) = element_id.filter(|id| !id.is_empty()) else {
            return;
        };
        self.anchors
            .entry(id.clone())
            .or_insert_with(|| self_ref.to_owned());
    }

    // ------------------------------------------------------------------ info

    /// `XmlInfo` names the document and the dialect that mapped it.
    ///
    /// The dialect and the root element go on the body's meta rather than on
    /// each item, because they are true of the whole fragment. The merge
    /// downstream is first-writer-wins for root meta: if another collector's
    /// fragment already wrote `xml.dialect`, ours is dropped, which is
    /// harmless because every item still carries the dialect in its
    /// `CollectorSource.model`.
    fn on_info(&mut self, info: &pb::XmlInfo) {
        let dialect = pb::XmlDialect::try_from(info.dialect).unwrap_or_default();
        let model = model_name(dialect);
        self.fallback_source.model = Some(model.to_owned());
        if let Some(origin) = self.document.origin.as_mut() {
            mimetype(dialect).clone_into(&mut origin.mimetype);
        }
        if let Some(title) = info.title.as_ref().filter(|t| !t.is_empty()) {
            self.document.name.clone_from(title);
        }
        if let Some(language) = info.language.as_ref().filter(|l| !l.is_empty()) {
            self.source_meta.language = Some(language.clone());
        }
        // The bindings that make a prefixed path resolvable, and the schema
        // documents the root associates with the instance, both field for
        // field: the wire decoded them, so the projection carries the pairs
        // rather than a re-rendering of them.
        self.source_meta.namespaces = info
            .namespaces
            .iter()
            .map(|binding| doc::NamespaceBinding {
                prefix: binding.prefix.clone(),
                uri: binding.uri.clone(),
            })
            .collect();
        self.source_meta.schema_locations = info
            .schema_locations
            .iter()
            .map(|location| doc::SchemaLocation {
                namespace: location.namespace.clone(),
                location: location.location.clone(),
            })
            .collect();
        // The same declaration as one string, which is that field's shape.
        if let Some(location) = info
            .root_attributes
            .iter()
            .find(|a| local_name(&a.name) == "schemaLocation")
            .or_else(|| {
                info.root_attributes
                    .iter()
                    .find(|a| local_name(&a.name) == "noNamespaceSchemaLocation")
            })
            .filter(|a| !a.value.is_empty())
        {
            self.source_meta.schema_location = Some(location.value.clone());
        }
        let mut fields = BTreeMap::new();
        fields.insert("xml.dialect", string(model));
        if !info.root_namespace.is_empty() {
            fields.insert("xml.root_namespace", string(&info.root_namespace));
        }
        if !info.root_local_name.is_empty() {
            fields.insert("xml.root_local_name", string(&info.root_local_name));
        }
        if let Some(body) = self.document.body.as_mut() {
            let meta = body.meta.get_or_insert_default();
            for (key, value) in fields {
                meta.custom_fields.insert(key.to_owned(), value);
            }
        }
    }

    // ------------------------------------------------------------------ pages

    /// Register one page of an archive dialect.
    ///
    /// `Document.pages` had no writer in this crate, and neither did
    /// `ProvenanceItem`, even though a Google Books export is the one input
    /// here with real coordinates. A page is registered even when the source
    /// states no extent, because the page exists and the items on it name it.
    fn on_page(&mut self, page: &pb::Page) {
        let size = match (page.width, page.height) {
            (Some(width), Some(height)) => Some(doc::Size { width, height }),
            _ => None,
        };
        self.document.pages.insert(
            int(page.page_no),
            doc::PageItem {
                size,
                image: None,
                page_no: int(page.page_no),
                unit: (!page.unit.is_empty()).then(|| page.unit.clone()),
                // Extraction diagnostics belong to a producer that measured
                // them; this one reads what the OCR already decided, and an
                // hOCR page declares no style, label, media box, or unit
                // multiplier.
                ..Default::default()
            },
        );
    }

    // ------------------------------------------------------------------ text

    /// Append one text item and return its self ref.
    ///
    /// A section header opens a level on the heading ladder before it is
    /// placed, so it is parented to the header enclosing it rather than to the
    /// one it closes.
    fn push_text(&mut self, item: &pb::TextItem) -> String {
        let label = pb::XmlItemLabel::try_from(item.label).unwrap_or_default();
        let level = item.level.unwrap_or(1);
        if label == pb::XmlItemLabel::SectionHeader {
            self.close_headings(level);
        }
        let parent = self.current_parent();
        let self_ref = format!("#/texts/{}", self.document.texts.len());
        let fields = text_fields(item);
        let source = self.source_of(item.source.as_ref());
        let variant = if label == pb::XmlItemLabel::Code {
            // TRAP, and the reason this branch exists: CodeItem does not wrap
            // a TextItemBase. Its base fields are inlined so that its single
            // `meta` slot is a FloatingMeta; see the comment on CodeItem in
            // document.proto.
            doc::base_text_item::Item::Code(doc::CodeItem {
                self_ref: self_ref.clone(),
                parent: Some(reference(&parent)),
                content_layer: doc::ContentLayer::Body as i32,
                meta: Some(doc::FloatingMeta {
                    custom_fields: fields,
                    ..doc::FloatingMeta::default()
                }),
                label: doc::DocItemLabel::Code as i32,
                orig: item.text.clone(),
                text: item.text.clone(),
                source: vec![source],
                ..doc::CodeItem::default()
            })
        } else {
            let base = doc::TextItemBase {
                self_ref: self_ref.clone(),
                parent: Some(reference(&parent)),
                content_layer: doc::ContentLayer::Body as i32,
                meta: Some(doc::BaseMeta {
                    custom_fields: fields,
                    ..doc::BaseMeta::default()
                }),
                label: doc_label(label) as i32,
                // Provenance only where the source has coordinates. The
                // single-document dialects have no pages and no boxes, and
                // an invented one would outlive the honesty of this comment.
                prov: provenance(item),
                orig: item.text.clone(),
                text: item.text.clone(),
                source: vec![source],
                spans: inline_spans(&item.spans),
                ..doc::TextItemBase::default()
            };
            match label {
                pb::XmlItemLabel::Title => {
                    doc::base_text_item::Item::Title(doc::TitleItem { base: Some(base) })
                }
                pb::XmlItemLabel::SectionHeader => {
                    doc::base_text_item::Item::SectionHeader(doc::SectionHeaderItem {
                        base: Some(base),
                        // Redundant with the nesting, and kept anyway: the
                        // schema carries both.
                        level: int(level),
                    })
                }
                pb::XmlItemLabel::ListItem => {
                    doc::base_text_item::Item::ListItem(doc::ListItem {
                        base: Some(base),
                        // The source numbered it, so the list is ordered. The
                        // marker itself is not on the wire: the parser strips
                        // it into `ordinal`.
                        enumerated: item.ordinal.is_some(),
                        marker: None,
                    })
                }
                pb::XmlItemLabel::Formula => {
                    doc::base_text_item::Item::Formula(doc::FormulaItem { base: Some(base) })
                }
                _ => doc::base_text_item::Item::Text(doc::TextItem { base: Some(base) }),
            }
        };
        self.document.texts.push(doc::BaseTextItem {
            item: Some(variant),
        });
        self.claim_anchor(item.element_id.as_ref(), &self_ref);
        self.collect_source_meta(item, label);
        self.link_child(&parent, &self_ref);
        if label == pb::XmlItemLabel::SectionHeader {
            self.headings.push((level, self_ref.clone()));
        }
        if self.document.name.is_empty() && label == pb::XmlItemLabel::Title {
            // `XmlInfo.title` is only set when the dialect exposes the title
            // before the parser has passed it, which none of the four
            // currently do; the title arrives as the first TITLE item
            // instead, and a Document with no name is worse than one named
            // after its own title item.
            self.document.name.clone_from(&item.text);
        }
        self_ref
    }

    // --------------------------------------------------------------- pictures

    /// Append one picture as a placeholder `PictureItem`.
    ///
    /// `image` is left unset: an XML picture is a filename or an
    /// `xlink:href`, never pixels, and this collector fetches nothing. A
    /// picture whose reference is all we have is still a picture, and the
    /// arena is where a downstream stage that can resolve it will look.
    ///
    /// `captions` stays empty. The text a PICTURE event carries is not prose:
    /// every dialect lifts it from an attribute (the JATS `xlink:href`, the
    /// USPTO drawing `file`, the `DocLang` `uri`), so it is a locator and it
    /// goes where the other locators go. A figure's real caption reaches the
    /// fold as its own CAPTION event and folds as its own item.
    fn push_picture(&mut self, item: &pb::TextItem) {
        let parent = self.current_parent();
        let self_ref = format!("#/pictures/{}", self.document.pictures.len());
        let source = self.source_of(item.source.as_ref());
        let mut fields = text_fields(item);
        if !item.text.is_empty() {
            fields.insert("xml.href".to_owned(), string(&item.text));
        }
        self.document.pictures.push(doc::PictureItem {
            self_ref: self_ref.clone(),
            parent: Some(reference(&parent)),
            content_layer: doc::ContentLayer::Body as i32,
            // The locators the item would have carried as a text item, carried
            // here instead: the path, the source's own role vocabulary, the
            // element id, and the reference the parser lifted from the markup.
            meta: Some(doc::PictureMeta {
                custom_fields: fields,
                ..doc::PictureMeta::default()
            }),
            label: doc::DocItemLabel::Picture as i32,
            // No image: no bytes, no uri, no size. An ImageRef here would
            // claim a payload this collector does not have.
            image: None,
            source: vec![source],
            ..doc::PictureItem::default()
        });
        self.claim_anchor(item.element_id.as_ref(), &self_ref);
        self.link_child(&parent, &self_ref);
    }

    // ----------------------------------------------------------------- tables

    fn on_table_start(&mut self, start: &pb::TableStart) {
        // An unclosed predecessor cannot happen on a conformant stream, but
        // dropping its rows silently if it did would be worse than keeping
        // them.
        if let Some(previous) = self.table.take() {
            self.append_table(previous);
        }
        let mut fields = HashMap::new();
        if !start.path.is_empty() {
            fields.insert("xml.path".to_owned(), string(&start.path));
        }
        if let Some(id) = start.element_id.as_ref().filter(|id| !id.is_empty()) {
            fields.insert("xml.element_id".to_owned(), string(id));
        }
        let mut captions = Vec::new();
        if let Some(caption) = start.caption.as_ref().filter(|c| !c.is_empty()) {
            // The caption is an item in its own right — the Document plane
            // references captions, it does not inline them — so it is created
            // first and the table points at it.
            let caption_ref = self.push_text(&pb::TextItem {
                label: pb::XmlItemLabel::Caption as i32,
                text: caption.clone(),
                // The caption's own path is not on the wire; the table's is
                // the closest true locator for it.
                path: start.path.clone(),
                source: start.source.clone(),
                ..pb::TextItem::default()
            });
            captions.push(reference(&caption_ref));
        }
        let source = self.source_of(start.source.as_ref());
        // The parent is the ladder as it stood when the table opened, not as
        // it stands when the last row lands.
        let parent = self.current_parent();
        self.table = Some(PendingTable {
            reference: start.table_ref.clone(),
            item: doc::TableItem {
                parent: Some(reference(&parent)),
                content_layer: doc::ContentLayer::Body as i32,
                meta: Some(doc::FloatingMeta {
                    custom_fields: fields,
                    ..doc::FloatingMeta::default()
                }),
                label: doc::DocItemLabel::Table as i32,
                captions,
                source: vec![source],
                ..doc::TableItem::default()
            },
            grid: Vec::new(),
            cells: Vec::new(),
            occupied: BTreeSet::new(),
            num_cols: 0,
            columns: Vec::new(),
        });
    }

    /// Lay one wire row into the grid, resolving column positions against the
    /// spans already in flight.
    fn on_table_row(&mut self, row: &pb::TableRow) {
        let Some(table) = self.table.as_mut() else {
            return;
        };
        if table.reference != row.table_ref {
            return;
        }
        let row_index = int_from_usize(table.grid.len());
        let mut column = 0i32;
        let mut cells = Vec::with_capacity(row.cells.len());
        for cell in &row.cells {
            while table.occupied.contains(&(row_index, column)) {
                column += 1;
            }
            let col_span = int(cell.column_span.max(1));
            let row_span = int(cell.row_span.max(1));
            let end_row = row_index.saturating_add(row_span);
            let end_col = column.saturating_add(col_span);
            for r in row_index..end_row {
                for c in column..end_col {
                    table.occupied.insert((r, c));
                }
            }
            cells.push(doc::TableCell {
                row_span,
                col_span,
                start_row_offset_idx: row_index,
                end_row_offset_idx: end_row,
                start_col_offset_idx: column,
                end_col_offset_idx: end_col,
                text: cell.text.clone(),
                // A cell is a header when its row is one or when the source
                // marked the cell itself (`th` outside a `thead`).
                column_header: row.is_header || cell.is_header,
                // A cell's markup is a fact about the cell, so it folds the
                // same way a paragraph's does. Cross-references inside one
                // resolve in the same end-of-stream pass.
                spans: inline_spans(&cell.spans),
                align: doc_alignment(cell.align),
                valign: doc_vertical_alignment(cell.valign),
                ..doc::TableCell::default()
            });
            table.num_cols = table.num_cols.max(end_col);
            column = end_col;
        }
        table.cells.extend(cells.iter().cloned());
        table.grid.push(doc::TableRow { cells });
    }

    fn on_table_end(&mut self, end: &pb::TableEnd) {
        let Some(table) = self.table.as_mut() else {
            return;
        };
        if table.reference != end.table_ref {
            return;
        }
        // The declared geometry rides on the closing event because a
        // `colspec` is a child of the table; the fold has the whole table in
        // hand by now, so it lands on the same `TableData` either way.
        table.columns = end.columns.iter().map(column_schema).collect();
        let pending = self.table.take().expect("checked just above");
        self.append_table(pending);
    }

    /// Give the finished table its arena slot and link it into the body.
    fn append_table(&mut self, pending: PendingTable) {
        let mut item = pending.item;
        item.self_ref = format!("#/tables/{}", self.document.tables.len());
        item.data = Some(doc::TableData {
            num_rows: int_from_usize(pending.grid.len()),
            num_cols: pending.num_cols,
            // Both shapes are populated: `grid` is the row structure the
            // renderers walk, `table_cells` the flat list the analyzers read.
            table_cells: pending.cells,
            grid: pending.grid,
            columns: pending.columns,
            ..doc::TableData::default()
        });
        let self_ref = item.self_ref.clone();
        let element_id = item
            .meta
            .as_ref()
            .and_then(|meta| meta.custom_fields.get("xml.element_id"))
            .and_then(|value| match value.kind.as_ref() {
                Some(Kind::StringValue(id)) => Some(id.clone()),
                _ => None,
            });
        let parent = item
            .parent
            .as_ref()
            .map_or_else(|| BODY_REF.to_owned(), |parent| parent.r#ref.clone());
        self.document.tables.push(item);
        self.claim_anchor(element_id.as_ref(), &self_ref);
        self.link_child(&parent, &self_ref);
    }

    // ------------------------------------------------------------------ facts

    /// Fold one XBRL fact into a row of the single fact table.
    ///
    /// An instance is a flat list of facts, not a document tree, so the
    /// Document plane gets one table rather than thousands of paragraphs. The
    /// table is created when the first fact arrives, so an instance with no
    /// facts produces no empty table. Its row count is bounded only by the
    /// input: a large instance makes a large table, and the byte cap on the
    /// request is what bounds both.
    fn on_fact(&mut self, fact: &pb::Fact) {
        let index = if let Some(index) = self.facts_table {
            index
        } else {
            self.open_facts_table(fact)
        };
        // A dimensioned fact and an undimensioned one used to be
        // indistinguishable in the projection. The axes become a key-value
        // graph of their own and the context cell points at it.
        let dimensions = self.push_dimensions(fact);
        let entity = fact.context.as_ref();
        let values: [(String, Option<doc::CellValue>); 11] = [
            (concept_name(fact), None),
            (
                entity
                    .and_then(|c| c.entity_scheme.clone())
                    .unwrap_or_default(),
                None,
            ),
            (
                entity
                    .and_then(|c| c.entity_identifier.clone())
                    .unwrap_or_default(),
                None,
            ),
            (fact.context_ref.clone(), None),
            (period_text(entity), None),
            (unit_text(fact), None),
            (fact.value.clone(), numeric_value(fact)),
            (
                fact.decimals.clone().unwrap_or_default(),
                accuracy_value(fact.decimals.as_deref()),
            ),
            (
                fact.precision.clone().unwrap_or_default(),
                accuracy_value(fact.precision.as_deref()),
            ),
            (
                fact.sign.clone().unwrap_or_default(),
                Some(boolean(fact.sign.as_deref() == Some("-"))),
            ),
            (fact.is_nil.to_string(), Some(boolean(fact.is_nil))),
        ];
        let Some(data) = self
            .document
            .tables
            .get_mut(index)
            .and_then(|table| table.data.as_mut())
        else {
            return;
        };
        let row_index = int_from_usize(data.grid.len());
        let cells: Vec<doc::TableCell> = values
            .into_iter()
            .enumerate()
            .map(|(column, (text, value))| {
                let is_context = FACT_COLUMNS[column].0 == "context";
                let column = int_from_usize(column);
                doc::TableCell {
                    row_span: 1,
                    col_span: 1,
                    start_row_offset_idx: row_index,
                    end_row_offset_idx: row_index + 1,
                    start_col_offset_idx: column,
                    end_col_offset_idx: column + 1,
                    text,
                    value,
                    r#ref: is_context
                        .then(|| dimensions.as_deref().map(reference))
                        .flatten(),
                    ..doc::TableCell::default()
                }
            })
            .collect();
        data.table_cells.extend(cells.iter().cloned());
        data.grid.push(doc::TableRow { cells });
        data.num_rows = int_from_usize(data.grid.len());
    }

    /// Fold a fact's segment and scenario axes into a key-value graph.
    ///
    /// `GraphData` models exactly this and had no writer here. Each axis is
    /// a KEY cell linked to the VALUE cell holding its member, so a reader
    /// gets the dimension pairs rather than a rendering of them. Returns the
    /// item's self ref, for the fact row's context cell to point at.
    fn push_dimensions(&mut self, fact: &pb::Fact) -> Option<String> {
        let dimensions = fact.context.as_ref().map(|c| c.dimensions.as_slice())?;
        if dimensions.is_empty() {
            return None;
        }
        let self_ref = format!("#/key_value_items/{}", self.document.key_value_items.len());
        let parent = self.current_parent();
        let mut cells = Vec::with_capacity(dimensions.len() * 2);
        let mut links = Vec::with_capacity(dimensions.len());
        for (n, dimension) in dimensions.iter().enumerate() {
            let key_id = int_from_usize(n * 2);
            let value_id = key_id + 1;
            let member = dimension
                .member
                .clone()
                .or_else(|| dimension.typed_value.clone())
                .unwrap_or_default();
            cells.push(graph_cell(
                doc::GraphCellLabel::Key,
                key_id,
                &dimension.dimension,
            ));
            cells.push(graph_cell(doc::GraphCellLabel::Value, value_id, &member));
            links.push(doc::GraphLink {
                label: doc::GraphLinkLabel::ToValue as i32,
                source_cell_id: key_id,
                target_cell_id: value_id,
            });
        }
        self.document.key_value_items.push(doc::KeyValueItem {
            self_ref: self_ref.clone(),
            parent: Some(reference(&parent)),
            content_layer: doc::ContentLayer::Body as i32,
            label: doc::DocItemLabel::KeyValueRegion as i32,
            graph: Some(doc::GraphData { cells, links }),
            source: vec![self.source_of(fact.source.as_ref())],
            ..doc::KeyValueItem::default()
        });
        self.link_child(&parent, &self_ref);
        Some(self_ref)
    }

    /// Fold one XBRL footnote as a `FOOTNOTE` item.
    ///
    /// A footnote is the narrative a filer attached to a number, so it is
    /// content and folds like any other footnote. A label is a name for a
    /// concept in a schema this service never reads, so it has nothing in
    /// the arena to attach to and stays on the typed wire.
    fn on_xbrl_note(&mut self, note: &pb::XbrlNote) {
        if pb::XbrlNoteKind::try_from(note.kind) != Ok(pb::XbrlNoteKind::Footnote)
            || note.text.is_empty()
        {
            return;
        }
        self.push_text(&pb::TextItem {
            label: pb::XmlItemLabel::Footnote as i32,
            text: note.text.clone(),
            path: note.path.clone(),
            element_id: (!note.label.is_empty()).then(|| note.label.clone()),
            source: note.source.clone(),
            ..pb::TextItem::default()
        });
    }

    /// Create the fact table with its header row and return its arena index.
    fn open_facts_table(&mut self, fact: &pb::Fact) -> usize {
        let index = self.document.tables.len();
        let self_ref = format!("#/tables/{index}");
        let parent = self.current_parent();
        let mut fields = HashMap::new();
        fields.insert("xml.table".to_owned(), string("facts"));
        let header = doc::TableRow {
            cells: FACT_COLUMNS
                .iter()
                .enumerate()
                .map(|(column, (name, _))| header_cell(int_from_usize(column), name))
                .collect(),
        };
        self.document.tables.push(doc::TableItem {
            self_ref: self_ref.clone(),
            parent: Some(reference(&parent)),
            content_layer: doc::ContentLayer::Body as i32,
            meta: Some(doc::FloatingMeta {
                custom_fields: fields,
                ..doc::FloatingMeta::default()
            }),
            label: doc::DocItemLabel::Table as i32,
            data: Some(doc::TableData {
                table_cells: header.cells.clone(),
                num_rows: 1,
                num_cols: int_from_usize(FACT_COLUMNS.len()),
                grid: vec![header],
                // The columns declare what each one holds, so a reader does
                // not have to infer a fact's shape from its display text.
                columns: FACT_COLUMNS
                    .iter()
                    .map(|(name, declared)| doc::TableColumnSchema {
                        // Presence-tracked upstream; every fact column has a
                        // heading, so every one declares its name.
                        name: Some((*name).to_owned()),
                        declared_type: Some((*declared).to_owned()),
                        ..doc::TableColumnSchema::default()
                    })
                    .collect(),
                ..doc::TableData::default()
            }),
            source: vec![self.source_of(fact.source.as_ref())],
            ..doc::TableItem::default()
        });
        self.link_child(&parent, &self_ref);
        self.facts_table = Some(index);
        index
    }

    // ------------------------------------------------------------- primitives

    /// The ref new content parents to: the innermost open section header, or
    /// the body when no header has opened yet. Content before the first
    /// heading sits on the body, as it does upstream.
    fn current_parent(&self) -> String {
        self.headings
            .last()
            .map_or_else(|| BODY_REF.to_owned(), |(_, self_ref)| self_ref.clone())
    }

    /// Close every open header a level-`level` header ends, so that the next
    /// heading is nested under the nearest header of a lower level. A header
    /// of the same level closes the one before it: siblings, not parent and
    /// child.
    fn close_headings(&mut self, level: u32) {
        while self.headings.last().is_some_and(|(open, _)| *open >= level) {
            self.headings.pop();
        }
    }

    /// Both halves of the parent link: the item names its parent, and the
    /// parent lists the item. An integrity check fails on either one alone.
    ///
    /// The only parents this fold makes are the body and section headers, so
    /// a ref that is neither is a bug in the caller rather than something to
    /// resolve generically.
    fn link_child(&mut self, parent: &str, child: &str) {
        if parent == BODY_REF {
            if let Some(body) = self.document.body.as_mut() {
                body.children.push(reference(child));
            }
        } else if let Some(base) = self.heading_base(parent) {
            base.children.push(reference(child));
        }
    }

    /// The base of the section header at a `#/texts/N` ref.
    fn heading_base(&mut self, self_ref: &str) -> Option<&mut doc::TextItemBase> {
        let index: usize = self_ref.strip_prefix("#/texts/")?.parse().ok()?;
        match self.document.texts.get_mut(index)?.item.as_mut()? {
            doc::base_text_item::Item::SectionHeader(header) => header.base.as_mut(),
            _ => None,
        }
    }

    /// The item's own attribution, converted field for field, or this
    /// service's own when the event carried none.
    fn source_of(&self, wire: Option<&pb::CollectorSource>) -> doc::SourceType {
        let collector = wire.map_or_else(
            || self.fallback_source.clone(),
            |source| doc::CollectorSource {
                collector: source.collector.clone(),
                model: source.model.clone(),
                version: source.version.clone(),
                // Unset upstream and unset here: a declarative mapping is
                // deterministic, so a confidence would be noise. The one
                // exception is hOCR, which reports its own, and it reports a
                // calibrated confidence rather than a raw engine score.
                confidence: source.confidence,
                ..doc::CollectorSource::default()
            },
        );
        doc::SourceType {
            source: Some(doc::source_type::Source::Collector(collector)),
        }
    }
}

/// Where the item sits in the source, when the source says.
///
/// hOCR states the box in image pixels with the origin at the top left,
/// which is what `COORD_ORIGIN_TOPLEFT` means; the unit is on the page.
/// The character span covers the whole item, because a line's box bounds
/// the line rather than any part of it.
fn provenance(item: &pb::TextItem) -> Vec<doc::ProvenanceItem> {
    let (Some(bbox), Some(page_no)) = (item.bbox.as_ref(), item.page_no) else {
        return Vec::new();
    };
    vec![doc::ProvenanceItem {
        page_no: int(page_no),
        bbox: Some(doc::BoundingBox {
            l: bbox.left,
            t: bbox.top,
            r: bbox.right,
            b: bbox.bottom,
            coord_origin: Some(doc::CoordOrigin::Topleft as i32),
            coord_origin_raw: None,
        }),
        charspan: Some(doc::IntSpan {
            start: 0,
            end: int_from_usize(item.text.chars().count()),
        }),
        ..doc::ProvenanceItem::default()
    }]
}

/// The local part of an attribute name, whatever prefix the document bound.
fn local_name(name: &str) -> &str {
    name.rsplit(':').next().unwrap_or(name)
}

/// A date as the wall-clock value the source actually wrote.
///
/// `CivilDateTime` states a date without inventing a time zone, which is
/// what a publication date is. A part the source did not state stays at the
/// message's own default: a `pub-date` naming a year and a month leaves
/// `day` at zero rather than claiming the first, and zero is not a day of
/// any month.
fn civil_of(date: &pb::MetaDate) -> Option<doc::CivilDateTime> {
    let year = date.year?;
    Some(doc::CivilDateTime {
        year: int(year),
        month: date.month.map_or(0, int),
        day: date.day.map_or(0, int),
        ..doc::CivilDateTime::default()
    })
}

/// A date as an instant, and only when the source stated a whole calendar
/// date.
///
/// A `pub-date` that names a year and a month is not an instant, and
/// resolving it to the first of the month would invent two fields. Even a
/// whole date has no time zone in the source; midnight UTC is the
/// conventional reading and the `_raw` twin keeps what the source actually
/// wrote, which is why both are set together.
fn timestamp_of(date: &pb::MetaDate) -> Option<prost_types::Timestamp> {
    let (year, month, day) = (date.year?, date.month?, date.day?);
    let days = days_from_civil(i64::from(year), i64::from(month), i64::from(day))?;
    Some(prost_types::Timestamp {
        seconds: days * 86_400,
        nanos: 0,
    })
}

/// Days from the Unix epoch to a proleptic Gregorian calendar date.
///
/// Howard Hinnant's `days_from_civil`, shifted to the epoch. It is here
/// rather than from a date crate because this is the only date arithmetic
/// in the service and a dependency for one function is worse than ten lines
/// with a test.
fn days_from_civil(year: i64, month: i64, day: i64) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    Some(era * 146_097 + day_of_era - 719_468)
}

/// A date as far as the source stated it, in ISO 8601: the whole date when
/// it has one, otherwise the year and month or the year alone. `None` when
/// the source stated no part of it.
fn iso_date(date: &pb::MetaDate) -> Option<String> {
    if let Some(iso) = date.iso_date.as_ref().filter(|iso| !iso.is_empty()) {
        return Some(iso.clone());
    }
    let year = date.year?;
    Some(match (date.month, date.day) {
        (Some(month), Some(day)) => format!("{year:04}-{month:02}-{day:02}"),
        (Some(month), None) => format!("{year:04}-{month:02}"),
        _ => format!("{year:04}"),
    })
}

/// The `TextItemBase` of an arena item, for the variants that carry one.
///
/// `CodeItem` inlines its base fields instead of wrapping them, and has no
/// `spans` slot at all: a code block is verbatim, so it has no inline runs.
fn base_of(item: &mut doc::BaseTextItem) -> Option<&mut doc::TextItemBase> {
    match item.item.as_mut()? {
        doc::base_text_item::Item::Title(item) => item.base.as_mut(),
        doc::base_text_item::Item::SectionHeader(item) => item.base.as_mut(),
        doc::base_text_item::Item::ListItem(item) => item.base.as_mut(),
        doc::base_text_item::Item::Formula(item) => item.base.as_mut(),
        doc::base_text_item::Item::Text(item) => item.base.as_mut(),
        doc::base_text_item::Item::FieldHeading(item) => item.base.as_mut(),
        doc::base_text_item::Item::FieldValue(item) => item.base.as_mut(),
        doc::base_text_item::Item::Code(_) => None,
    }
}

/// The Document-plane runs for one wire item's inline spans.
///
/// A reference that names several targets becomes one span per target over
/// the same range, because `InlineSpan.target` holds one `FineRef`: the
/// alternative would be joining identifiers into a string, which is exactly
/// the shape the reference graph is supposed to escape. Targets are written
/// as the source name (`#B12`) and resolved onto item refs when the stream
/// ends; see [`DocumentFold::publish_anchors`].
fn inline_spans(spans: &[pb::InlineSpan]) -> Vec<doc::InlineSpan> {
    let mut folded = Vec::new();
    for span in spans {
        let Some(range) = span.range.as_ref() else {
            continue;
        };
        let charspan = doc::IntSpan {
            start: int(range.start),
            end: int(range.end),
        };
        let formatting = formatting_of(&span.styles);
        let hyperlink = span.hyperlink.clone().filter(|link| !link.is_empty());
        if span.references.is_empty() {
            if formatting.is_none() && hyperlink.is_none() {
                // A run that says nothing the flat text does not already say.
                continue;
            }
            folded.push(doc::InlineSpan {
                range: Some(charspan),
                formatting,
                hyperlink,
                ..doc::InlineSpan::default()
            });
            continue;
        }
        let reference_kind = doc_reference_kind(span.reference_kind);
        for name in &span.references {
            folded.push(doc::InlineSpan {
                range: Some(charspan),
                formatting: formatting.clone(),
                hyperlink: hyperlink.clone(),
                // The key the source wrote, alongside the ref that resolution
                // may or may not rewrite. A reference no anchor answers keeps
                // its key here, so a reader can tell an unresolved target
                // from a resolved one without parsing the ref back apart.
                reference: Some(name.clone()),
                target: Some(fine(&format!("#{name}"))),
                reference_kind,
                ..doc::InlineSpan::default()
            });
        }
    }
    folded
}

/// Point every run in one list at the item that answers to its key.
///
/// A run whose key no item declared keeps the `#<id>` the source wrote,
/// which is what makes an unresolved reference legible rather than silently
/// dangling.
fn resolve_targets(anchors: &BTreeMap<String, String>, spans: &mut [doc::InlineSpan]) {
    for span in spans {
        let Some(target) = span.target.as_mut() else {
            continue;
        };
        if let Some(name) = target.r#ref.strip_prefix('#')
            && let Some(self_ref) = anchors.get(name)
        {
            target.r#ref.clone_from(self_ref);
        }
    }
}

/// One declared column as the Document plane's own column schema.
///
/// The width is whatever the source wrote, and `width_raw` is where it goes:
/// a CALS `2*` is a share of the table and a `30%` a share of something else,
/// so neither is the page-unit `width` the schema's other field means. A
/// column the source did not name declares no name rather than an empty one,
/// which is what the presence-tracked field is for.
fn column_schema(column: &pb::ColumnSpec) -> doc::TableColumnSchema {
    let width = column.width.trim();
    doc::TableColumnSchema {
        name: (!column.name.is_empty()).then(|| column.name.clone()),
        width_raw: (!width.is_empty()).then(|| width.to_owned()),
        align: doc_alignment(column.align),
        valign: doc_vertical_alignment(column.valign),
        ..doc::TableColumnSchema::default()
    }
}

/// The Document plane's horizontal alignment for the one the source stated.
///
/// `ALIGNMENT_CHAR` has no member there: a source that aligns on a nominated
/// character said something this plane cannot express, and leaving the slot
/// unset says "not stated here" rather than claiming a different alignment.
/// The wire keeps the full answer.
fn doc_alignment(align: i32) -> Option<i32> {
    let mapped = match pb::Alignment::try_from(align).ok()? {
        pb::Alignment::Left => doc::Alignment::Left,
        pb::Alignment::Center => doc::Alignment::Center,
        pb::Alignment::Right => doc::Alignment::Right,
        pb::Alignment::Justify => doc::Alignment::Justify,
        pb::Alignment::Unspecified | pb::Alignment::Char => return None,
    };
    Some(mapped as i32)
}

/// The Document plane's vertical alignment for the one the source stated.
fn doc_vertical_alignment(valign: i32) -> Option<i32> {
    let mapped = match pb::VerticalAlignment::try_from(valign).ok()? {
        pb::VerticalAlignment::Top => doc::VerticalAlignment::Top,
        pb::VerticalAlignment::Middle => doc::VerticalAlignment::Middle,
        pb::VerticalAlignment::Bottom => doc::VerticalAlignment::Bottom,
        pb::VerticalAlignment::Unspecified => return None,
    };
    Some(mapped as i32)
}

/// The Document plane's `Formatting` for a run's styles, or `None` when the
/// source said nothing this schema can express.
///
/// Every style this collector recognizes has a field: weight, slant,
/// decoration and vertical position from the upstream set, and monospace,
/// small capitals and mathematical notation from the extension the schema
/// grew for exactly this.
fn formatting_of(styles: &[i32]) -> Option<doc::Formatting> {
    let mut formatting = doc::Formatting::default();
    let mut expressed = false;
    for style in styles
        .iter()
        .filter_map(|s| pb::SpanStyle::try_from(*s).ok())
    {
        match style {
            pb::SpanStyle::Bold => formatting.bold = true,
            pb::SpanStyle::Italic => formatting.italic = true,
            pb::SpanStyle::Underline => formatting.underline = true,
            pb::SpanStyle::Strikethrough => formatting.strikethrough = true,
            pb::SpanStyle::Superscript => formatting.script = doc::Script::Super as i32,
            pb::SpanStyle::Subscript => formatting.script = doc::Script::Sub as i32,
            pb::SpanStyle::Monospace => formatting.monospace = true,
            pb::SpanStyle::SmallCaps => formatting.small_caps = true,
            pb::SpanStyle::Math => formatting.math = true,
            pb::SpanStyle::Unspecified => continue,
        }
        expressed = true;
    }
    expressed.then_some(formatting)
}

/// The Document plane's `ReferenceKind` for the kind the source stated.
///
/// The vocabularies match now, member for member. `UNSPECIFIED` stays
/// `None` rather than folding to the zero member: a source that wrote no
/// `ref-type` did not distinguish, and saying so is not the same as saying
/// nothing.
fn doc_reference_kind(kind: i32) -> Option<i32> {
    let mapped = match pb::ReferenceKind::try_from(kind).ok()? {
        pb::ReferenceKind::Citation => doc::ReferenceKind::Citation,
        pb::ReferenceKind::Footnote => doc::ReferenceKind::Footnote,
        pb::ReferenceKind::Claim => doc::ReferenceKind::Claim,
        pb::ReferenceKind::Section => doc::ReferenceKind::Section,
        pb::ReferenceKind::CrossRef => doc::ReferenceKind::CrossRef,
        pb::ReferenceKind::Figure => doc::ReferenceKind::Figure,
        pb::ReferenceKind::Table => doc::ReferenceKind::Table,
        pb::ReferenceKind::Equation => doc::ReferenceKind::Equation,
        pb::ReferenceKind::Unspecified => return None,
    };
    Some(mapped as i32)
}

/// Per-item `meta.custom_fields`: everything the wire item says that the
/// Document plane has no typed slot for.
fn text_fields(item: &pb::TextItem) -> HashMap<String, Value> {
    let mut fields = HashMap::new();
    if !item.path.is_empty() {
        fields.insert("xml.path".to_owned(), string(&item.path));
    }
    if !item.role.is_empty() {
        fields.insert("xml.role".to_owned(), string(&item.role));
    }
    if let Some(id) = item.element_id.as_ref().filter(|id| !id.is_empty()) {
        fields.insert("xml.element_id".to_owned(), string(id));
    }
    if let Some(ordinal) = item.ordinal {
        fields.insert("xml.ordinal".to_owned(), number(ordinal));
    }
    fields
}

/// The Document label for a wire label.
///
/// The two vocabularies were written to match, so this is a rename and not an
/// interpretation. `XML_ITEM_LABEL_UNSPECIFIED` means "free text", which is
/// what `DOC_ITEM_LABEL_TEXT` means.
const fn doc_label(label: pb::XmlItemLabel) -> doc::DocItemLabel {
    match label {
        pb::XmlItemLabel::Unspecified | pb::XmlItemLabel::Text => doc::DocItemLabel::Text,
        pb::XmlItemLabel::Title => doc::DocItemLabel::Title,
        pb::XmlItemLabel::SectionHeader => doc::DocItemLabel::SectionHeader,
        pb::XmlItemLabel::Paragraph => doc::DocItemLabel::Paragraph,
        pb::XmlItemLabel::ListItem => doc::DocItemLabel::ListItem,
        pb::XmlItemLabel::Caption => doc::DocItemLabel::Caption,
        pb::XmlItemLabel::Reference => doc::DocItemLabel::Reference,
        pb::XmlItemLabel::Footnote => doc::DocItemLabel::Footnote,
        pb::XmlItemLabel::Code => doc::DocItemLabel::Code,
        pb::XmlItemLabel::Formula => doc::DocItemLabel::Formula,
        // A PICTURE event does not reach this mapping: it folds into a
        // placeholder `PictureItem` in the picture arena rather than into a
        // text item. The arm is here so the label vocabulary stays covered.
        pb::XmlItemLabel::Picture => doc::DocItemLabel::Picture,
    }
}

/// The dialect short name, matching `CollectorSource.model` on the wire.
const fn model_name(dialect: pb::XmlDialect) -> &'static str {
    match dialect {
        pb::XmlDialect::Unspecified => "",
        pb::XmlDialect::Jats => "jats",
        pb::XmlDialect::Uspto => "uspto",
        pb::XmlDialect::Xbrl => "xbrl",
        pb::XmlDialect::Doclang => "doclang",
        pb::XmlDialect::Dclx => "dclx",
        pb::XmlDialect::MetsGbs => "mets-gbs",
    }
}

/// The origin mimetype, which describes the payload the caller uploaded: the
/// archive for an archive dialect, XML for everything else.
const fn mimetype(dialect: pb::XmlDialect) -> &'static str {
    match dialect {
        pb::XmlDialect::Dclx => "application/zip",
        pb::XmlDialect::MetsGbs => "application/mets+xml",
        _ => MIMETYPE,
    }
}

/// `prefix:localName` as the instance wrote it, or the local name alone.
fn concept_name(fact: &pb::Fact) -> String {
    if fact.concept_prefix.is_empty() {
        fact.concept_local_name.clone()
    } else {
        format!("{}:{}", fact.concept_prefix, fact.concept_local_name)
    }
}

/// The reporting period as one deterministic string: an instant date, an
/// ISO 8601 `start/end` interval, `forever`, or empty when the context was
/// not resolved.
fn period_text(context: Option<&pb::XbrlContext>) -> String {
    let Some(period) = context.and_then(|c| c.period.as_ref()) else {
        return String::new();
    };
    if let Some(instant) = period.instant.as_ref().filter(|i| !i.is_empty()) {
        return instant.clone();
    }
    match (period.start_date.as_deref(), period.end_date.as_deref()) {
        (Some(start), Some(end)) => format!("{start}/{end}"),
        (Some(start), None) => start.to_owned(),
        (None, Some(end)) => end.to_owned(),
        (None, None) => {
            if period.forever {
                "forever".to_owned()
            } else {
                String::new()
            }
        }
    }
}

/// The unit as measures when it was resolved, otherwise the reference the
/// fact named. A divide unit is written `numerator/denominator`.
fn unit_text(fact: &pb::Fact) -> String {
    let Some(unit) = fact.unit.as_ref() else {
        return fact.unit_ref.clone().unwrap_or_default();
    };
    if !unit.measures.is_empty() {
        return unit.measures.join(" ");
    }
    if !unit.numerator_measures.is_empty() || !unit.denominator_measures.is_empty() {
        return format!(
            "{}/{}",
            unit.numerator_measures.join(" "),
            unit.denominator_measures.join(" ")
        );
    }
    unit.id.clone()
}

/// One cell of a key-value graph.
fn graph_cell(label: doc::GraphCellLabel, cell_id: i32, text: &str) -> doc::GraphCell {
    doc::GraphCell {
        label: label as i32,
        cell_id,
        text: text.to_owned(),
        orig: text.to_owned(),
        prov: None,
        item_ref: None,
    }
}

/// A cell value holding a boolean.
fn boolean(value: bool) -> doc::CellValue {
    doc::CellValue {
        kind: Some(doc::cell_value::Kind::Boolean(value)),
        number_format: None,
    }
}

/// A fact's value as a number, with the source's sign applied.
///
/// A nil fact has no value to type, and a value the source did not write as
/// a number (a string-valued concept) has none either: both leave the cell's
/// display text as the only thing said about it.
fn numeric_value(fact: &pb::Fact) -> Option<doc::CellValue> {
    if fact.is_nil {
        return None;
    }
    let magnitude: f64 = fact.value.trim().parse().ok()?;
    let number = if fact.sign.as_deref() == Some("-") {
        -magnitude.abs()
    } else {
        magnitude
    };
    Some(doc::CellValue {
        kind: Some(doc::cell_value::Kind::Number(number)),
        number_format: None,
    })
}

/// A `decimals` or `precision` attribute as a number.
///
/// XBRL writes unbounded accuracy as `INF`, which is the floating-point
/// infinity and not a special case this fold has to invent a marker for.
fn accuracy_value(raw: Option<&str>) -> Option<doc::CellValue> {
    let raw = raw?.trim();
    let number = if raw.eq_ignore_ascii_case("INF") {
        f64::INFINITY
    } else {
        raw.parse().ok()?
    };
    Some(doc::CellValue {
        kind: Some(doc::cell_value::Kind::Number(number)),
        number_format: None,
    })
}

/// One header cell of the fact table.
fn header_cell(column: i32, text: &str) -> doc::TableCell {
    doc::TableCell {
        row_span: 1,
        col_span: 1,
        start_row_offset_idx: 0,
        end_row_offset_idx: 1,
        start_col_offset_idx: column,
        end_col_offset_idx: column + 1,
        text: text.to_owned(),
        column_header: true,
        ..doc::TableCell::default()
    }
}

/// A root group with nothing in it yet.
fn group(self_ref: &str, layer: doc::ContentLayer) -> doc::GroupItem {
    doc::GroupItem {
        self_ref: self_ref.to_owned(),
        content_layer: layer as i32,
        ..doc::GroupItem::default()
    }
}

/// A JSON-Pointer reference to another item, refined to a sub-range only
/// when the caller has one; none of these do.
fn fine(target: &str) -> doc::FineRef {
    doc::FineRef {
        r#ref: target.to_owned(),
        range: None,
    }
}

/// A JSON-Pointer reference to another item.
fn reference(target: &str) -> doc::RefItem {
    doc::RefItem {
        r#ref: target.to_owned(),
    }
}

/// A `google.protobuf.Value` holding a string.
fn string(text: &str) -> Value {
    Value {
        kind: Some(Kind::StringValue(text.to_owned())),
    }
}

/// A `google.protobuf.Value` holding a number.
fn number(value: u64) -> Value {
    // JSON numbers are doubles, so this is the schema's own precision limit,
    // not one this fold introduces. The counts and ordinals that reach here
    // (claim numbers, list positions, island counts) are many orders of
    // magnitude below 2^53.
    #[allow(clippy::cast_precision_loss)]
    let number = value as f64;
    Value {
        kind: Some(Kind::NumberValue(number)),
    }
}

/// A wire `uint32` as the schema's `int32`, saturating rather than wrapping.
fn int(value: u32) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

/// A count as the schema's `int32`, saturating rather than wrapping.
fn int_from_usize(value: usize) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(label: pb::XmlItemLabel, body: &str) -> pb::ParseXmlResponse {
        pb::ParseXmlResponse {
            event: Some(pb::parse_xml_response::Event::TextItem(pb::TextItem {
                label: label as i32,
                text: body.to_owned(),
                path: "/doc/p".to_owned(),
                ..pb::TextItem::default()
            })),
        }
    }

    #[test]
    fn an_empty_fold_is_a_sound_empty_fragment() {
        let mut fold = DocumentFold::new();
        let document = fold.take();
        assert_eq!(document.schema_name.as_deref(), Some(SCHEMA_NAME));
        assert_eq!(
            document.origin.as_ref().map(|o| o.mimetype.as_str()),
            Some(MIMETYPE)
        );
        assert!(integrity_errors(&document).is_empty());
    }

    #[test]
    fn the_checker_catches_a_child_nobody_created() {
        let mut fold = DocumentFold::new();
        fold.consume(&text(pb::XmlItemLabel::Paragraph, "one"));
        let mut document = fold.take();
        document
            .body
            .as_mut()
            .expect("body")
            .children
            .push(reference("#/texts/7"));
        let errors = integrity_errors(&document);
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("#/texts/7"), "{errors:?}");
    }

    #[test]
    fn the_checker_catches_a_parent_that_disowns_its_child() {
        let mut fold = DocumentFold::new();
        fold.consume(&text(pb::XmlItemLabel::Paragraph, "one"));
        let mut document = fold.take();
        document.body.as_mut().expect("body").children.clear();
        let errors = integrity_errors(&document);
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("does not list"), "{errors:?}");
    }

    #[test]
    fn the_checker_catches_a_ref_that_lies_about_its_position() {
        let mut fold = DocumentFold::new();
        fold.consume(&text(pb::XmlItemLabel::Paragraph, "one"));
        let mut document = fold.take();
        let Some(doc::base_text_item::Item::Text(item)) = document.texts[0].item.as_mut() else {
            panic!("a paragraph folds to a TextItem");
        };
        item.base.as_mut().expect("base").self_ref = "#/texts/4".to_owned();
        let errors = integrity_errors(&document);
        assert!(
            errors.iter().any(|e| e.contains("arena position")),
            "{errors:?}"
        );
    }

    /// A heading and one paragraph under it, which is the shallowest fragment
    /// with a parent link that is not the body's.
    fn nested() -> doc::Document {
        let mut fold = DocumentFold::new();
        fold.consume(&pb::ParseXmlResponse {
            event: Some(pb::parse_xml_response::Event::TextItem(pb::TextItem {
                label: pb::XmlItemLabel::SectionHeader as i32,
                level: Some(1),
                text: "Introduction".to_owned(),
                path: "/doc/sec/title".to_owned(),
                ..pb::TextItem::default()
            })),
        });
        fold.consume(&text(pb::XmlItemLabel::Paragraph, "under the heading"));
        let document = fold.take();
        assert!(integrity_errors(&document).is_empty());
        document
    }

    /// The base of a text item that has one, for the tests that damage a
    /// fragment deliberately.
    fn base_at(document: &mut doc::Document, index: usize) -> &mut doc::TextItemBase {
        match document.texts[index].item.as_mut().expect("a variant") {
            doc::base_text_item::Item::SectionHeader(header) => header.base.as_mut(),
            doc::base_text_item::Item::Text(item) => item.base.as_mut(),
            _ => panic!("this fragment holds a header and a paragraph"),
        }
        .expect("the base is set")
    }

    #[test]
    fn the_checker_catches_a_heading_that_disowns_its_child() {
        // The same break as on the body, one level down: the merge needs both
        // halves of every link, not only the ones the body holds.
        let mut document = nested();
        base_at(&mut document, 0).children.clear();
        let errors = integrity_errors(&document);
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("#/texts/0 does not list"), "{errors:?}");
    }

    #[test]
    fn the_checker_catches_a_nested_parent_that_does_not_exist() {
        let mut document = nested();
        base_at(&mut document, 1).parent = Some(reference("#/texts/9"));
        let errors = integrity_errors(&document);
        assert!(
            errors.iter().any(|e| e.contains("parent #/texts/9")),
            "{errors:?}"
        );
    }

    #[test]
    fn a_heading_parents_the_content_that_follows_it() {
        let mut document = nested();
        let header = base_at(&mut document, 0).clone();
        let paragraph = base_at(&mut document, 1).clone();
        assert_eq!(header.parent.map(|p| p.r#ref).as_deref(), Some(BODY_REF));
        assert_eq!(
            paragraph.parent.map(|p| p.r#ref).as_deref(),
            Some("#/texts/0")
        );
        assert_eq!(
            header
                .children
                .iter()
                .map(|c| c.r#ref.as_str())
                .collect::<Vec<_>>(),
            vec!["#/texts/1"]
        );
        assert_eq!(
            document
                .body
                .as_ref()
                .expect("body")
                .children
                .iter()
                .map(|c| c.r#ref.as_str())
                .collect::<Vec<_>>(),
            vec!["#/texts/0"],
            "only the heading is a child of the body"
        );
    }

    #[test]
    fn taking_twice_does_not_repeat_the_first_fragment() {
        let mut fold = DocumentFold::new();
        fold.consume(&text(pb::XmlItemLabel::Paragraph, "one"));
        assert_eq!(fold.take().texts.len(), 1);
        assert_eq!(fold.take().texts.len(), 0);
    }

    #[test]
    fn every_stated_reference_kind_maps_and_an_unstated_one_stays_unset() {
        for (wire, expected) in [
            (pb::ReferenceKind::Citation, doc::ReferenceKind::Citation),
            (pb::ReferenceKind::Footnote, doc::ReferenceKind::Footnote),
            (pb::ReferenceKind::Claim, doc::ReferenceKind::Claim),
            (pb::ReferenceKind::Section, doc::ReferenceKind::Section),
            (pb::ReferenceKind::CrossRef, doc::ReferenceKind::CrossRef),
            (pb::ReferenceKind::Figure, doc::ReferenceKind::Figure),
            (pb::ReferenceKind::Table, doc::ReferenceKind::Table),
            (pb::ReferenceKind::Equation, doc::ReferenceKind::Equation),
        ] {
            assert_eq!(
                doc_reference_kind(wire as i32),
                Some(expected as i32),
                "{wire:?} has a member of its own"
            );
        }
        // A source that stated no kind did not distinguish, and folding
        // that to the zero member would say it did.
        assert_eq!(
            doc_reference_kind(pb::ReferenceKind::Unspecified as i32),
            None
        );
        // A kind from a future wire this build does not know is not guessed
        // at either.
        assert_eq!(doc_reference_kind(9999), None);
    }

    #[test]
    fn civil_dates_convert_at_the_epoch_and_across_leap_rules() {
        // The epoch itself, a leap day, a century that is not a leap year
        // and one that is, and the day before the epoch.
        assert_eq!(days_from_civil(1970, 1, 1), Some(0));
        assert_eq!(days_from_civil(1969, 12, 31), Some(-1));
        assert_eq!(days_from_civil(2000, 3, 1), Some(11_017));
        assert_eq!(days_from_civil(2026, 3, 4), Some(20_516));
        // 1900 is not a leap year and 2000 is, which is the rule a naive
        // conversion gets wrong.
        assert_eq!(
            days_from_civil(1900, 3, 1).unwrap() - days_from_civil(1900, 2, 28).unwrap(),
            1
        );
        assert_eq!(
            days_from_civil(2000, 3, 1).unwrap() - days_from_civil(2000, 2, 28).unwrap(),
            2
        );
        assert_eq!(days_from_civil(2026, 13, 1), None);
        assert_eq!(days_from_civil(2026, 1, 0), None);
    }

    #[test]
    fn a_partial_date_has_a_spelling_but_no_instant() {
        let partial = pb::MetaDate {
            kind: "epub".to_owned(),
            year: Some(2026),
            month: Some(2),
            day: None,
            iso_date: None,
        };
        assert_eq!(iso_date(&partial).as_deref(), Some("2026-02"));
        assert!(timestamp_of(&partial).is_none());

        let whole = pb::MetaDate {
            day: Some(4),
            month: Some(3),
            ..partial
        };
        assert_eq!(iso_date(&whole).as_deref(), Some("2026-03-04"));
        assert_eq!(timestamp_of(&whole).map(|t| t.seconds), Some(1_772_582_400));
    }

    #[test]
    fn a_period_renders_deterministically_in_every_shape() {
        let instant = pb::XbrlContext {
            period: Some(pb::XbrlPeriod {
                instant: Some("2026-12-31".to_owned()),
                ..pb::XbrlPeriod::default()
            }),
            ..pb::XbrlContext::default()
        };
        let duration = pb::XbrlContext {
            period: Some(pb::XbrlPeriod {
                start_date: Some("2026-01-01".to_owned()),
                end_date: Some("2026-12-31".to_owned()),
                ..pb::XbrlPeriod::default()
            }),
            ..pb::XbrlContext::default()
        };
        let forever = pb::XbrlContext {
            period: Some(pb::XbrlPeriod {
                forever: true,
                ..pb::XbrlPeriod::default()
            }),
            ..pb::XbrlContext::default()
        };
        assert_eq!(period_text(Some(&instant)), "2026-12-31");
        assert_eq!(period_text(Some(&duration)), "2026-01-01/2026-12-31");
        assert_eq!(period_text(Some(&forever)), "forever");
        assert_eq!(period_text(None), "");
    }
}
