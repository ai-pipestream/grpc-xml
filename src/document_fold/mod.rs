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
//! - **Provenance honesty.** XML has no pages and no boxes, so no item carries
//!   `prov`. Source locators — the positional path, the element id, the
//!   source's own role vocabulary — go in per-item `meta.custom_fields` under
//!   `xml.` keys, where they are data rather than a fabricated coordinate.
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

/// Column headings of the XBRL fact table, in order.
const FACT_COLUMNS: [&str; 6] = ["concept", "context", "period", "unit", "value", "decimals"];

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
            },
            table: None,
            facts_table: None,
            headings: Vec::new(),
            islands_skipped: 0,
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
        self.islands_skipped = 0;
        self.facts_table = None;
        self.headings.clear();
        std::mem::replace(&mut self.document, Self::new().document)
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
                // No prov: these dialects have no pages and no boxes, and an
                // invented one would outlive the honesty of this comment.
                orig: item.text.clone(),
                text: item.text.clone(),
                source: vec![source],
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
                ..doc::TableCell::default()
            });
            table.num_cols = table.num_cols.max(end_col);
            column = end_col;
        }
        table.cells.extend(cells.iter().cloned());
        table.grid.push(doc::TableRow { cells });
    }

    fn on_table_end(&mut self, end: &pb::TableEnd) {
        let Some(table) = self.table.as_ref() else {
            return;
        };
        if table.reference != end.table_ref {
            return;
        }
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
            ..doc::TableData::default()
        });
        let self_ref = item.self_ref.clone();
        let parent = item
            .parent
            .as_ref()
            .map_or_else(|| BODY_REF.to_owned(), |parent| parent.r#ref.clone());
        self.document.tables.push(item);
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
        let values = [
            concept_name(fact),
            fact.context_ref.clone(),
            period_text(fact.context.as_ref()),
            unit_text(fact),
            fact.value.clone(),
            fact.decimals.clone().unwrap_or_default(),
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
            .iter()
            .enumerate()
            .map(|(column, text)| {
                let column = int_from_usize(column);
                doc::TableCell {
                    row_span: 1,
                    col_span: 1,
                    start_row_offset_idx: row_index,
                    end_row_offset_idx: row_index + 1,
                    start_col_offset_idx: column,
                    end_col_offset_idx: column + 1,
                    text: text.clone(),
                    ..doc::TableCell::default()
                }
            })
            .collect();
        data.table_cells.extend(cells.iter().cloned());
        data.grid.push(doc::TableRow { cells });
        data.num_rows = int_from_usize(data.grid.len());
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
                .map(|(column, name)| header_cell(int_from_usize(column), name))
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
                // deterministic, so a confidence would be noise.
                confidence: source.confidence,
            },
        );
        doc::SourceType {
            source: Some(doc::source_type::Source::Collector(collector)),
        }
    }
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
