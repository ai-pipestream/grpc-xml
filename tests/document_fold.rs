// SPDX-License-Identifier: Apache-2.0

//! The Document projection: the fold in isolation, and the `document` event
//! on the wire.
//!
//! The fold tests drive the real parser over the same fixtures the mapping
//! tests use and fold its real event stream, because a fold asserted against
//! hand-written events only proves the fold agrees with the test author. The
//! wire tests then prove the server puts the folded Document where the
//! contract says it goes, and nowhere else.
//!
//! Every fold test asserts [`integrity_errors`] is empty. That is the merge
//! contract — dense unique refs, parent and child pointing at each other —
//! and it is what makes a fragment safe for the coordinator to renumber into
//! a larger document.

mod common;

use std::collections::HashMap;

use common::{
    DOCLANG, DOCLANG_NESTED_LISTS, JATS, JATS_CALS_TABLE, JATS_WITH_ISLAND, USPTO, XBRL, client,
    options, parse_ok,
};
use grpc_xml::document::v1 as doc;
use grpc_xml::document_fold::{DocumentFold, MIMETYPE, SCHEMA_NAME, integrity_errors};
use grpc_xml::parse::{InputStats, ParseConfig};
use grpc_xml::proto::v1 as pb;
use prost_types::value::Kind;

// ------------------------------------------------------------------ harness

/// Parse a document with the real driver and fold every event it produced.
fn fold(document: &str, config: &ParseConfig) -> (Vec<pb::ParseXmlResponse>, doc::Document) {
    let stats = InputStats::with_limit(64 * 1024 * 1024);
    let mut events = Vec::new();
    let mut emit = |event: pb::ParseXmlResponse| {
        events.push(event);
        true
    };
    grpc_xml::parse::parse(
        std::io::BufReader::new(document.as_bytes()),
        config,
        &stats,
        &mut emit,
    )
    .expect("the fixture parses");
    let mut fold = DocumentFold::new();
    for event in &events {
        fold.consume(event);
    }
    let folded = fold.take();
    let errors = integrity_errors(&folded);
    assert!(errors.is_empty(), "integrity: {errors:?}");
    (events, folded)
}

/// Parse and fold with the default options (sniff the dialect, flatten XHTML).
fn fold_default(document: &str) -> (Vec<pb::ParseXmlResponse>, doc::Document) {
    fold(document, &ParseConfig::default())
}

/// Parse and fold with the inline runs switched on, which is the only way
/// the spans of an item or a cell reach the projection at all.
fn fold_with_spans(document: &str) -> (Vec<pb::ParseXmlResponse>, doc::Document) {
    fold(
        document,
        &ParseConfig {
            emit_inline_spans: true,
            ..ParseConfig::default()
        },
    )
}

/// The shared base of a text item, for every variant that has one.
fn base(item: &doc::BaseTextItem) -> &doc::TextItemBase {
    match item.item.as_ref().expect("a variant is set") {
        doc::base_text_item::Item::Title(i) => i.base.as_ref(),
        doc::base_text_item::Item::SectionHeader(i) => i.base.as_ref(),
        doc::base_text_item::Item::ListItem(i) => i.base.as_ref(),
        doc::base_text_item::Item::Formula(i) => i.base.as_ref(),
        doc::base_text_item::Item::Text(i) => i.base.as_ref(),
        doc::base_text_item::Item::FieldHeading(i) => i.base.as_ref(),
        doc::base_text_item::Item::FieldValue(i) => i.base.as_ref(),
        doc::base_text_item::Item::Code(_) => panic!("CodeItem has no base; it inlines its fields"),
    }
    .expect("the base is set")
}

/// Texts of every item carrying a label.
fn texts_labelled(document: &doc::Document, label: doc::DocItemLabel) -> Vec<String> {
    document
        .texts
        .iter()
        .filter_map(|item| match item.item.as_ref() {
            Some(doc::base_text_item::Item::Code(code)) if code.label == label as i32 => {
                Some(code.text.clone())
            }
            Some(doc::base_text_item::Item::Code(_)) | None => None,
            Some(_) => {
                let base = base(item);
                (base.label == label as i32).then(|| base.text.clone())
            }
        })
        .collect()
}

/// A string custom field, or `None` when it was not set.
fn field<'a>(fields: &'a HashMap<String, prost_types::Value>, key: &str) -> Option<&'a str> {
    match fields.get(key)?.kind.as_ref()? {
        Kind::StringValue(text) => Some(text.as_str()),
        _ => None,
    }
}

/// A numeric custom field, or `None` when it was not set.
fn number(fields: &HashMap<String, prost_types::Value>, key: &str) -> Option<f64> {
    match fields.get(key)?.kind.as_ref()? {
        Kind::NumberValue(value) => Some(*value),
        _ => None,
    }
}

/// The collector attribution of a source list, which must be exactly one.
fn collector(source: &[doc::SourceType]) -> &doc::CollectorSource {
    assert_eq!(source.len(), 1, "one collector stamps each item once");
    match source[0].source.as_ref().expect("a source is set") {
        doc::source_type::Source::Collector(collector) => collector,
        doc::source_type::Source::Track(_) => panic!("this collector emits no track sources"),
        doc::source_type::Source::Generation(_) => {
            panic!("this collector generates nothing; it maps what the document says")
        }
    }
}

/// Assert every item in the fragment is attributed to this collector and
/// dialect, and that nothing claims a confidence.
fn assert_attribution(document: &doc::Document, model: &str) {
    let sources = document
        .texts
        .iter()
        .map(|item| match item.item.as_ref() {
            Some(doc::base_text_item::Item::Code(code)) => code.source.as_slice(),
            _ => base(item).source.as_slice(),
        })
        .chain(
            document
                .pictures
                .iter()
                .map(|picture| picture.source.as_slice()),
        )
        .chain(document.tables.iter().map(|table| table.source.as_slice()));
    let mut stamped = 0;
    for source in sources {
        let collector = collector(source);
        assert_eq!(collector.collector, "xml");
        assert_eq!(collector.model.as_deref(), Some(model));
        assert_eq!(collector.version.as_deref(), Some(grpc_xml::VERSION));
        assert_eq!(collector.confidence, None, "a mapping has no confidence");
        stamped += 1;
    }
    assert!(stamped > 0, "the fragment has items to attribute");
}

/// The `#/body` group, which parents everything that is not under a heading.
fn body(document: &doc::Document) -> &doc::GroupItem {
    document.body.as_ref().expect("the body group is set")
}

/// The self ref of the one text item carrying this text.
fn ref_of(document: &doc::Document, text: &str) -> String {
    let found: Vec<&doc::TextItemBase> = document
        .texts
        .iter()
        .filter(|item| !matches!(item.item, Some(doc::base_text_item::Item::Code(_))))
        .map(base)
        .filter(|item| item.text == text)
        .collect();
    assert_eq!(found.len(), 1, "{text:?} names exactly one item");
    found[0].self_ref.clone()
}

/// The `parent` of the one text item carrying this text.
fn parent_of(document: &doc::Document, text: &str) -> String {
    let index = arena_index(&ref_of(document, text));
    base(&document.texts[index])
        .parent
        .as_ref()
        .expect("every folded item names a parent")
        .r#ref
        .clone()
}

/// The position an item's self ref gives it in the text arena.
fn arena_index(self_ref: &str) -> usize {
    self_ref
        .strip_prefix("#/texts/")
        .expect("a ref into the text arena")
        .parse()
        .expect("a dense index")
}

/// The `children` of any ref this fold can parent to: the body, a list
/// group, or a section header in the text arena.
fn children_of(document: &doc::Document, self_ref: &str) -> Vec<String> {
    let children = if self_ref == "#/body" {
        &body(document).children
    } else if let Some(index) = self_ref.strip_prefix("#/groups/") {
        &document.groups[index.parse::<usize>().expect("a dense index")].children
    } else {
        &base(&document.texts[arena_index(self_ref)]).children
    };
    children.iter().map(|child| child.r#ref.clone()).collect()
}

/// The texts of the items a list of refs names, skipping refs that are not
/// text items.
fn texts_of(document: &doc::Document, refs: &[String]) -> Vec<String> {
    refs.iter()
        .filter(|r| r.starts_with("#/texts/"))
        .map(|r| base(&document.texts[arena_index(r)]).text.clone())
        .collect()
}

/// Assert the two halves of one parent link, which is what the merge needs.
fn assert_parented(document: &doc::Document, child: &str, parent: &str) {
    assert_eq!(
        parent_of(document, child),
        parent,
        "{child:?} names {parent}"
    );
    assert!(
        children_of(document, parent).contains(&ref_of(document, child)),
        "{parent} lists {child:?}"
    );
}

// ------------------------------------------------------------- fold, by dialect

#[test]
fn a_jats_article_folds_into_a_named_document_with_its_own_root_meta() {
    let (_, document) = fold_default(JATS);
    assert_eq!(document.schema_name.as_deref(), Some(SCHEMA_NAME));
    assert_eq!(
        document.origin.as_ref().map(|o| o.mimetype.as_str()),
        Some(MIMETYPE)
    );
    // XmlInfo carries no title for these dialects — it goes out before the
    // parser reaches one — so the document is named after its TITLE item.
    assert_eq!(document.name, "Streaming XML Without a DOM");

    let meta = body(&document).meta.as_ref().expect("body meta");
    assert_eq!(field(&meta.custom_fields, "xml.dialect"), Some("jats"));
    assert_eq!(
        field(&meta.custom_fields, "xml.root_namespace"),
        Some("http://jats.nlm.nih.gov/ns/archiving/1.3/")
    );
    assert_eq!(
        field(&meta.custom_fields, "xml.root_local_name"),
        Some("article")
    );
    assert_attribution(&document, "jats");
}

#[test]
fn jats_labels_become_their_document_variants_in_document_order() {
    let (events, document) = fold_default(JATS);
    assert_eq!(
        texts_labelled(&document, doc::DocItemLabel::Title),
        vec!["Streaming XML Without a DOM"]
    );
    assert_eq!(
        texts_labelled(&document, doc::DocItemLabel::SectionHeader),
        vec!["Introduction", "Scope", "Results"]
    );
    assert!(
        texts_labelled(&document, doc::DocItemLabel::Paragraph)
            .contains(&"A pull parser yields events in document order.".to_owned())
    );
    assert_eq!(
        texts_labelled(&document, doc::DocItemLabel::Reference),
        vec!["Rivera A. Parsers. 2025.", "Okafor C. Streams. 2026."]
    );

    // A section header keeps the depth the parser counted.
    let scope = document
        .texts
        .iter()
        .find_map(|item| match item.item.as_ref() {
            Some(doc::base_text_item::Item::SectionHeader(header))
                if header.base.as_ref().is_some_and(|b| b.text == "Scope") =>
            {
                Some(header)
            }
            _ => None,
        })
        .expect("the nested section header is folded");
    assert_eq!(scope.level, 2);

    // Every text event became exactly one text item, less the picture events
    // that went to the picture arena, plus the caption the table start
    // carried.
    let text_events = common::text_items(&events).len();
    let pictures = document.pictures.len();
    let captions = events
        .iter()
        .filter(|e| match e.event.as_ref() {
            Some(pb::parse_xml_response::Event::TableStart(start)) => start.caption.is_some(),
            _ => false,
        })
        .count();
    assert_eq!(document.texts.len(), text_events - pictures + captions);
}

#[test]
fn a_jats_item_carries_its_locators_in_meta_and_the_bytes_it_came_from() {
    let (_, document) = fold_default(JATS);
    let item = document
        .texts
        .iter()
        .map(base)
        .find(|base| base.text == "A pull parser yields events in document order.")
        .expect("the first body paragraph is folded");
    let fields = &item.meta.as_ref().expect("item meta").custom_fields;
    assert_eq!(
        field(fields, "xml.path"),
        Some("/article/body/sec/p"),
        "the positional path is the locator, in place of a page"
    );
    assert_eq!(field(fields, "xml.role"), None, "a body paragraph has none");
    assert_eq!(
        field(fields, "xml.element_name"),
        Some("p"),
        "the source's own name for the element, beside its position"
    );
    assert_eq!(
        field(fields, "xml.namespace"),
        None,
        "the item is in the root's namespace, which the body meta already states"
    );

    // XML has no pages and no boxes, and it does have byte offsets: the
    // range names the element the item was read from, in the bytes the
    // parser read.
    assert_eq!(item.prov.len(), 1, "{:?}", item.prov);
    let prov = &item.prov[0];
    assert_eq!(prov.page_no, 0, "an XML document has no pages");
    assert!(prov.bbox.is_none(), "and no boxes");
    let range = prov.byte_range.as_ref().expect("a byte range instead");
    assert_eq!(
        &JATS[range.start as usize..range.end as usize],
        "<p>A pull parser yields events in document order.</p>",
        "the range names the element the item was read from"
    );
    assert_eq!(
        prov.charspan.as_ref().map(|s| (s.start, s.end)),
        Some((0, item.text.chars().count() as i32)),
        "the span covers the whole item, as the element bounds the whole item"
    );
    assert_eq!(item.orig, item.text, "orig is set alongside text");
    assert_eq!(
        item.parent.as_ref().map(|p| p.r#ref.as_str()),
        Some(ref_of(&document, "Introduction").as_str()),
        "a paragraph hangs off the section header that opened its section"
    );
}

#[test]
fn jats_sections_nest_under_the_heading_that_opened_them() {
    let (_, document) = fold_default(JATS);

    // Front matter arrives before any heading, so it sits on the body — as it
    // does upstream, where content before the first heading has no other
    // parent.
    assert_parented(&document, "Streaming XML Without a DOM", "#/body");
    assert_parented(
        &document,
        "A collector need not build a tree to produce a document.",
        "#/body",
    );

    // A level 1 heading is a child of the body; its content is a child of it.
    assert_parented(&document, "Introduction", "#/body");
    let introduction = ref_of(&document, "Introduction");
    assert_parented(
        &document,
        "A pull parser yields events in document order.",
        &introduction,
    );

    // A level 2 heading nests under the level 1 heading, two deep.
    assert_parented(&document, "Scope", &introduction);
    let scope = ref_of(&document, "Scope");
    assert_parented(&document, "Four dialects are in scope.", &scope);

    // The next level 1 heading closes both: it is a sibling of the first, not
    // a descendant of the section it ended.
    assert_parented(&document, "Results", "#/body");
    let results = ref_of(&document, "Results");
    assert_parented(
        &document,
        "Throughput scales with the number of concurrent streams.",
        &results,
    );

    // Tables and pictures take the same parent as any other content, in
    // arrival order among the heading's children.
    let children = children_of(&document, &results);
    let position = |item: &str| {
        children
            .iter()
            .position(|child| child == item)
            .unwrap_or_else(|| panic!("{item} is a child of the Results heading"))
    };
    assert!(position("#/tables/0") < position("#/pictures/0"));
    assert_eq!(
        document.tables[0].parent.as_ref().map(|p| &p.r#ref),
        Some(&results)
    );

    // The body lists only what is not under a heading.
    let body_children = children_of(&document, "#/body");
    assert!(body_children.contains(&introduction));
    assert!(body_children.contains(&results));
    assert!(
        !body_children.contains(&scope),
        "a nested heading is not also a child of the body"
    );
}

#[test]
fn a_heading_ladder_pops_on_a_shallower_heading_and_on_a_sibling() {
    // Hand-built: the fixtures nest tidily, and the pops are the part of the
    // ladder they do not exercise.
    let heading = |level: u32, body: &str| pb::ParseXmlResponse {
        event: Some(pb::parse_xml_response::Event::TextItem(pb::TextItem {
            label: pb::XmlItemLabel::SectionHeader as i32,
            level: Some(level),
            text: body.to_owned(),
            path: "/doc/sec/title".to_owned(),
            ..pb::TextItem::default()
        })),
    };
    let paragraph = |body: &str| pb::ParseXmlResponse {
        event: Some(pb::parse_xml_response::Event::TextItem(pb::TextItem {
            label: pb::XmlItemLabel::Paragraph as i32,
            text: body.to_owned(),
            path: "/doc/sec/p".to_owned(),
            ..pb::TextItem::default()
        })),
    };

    let mut fold = DocumentFold::new();
    fold.consume(&paragraph("before any heading"));
    fold.consume(&heading(1, "one"));
    fold.consume(&heading(2, "one.one"));
    fold.consume(&heading(3, "one.one.one"));
    fold.consume(&paragraph("deep"));
    fold.consume(&heading(2, "one.two"));
    fold.consume(&paragraph("back up one"));
    fold.consume(&heading(1, "two"));
    fold.consume(&paragraph("back at the top"));
    let document = fold.take();
    assert!(integrity_errors(&document).is_empty());

    assert_parented(&document, "before any heading", "#/body");
    assert_parented(&document, "one", "#/body");
    assert_parented(&document, "one.one", &ref_of(&document, "one"));
    assert_parented(&document, "one.one.one", &ref_of(&document, "one.one"));
    assert_parented(&document, "deep", &ref_of(&document, "one.one.one"));
    // A level 2 heading closes the level 3 and the level 2 before it.
    assert_parented(&document, "one.two", &ref_of(&document, "one"));
    assert_parented(&document, "back up one", &ref_of(&document, "one.two"));
    // A level 1 heading closes everything below it.
    assert_parented(&document, "two", "#/body");
    assert_parented(&document, "back at the top", &ref_of(&document, "two"));
    assert_eq!(
        children_of(&document, "#/body"),
        vec![
            ref_of(&document, "before any heading"),
            ref_of(&document, "one"),
            ref_of(&document, "two"),
        ],
        "the body lists only the top level, in arrival order"
    );
}

#[test]
fn a_jats_table_folds_into_a_grid_with_its_caption_referenced() {
    let (_, document) = fold_default(JATS);
    assert_eq!(document.tables.len(), 1);
    let table = &document.tables[0];
    assert_eq!(table.self_ref, "#/tables/0");
    assert_eq!(table.label, doc::DocItemLabel::Table as i32);
    let fields = &table.meta.as_ref().expect("table meta").custom_fields;
    assert_eq!(
        field(fields, "xml.path"),
        Some("/article/body/sec[2]/table-wrap/table")
    );

    let data = table.data.as_ref().expect("table data");
    assert_eq!(data.num_rows, 3);
    assert_eq!(data.num_cols, 2);
    assert_eq!(data.grid.len(), 3);
    assert_eq!(
        data.table_cells.len(),
        6,
        "the flat list carries the same cells as the grid"
    );
    let header = &data.grid[0].cells;
    assert_eq!(header[0].text, "Dialect");
    assert!(header.iter().all(|cell| cell.column_header));
    assert_eq!(header[1].start_col_offset_idx, 1);
    assert_eq!(header[1].end_col_offset_idx, 2);
    let body_row = &data.grid[1].cells;
    assert_eq!(body_row[0].text, "JATS");
    assert!(!body_row[0].column_header);
    assert_eq!(body_row[0].start_row_offset_idx, 1);
    assert_eq!(body_row[0].end_row_offset_idx, 2);

    // The caption is an item of its own, created before the table so the
    // table can point at a ref that already resolves.
    assert_eq!(table.captions.len(), 1);
    let caption_ref = table.captions[0].r#ref.as_str();
    let index: usize = caption_ref
        .strip_prefix("#/texts/")
        .expect("a caption ref points into the text arena")
        .parse()
        .expect("a dense index");
    let caption = base(&document.texts[index]);
    assert_eq!(caption.label, doc::DocItemLabel::Caption as i32);
    assert_eq!(caption.text, "Throughput by dialect.");
}

#[test]
fn uspto_claims_fold_as_numbered_text_items_keeping_their_role() {
    let (_, document) = fold_default(USPTO);
    assert_eq!(document.name, "Method for streaming structured documents");
    assert_attribution(&document, "uspto");

    // Claims stream as TEXT items refined by `role`, not as list items, so
    // that is what they fold to; the claim number rides in the meta.
    let claims: Vec<&doc::TextItemBase> = document
        .texts
        .iter()
        .map(base)
        .filter(|base| {
            base.meta
                .as_ref()
                .is_some_and(|m| field(&m.custom_fields, "xml.role") == Some("claim"))
        })
        .collect();
    assert_eq!(claims.len(), 3);
    assert_eq!(claims[0].label, doc::DocItemLabel::Text as i32);
    assert_eq!(claims[0].text, "1. A method comprising streaming items.");
    let fields = &claims[0].meta.as_ref().expect("meta").custom_fields;
    assert_eq!(number(fields, "xml.ordinal"), Some(1.0));
    assert_eq!(field(fields, "xml.element_id"), Some("CLM-00001"));
    assert_eq!(
        number(
            &claims[2].meta.as_ref().expect("meta").custom_fields,
            "xml.ordinal"
        ),
        Some(3.0)
    );

    // Claims arrive after the last heading, so that is what parents them.
    assert_parented(
        &document,
        "1. A method comprising streaming items.",
        &ref_of(&document, "DETAILED DESCRIPTION"),
    );

    // A drawing is a picture item, not a text item labelled PICTURE.
    assert!(
        texts_labelled(&document, doc::DocItemLabel::Picture).is_empty(),
        "the picture arena is where a picture goes"
    );
}

#[test]
fn a_drawing_folds_into_a_placeholder_picture_carrying_the_reference_it_named() {
    let (_, document) = fold_default(USPTO);
    assert_eq!(document.pictures.len(), 1);
    let picture = &document.pictures[0];
    assert_eq!(picture.self_ref, "#/pictures/0");
    assert_eq!(picture.label, doc::DocItemLabel::Picture as i32);
    assert_eq!(picture.content_layer, doc::ContentLayer::Body as i32);
    assert_eq!(
        picture.image, None,
        "no bytes, no uri, no size: the XML names a file, it does not carry pixels"
    );
    assert!(picture.prov.is_empty(), "no pages and no boxes here either");
    assert_eq!(collector(&picture.source).model.as_deref(), Some("uspto"));

    // The locators the item would have carried as a text item are carried
    // here instead, the reference the parser lifted from `file` among them.
    let fields = &picture.meta.as_ref().expect("picture meta").custom_fields;
    assert_eq!(
        field(fields, "xml.path"),
        Some("/us-patent-grant/description/img")
    );
    assert_eq!(field(fields, "xml.role"), Some("drawing"));
    assert_eq!(field(fields, "xml.element_id"), Some("img-0001"));
    assert_eq!(
        field(fields, "xml.href"),
        Some("US11999999-20260210-D00001.TIF"),
        "an attribute value is a locator, not a caption"
    );

    // Nothing is captioned with that filename, here or in the text arena.
    assert!(
        picture.captions.is_empty(),
        "a figure's caption arrives as its own CAPTION event, if at all"
    );
    assert!(
        !texts_labelled(&document, doc::DocItemLabel::Caption)
            .iter()
            .any(|text| text == "US11999999-20260210-D00001.TIF")
    );

    // And it hangs off the heading ladder like any other content item.
    let detail = ref_of(&document, "DETAILED DESCRIPTION");
    assert_eq!(picture.parent.as_ref().map(|p| &p.r#ref), Some(&detail));
    assert!(children_of(&document, &detail).contains(&picture.self_ref));
}

#[test]
fn a_picture_with_no_reference_carries_no_href() {
    // The wire says a picture is there but names nothing: a placeholder with
    // no locator is still the honest projection.
    let event = pb::ParseXmlResponse {
        event: Some(pb::parse_xml_response::Event::TextItem(pb::TextItem {
            label: pb::XmlItemLabel::Picture as i32,
            text: String::new(),
            path: "/doc/figure".to_owned(),
            ..pb::TextItem::default()
        })),
    };
    let mut fold = DocumentFold::new();
    fold.consume(&event);
    let document = fold.take();
    assert!(integrity_errors(&document).is_empty());
    assert_eq!(document.pictures.len(), 1);
    assert!(document.pictures[0].captions.is_empty());
    let fields = &document.pictures[0]
        .meta
        .as_ref()
        .expect("picture meta")
        .custom_fields;
    assert_eq!(field(fields, "xml.path"), Some("/doc/figure"));
    assert_eq!(
        field(fields, "xml.href"),
        None,
        "no key is invented for a picture that names nothing"
    );
    assert!(
        document.texts.is_empty(),
        "and no item of any kind is invented for it either"
    );
    assert_eq!(
        document.pictures[0]
            .parent
            .as_ref()
            .map(|p| p.r#ref.as_str()),
        Some("#/body")
    );
}

#[test]
fn a_document_with_no_figures_has_an_empty_picture_arena() {
    for fixture in [XBRL, DOCLANG] {
        let (_, document) = fold_default(fixture);
        assert!(
            document.pictures.is_empty(),
            "an arena is filled by figures, not created for them"
        );
    }
}

#[test]
fn xbrl_facts_fold_into_one_table_with_a_deterministic_row_per_fact() {
    let (events, document) = fold_default(XBRL);
    assert_attribution(&document, "xbrl");
    assert_eq!(
        texts_labelled(&document, doc::DocItemLabel::Footnote),
        ["Includes restricted cash of 12 million."],
        "an instance is facts plus the narrative a filer attached to them"
    );
    assert_eq!(
        document.texts.len(),
        1,
        "and nothing else: an instance is not prose"
    );
    assert_eq!(document.tables.len(), 1, "one fact table, created lazily");

    let table = &document.tables[0];
    let fields = &table.meta.as_ref().expect("table meta").custom_fields;
    assert_eq!(field(fields, "xml.table"), Some("facts"));
    let data = table.data.as_ref().expect("table data");
    assert_eq!(data.num_cols, 11);
    assert_eq!(
        usize::try_from(data.num_rows).expect("a row count is not negative"),
        common::facts(&events).len() + 1,
        "one header row plus one row per fact"
    );
    let header: Vec<&str> = data.grid[0].cells.iter().map(|c| c.text.as_str()).collect();
    assert_eq!(
        header,
        [
            "concept",
            "entity_scheme",
            "entity",
            "context",
            "period",
            "unit",
            "value",
            "decimals",
            "precision",
            "sign",
            "nil"
        ]
    );
    assert!(data.grid[0].cells.iter().all(|cell| cell.column_header));

    let row: Vec<&str> = data.grid[1].cells.iter().map(|c| c.text.as_str()).collect();
    assert_eq!(
        row,
        [
            "us-gaap:Assets",
            "http://www.sec.gov/CIK",
            "0000123456",
            "I2026",
            "2026-12-31",
            "iso4217:USD",
            "1234000000",
            "-6",
            "",
            "",
            "false"
        ]
    );
    let duration: Vec<&str> = data.grid[2].cells.iter().map(|c| c.text.as_str()).collect();
    assert_eq!(duration[4], "2026-01-01/2026-12-31");
    let divided: Vec<&str> = data.grid[4].cells.iter().map(|c| c.text.as_str()).collect();
    assert_eq!(divided[5], "iso4217:USD/xbrli:shares");
    assert_eq!(divided[6], "", "a nil fact has no value to render");
    assert!(!data.grid[1].cells[0].column_header, "rows are not headers");
}

#[test]
fn the_fact_table_declares_its_column_types_and_types_its_cells() {
    let (_, document) = fold_default(XBRL);
    let data = document.tables[0].data.as_ref().expect("table data");
    let declared: Vec<(Option<&str>, Option<&str>)> = data
        .columns
        .iter()
        .map(|column| (column.name.as_deref(), column.declared_type.as_deref()))
        .collect();
    assert_eq!(declared.len(), 11);
    assert!(
        declared.contains(&(Some("value"), Some("decimal"))),
        "{declared:?}"
    );
    assert!(
        declared.contains(&(Some("nil"), Some("boolean"))),
        "{declared:?}"
    );

    // The value cell is a number, not a string that looks like one.
    let value = data.grid[1].cells[6].value.as_ref().expect("a typed value");
    assert_eq!(
        value.kind,
        Some(doc::cell_value::Kind::Number(1_234_000_000.0))
    );
    let decimals = data.grid[1].cells[7]
        .value
        .as_ref()
        .expect("decimals is a number");
    assert_eq!(decimals.kind, Some(doc::cell_value::Kind::Number(-6.0)));
    let nil = data.grid[1].cells[10].value.as_ref().expect("nil is typed");
    assert_eq!(nil.kind, Some(doc::cell_value::Kind::Boolean(false)));

    // The nil fact carries the nil flag, and has no number to carry.
    let nil_row = &data.grid[4].cells;
    assert!(nil_row[6].value.is_none(), "a nil fact has no value");
    assert_eq!(
        nil_row[10].value.as_ref().and_then(|v| v.kind.clone()),
        Some(doc::cell_value::Kind::Boolean(true))
    );
}

#[test]
fn a_dimensioned_fact_points_at_its_axes() {
    let (_, document) = fold_default(XBRL);
    assert_eq!(
        document.key_value_items.len(),
        1,
        "one fact of the fixture is dimensioned"
    );
    let graph = document.key_value_items[0]
        .graph
        .as_ref()
        .expect("the axes are a graph, not a rendering of one");
    let keys: Vec<&str> = graph
        .cells
        .iter()
        .filter(|c| c.label == doc::GraphCellLabel::Key as i32)
        .map(|c| c.text.as_str())
        .collect();
    let values: Vec<&str> = graph
        .cells
        .iter()
        .filter(|c| c.label == doc::GraphCellLabel::Value as i32)
        .map(|c| c.text.as_str())
        .collect();
    assert_eq!(keys, ["us-gaap:StatementGeographicalAxis"]);
    assert_eq!(values, ["us-gaap:NorthAmericaMember"]);
    assert_eq!(graph.links.len(), 1);
    assert_eq!(
        graph.links[0].label,
        doc::GraphLinkLabel::ToValue as i32,
        "the axis names the member"
    );

    let data = document.tables[0].data.as_ref().expect("table data");
    let dimensioned = &data.grid[3].cells[3];
    assert_eq!(
        dimensioned.r#ref.as_ref().map(|r| r.r#ref.as_str()),
        Some("#/key_value_items/0"),
        "the context cell points at the axes rather than hiding them"
    );
    let undimensioned = &data.grid[1].cells[3];
    assert!(undimensioned.r#ref.is_none());
}

#[test]
fn doclang_folds_lists_captions_footnotes_and_a_table() {
    let (_, document) = fold_default(DOCLANG);
    assert_eq!(document.name, "Quarterly Operations Review");
    assert_attribution(&document, "doclang");
    assert_eq!(
        texts_labelled(&document, doc::DocItemLabel::SectionHeader),
        vec!["Summary", "Outlook"]
    );
    assert_eq!(
        texts_labelled(&document, doc::DocItemLabel::Footnote),
        vec!["Figures are unaudited."]
    );

    let list: Vec<&doc::ListItem> = document
        .texts
        .iter()
        .filter_map(|item| match item.item.as_ref() {
            Some(doc::base_text_item::Item::ListItem(list)) => Some(list),
            _ => None,
        })
        .collect();
    assert_eq!(list.len(), 2);
    assert!(
        list[0].enumerated,
        "the source numbered the item, so the list is ordered"
    );
    assert_eq!(list[0].marker, None, "the wire carries no marker text");

    // The caption before the table is consumed by it rather than left loose.
    assert_eq!(document.tables.len(), 1);
    assert_eq!(document.tables[0].captions.len(), 1);
    let data = document.tables[0].data.as_ref().expect("table data");
    assert_eq!((data.num_rows, data.num_cols), (3, 2));
}

#[test]
fn a_code_block_folds_into_a_code_item_that_inlines_its_base_fields() {
    // The one text variant in the schema with no `TextItemBase` wrapper.
    let source = r#"<?xml version="1.0" encoding="UTF-8"?>
<doclang xmlns="http://docling-project.org/ns/doclang/v1">
  <title>Snippets</title>
  <code id="snippet-1">cargo test --offline</code>
</doclang>
"#;
    let (_, document) = fold_default(source);
    let code = document
        .texts
        .iter()
        .find_map(|item| match item.item.as_ref() {
            Some(doc::base_text_item::Item::Code(code)) => Some(code),
            _ => None,
        })
        .expect("the code block folds to a CodeItem");
    assert_eq!(code.self_ref, "#/texts/1");
    assert_eq!(
        code.parent.as_ref().map(|p| p.r#ref.as_str()),
        Some("#/body")
    );
    assert_eq!(code.label, doc::DocItemLabel::Code as i32);
    assert_eq!(code.text, "cargo test --offline");
    assert_eq!(code.orig, code.text);
    assert_eq!(code.content_layer, doc::ContentLayer::Body as i32);
    let range = code.prov[0]
        .byte_range
        .as_ref()
        .expect("a code block is located by its bytes like any other item");
    assert_eq!(
        &source[range.start as usize..range.end as usize],
        "<code id=\"snippet-1\">cargo test --offline</code>"
    );
    assert_eq!(collector(&code.source).model.as_deref(), Some("doclang"));
    let fields = &code.meta.as_ref().expect("code meta").custom_fields;
    assert_eq!(field(fields, "xml.element_id"), Some("snippet-1"));
    assert_eq!(field(fields, "xml.path"), Some("/doclang/code"));
    assert!(
        body(&document)
            .children
            .iter()
            .any(|child| child.r#ref == "#/texts/1"),
        "the body lists the code item like any other"
    );
}

#[test]
fn html_islands_are_left_to_the_html_collector_and_counted_on_the_body() {
    let config = ParseConfig {
        emit_html_islands: true,
        ..ParseConfig::default()
    };
    let (events, document) = fold(JATS_WITH_ISLAND, &config);
    assert_eq!(common::islands(&events).len(), 1);
    assert_eq!(
        texts_labelled(&document, doc::DocItemLabel::Paragraph),
        vec!["Text before the island.", "Text after the island."],
        "the island contributes no items of its own"
    );
    let meta = body(&document).meta.as_ref().expect("body meta");
    assert_eq!(
        number(&meta.custom_fields, "xml.html_islands"),
        Some(1.0),
        "what the projection dropped is stated, not hidden"
    );
}

// ------------------------------------------------------------- fold, by shape

#[test]
fn spanning_cells_take_their_grid_position_from_the_spans_before_them() {
    // Hand-built events: the fixtures have no spans, and the offsets are the
    // one part of the table mapping the wire does not already compute.
    let start = pb::ParseXmlResponse {
        event: Some(pb::parse_xml_response::Event::TableStart(pb::TableStart {
            table_ref: "t1".to_owned(),
            path: "/doc/table".to_owned(),
            ..pb::TableStart::default()
        })),
    };
    let row = |cells: Vec<pb::TableCell>, is_header: bool| pb::ParseXmlResponse {
        event: Some(pb::parse_xml_response::Event::TableRow(pb::TableRow {
            table_ref: "t1".to_owned(),
            is_header,
            cells,
            ..pb::TableRow::default()
        })),
    };
    let cell = |text: &str, column_span: u32, row_span: u32| pb::TableCell {
        text: text.to_owned(),
        column_span,
        row_span,
        ..pb::TableCell::default()
    };
    let end = pb::ParseXmlResponse {
        event: Some(pb::parse_xml_response::Event::TableEnd(pb::TableEnd {
            table_ref: "t1".to_owned(),
            ..pb::TableEnd::default()
        })),
    };

    let mut fold = DocumentFold::new();
    fold.consume(&start);
    fold.consume(&row(vec![cell("stub", 1, 2), cell("wide", 2, 1)], true));
    fold.consume(&row(vec![cell("a", 1, 1), cell("b", 1, 1)], false));
    fold.consume(&end);
    let document = fold.take();
    assert!(integrity_errors(&document).is_empty());

    let data = document.tables[0].data.as_ref().expect("table data");
    assert_eq!((data.num_rows, data.num_cols), (2, 3));
    let first = &data.grid[0].cells;
    assert_eq!(
        (
            first[0].start_row_offset_idx,
            first[0].end_row_offset_idx,
            first[0].start_col_offset_idx,
            first[0].end_col_offset_idx
        ),
        (0, 2, 0, 1)
    );
    assert_eq!(
        (
            first[1].start_col_offset_idx,
            first[1].end_col_offset_idx,
            first[1].col_span
        ),
        (1, 3, 2)
    );
    let second = &data.grid[1].cells;
    assert_eq!(
        (second[0].start_col_offset_idx, second[0].end_col_offset_idx),
        (1, 2),
        "column 0 of the second row is still under the rowspan"
    );
    assert_eq!(
        (second[1].start_col_offset_idx, second[1].end_col_offset_idx),
        (2, 3)
    );
    assert!(second.iter().all(|c| !c.column_header));
    assert_eq!(data.table_cells.len(), 4);
}

// -------------------------------------------------------------------- wire

/// Options asking the server for the Document projection.
fn with_document() -> pb::ParseOptions {
    pb::ParseOptions {
        emit_document: true,
        ..options()
    }
}

/// The `document` events of a stream.
fn documents(events: &[pb::ParseXmlResponse]) -> Vec<&doc::Document> {
    events
        .iter()
        .filter_map(|e| match e.event.as_ref() {
            Some(pb::parse_xml_response::Event::Document(document)) => Some(document),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn the_document_event_is_sent_once_and_immediately_before_the_trailer() {
    let client = client().await;
    let events = parse_ok(&client, JATS, with_document()).await;
    let documents = documents(&events);
    assert_eq!(documents.len(), 1, "exactly one document per parse");

    let position = events
        .iter()
        .position(|e| matches!(e.event, Some(pb::parse_xml_response::Event::Document(_))))
        .expect("the document event is in the stream");
    assert_eq!(
        position,
        events.len() - 2,
        "the document is the last event before the trailer"
    );

    let document = documents[0];
    assert!(integrity_errors(document).is_empty());
    assert_eq!(document.name, "Streaming XML Without a DOM");
    assert_eq!(document.tables.len(), 1);
    // The projection is of these events, not of a second parse.
    let text_events = common::text_items(&events).len();
    assert_eq!(document.pictures.len(), 1, "the figure, as a placeholder");
    assert_eq!(
        document.texts.len(),
        text_events,
        "- the figure, + the table caption"
    );
}

#[tokio::test]
async fn no_document_is_sent_when_the_request_did_not_ask_for_one() {
    let client = client().await;
    for fixture in [JATS, USPTO, XBRL, DOCLANG] {
        let events = parse_ok(&client, fixture, options()).await;
        assert!(
            documents(&events).is_empty(),
            "the fold must not run unless it was asked for"
        );
    }
}

#[tokio::test]
async fn every_dialect_folds_over_the_wire_into_a_sound_fragment() {
    let client = client().await;
    for (fixture, model) in [
        (JATS, "jats"),
        (USPTO, "uspto"),
        (XBRL, "xbrl"),
        (DOCLANG, "doclang"),
    ] {
        let events = parse_ok(&client, fixture, with_document()).await;
        let documents = documents(&events);
        assert_eq!(documents.len(), 1, "{model}");
        let document = documents[0];
        let errors = integrity_errors(document);
        assert!(errors.is_empty(), "{model}: {errors:?}");
        assert_eq!(
            document.schema_name.as_deref(),
            Some(SCHEMA_NAME),
            "{model}"
        );
        assert_attribution(document, model);
    }
}

// -------------------------------------------------------------- inline spans

#[test]
fn a_link_run_folds_onto_the_text_it_covers() {
    let (_, document) = fold(
        JATS,
        &ParseConfig {
            emit_inline_spans: true,
            ..ParseConfig::default()
        },
    );
    let paragraph = document
        .texts
        .iter()
        .map(base)
        .find(|base| base.text.starts_with("See Rivera"))
        .expect("the paragraph that links and cites");
    let link = paragraph
        .spans
        .iter()
        .find(|span| span.hyperlink.is_some())
        .expect("the ext-link becomes a hyperlink run");
    assert_eq!(
        link.hyperlink.as_deref(),
        Some("https://example.org/spec"),
        "TextItemBase.hyperlink had no writer in this crate before this run"
    );
    let range = link.range.as_ref().expect("a run has a range");
    let start = usize::try_from(range.start).expect("a range starts at or after zero");
    let width = usize::try_from(range.end - range.start).expect("a range does not run backwards");
    let covered: String = paragraph.text.chars().skip(start).take(width).collect();
    assert_eq!(covered, "specification");
}

#[test]
fn a_citation_resolves_forward_onto_the_reference_it_names() {
    // The citation is in the body and its target is in the back matter, so
    // this only works because resolution waits for the end of the stream.
    let (_, document) = fold(
        JATS,
        &ParseConfig {
            emit_inline_spans: true,
            ..ParseConfig::default()
        },
    );
    let target = document
        .anchors
        .iter()
        .find(|anchor| anchor.name == "b1")
        .and_then(|anchor| anchor.target.as_ref())
        .expect("the reference list declares b1")
        .r#ref
        .clone();
    let entry = document
        .texts
        .iter()
        .map(base)
        .find(|base| base.self_ref == target)
        .expect("the anchor points at a real item");
    assert_eq!(entry.text, "Rivera A. Parsers. 2025.");
    assert_eq!(entry.label, doc::DocItemLabel::Reference as i32);

    let citation = document
        .texts
        .iter()
        .map(base)
        .find(|base| base.text.starts_with("See Rivera"))
        .expect("the citing paragraph")
        .spans
        .iter()
        .find_map(|span| span.target.clone())
        .expect("the xref becomes a target");
    assert_eq!(citation.r#ref, target);
}

#[test]
fn a_cross_reference_the_document_never_defines_keeps_the_source_name() {
    let article = JATS.replace(r#"rid="b1""#, r#"rid="nowhere""#);
    let (_, document) = fold(
        &article,
        &ParseConfig {
            emit_inline_spans: true,
            ..ParseConfig::default()
        },
    );
    let target = document
        .texts
        .iter()
        .map(base)
        .find(|base| base.text.starts_with("See Rivera"))
        .expect("the citing paragraph")
        .spans
        .iter()
        .find_map(|span| span.target.clone())
        .expect("a dangling reference is still a reference");
    assert_eq!(
        target.r#ref, "#nowhere",
        "an unresolved target names the source identifier rather than lying about an item"
    );
    let key = document
        .texts
        .iter()
        .map(base)
        .find(|b| b.text.starts_with("See Rivera"))
        .expect("the citing paragraph")
        .spans
        .iter()
        .find_map(|span| span.reference.clone())
        .expect("the key the source wrote survives resolution failing");
    assert_eq!(key, "nowhere");
    assert!(
        document
            .anchors
            .iter()
            .all(|anchor| anchor.name != "nowhere"),
        "nothing declares the name, so nothing anchors it"
    );
}

#[test]
fn every_source_identifier_becomes_an_anchor_at_a_real_item() {
    let (_, document) = fold(
        JATS,
        &ParseConfig {
            emit_inline_spans: true,
            ..ParseConfig::default()
        },
    );
    let refs: Vec<String> = document
        .texts
        .iter()
        .map(|item| base(item).self_ref.clone())
        .chain(document.pictures.iter().map(|p| p.self_ref.clone()))
        .chain(document.tables.iter().map(|t| t.self_ref.clone()))
        .collect();
    assert!(!document.anchors.is_empty());
    for anchor in &document.anchors {
        assert!(!anchor.name.is_empty());
        let target = anchor.target.as_ref().expect("an anchor points somewhere");
        assert!(
            refs.contains(&target.r#ref),
            "anchor {} points at {} which is not in the arena",
            anchor.name,
            target.r#ref
        );
    }
    let names: Vec<&str> = document
        .anchors
        .iter()
        .map(|anchor| anchor.name.as_str())
        .collect();
    assert!(names.contains(&"b1") && names.contains(&"b2"), "{names:?}");
}

#[test]
fn the_default_fold_carries_no_spans_and_no_targets() {
    let (_, document) = fold_default(JATS);
    assert!(
        document.texts.iter().map(base).all(|b| b.spans.is_empty()),
        "spans follow the option that produced them"
    );
}

// --------------------------------------------------------------- source meta

#[test]
fn the_document_metadata_lands_in_typed_slots() {
    let (_, document) = fold_default(JATS);
    let meta = document
        .source_meta
        .as_ref()
        .expect("the article declares metadata about itself");
    assert_eq!(meta.title.as_deref(), Some("Streaming XML Without a DOM"));
    assert_eq!(
        meta.language.as_deref(),
        Some("en"),
        "xml:lang on the root is the document's language"
    );
    assert_eq!(meta.keywords, ["streaming", "xml"]);
    assert_eq!(meta.authors, ["Rivera Ana", "Okafor Chidi"]);
    assert_eq!(
        meta.schema_location.as_deref(),
        Some("http://jats.nlm.nih.gov/ns/archiving/1.3/ JATS-archivearticle1.xsd"),
        "the modern schema signal, as the root wrote it"
    );
}

#[test]
fn the_abstract_becomes_the_body_summary() {
    let (_, document) = fold_default(JATS);
    let summary = document
        .body
        .as_ref()
        .and_then(|body| body.meta.as_ref())
        .and_then(|meta| meta.summary.as_ref())
        .expect("an abstract is a summary the document wrote itself");
    assert_eq!(
        summary.text,
        "A collector need not build a tree to produce a document."
    );
    assert!(
        summary.confidence.is_none(),
        "a quoted abstract has no confidence to claim"
    );
}

#[test]
fn a_document_that_declares_nothing_about_itself_carries_no_metadata() {
    // No namespace binding, no schema location, no language, no title, no
    // keywords: a document that says nothing about itself.
    let bare = "<?xml version=\"1.0\"?>\n<article><body><p>Just prose.</p></body></article>\n";
    let (_, document) = fold_default(bare);
    assert!(
        document.source_meta.is_none(),
        "an absent declaration is not an empty one"
    );
}

#[test]
fn a_publication_date_becomes_the_documents_created_date() {
    let (_, document) = fold(
        JATS,
        &ParseConfig {
            emit_source_metadata: true,
            ..ParseConfig::default()
        },
    );
    let meta = document.source_meta.as_ref().expect("declared metadata");
    assert_eq!(
        meta.created_raw.as_deref(),
        Some("2026-02"),
        "the source states a year and a month, so the document says a year and a month"
    );
    assert!(
        meta.created.is_none(),
        "a year and a month is not an instant, and inventing a day would be a lie"
    );
    let created = meta
        .created_civil
        .as_ref()
        .expect("a partial date is still a civil date");
    assert_eq!((created.year, created.month), (2026, 2));
    assert_eq!(
        created.day, 0,
        "the source stated no day, and zero is not a day of any month"
    );

    assert_eq!(meta.modified_raw.as_deref(), Some("2026-03-04"));
    let modified = meta
        .modified
        .as_ref()
        .expect("a whole calendar date resolves to an instant");
    // 2026-03-04T00:00:00Z.
    assert_eq!(modified.seconds, 1_772_582_400);
    assert_eq!(modified.nanos, 0);
    let modified_civil = meta
        .modified_civil
        .as_ref()
        .expect("the wall-clock value the source wrote");
    assert_eq!(
        (
            modified_civil.year,
            modified_civil.month,
            modified_civil.day
        ),
        (2026, 3, 4)
    );
}

#[test]
fn a_cited_reference_block_folds_as_reference_items() {
    let (_, document) = fold(
        USPTO,
        &ParseConfig {
            emit_source_metadata: true,
            ..ParseConfig::default()
        },
    );
    let references = texts_labelled(&document, doc::DocItemLabel::Reference);
    assert_eq!(references.len(), 1, "{references:?}");
    assert!(references[0].contains("9876543"), "{references:?}");
    assert!(
        document.anchors.iter().any(|a| a.name == "cit-0001"),
        "a cited reference is addressable like any other item"
    );
}

#[test]
fn a_classification_code_lands_in_its_own_typed_field() {
    let (_, document) = fold(
        USPTO,
        &ParseConfig {
            emit_source_metadata: true,
            ..ParseConfig::default()
        },
    );
    let meta = document.source_meta.as_ref().expect("declared metadata");
    let codes: Vec<(&str, &str)> = meta
        .classifications
        .iter()
        .map(|c| (c.scheme.as_str(), c.code.as_str()))
        .collect();
    assert_eq!(
        codes,
        [("cpc", "G06F16/93")],
        "a code is a scheme and a code, not a string in a map"
    );
    // And nothing was smuggled into the escape hatch on the way.
    let body_fields = &document
        .body
        .as_ref()
        .expect("body")
        .meta
        .as_ref()
        .expect("body meta")
        .custom_fields;
    assert!(
        body_fields
            .keys()
            .all(|key| !key.contains("classification")),
        "a typed field exists, so nothing belongs in custom_fields"
    );
}

#[test]
fn the_licence_funding_and_identifiers_land_in_their_own_typed_fields() {
    let (_, document) = fold(
        JATS,
        &ParseConfig {
            emit_source_metadata: true,
            ..ParseConfig::default()
        },
    );
    let meta = document.source_meta.as_ref().expect("declared metadata");

    let license = meta.license.as_ref().expect("the permissions block");
    assert_eq!(
        license.type_uri.as_deref(),
        Some("https://creativecommons.org/licenses/by/4.0/")
    );
    assert_eq!(
        license.copyright_year,
        Some(2026),
        "the schema wants a number, so the year is parsed rather than copied"
    );
    assert_eq!(
        license.statement.as_deref(),
        Some("Distributed under CC BY 4.0.")
    );

    assert_eq!(meta.funding.len(), 1);
    assert_eq!(
        meta.funding[0].funder.as_deref(),
        Some("National Science Foundation")
    );
    assert_eq!(meta.funding[0].award_id.as_deref(), Some("NSF-1234567"));

    let identifiers: Vec<(&str, &str, Option<&str>)> = meta
        .identifiers
        .iter()
        .map(|id| (id.kind.as_str(), id.value.as_str(), id.scope.as_deref()))
        .collect();
    assert!(
        identifiers.contains(&("issn", "1234-5678", Some("epub"))),
        "the scope the source attaches survives: {identifiers:?}"
    );
    assert!(
        identifiers.contains(&("nlm-ta", "J Stream Parse", None)),
        "{identifiers:?}"
    );

    let subjects: Vec<&str> = meta
        .classifications
        .iter()
        .filter(|c| c.scheme == "jats-subject")
        .map(|c| c.code.as_str())
        .collect();
    assert_eq!(subjects, ["Research Article"]);
}

#[test]
fn the_namespace_bindings_and_schema_locations_reach_the_document() {
    let (_, document) = fold_default(JATS);
    let meta = document.source_meta.as_ref().expect("declared metadata");
    let bindings: Vec<(&str, &str)> = meta
        .namespaces
        .iter()
        .map(|n| (n.prefix.as_str(), n.uri.as_str()))
        .collect();
    assert!(
        bindings.contains(&("", "http://jats.nlm.nih.gov/ns/archiving/1.3/")),
        "{bindings:?}"
    );
    assert!(
        bindings.contains(&("xlink", "http://www.w3.org/1999/xlink")),
        "a path written in qualified names is resolvable now: {bindings:?}"
    );
    let locations: Vec<(&str, &str)> = meta
        .schema_locations
        .iter()
        .map(|l| (l.namespace.as_str(), l.location.as_str()))
        .collect();
    assert_eq!(
        locations,
        [(
            "http://jats.nlm.nih.gov/ns/archiving/1.3/",
            "JATS-archivearticle1.xsd"
        )],
        "the pairs stay pairs in the projection too"
    );
}

#[test]
fn an_xbrl_footnote_folds_as_a_footnote_addressable_by_its_label() {
    let (_, document) = fold_default(XBRL);
    let footnote = document
        .texts
        .iter()
        .map(base)
        .find(|b| b.label == doc::DocItemLabel::Footnote as i32)
        .expect("the footnote linkbase is content");
    assert!(footnote.text.starts_with("Includes restricted cash"));
    assert!(
        document.anchors.iter().any(|a| a.name == "fn-1"),
        "the xlink:label is the name the arcs address it by"
    );
}

#[test]
fn a_citation_run_names_what_it_points_at() {
    let (_, document) = fold(
        JATS,
        &ParseConfig {
            emit_inline_spans: true,
            ..ParseConfig::default()
        },
    );
    let citation = document
        .texts
        .iter()
        .map(base)
        .find(|b| b.text.starts_with("See Rivera"))
        .expect("the citing paragraph")
        .spans
        .iter()
        .find(|span| span.target.is_some())
        .expect("the xref becomes a target")
        .clone();
    assert_eq!(
        citation.reference_kind,
        Some(doc::ReferenceKind::Citation as i32),
        "ref-type=bibr is a citation on both planes"
    );
}

#[test]
fn a_claim_dependency_names_itself_a_claim_reference() {
    let (_, document) = fold(
        USPTO,
        &ParseConfig {
            emit_inline_spans: true,
            ..ParseConfig::default()
        },
    );
    let dependency = document
        .texts
        .iter()
        .map(base)
        .find(|b| b.text.starts_with("2. The method"))
        .expect("the dependent claim")
        .spans
        .iter()
        .find(|span| span.target.is_some())
        .expect("the claim-ref becomes a target")
        .clone();
    assert_eq!(
        dependency.reference_kind,
        Some(doc::ReferenceKind::Claim as i32)
    );
}

/// The folded article and the reference run of its citing paragraph, with
/// the `xref` rewritten to a different `ref-type` and target.
fn reference_run(xref_attributes: &str) -> (doc::Document, doc::InlineSpan) {
    let article = JATS.replace(r#"ref-type="bibr" rid="b1""#, xref_attributes);
    let (_, document) = fold(
        &article,
        &ParseConfig {
            emit_inline_spans: true,
            ..ParseConfig::default()
        },
    );
    let run = document
        .texts
        .iter()
        .map(base)
        .find(|b| b.text.starts_with("See Rivera"))
        .expect("the citing paragraph")
        .spans
        .iter()
        .find(|span| span.target.is_some())
        .expect("the xref is a target whatever it points at")
        .clone();
    (document, run)
}

#[test]
fn every_kind_the_source_states_lands_on_the_document_plane() {
    // The two vocabularies match member for member now, so a figure
    // reference is a figure reference rather than an unstated one.
    for (attributes, kind) in [
        (r#"ref-type="fig" rid="f1""#, doc::ReferenceKind::Figure),
        (r#"ref-type="table" rid="t1""#, doc::ReferenceKind::Table),
        (
            r#"ref-type="disp-formula" rid="e1""#,
            doc::ReferenceKind::Equation,
        ),
        (r#"ref-type="sec" rid="intro""#, doc::ReferenceKind::Section),
        // A source that names no ref-type still names a cross-reference by
        // writing an xref at all.
        (r#"rid="anything""#, doc::ReferenceKind::CrossRef),
    ] {
        let (_, span) = reference_run(attributes);
        assert_eq!(
            span.reference_kind,
            Some(kind as i32),
            "xref {attributes} folds to {kind:?}"
        );
    }
}

#[test]
fn a_reference_keeps_the_key_the_source_wrote() {
    // A reference into the reference list resolves, and one into nothing
    // does not; both keep the key, so a reader tells them apart without
    // taking the ref string back apart.
    let (document, resolved) = reference_run(r#"ref-type="bibr" rid="b1""#);
    assert_eq!(resolved.reference.as_deref(), Some("b1"));
    let target = resolved.target.expect("a resolved reference has a target");
    let entry = document
        .texts
        .iter()
        .map(base)
        .find(|b| b.self_ref == target.r#ref)
        .expect("the target is an item of this fragment");
    assert_eq!(
        entry.text, "Rivera A. Parsers. 2025.",
        "the reference-list entry the citation names"
    );

    let (_, dangling) = reference_run(r#"ref-type="fig" rid="nowhere""#);
    assert_eq!(dangling.reference.as_deref(), Some("nowhere"));
    assert_eq!(
        dangling.target.map(|t| t.r#ref).as_deref(),
        Some("#nowhere"),
        "an unresolved target still names the source identifier"
    );
}

#[test]
fn monospace_small_caps_and_math_runs_have_their_own_bits() {
    let article = JATS.replace(
        "<italic>concurrent</italic>",
        "<monospace>epoll</monospace> and <sc>MPI</sc> and          <inline-formula>O(n)</inline-formula>",
    );
    let (_, document) = fold(
        &article,
        &ParseConfig {
            emit_inline_spans: true,
            ..ParseConfig::default()
        },
    );
    let spans = document
        .texts
        .iter()
        .map(base)
        .find(|b| b.text.starts_with("Throughput scales"))
        .expect("the paragraph with styled runs")
        .spans
        .clone();
    let bits: Vec<(bool, bool, bool)> = spans
        .iter()
        .filter_map(|span| span.formatting.as_ref())
        .map(|f| (f.monospace, f.small_caps, f.math))
        .collect();
    assert_eq!(
        bits,
        [
            (true, false, false),
            (false, true, false),
            (false, false, true)
        ],
        "three styles the upstream Formatting cannot express, each in its own field"
    );
}

#[test]
fn a_table_cell_carries_its_runs_and_its_declared_alignment() {
    let (_, document) = fold_with_spans(JATS_CALS_TABLE);
    let data = document.tables[0].data.as_ref().expect("table data");

    let emphasized = &data.grid[1].cells[0];
    assert_eq!(emphasized.text, "Für λόγος gemessen");
    assert_eq!(emphasized.spans.len(), 1, "{:?}", emphasized.spans);
    let run = &emphasized.spans[0];
    // Code points, not bytes: the run covers the Greek word exactly.
    let range = run.range.as_ref().expect("a run states its range");
    let covered: String = emphasized
        .text
        .chars()
        .skip(range.start as usize)
        .take((range.end - range.start) as usize)
        .collect();
    assert_eq!(covered, "λόγος");
    assert!(
        run.formatting.as_ref().is_some_and(|f| f.italic),
        "the style folds onto Formatting"
    );
    assert_eq!(
        emphasized.align,
        Some(doc::Alignment::Center as i32),
        "the cell declared its own alignment"
    );
    assert_eq!(
        data.grid[2].cells[1].valign,
        Some(doc::VerticalAlignment::Top as i32)
    );
    assert_eq!(data.grid[1].cells[1].align, None, "nothing declared");

    // The flat list and the grid carry the same cells, runs included.
    let flat = data
        .table_cells
        .iter()
        .find(|cell| cell.text == "Für λόγος gemessen")
        .expect("the flat list holds the same cell");
    assert_eq!(flat.spans.len(), 1);
}

#[test]
fn a_cross_reference_inside_a_cell_resolves_like_one_in_a_paragraph() {
    let (_, document) = fold_with_spans(JATS_CALS_TABLE);
    let entry = ref_of(&document, "Rivera A. Parsers. 2025.");
    let data = document.tables[0].data.as_ref().expect("table data");
    let citing = &data.grid[2].cells[0];
    assert_eq!(citing.text, "Wie Rivera für μ zeigt");
    let span = citing.spans.first().expect("the cell carries its run");
    assert_eq!(
        span.reference.as_deref(),
        Some("b1"),
        "the key the source wrote survives"
    );
    assert_eq!(
        span.target.as_ref().map(|t| t.r#ref.as_str()),
        Some(entry.as_str()),
        "and it points at the reference-list item"
    );
    assert_eq!(
        span.reference_kind,
        Some(doc::ReferenceKind::Citation as i32)
    );
}

#[test]
fn the_declared_column_geometry_reaches_the_table_data() {
    let (_, document) = fold_default(JATS_CALS_TABLE);
    let data = document.tables[0].data.as_ref().expect("table data");
    assert_eq!(data.columns.len(), 2);
    assert_eq!(data.columns[0].name.as_deref(), Some("dialekt"));
    assert_eq!(
        data.columns[0].width_raw.as_deref(),
        Some("2*"),
        "a proportional width keeps its spelling rather than becoming a length"
    );
    assert_eq!(data.columns[0].width, None, "and claims no page unit");
    assert_eq!(data.columns[0].align, Some(doc::Alignment::Left as i32));
    assert_eq!(data.columns[0].valign, None);
    assert_eq!(data.columns[1].name.as_deref(), Some("wert"));
    assert_eq!(data.columns[1].align, Some(doc::Alignment::Right as i32));
    assert_eq!(
        data.columns[1].valign,
        Some(doc::VerticalAlignment::Bottom as i32)
    );
}

#[test]
fn a_foreign_namespace_and_a_cdata_section_reach_the_item_meta() {
    let source = r#"<?xml version="1.0"?>
<doclang xmlns="http://docling-project.org/ns/doclang/v1"
         xmlns:mml="http://www.w3.org/1998/Math/MathML">
  <paragraph><![CDATA[if a < b then « oui »]]></paragraph>
  <mml:formula>α + β</mml:formula>
</doclang>
"#;
    let (_, document) = fold_default(source);
    let paragraph = document
        .texts
        .iter()
        .map(base)
        .find(|item| item.label == doc::DocItemLabel::Paragraph as i32)
        .expect("the paragraph");
    let fields = &paragraph.meta.as_ref().expect("meta").custom_fields;
    assert_eq!(field(fields, "xml.element_name"), Some("paragraph"));
    assert_eq!(
        fields.get("xml.from_cdata").and_then(|v| match v.kind {
            Some(Kind::BoolValue(flag)) => Some(flag),
            _ => None,
        }),
        Some(true)
    );
    assert_eq!(
        field(fields, "xml.namespace"),
        None,
        "the root's own namespace is on the body meta, not repeated per item"
    );

    let formula = document
        .texts
        .iter()
        .map(base)
        .find(|item| item.label == doc::DocItemLabel::Formula as i32)
        .expect("the formula");
    let fields = &formula.meta.as_ref().expect("meta").custom_fields;
    assert_eq!(field(fields, "xml.element_name"), Some("mml:formula"));
    assert_eq!(
        field(fields, "xml.namespace"),
        Some("http://www.w3.org/1998/Math/MathML"),
        "an item from another namespace says which one, because it differs"
    );
    assert_eq!(
        fields.get("xml.from_cdata"),
        None,
        "ordinary character data says nothing rather than saying false"
    );
}

#[test]
fn list_items_fold_into_the_groups_of_the_lists_they_belong_to() {
    let (_, document) = fold_default(DOCLANG_NESTED_LISTS);

    // Two runs of lists, and the nested one, which is three groups. The
    // ordered outer list and the bulleted inner one are labelled apart.
    let labels: Vec<i32> = document.groups.iter().map(|g| g.label).collect();
    assert_eq!(
        labels,
        [
            doc::GroupLabel::OrderedList as i32,
            doc::GroupLabel::List as i32,
            doc::GroupLabel::List as i32,
        ],
        "an ordered list and a bulleted one are not the same group"
    );

    // The inner group hangs off the outer one, not off the section, and the
    // outer one off the body, since nothing has opened a heading.
    assert_eq!(
        document.groups[1].parent.as_ref().map(|p| p.r#ref.as_str()),
        Some("#/groups/0")
    );
    assert_eq!(
        document.groups[0].parent.as_ref().map(|p| p.r#ref.as_str()),
        Some("#/body")
    );

    // Each item names its own list as its parent, and each list lists it.
    let outer = children_of(&document, "#/groups/0");
    let inner = children_of(&document, "#/groups/1");
    assert_eq!(
        texts_of(&document, &inner),
        ["Untereintrag α", "Untereintrag β"],
        "the nested list holds its own items"
    );
    assert_eq!(
        texts_of(&document, &outer),
        ["Erste Stufe", "Zweite Stufe", "Dritte Stufe"],
        "and the outer one keeps the items that surround it"
    );
    assert!(
        outer.contains(&"#/groups/1".to_owned()),
        "the nested list is a child of the list it sits in: {outer:?}"
    );

    // The paragraph between the two runs is not in either list, and it is
    // what ends the first run: the second `ul` opens a group of its own
    // rather than continuing the ordered list.
    assert_eq!(parent_of(&document, "Nach der Liste."), "#/body");
    assert_eq!(
        texts_of(&document, &children_of(&document, "#/groups/2")),
        ["Ein zweiter Lauf"]
    );
}

#[test]
fn a_list_group_knows_it_is_ordered_from_the_list_rather_than_from_an_ordinal() {
    let (_, document) = fold_default(DOCLANG_NESTED_LISTS);
    let ordered: Vec<bool> = document
        .texts
        .iter()
        .filter_map(|item| match item.item.as_ref() {
            Some(doc::base_text_item::Item::ListItem(list)) => Some(list.enumerated),
            _ => None,
        })
        .collect();
    assert_eq!(
        ordered,
        [true, true, false, false, true, false],
        "no item here carries an ordinal; the container is what says so"
    );
}
