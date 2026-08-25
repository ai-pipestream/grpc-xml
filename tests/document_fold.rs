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

use common::{DOCLANG, JATS, JATS_WITH_ISLAND, USPTO, XBRL, client, options, parse_ok};
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

/// The `children` of any ref this fold can parent to: the body, or a section
/// header in the text arena.
fn children_of(document: &doc::Document, self_ref: &str) -> Vec<String> {
    let children = if self_ref == "#/body" {
        &body(document).children
    } else {
        &base(&document.texts[arena_index(self_ref)]).children
    };
    children.iter().map(|child| child.r#ref.clone()).collect()
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
fn a_jats_item_carries_its_locators_in_meta_and_no_provenance() {
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
    assert!(
        item.prov.is_empty(),
        "XML has no pages and no boxes; prov stays empty"
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
    assert!(
        document.texts.is_empty(),
        "an instance is facts, not prose: {:?}",
        document.texts.len()
    );
    assert_eq!(document.tables.len(), 1, "one fact table, created lazily");

    let table = &document.tables[0];
    let fields = &table.meta.as_ref().expect("table meta").custom_fields;
    assert_eq!(field(fields, "xml.table"), Some("facts"));
    let data = table.data.as_ref().expect("table data");
    assert_eq!(data.num_cols, 6);
    assert_eq!(
        usize::try_from(data.num_rows).expect("a row count is not negative"),
        common::facts(&events).len() + 1,
        "one header row plus one row per fact"
    );
    let header: Vec<&str> = data.grid[0].cells.iter().map(|c| c.text.as_str()).collect();
    assert_eq!(
        header,
        ["concept", "context", "period", "unit", "value", "decimals"]
    );
    assert!(data.grid[0].cells.iter().all(|cell| cell.column_header));

    let row: Vec<&str> = data.grid[1].cells.iter().map(|c| c.text.as_str()).collect();
    assert_eq!(
        row,
        [
            "us-gaap:Assets",
            "I2026",
            "2026-12-31",
            "iso4217:USD",
            "1234000000",
            "-6"
        ]
    );
    let duration: Vec<&str> = data.grid[2].cells.iter().map(|c| c.text.as_str()).collect();
    assert_eq!(duration[2], "2026-01-01/2026-12-31");
    let divided: Vec<&str> = data.grid[4].cells.iter().map(|c| c.text.as_str()).collect();
    assert_eq!(divided[3], "iso4217:USD/xbrli:shares");
    assert_eq!(divided[4], "", "a nil fact has no value to render");
    assert!(!data.grid[1].cells[0].column_header, "rows are not headers");
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
    assert!(code.prov.is_empty());
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
    let bare = r#"<?xml version="1.0"?>
<xbrl xmlns="http://www.xbrl.org/2003/instance">
  <context id="c"><entity><identifier scheme="s">e</identifier></entity>
  <period><instant>2026-01-01</instant></period></context>
</xbrl>
"#;
    let (_, document) = fold_default(bare);
    assert!(
        document.source_meta.is_none(),
        "an absent declaration is not an empty one"
    );
}
