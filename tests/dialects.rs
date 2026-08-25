// SPDX-License-Identifier: Apache-2.0

//! Golden tests for the four dialect mappings, over a real server.
//!
//! Following the fleet rule, these compare labels, roles, text and ordering
//! rather than whole protobuf messages: the wire model will gain fields, and
//! a test that fails when it does is testing the schema instead of the
//! mapping.

mod common;

use common::{
    DOCLANG, JATS, JATS_WITH_ISLAND, USPTO, XBRL, client, facts, info, islands, items_labelled,
    items_with_role, options, parse_ok, status, table_rows, text_items, texts, warned,
};
use grpc_xml::proto::v1 as pb;

/// Default options plus the inline-span switch.
fn with_spans() -> pb::ParseOptions {
    pb::ParseOptions {
        emit_inline_spans: true,
        ..options()
    }
}

// ------------------------------------------------------------------- JATS

#[tokio::test]
async fn jats_maps_title_authors_abstract_and_sections() {
    let client = client().await;
    let events = parse_ok(&client, JATS, options()).await;

    let info = info(&events);
    assert_eq!(info.dialect, pb::XmlDialect::Jats as i32);
    assert_eq!(info.evidence, pb::DialectEvidence::RootNamespace as i32);
    assert_eq!(info.root_local_name, "article");
    assert_eq!(info.xml_version.as_deref(), Some("1.0"));
    assert_eq!(info.encoding.as_deref(), Some("UTF-8"));

    assert_eq!(
        texts(&items_labelled(&events, pb::XmlItemLabel::Title)),
        ["Streaming XML Without a DOM"]
    );
    // Inline element siblings inside one capture become separate words, not
    // one run-on token.
    assert_eq!(
        texts(&items_with_role(&events, "author")),
        ["Rivera Ana", "Okafor Chidi"]
    );
    assert_eq!(
        texts(&items_with_role(&events, "abstract")),
        ["A collector need not build a tree to produce a document."]
    );
    assert_eq!(
        texts(&items_with_role(&events, "keyword")),
        ["streaming", "xml"]
    );
    assert_eq!(
        texts(&items_with_role(&events, "affiliation")),
        ["Institute for Pull Parsing"]
    );
    assert_eq!(
        texts(&items_with_role(&events, "article-id:doi")),
        ["10.1234/jsp.2026.001"]
    );
}

#[tokio::test]
async fn jats_section_headers_carry_their_nesting_depth() {
    let client = client().await;
    let events = parse_ok(&client, JATS, options()).await;
    let headers: Vec<(String, Option<u32>)> =
        items_labelled(&events, pb::XmlItemLabel::SectionHeader)
            .iter()
            .map(|i| (i.text.clone(), i.level))
            .collect();
    assert_eq!(
        headers,
        [
            ("Introduction".to_owned(), Some(1)),
            ("Scope".to_owned(), Some(2)),
            ("Results".to_owned(), Some(1)),
        ]
    );
}

#[tokio::test]
async fn jats_items_carry_provenance_and_a_positional_path() {
    let client = client().await;
    let events = parse_ok(&client, JATS, options()).await;
    let items = text_items(&events);
    assert!(!items.is_empty());
    for item in &items {
        let source = item.source.as_ref().expect("every item is attributed");
        assert_eq!(source.collector, "xml");
        assert_eq!(source.model.as_deref(), Some("jats"));
        assert_eq!(source.version.as_deref(), Some(env!("CARGO_PKG_VERSION")));
        assert!(item.path.starts_with("/article"), "path {}", item.path);
    }
    // The second paragraph of the first section is addressed by position; the
    // first is not, so a path is stable and readable at the same time.
    let second = items
        .iter()
        .find(|i| i.text.starts_with("Each item is forwarded"))
        .expect("second body paragraph");
    assert_eq!(second.path, "/article/body/sec/p[2]");
    let first_section_title = items
        .iter()
        .find(|i| i.text == "Introduction")
        .expect("first section title");
    assert_eq!(first_section_title.path, "/article/body/sec/title");
}

#[tokio::test]
async fn jats_tables_stream_as_start_rows_end_with_the_caption_on_the_start() {
    let client = client().await;
    let events = parse_ok(&client, JATS, options()).await;

    let starts: Vec<&pb::TableStart> = events
        .iter()
        .filter_map(|e| match e.event.as_ref() {
            Some(pb::parse_xml_response::Event::TableStart(s)) => Some(s),
            _ => None,
        })
        .collect();
    assert_eq!(starts.len(), 1);
    assert_eq!(starts[0].caption.as_deref(), Some("Throughput by dialect."));
    // The id in JATS lives on the `table-wrap`, not the `table`, so the id
    // field is empty and the path is what locates the table in the source.
    assert_eq!(starts[0].element_id, None);
    assert_eq!(starts[0].path, "/article/body/sec[2]/table-wrap/table");

    let rows = table_rows(&events);
    assert_eq!(rows.len(), 3);
    assert!(rows[0].is_header, "the thead row is a header row");
    assert!(!rows[1].is_header);
    assert_eq!(
        rows[0]
            .cells
            .iter()
            .map(|c| c.text.clone())
            .collect::<Vec<_>>(),
        ["Dialect", "MB/s"]
    );
    assert_eq!(
        rows[2]
            .cells
            .iter()
            .map(|c| c.text.clone())
            .collect::<Vec<_>>(),
        ["XBRL", "240"]
    );
    assert_eq!(rows[1].row_index, 1);

    let ends: Vec<&pb::TableEnd> = events
        .iter()
        .filter_map(|e| match e.event.as_ref() {
            Some(pb::parse_xml_response::Event::TableEnd(end)) => Some(end),
            _ => None,
        })
        .collect();
    assert_eq!(ends.len(), 1);
    assert_eq!(ends[0].row_count, 3);
    assert_eq!(ends[0].column_count, 2);
    assert_eq!(ends[0].table_ref, starts[0].table_ref);
}

#[tokio::test]
async fn jats_figures_and_references_survive_the_mapping() {
    let client = client().await;
    let events = parse_ok(&client, JATS, options()).await;
    assert_eq!(
        texts(&items_labelled(&events, pb::XmlItemLabel::Picture)),
        ["pipeline.png"]
    );
    // A figure caption has no table to attach to, so it arrives on its own.
    assert!(
        texts(&items_labelled(&events, pb::XmlItemLabel::Caption))
            .contains(&"The event pipeline.".to_owned())
    );
    assert_eq!(
        texts(&items_labelled(&events, pb::XmlItemLabel::Reference)),
        ["Rivera A. Parsers. 2025.", "Okafor C. Streams. 2026."]
    );
}

#[tokio::test]
async fn jats_inline_markup_is_flattened_into_one_paragraph() {
    let client = client().await;
    let events = parse_ok(&client, JATS, options()).await;
    let paragraphs = texts(&items_labelled(&events, pb::XmlItemLabel::Paragraph));
    assert!(
        paragraphs.contains(&"Throughput scales with the number of concurrent streams.".to_owned()),
        "inline markup must not split or duplicate the paragraph: {paragraphs:?}"
    );
}

#[tokio::test]
async fn inline_spans_are_off_by_default_and_never_change_the_flat_text() {
    let client = client().await;
    let default_events = parse_ok(&client, JATS, options()).await;
    assert!(
        text_items(&default_events)
            .iter()
            .all(|i| i.spans.is_empty()),
        "inline spans are opt-in"
    );

    let events = parse_ok(&client, JATS, with_spans()).await;
    assert!(
        text_items(&events).iter().any(|i| !i.spans.is_empty()),
        "asking for spans must produce some"
    );
    // The flattening is unchanged: spans describe the same string, they do
    // not rewrite it.
    assert_eq!(
        texts(&text_items(&default_events)),
        texts(&text_items(&events)),
        "the flat text is identical with and without spans"
    );
}

#[tokio::test]
async fn jats_link_and_citation_runs_survive_the_flattening() {
    let client = client().await;
    let events = parse_ok(&client, JATS, with_spans()).await;
    let paragraph = text_items(&events)
        .into_iter()
        .find(|i| i.text.starts_with("See Rivera"))
        .expect("the paragraph that links and cites");
    assert_eq!(paragraph.text, "See Rivera and the specification.");
    assert_eq!(paragraph.spans.len(), 2);

    let citation = &paragraph.spans[0];
    assert_eq!(common::span_text(paragraph, citation), "Rivera");
    assert_eq!(citation.references, ["b1"]);
    assert_eq!(
        citation.reference_kind,
        pb::ReferenceKind::Citation as i32,
        "ref-type=bibr is a citation, not a bare cross-reference"
    );
    assert_eq!(citation.element_name, "xref");

    let link = &paragraph.spans[1];
    assert_eq!(common::span_text(paragraph, link), "specification");
    assert_eq!(
        link.hyperlink.as_deref(),
        Some("https://example.org/spec"),
        "the xlink:href of an ext-link is the whole point"
    );
    assert!(link.references.is_empty());
}

#[tokio::test]
async fn jats_emphasis_lands_on_the_exact_word_it_marked() {
    let client = client().await;
    let events = parse_ok(&client, JATS, with_spans()).await;
    let paragraph = text_items(&events)
        .into_iter()
        .find(|i| i.text.starts_with("Throughput scales"))
        .expect("the paragraph with emphasis");
    let italic = paragraph.spans.first().expect("one italic run");
    assert_eq!(italic.styles, [pb::SpanStyle::Italic as i32]);
    // Offsets are into the collapsed text the item actually carries, which
    // is the property that makes them usable without re-parsing the source.
    assert_eq!(common::span_text(paragraph, italic), "concurrent");
}

#[tokio::test]
async fn uspto_claim_dependencies_reach_the_wire() {
    let client = client().await;
    let events = parse_ok(&client, USPTO, with_spans()).await;
    let claims = items_with_role(&events, "claim");
    assert!(claims[0].spans.is_empty(), "claim 1 depends on nothing");
    let dependency = claims[1]
        .spans
        .first()
        .expect("claim 2 depends on claim 1 and says so");
    assert_eq!(common::span_text(claims[1], dependency), "claim 1");
    assert_eq!(dependency.references, ["CLM-00001"]);
    assert_eq!(dependency.reference_kind, pb::ReferenceKind::Claim as i32);
    assert!(claims[2].spans.is_empty() || !claims[2].spans[0].references.is_empty());
}

#[tokio::test]
async fn span_attributes_follow_the_attribute_option() {
    let client = client().await;
    let events = parse_ok(&client, JATS, with_spans()).await;
    let paragraph = text_items(&events)
        .into_iter()
        .find(|i| i.text.starts_with("See Rivera"))
        .expect("the paragraph that links and cites");
    assert!(
        paragraph.spans.iter().all(|s| s.attributes.is_empty()),
        "span attributes ride include_attributes like every other attribute"
    );

    let events = parse_ok(
        &client,
        JATS,
        pb::ParseOptions {
            include_attributes: true,
            ..with_spans()
        },
    )
    .await;
    let paragraph = text_items(&events)
        .into_iter()
        .find(|i| i.text.starts_with("See Rivera"))
        .expect("the paragraph that links and cites");
    let names: Vec<&str> = paragraph.spans[0]
        .attributes
        .iter()
        .map(|a| a.name.as_str())
        .collect();
    assert_eq!(names, ["ref-type", "rid"]);
}

#[tokio::test]
async fn attributes_are_off_by_default_and_exclude_namespace_declarations() {
    let client = client().await;
    let default_events = parse_ok(&client, JATS, options()).await;
    assert!(
        text_items(&default_events)
            .iter()
            .all(|i| i.attributes.is_empty()),
        "attributes must be opt-in"
    );

    let events = parse_ok(
        &client,
        JATS,
        pb::ParseOptions {
            include_attributes: true,
            ..options()
        },
    )
    .await;
    let with_attrs = text_items(&events);
    let doi = with_attrs
        .iter()
        .find(|i| i.role == "article-id:doi")
        .expect("doi item");
    assert_eq!(
        doi.attributes
            .iter()
            .map(|a| (a.name.as_str(), a.value.as_str()))
            .collect::<Vec<_>>(),
        [("pub-id-type", "doi")]
    );
    assert!(
        with_attrs
            .iter()
            .all(|i| i.attributes.iter().all(|a| !a.name.starts_with("xmlns"))),
        "namespace declarations are not content"
    );
}

#[tokio::test]
async fn the_root_element_attributes_reach_the_wire_without_being_asked_for() {
    let client = client().await;
    let events = parse_ok(&client, JATS, options()).await;
    let info = info(&events);
    let attrs: Vec<(&str, &str)> = info
        .root_attributes
        .iter()
        .map(|a| (a.name.as_str(), a.value.as_str()))
        .collect();
    assert!(
        attrs.contains(&("article-type", "research-article")),
        "{attrs:?}"
    );
    assert!(attrs.contains(&("dtd-version", "1.3")), "{attrs:?}");
    assert!(
        attrs.iter().all(|(name, _)| !name.starts_with("xmlns")),
        "namespace declarations are bindings, not attributes: {attrs:?}"
    );
    assert_eq!(info.language.as_deref(), Some("en"));
}

#[tokio::test]
async fn the_modern_schema_signal_is_decoded_into_pairs() {
    let client = client().await;
    let events = parse_ok(&client, JATS, options()).await;
    let locations: Vec<(&str, &str)> = info(&events)
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
        "xsi:schemaLocation is alternating namespaces and locations, not one string"
    );
}

#[tokio::test]
async fn namespace_bindings_make_a_prefixed_path_resolvable() {
    let client = client().await;
    let events = parse_ok(&client, JATS, options()).await;
    let bindings: Vec<(&str, &str)> = info(&events)
        .namespaces
        .iter()
        .map(|n| (n.prefix.as_str(), n.uri.as_str()))
        .collect();
    assert!(
        bindings.contains(&("", "http://jats.nlm.nih.gov/ns/archiving/1.3/")),
        "the default binding is the one with the empty prefix: {bindings:?}"
    );
    assert!(
        bindings.contains(&("xlink", "http://www.w3.org/1999/xlink")),
        "{bindings:?}"
    );
}

#[tokio::test]
async fn a_malformed_schema_location_drops_the_unpaired_tail() {
    // An odd token count means the last namespace names no document; a
    // fabricated empty location would be worse than reporting nothing.
    let article = JATS.replace(
        r#"xsi:schemaLocation="http://jats.nlm.nih.gov/ns/archiving/1.3/ JATS-archivearticle1.xsd""#,
        r#"xsi:schemaLocation="urn:a a.xsd urn:b""#,
    );
    let client = client().await;
    let events = parse_ok(&client, &article, options()).await;
    let locations: Vec<(&str, &str)> = info(&events)
        .schema_locations
        .iter()
        .map(|l| (l.namespace.as_str(), l.location.as_str()))
        .collect();
    assert_eq!(locations, [("urn:a", "a.xsd")]);
}

// ------------------------------------------------------------------ USPTO

#[tokio::test]
async fn uspto_is_sniffed_from_its_doctype_and_maps_the_grant() {
    let client = client().await;
    let events = parse_ok(&client, USPTO, options()).await;

    let info = info(&events);
    assert_eq!(info.dialect, pb::XmlDialect::Uspto as i32);
    assert_eq!(info.evidence, pb::DialectEvidence::PublicId as i32);
    assert_eq!(info.doctype_name.as_deref(), Some("us-patent-grant"));
    assert_eq!(
        info.public_id.as_deref(),
        Some("-//USPTO//DTD ICE Patent Grant V4.5 2014//EN")
    );
    assert_eq!(
        info.system_id.as_deref(),
        Some("us-patent-grant-v45-2014-04-03.dtd"),
        "a relative DTD name is recorded, never fetched"
    );
    assert!(
        warned(&events, pb::WarningCode::ExternalIdIgnored),
        "recording an external identifier is reported, not silent"
    );

    assert_eq!(
        texts(&items_labelled(&events, pb::XmlItemLabel::Title)),
        ["Method for streaming structured documents"]
    );
    assert_eq!(texts(&items_with_role(&events, "inventor")), ["Rivera Ana"]);
    assert_eq!(
        texts(&items_with_role(&events, "assignee")),
        ["Acme Streaming Corp"]
    );
    assert_eq!(
        texts(&items_with_role(&events, "document-number")),
        ["11999999"]
    );
    assert_eq!(
        texts(&items_with_role(&events, "application-number")),
        ["17123456"]
    );
}

#[tokio::test]
async fn uspto_claims_stream_in_order_with_their_numbers() {
    let client = client().await;
    let events = parse_ok(&client, USPTO, options()).await;
    let claims = items_with_role(&events, "claim");
    assert_eq!(claims.len(), 3);
    assert_eq!(
        claims.iter().map(|c| c.ordinal).collect::<Vec<_>>(),
        [Some(1), Some(2), Some(3)],
        "zero-padded claim numbers become ordinals"
    );
    assert!(claims[0].text.starts_with("1. A method comprising"));
    // Claims arrive in document order, and their stream indexes say so.
    assert!(claims[0].index < claims[1].index && claims[1].index < claims[2].index);
}

#[tokio::test]
async fn uspto_separates_description_abstract_and_drawing_prose() {
    let client = client().await;
    let events = parse_ok(&client, USPTO, options()).await;
    assert_eq!(
        texts(&items_with_role(&events, "abstract")),
        ["A method streams document items as they are parsed."]
    );
    assert_eq!(
        texts(&items_with_role(&events, "drawing-description")),
        ["FIG. 1 is a block diagram of the pipeline."]
    );
    assert_eq!(
        texts(&items_with_role(&events, "description")),
        [
            "Prior systems buffered the entire document before emitting it.",
            "The parser emits an event per completed element.",
        ]
    );
    assert_eq!(
        texts(&items_labelled(&events, pb::XmlItemLabel::SectionHeader)),
        ["BACKGROUND", "DETAILED DESCRIPTION"]
    );
    assert_eq!(
        texts(&items_labelled(&events, pb::XmlItemLabel::Picture)),
        ["US11999999-20260210-D00001.TIF"]
    );
}

// ------------------------------------------------------------------- XBRL

#[tokio::test]
async fn xbrl_streams_facts_with_their_contexts_and_units_resolved() {
    let client = client().await;
    let events = parse_ok(&client, XBRL, options()).await;

    assert_eq!(info(&events).dialect, pb::XmlDialect::Xbrl as i32);
    let counts = status(&events).counts.as_ref().unwrap();
    assert_eq!(counts.contexts, 3);
    assert_eq!(counts.units, 2);
    assert_eq!(counts.facts, 4);

    let facts = facts(&events);
    assert_eq!(facts.len(), 4);

    let assets = facts[0];
    assert_eq!(assets.concept_local_name, "Assets");
    assert_eq!(assets.concept_prefix, "us-gaap");
    assert_eq!(assets.concept_namespace, "http://fasb.org/us-gaap/2026");
    assert_eq!(assets.label, "Assets", "v1 labels are local names");
    assert_eq!(assets.value, "1234000000");
    assert_eq!(assets.decimals.as_deref(), Some("-6"));
    let context = assets.context.as_ref().expect("context resolved inline");
    assert_eq!(context.id, "I2026");
    assert_eq!(context.entity_identifier.as_deref(), Some("0000123456"));
    assert_eq!(
        context.entity_scheme.as_deref(),
        Some("http://www.sec.gov/CIK")
    );
    assert_eq!(
        context.period.as_ref().unwrap().instant.as_deref(),
        Some("2026-12-31")
    );
    let unit = assets.unit.as_ref().expect("unit resolved inline");
    assert_eq!(unit.measures, ["iso4217:USD"]);

    let duration = facts[1].context.as_ref().unwrap().period.as_ref().unwrap();
    assert_eq!(duration.start_date.as_deref(), Some("2026-01-01"));
    assert_eq!(duration.end_date.as_deref(), Some("2026-12-31"));
}

#[tokio::test]
async fn xbrl_carries_dimensions_divide_units_and_nil_facts() {
    let client = client().await;
    let events = parse_ok(&client, XBRL, options()).await;
    let facts = facts(&events);

    let dimensioned = facts[2].context.as_ref().unwrap();
    assert_eq!(dimensioned.dimensions.len(), 1);
    assert_eq!(
        dimensioned.dimensions[0].dimension,
        "us-gaap:StatementGeographicalAxis"
    );
    assert_eq!(
        dimensioned.dimensions[0].member.as_deref(),
        Some("us-gaap:NorthAmericaMember")
    );
    assert!(
        !dimensioned.dimensions[0].is_scenario,
        "this one is a segment"
    );

    let per_share = facts[3];
    assert!(
        per_share.is_nil,
        "xsi:nil is decoded, not guessed from an empty value"
    );
    assert_eq!(per_share.value, "");
    let unit = per_share.unit.as_ref().expect("divide unit resolved");
    assert!(unit.measures.is_empty());
    assert_eq!(unit.numerator_measures, ["iso4217:USD"]);
    assert_eq!(unit.denominator_measures, ["xbrli:shares"]);
}

#[tokio::test]
async fn xbrl_without_a_taxonomy_still_returns_facts_and_says_so_when_given_one() {
    let client = client().await;
    let without = parse_ok(&client, XBRL, options()).await;
    assert_eq!(facts(&without).len(), 4);
    assert!(!warned(&without, pb::WarningCode::TaxonomyIgnored));

    let with = parse_ok(
        &client,
        XBRL,
        pb::ParseOptions {
            taxonomy: b"PK\x03\x04 not really a taxonomy package".to_vec(),
            ..options()
        },
    )
    .await;
    assert_eq!(facts(&with).len(), 4, "a taxonomy never costs facts");
    assert!(
        warned(&with, pb::WarningCode::TaxonomyIgnored),
        "v1 must admit it ignored the taxonomy rather than imply it used it"
    );
    assert!(
        facts(&with).iter().all(|f| f.label == f.concept_local_name),
        "labels stay local names"
    );
}

#[tokio::test]
async fn xbrl_records_the_schema_reference_without_fetching_it() {
    let client = client().await;
    let events = parse_ok(&client, XBRL, options()).await;
    assert!(warned(&events, pb::WarningCode::ExternalIdIgnored));
}

// ---------------------------------------------------------------- DocLang

#[tokio::test]
async fn doclang_decodes_both_element_named_and_label_attributed_items() {
    let client = client().await;
    let events = parse_ok(&client, DOCLANG, options()).await;

    assert_eq!(info(&events).dialect, pb::XmlDialect::Doclang as i32);
    assert_eq!(
        texts(&items_labelled(&events, pb::XmlItemLabel::Title)),
        ["Quarterly Operations Review"]
    );
    let headers: Vec<(String, Option<u32>)> =
        items_labelled(&events, pb::XmlItemLabel::SectionHeader)
            .iter()
            .map(|i| (i.text.clone(), i.level))
            .collect();
    assert_eq!(
        headers,
        [
            ("Summary".to_owned(), Some(1)),
            // No level attribute here, so nesting supplies it.
            ("Outlook".to_owned(), Some(2)),
        ]
    );
    let list = items_labelled(&events, pb::XmlItemLabel::ListItem);
    assert_eq!(
        list.iter().map(|i| i.ordinal).collect::<Vec<_>>(),
        [Some(1), Some(2)]
    );
    assert_eq!(
        texts(&items_labelled(&events, pb::XmlItemLabel::Footnote)),
        ["Figures are unaudited."],
        "the generic `item` form is decoded from its label attribute"
    );
    // `metadata` is skipped, so its text never reaches the stream.
    assert!(
        !text_items(&events)
            .iter()
            .any(|i| i.text.contains("quarterly.pdf")),
        "skipped subtrees produce nothing"
    );
}

#[tokio::test]
async fn doclang_tables_use_the_cals_style_row_and_cell_names() {
    let client = client().await;
    let events = parse_ok(&client, DOCLANG, options()).await;
    let rows = table_rows(&events);
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0]
            .cells
            .iter()
            .map(|c| c.text.clone())
            .collect::<Vec<_>>(),
        ["Region", "Delta"]
    );
    let starts: Vec<&pb::TableStart> = events
        .iter()
        .filter_map(|e| match e.event.as_ref() {
            Some(pb::parse_xml_response::Event::TableStart(s)) => Some(s),
            _ => None,
        })
        .collect();
    assert_eq!(starts[0].caption.as_deref(), Some("Regional throughput."));
}

// ---------------------------------------------------------------- islands

#[tokio::test]
async fn xhtml_is_flattened_by_default_and_handed_off_on_request() {
    let client = client().await;

    let flattened = parse_ok(&client, JATS_WITH_ISLAND, options()).await;
    assert!(islands(&flattened).is_empty(), "islands are opt-in");

    let events = parse_ok(
        &client,
        JATS_WITH_ISLAND,
        pb::ParseOptions {
            emit_html_islands: true,
            ..options()
        },
    )
    .await;
    let islands = islands(&events);
    assert_eq!(islands.len(), 1);
    let island = islands[0];
    assert_eq!(island.namespace, "http://www.w3.org/1999/xhtml");
    assert_eq!(island.element_id.as_deref(), Some("widget"));
    let html = String::from_utf8(island.html.clone()).expect("islands are UTF-8");
    assert!(html.starts_with("<xhtml:div"), "{html}");
    assert!(html.contains("<xhtml:em>HTML</xhtml:em>"), "{html}");
    assert!(html.ends_with("</xhtml:div>"), "{html}");
    assert_eq!(island.source.as_ref().unwrap().collector, "xml");

    // The island replaces the flattening; the paragraphs around it are
    // untouched.
    let paragraphs = texts(&items_labelled(&events, pb::XmlItemLabel::Paragraph));
    assert_eq!(
        paragraphs,
        ["Text before the island.", "Text after the island."]
    );
}

// ----------------------------------------------------------- service info

#[tokio::test]
async fn service_info_reports_the_policy_that_is_compiled_in() {
    let mut client = client().await;
    let info = client
        .get_service_info(pb::GetServiceInfoRequest {})
        .await
        .expect("service info")
        .into_inner();
    assert_eq!(info.service, "grpc-xml");
    assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
    assert!(info.parser.starts_with("quick-xml"));
    assert_eq!(
        info.dialects,
        [
            pb::XmlDialect::Jats as i32,
            pb::XmlDialect::Uspto as i32,
            pb::XmlDialect::Xbrl as i32,
            pb::XmlDialect::Doclang as i32,
            pb::XmlDialect::Dclx as i32,
            pb::XmlDialect::MetsGbs as i32,
        ]
    );
    assert!(info.entity_expansion_disabled);
    assert!(info.default_max_document_mib > 0);
    assert!(info.ceiling_max_document_mib >= info.default_max_document_mib);
    assert!(info.max_concurrent_parses > 0);
    let ui = info.ui.expect("ui advertisement");
    assert_eq!(ui.title, "XML");
    assert_eq!(ui.path, "/ui/xml");
    assert_eq!(
        ui.description,
        "Declarative XML to the gRParse Document data plane"
    );
}
