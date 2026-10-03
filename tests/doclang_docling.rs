// SPDX-License-Identifier: Apache-2.0

//! `DocLang` as Docling itself writes it, checked against the deserializer
//! fixes docling-core shipped in October 2026 (docling-core #803, #804, #811,
//! #820 and #824).
//!
//! Each upstream fix came with an input that its deserializer got wrong. This
//! reader is built differently — one streaming pass that captures an item's
//! text instead of a tree walk that rebuilds inline groups — so most of those
//! bugs cannot happen here. The inputs are kept anyway, adapted where the
//! vocabulary differs, so a regression that would reproduce one of them fails
//! here first. Where the input did show a gap in this reader, the test pins
//! the fix.
//!
//! The fragments use Docling's own tag names (`text`, `heading`,
//! `superscript`, `page_header`, `location`, `layer`) with no namespace and
//! `version="0.7"` on the root, which is what a Docling serializer emits.

mod common;

use common::{client, items_labelled, options, parse, parse_ok, status, text_items, warned};
use grpc_xml::document::v1 as doc;
use grpc_xml::document_fold::integrity_errors;
use grpc_xml::proto::v1 as pb;
use tonic::Code;

/// A fragment wrapped in the root element a Docling serializer writes.
fn doclang(body: &str) -> String {
    format!("<doclang version=\"0.7\">{body}</doclang>")
}

/// Spans on, so formatting runs are visible.
fn with_spans() -> pb::ParseOptions {
    pb::ParseOptions {
        emit_inline_spans: true,
        ..options()
    }
}

/// Spans and the Document projection on.
fn with_document() -> pb::ParseOptions {
    pb::ParseOptions {
        emit_document: true,
        ..with_spans()
    }
}

/// Opt in to repairing unescaped text.
fn repairing() -> pb::ParseOptions {
    pb::ParseOptions {
        repair_unescaped_text: true,
        ..options()
    }
}

/// The folded Document of a stream, checked against the merge contract.
fn document(events: &[pb::ParseXmlResponse]) -> &doc::Document {
    let document = events
        .iter()
        .find_map(|e| match e.event.as_ref() {
            Some(pb::parse_xml_response::Event::Document(document)) => Some(document),
            _ => None,
        })
        .expect("emit_document was set");
    let errors = integrity_errors(document);
    assert!(errors.is_empty(), "integrity: {errors:?}");
    document
}

/// The base fields of a folded text item, whichever variant holds them.
fn base(item: &doc::BaseTextItem) -> &doc::TextItemBase {
    match item.item.as_ref().expect("variant set") {
        doc::base_text_item::Item::Text(t) => t.base.as_ref(),
        doc::base_text_item::Item::Title(t) => t.base.as_ref(),
        doc::base_text_item::Item::SectionHeader(t) => t.base.as_ref(),
        doc::base_text_item::Item::ListItem(t) => t.base.as_ref(),
        doc::base_text_item::Item::Formula(t) => t.base.as_ref(),
        other => panic!("unexpected variant {other:?}"),
    }
    .expect("base set")
}

/// Text of every item, in stream order.
fn all_texts(events: &[pb::ParseXmlResponse]) -> Vec<String> {
    text_items(events).iter().map(|i| i.text.clone()).collect()
}

/// The `(run text, styles)` pairs of an item.
fn runs(item: &pb::TextItem) -> Vec<(String, Vec<pb::SpanStyle>)> {
    item.spans
        .iter()
        .map(|span| {
            (
                common::span_text(item, span),
                span.styles
                    .iter()
                    .map(|s| pb::SpanStyle::try_from(*s).expect("known style"))
                    .collect(),
            )
        })
        .collect()
}

// ------------------------------------------------- #803: lenient text repair

#[tokio::test]
async fn unescaped_text_is_refused_unless_the_caller_opts_in() {
    let client = client().await;
    for body in ["<text>a & b</text>", "<text>p <0.05</text>"] {
        let refused = parse(&client, &doclang(body), options())
            .await
            .expect_err("unescaped text is not well-formed XML");
        assert_eq!(refused.code(), Code::InvalidArgument, "{body}");
        let explicit_off = pb::ParseOptions {
            repair_unescaped_text: false,
            ..options()
        };
        let refused = parse(&client, &doclang(body), explicit_off)
            .await
            .expect_err("false is the default");
        assert_eq!(refused.code(), Code::InvalidArgument, "{body}");
    }
}

#[tokio::test]
async fn repair_escapes_what_can_only_be_text_like_docling_does() {
    let client = client().await;
    // docling-core #803's own table. The last case differs on purpose:
    // Docling splits a CDATA section into its own inline run, and this reader
    // captures the element's text as one item.
    for (body, expected) in [
        ("Wiley & Sons", "Wiley & Sons"),
        (
            "$q < 0$ and p <0.05 and $a<\\ln b$",
            "$q < 0$ and p <0.05 and $a<\\ln b$",
        ),
        ("x]]> y", "x]]> y"),
        ("a\u{18}b", "ab"),
        ("<![CDATA[a & b < c]]> & d", "a & b < c & d"),
    ] {
        let events = parse_ok(
            &client,
            &doclang(&format!("<text>{body}</text>")),
            repairing(),
        )
        .await;
        assert_eq!(all_texts(&events), [expected], "{body:?}");
        if body != "x]]> y" {
            // A stray `]]>` was never refused, so nothing was repaired.
            assert!(
                warned(&events, pb::WarningCode::TextRepaired),
                "a repair is never silent: {body:?}"
            );
        }
    }
}

#[tokio::test]
async fn repair_leaves_well_formed_markup_and_references_alone() {
    let client = client().await;
    let source = doclang(
        "<text>a &amp; &#60; &#x3E;<![CDATA[ & < ]]></text><heading level=\"1\">H</heading>\
         <!-- a < b & c --><text>\u{65e5}<bold>\u{672c}</bold></text>",
    );
    let plain = parse_ok(&client, &source, with_spans()).await;
    let repaired = parse_ok(
        &client,
        &source,
        pb::ParseOptions {
            repair_unescaped_text: true,
            ..with_spans()
        },
    )
    .await;
    assert_eq!(text_items(&plain), text_items(&repaired));
    assert!(!warned(&repaired, pb::WarningCode::TextRepaired));
}

// ------------------------------------ #804: mixed content and formatting runs

#[tokio::test]
async fn leading_text_before_a_lone_formatting_child_is_kept() {
    let client = client().await;
    // docling-core #804's table. Docling dropped the "2" of "2nd" and kept
    // only the innermost run of nested formatting; a capture flattens the
    // whole element, so the text is complete and the runs overlay it.
    for (body, label, text) in [
        (
            "<text>2<superscript>nd</superscript></text>",
            pb::XmlItemLabel::Paragraph,
            "2nd",
        ),
        (
            "<text>plain <bold>b</bold></text>",
            pb::XmlItemLabel::Paragraph,
            "plain b",
        ),
        (
            "<footnote>see <italic>ibid</italic></footnote>",
            pb::XmlItemLabel::Footnote,
            "see ibid",
        ),
        (
            "<formula>E <bold>=</bold></formula>",
            pb::XmlItemLabel::Formula,
            "E =",
        ),
        (
            "<text>x <bold>a <italic>b</italic></bold></text>",
            pb::XmlItemLabel::Paragraph,
            "x a b",
        ),
        (
            "<text><superscript>nd</superscript> place</text>",
            pb::XmlItemLabel::Paragraph,
            "nd place",
        ),
        (
            "<text>H<subscript>2</subscript>O</text>",
            pb::XmlItemLabel::Paragraph,
            "H2O",
        ),
    ] {
        let events = parse_ok(&client, &doclang(body), with_spans()).await;
        let items = text_items(&events);
        assert_eq!(items.len(), 1, "{body}");
        assert_eq!(items[0].label, label as i32, "{body}");
        assert_eq!(items[0].text, text, "{body}");
    }
}

#[tokio::test]
async fn nested_formatting_keeps_every_style_on_the_words_it_covers() {
    let client = client().await;
    let events = parse_ok(
        &client,
        &doclang("<text>x <bold>a <italic>b</italic></bold></text>"),
        with_spans(),
    )
    .await;
    // Runs overlap rather than being cut into exclusive pieces, so "b" is
    // under both the bold run and the italic one and "x" under neither.
    assert_eq!(
        runs(text_items(&events)[0]),
        [
            ("a b".to_owned(), vec![pb::SpanStyle::Bold]),
            ("b".to_owned(), vec![pb::SpanStyle::Italic]),
        ]
    );
}

#[tokio::test]
async fn doclings_own_formatting_names_become_runs() {
    let client = client().await;
    let events = parse_ok(
        &client,
        &doclang(
            "<text>2<superscript>nd</superscript></text>\
             <text>H<subscript>2</subscript>O</text>\
             <text>was <strikethrough>struck</strikethrough></text>",
        ),
        with_spans(),
    )
    .await;
    let items = text_items(&events);
    assert_eq!(
        items.iter().map(|i| runs(i)).collect::<Vec<_>>(),
        [
            vec![("nd".to_owned(), vec![pb::SpanStyle::Superscript])],
            vec![("2".to_owned(), vec![pb::SpanStyle::Subscript])],
            vec![("struck".to_owned(), vec![pb::SpanStyle::Strikethrough])],
        ]
    );
}

#[tokio::test]
async fn a_mixed_content_footnote_keeps_its_label() {
    let client = client().await;
    let events = parse_ok(
        &client,
        &doclang(
            "<footnote><location value=\"87\"/><location value=\"417\"/>\
             <location value=\"387\"/><location value=\"426\"/>\
             <superscript>\u{2460}</superscript>\u{76d0}\u{6c60}\u{53bf}\u{53bf}\u{5fd7}</footnote>",
        ),
        with_spans(),
    )
    .await;
    let footnotes = items_labelled(&events, pb::XmlItemLabel::Footnote);
    assert_eq!(footnotes.len(), 1);
    assert_eq!(
        footnotes[0].text,
        "\u{2460}\u{76d0}\u{6c60}\u{53bf}\u{53bf}\u{5fd7}"
    );
    assert_eq!(
        runs(footnotes[0]),
        [("\u{2460}".to_owned(), vec![pb::SpanStyle::Superscript])]
    );
}

#[tokio::test]
async fn page_headers_and_footers_keep_their_label_and_fold_into_furniture() {
    let client = client().await;
    let events = parse_ok(
        &client,
        &doclang(
            "<page_header><layer value=\"furniture\"/><location value=\"1\"/>\
             <location value=\"2\"/><location value=\"3\"/><location value=\"4\"/>\
             Running <italic>head</italic></page_header>\
             <text>Body text.</text>\
             <page_footer>Page <bold>3</bold></page_footer>",
        ),
        with_document(),
    )
    .await;
    // Before: both were unmapped, their text dropped with a warning, which
    // is the worse form of the bug docling-core #804 fixed.
    assert!(
        !warned(&events, pb::WarningCode::UnmappedElement),
        "{:?}",
        status(&events).warnings
    );
    let headers = items_labelled(&events, pb::XmlItemLabel::PageHeader);
    assert_eq!(
        headers.iter().map(|i| i.text.as_str()).collect::<Vec<_>>(),
        ["Running head"]
    );
    assert_eq!(
        runs(headers[0]),
        [("head".to_owned(), vec![pb::SpanStyle::Italic])]
    );
    let footers = items_labelled(&events, pb::XmlItemLabel::PageFooter);
    assert_eq!(
        footers.iter().map(|i| i.text.as_str()).collect::<Vec<_>>(),
        ["Page 3"]
    );

    // In the Document, page chrome is furniture: under the furniture root,
    // in the furniture layer, out of the body a reader iterates.
    let document = document(&events);
    let placed: Vec<(String, i32, i32, String)> = document
        .texts
        .iter()
        .map(base)
        .map(|b| {
            (
                b.text.clone(),
                b.label,
                b.content_layer,
                b.parent
                    .as_ref()
                    .map(|p| p.r#ref.clone())
                    .unwrap_or_default(),
            )
        })
        .collect();
    assert_eq!(
        placed,
        [
            (
                "Running head".to_owned(),
                doc::DocItemLabel::PageHeader as i32,
                doc::ContentLayer::Furniture as i32,
                "#/furniture".to_owned()
            ),
            (
                "Body text.".to_owned(),
                doc::DocItemLabel::Paragraph as i32,
                doc::ContentLayer::Body as i32,
                "#/body".to_owned()
            ),
            (
                "Page 3".to_owned(),
                doc::DocItemLabel::PageFooter as i32,
                doc::ContentLayer::Furniture as i32,
                "#/furniture".to_owned()
            ),
        ]
    );
}

#[tokio::test]
async fn the_label_attribute_spells_page_chrome_too() {
    let client = client().await;
    let events = parse_ok(
        &client,
        &doclang(
            "<item label=\"page_header\">Kopf</item><item label=\"page-footer\">Fu\u{df}</item>",
        ),
        options(),
    )
    .await;
    assert_eq!(
        text_items(&events)
            .iter()
            .map(|i| (i.label, i.text.as_str()))
            .collect::<Vec<_>>(),
        [
            (pb::XmlItemLabel::PageHeader as i32, "Kopf"),
            (pb::XmlItemLabel::PageFooter as i32, "Fu\u{df}"),
        ]
    );
}

// ------------------------------------------------------ #811: empty <text>

#[tokio::test]
async fn an_empty_element_is_no_item_and_never_an_empty_group() {
    let client = client().await;
    let locations = concat!(
        "<location value=\"10\"/><location value=\"20\"/>",
        "<location value=\"30\"/><location value=\"40\"/>"
    );
    for body in [
        "<text>a</text><text></text>".to_owned(),
        "<text>a</text><text>\n  </text>".to_owned(),
        format!("<text>a</text><text><layer value=\"furniture\"/>{locations}</text>"),
        "<text>a</text><formula></formula><formula/>".to_owned(),
        "<text>a</text><code></code><code/>".to_owned(),
    ] {
        let events = parse_ok(&client, &doclang(&body), with_document()).await;
        // Docling now keeps an empty item so the item's box and layer
        // survive. This reader reads neither from a plain DocLang document,
        // so an empty item would carry nothing; it is dropped instead, and,
        // unlike Docling before the fix, never becomes a childless group.
        assert_eq!(all_texts(&events), ["a"], "{body}");
        let document = document(&events);
        assert!(document.groups.is_empty(), "{body}: {:?}", document.groups);
    }
}

// -------------------------------------------- #820: empty list item bodies

#[tokio::test]
async fn an_empty_list_item_never_replays_its_siblings() {
    let client = client().await;
    let events = parse_ok(
        &client,
        &doclang(
            "<list><list-item>Alpha</list-item>\
             <list-item><location value=\"1\"/><location value=\"2\"/>\
             <location value=\"3\"/><location value=\"4\"/></list-item>\
             <list-item>Beta</list-item></list>",
        ),
        with_document(),
    )
    .await;
    assert_eq!(all_texts(&events), ["Alpha", "Beta"]);
    let document = document(&events);
    assert!(document.groups.iter().all(|g| !g.children.is_empty()));
}

#[tokio::test]
async fn doclings_ldiv_list_never_duplicates_and_never_drops_silently() {
    // Docling writes a list as `<ldiv/>` markers between item bodies. This
    // reader does not map that shape yet; what it must never do is the
    // docling-core #820 bug (replay the list into the empty slot), or drop
    // the items without saying so.
    let client = client().await;
    let events = parse_ok(
        &client,
        &doclang(
            "<list>\n<ldiv/><content>Alpha</content>\n\
             <ldiv/><location value=\"1\"/><location value=\"2\"/>\
             <location value=\"3\"/><location value=\"4\"/>\n\
             <ldiv/><content>Beta</content>\n</list>",
        ),
        options(),
    )
    .await;
    for word in ["Alpha", "Beta"] {
        assert!(
            all_texts(&events)
                .iter()
                .filter(|t| t.contains(word))
                .count()
                <= 1,
            "{word} appears at most once"
        );
    }
    if !all_texts(&events).iter().any(|t| t.contains("Alpha")) {
        assert!(warned(&events, pb::WarningCode::UnmappedElement));
    }
}

// ------------------------------------ #824: satellites with nested content

#[tokio::test]
async fn a_float_footnote_with_nested_content_keeps_that_content() {
    let client = client().await;
    let events = parse_ok(
        &client,
        &doclang(
            "<group><table><row><cell>a</cell></row></table>\
             <footnote><field_region><field_item><key>K:</key><value>V</value>\
             </field_item></field_region></footnote>\
             <footnote>Plain note.</footnote></group>",
        ),
        with_document(),
    )
    .await;
    // Docling skipped a satellite with no text of its own together with its
    // content. A capture keeps every descendant's text; the field structure
    // flattens, which is what a capture does to any markup it does not map.
    assert_eq!(
        items_labelled(&events, pb::XmlItemLabel::Footnote)
            .iter()
            .map(|i| i.text.as_str())
            .collect::<Vec<_>>(),
        ["K: V", "Plain note."]
    );
    document(&events);
}

#[tokio::test]
async fn an_extra_caption_with_nested_content_keeps_that_content() {
    let client = client().await;
    let events = parse_ok(
        &client,
        &doclang(
            "<group><caption>First</caption><table><row><cell>a</cell></row></table>\
             <caption><field_region><field_item><key>K:</key><value>V</value>\
             </field_item></field_region></caption></group>",
        ),
        with_document(),
    )
    .await;
    let starts: Vec<Option<&str>> = events
        .iter()
        .filter_map(|e| match e.event.as_ref() {
            Some(pb::parse_xml_response::Event::TableStart(s)) => Some(s.caption.as_deref()),
            _ => None,
        })
        .collect();
    assert_eq!(starts, [Some("First")]);
    assert_eq!(
        items_labelled(&events, pb::XmlItemLabel::Caption)
            .iter()
            .map(|i| i.text.as_str())
            .collect::<Vec<_>>(),
        ["K: V"]
    );
    document(&events);
}
