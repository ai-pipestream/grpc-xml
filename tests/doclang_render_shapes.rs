// SPDX-License-Identifier: Apache-2.0

//! The nested `DocLang` shapes gRParse's renderer writes, read back here.
//!
//! The fragments are that renderer's output, indentation included: a host
//! holding its inline runs, a nested list as a sibling inside its list, a
//! float's captions before it and footnotes after it with nested blocks
//! inside them, and a list holding blocks that are not list items. Each
//! must read back with its text whole, escaped text decoded, every caption
//! kept and in document order.

mod common;

use common::{client, items_labelled, options, parse_ok, text_items};
use grpc_xml::document_fold::integrity_errors;
use grpc_xml::proto::v1 as pb;

/// A rendered body inside the namespaced root the renderer writes.
fn rendered(body: &str) -> String {
    format!("<doclang xmlns=\"http://docling-project.org/ns/doclang/v1\">\n{body}</doclang>")
}

/// The Document projection on, so every case also passes the fold.
fn with_document() -> pb::ParseOptions {
    pb::ParseOptions {
        emit_document: true,
        ..options()
    }
}

/// `(label, text)` of every item, in stream order.
fn labelled(events: &[pb::ParseXmlResponse]) -> Vec<(pb::XmlItemLabel, String)> {
    text_items(events)
        .iter()
        .map(|item| {
            (
                pb::XmlItemLabel::try_from(item.label).expect("known label"),
                item.text.clone(),
            )
        })
        .collect()
}

/// The folded Document passes the merge contract.
fn assert_document_is_sound(events: &[pb::ParseXmlResponse]) {
    let document = events
        .iter()
        .find_map(|e| match e.event.as_ref() {
            Some(pb::parse_xml_response::Event::Document(document)) => Some(document),
            _ => None,
        })
        .expect("emit_document was set");
    let errors = integrity_errors(document);
    assert!(errors.is_empty(), "integrity: {errors:?}");
}

#[tokio::test]
async fn hosts_keep_their_runs_and_nested_lists_keep_their_depth() {
    let client = client().await;
    let events = parse_ok(
        &client,
        &rendered(
            "  <footnote>\u{2460} Yanchi county gazetteer</footnote>\n\
             \x20 <footnote>See p. 4</footnote>\n\
             \x20 <paragraph>a later child</paragraph>\n\
             \x20 <list ordered=\"false\">\n\
             \x20   <list-item>bold lead</list-item>\n\
             \x20   <list ordered=\"false\">\n\
             \x20     <list-item>nested</list-item>\n\
             \x20   </list>\n\
             \x20 </list>\n",
        ),
        with_document(),
    )
    .await;
    assert_eq!(
        labelled(&events),
        [
            (
                pb::XmlItemLabel::Footnote,
                "\u{2460} Yanchi county gazetteer".to_owned()
            ),
            (pb::XmlItemLabel::Footnote, "See p. 4".to_owned()),
            (pb::XmlItemLabel::Paragraph, "a later child".to_owned()),
            (pb::XmlItemLabel::ListItem, "bold lead".to_owned()),
            (pb::XmlItemLabel::ListItem, "nested".to_owned()),
        ]
    );
    let depths: Vec<Option<u32>> = items_labelled(&events, pb::XmlItemLabel::ListItem)
        .iter()
        .map(|item| item.list_depth)
        .collect();
    assert_eq!(depths, [Some(1), Some(2)]);
    assert_document_is_sound(&events);
}

#[tokio::test]
async fn a_footnote_after_its_table_keeps_its_nested_blocks_as_one_item() {
    let client = client().await;
    let events = parse_ok(
        &client,
        &rendered(
            "  <table>\n    <tr>\n      <td>x</td>\n    </tr>\n  </table>\n\
             \x20 <footnote>\n\
             \x20   <paragraph>K:</paragraph>\n\
             \x20   <paragraph>V</paragraph>\n\
             \x20 </footnote>\n\
             \x20 <footnote>Plain note.</footnote>\n\
             \x20 <paragraph>after</paragraph>\n",
        ),
        with_document(),
    )
    .await;
    assert_eq!(
        labelled(&events),
        [
            (pb::XmlItemLabel::Footnote, "K: V".to_owned()),
            (pb::XmlItemLabel::Footnote, "Plain note.".to_owned()),
            (pb::XmlItemLabel::Paragraph, "after".to_owned()),
        ]
    );
    assert_document_is_sound(&events);
}

#[tokio::test]
async fn every_caption_of_a_picture_is_kept_ahead_of_it() {
    let client = client().await;
    let events = parse_ok(
        &client,
        &rendered(
            "  <caption>A chart</caption>\n\
             \x20 <caption>\n\
             \x20   <href uri=\"https://e.x/?a=1&amp;b=&quot;2&quot;\"/>\n\
             \x20   Source: survey &amp; &lt;poll&gt;\n\
             \x20   <paragraph>n = 40</paragraph>\n\
             \x20 </caption>\n\
             \x20 <picture uri=\"figs/a.png\"/>\n\
             \x20 <footnote>estimated</footnote>\n\
             \x20 <paragraph>after</paragraph>\n",
        ),
        with_document(),
    )
    .await;
    assert_eq!(
        labelled(&events),
        [
            (pb::XmlItemLabel::Caption, "A chart".to_owned()),
            (
                pb::XmlItemLabel::Caption,
                "Source: survey & <poll> n = 40".to_owned()
            ),
            (pb::XmlItemLabel::Picture, "figs/a.png".to_owned()),
            (pb::XmlItemLabel::Footnote, "estimated".to_owned()),
            (pb::XmlItemLabel::Paragraph, "after".to_owned()),
        ]
    );
    assert_document_is_sound(&events);
}

#[tokio::test]
async fn a_table_with_two_captions_keeps_both() {
    let client = client().await;
    let events = parse_ok(
        &client,
        &rendered(
            "  <caption>First</caption>\n\
             \x20 <caption>Second</caption>\n\
             \x20 <table>\n    <tr>\n      <td>x</td>\n    </tr>\n  </table>\n",
        ),
        with_document(),
    )
    .await;
    // The caption next to the table is the table's; the one before it is
    // emitted as a caption of its own rather than lost.
    let starts: Vec<Option<&str>> = events
        .iter()
        .filter_map(|e| match e.event.as_ref() {
            Some(pb::parse_xml_response::Event::TableStart(s)) => Some(s.caption.as_deref()),
            _ => None,
        })
        .collect();
    assert_eq!(starts, [Some("Second")]);
    assert_eq!(
        labelled(&events),
        [(pb::XmlItemLabel::Caption, "First".to_owned())]
    );
    assert_document_is_sound(&events);
}

#[tokio::test]
async fn a_list_keeps_blocks_that_are_not_list_items() {
    let client = client().await;
    let events = parse_ok(
        &client,
        &rendered(
            "  <list ordered=\"false\">\n\
             \x20   <list-item>one</list-item>\n\
             \x20   <paragraph>run a run b</paragraph>\n\
             \x20   <table>\n      <tr>\n        <td>cell</td>\n      </tr>\n    </table>\n\
             \x20   <list-item>two</list-item>\n\
             \x20 </list>\n",
        ),
        with_document(),
    )
    .await;
    assert_eq!(
        labelled(&events),
        [
            (pb::XmlItemLabel::ListItem, "one".to_owned()),
            (pb::XmlItemLabel::Paragraph, "run a run b".to_owned()),
            (pb::XmlItemLabel::ListItem, "two".to_owned()),
        ]
    );
    assert_document_is_sound(&events);
}
