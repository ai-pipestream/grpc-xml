// SPDX-License-Identifier: Apache-2.0

//! The generic fallback: well-formed XML that no specific dialect claims.
//!
//! The fixtures are synthetic stand-ins for what real corpora hold: an
//! Office `docProps/app.xml` (a namespaced `Properties` root), and an
//! unqualified data file of the "list of records" shape. They are written
//! here rather than copied from a corpus so nothing in the repository is
//! somebody else's document.

mod common;

use std::fmt::Write as _;

use common::{
    JATS, LiveParse, client, info, items_labelled, items_with_role, options, parse_ok, status,
    text_items, texts, warned,
};
use grpc_xml::document::v1 as doc;
use grpc_xml::document_fold::integrity_errors;
use grpc_xml::proto::v1 as pb;

/// An Office extended-properties part, shaped like `docProps/app.xml`.
const APP_PROPERTIES: &str = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes"><Template>Normal.dotm</Template><TotalTime>12</TotalTime><Pages>3</Pages><Words>812</Words><Application>Example Writer</Application><DocSecurity>0</DocSecurity><HeadingPairs><vt:vector size="2" baseType="variant"><vt:variant><vt:lpstr>Title</vt:lpstr></vt:variant><vt:variant><vt:i4>1</vt:i4></vt:variant></vt:vector></HeadingPairs><TitlesOfParts><vt:vector size="1" baseType="lpstr"><vt:lpstr></vt:lpstr></vt:vector></TitlesOfParts><Company>Example Org</Company><LinksUpToDate>false</LinksUpToDate><AppVersion>16.0000</AppVersion></Properties>"#;

/// An unqualified data file: records of named fields, a top-level title, and
/// one element with mixed content.
const STATIONS: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<monitoringStations>
  <title>Sample monitoring stations</title>
  <station id="s1" kind="automatic">
    <locationName>Harbour Road</locationName>
    <x-coord>333898</x-coord>
    <pollutants>Nitrogen dioxide, ozone</pollutants>
  </station>
  <station id="s2">
    <locationName>Mill Lane &amp; Park</locationName>
    <x-coord>334272</x-coord>
    <note>Moved in <year>2019</year> after works</note>
    <empty/>
  </station>
</monitoringStations>
"#;

/// Records whose text-bearing elements also carry attributes.
const WITH_ATTRS: &str =
    r#"<records><record id="r1" status="ok">first</record><record>second</record></records>"#;

/// No texts, typed for comparison.
const NONE: [String; 0] = [];

fn with_attributes() -> pb::ParseOptions {
    pb::ParseOptions {
        include_attributes: true,
        ..options()
    }
}

#[tokio::test]
async fn an_office_properties_part_maps_every_field_with_its_name() {
    let client = client().await;
    let events = parse_ok(&client, APP_PROPERTIES, options()).await;

    let info = info(&events);
    assert_eq!(info.dialect, pb::XmlDialect::Generic as i32);
    assert_eq!(info.evidence, pb::DialectEvidence::GenericFallback as i32);
    assert_eq!(info.root_local_name, "Properties");
    assert_eq!(status(&events).dialect, pb::XmlDialect::Generic as i32);

    let items = text_items(&events);
    let pairs: Vec<(&str, &str)> = items
        .iter()
        .map(|item| (item.role.as_str(), item.text.as_str()))
        .collect();
    assert_eq!(
        pairs,
        [
            ("Template", "Normal.dotm"),
            ("TotalTime", "12"),
            ("Pages", "3"),
            ("Words", "812"),
            ("Application", "Example Writer"),
            ("DocSecurity", "0"),
            ("lpstr", "Title"),
            ("i4", "1"),
            ("vector", r#"size="2" baseType="variant""#),
            ("vector", r#"size="1" baseType="lpstr""#),
            ("Company", "Example Org"),
            ("LinksUpToDate", "false"),
            ("AppVersion", "16.0000"),
        ]
    );
    // Every item is a paragraph: nothing in this vocabulary is a title.
    assert!(
        items
            .iter()
            .all(|item| item.label == pb::XmlItemLabel::Paragraph as i32)
    );
    let company = items_with_role(&events, "Company")[0];
    assert_eq!(company.path, "/Properties/Company");
    assert_eq!(company.element_name, "Company");
    let source = company.source.as_ref().expect("a source");
    assert_eq!(source.model.as_deref(), Some("generic"));
    // The byte range is the element, start tag through end tag.
    let start = usize::try_from(company.byte_start.unwrap()).unwrap();
    let end = usize::try_from(company.byte_end.unwrap()).unwrap();
    assert_eq!(
        &APP_PROPERTIES[start..end],
        "<Company>Example Org</Company>"
    );

    let lpstr = items_with_role(&events, "lpstr")[0];
    assert_eq!(
        lpstr.path,
        "/Properties/HeadingPairs/vt:vector/vt:variant/vt:lpstr"
    );
    assert_eq!(lpstr.element_name, "vt:lpstr");
    assert_eq!(
        lpstr.namespace,
        "http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes"
    );
}

#[tokio::test]
async fn an_unqualified_data_file_keeps_every_value_in_document_order() {
    let client = client().await;
    let events = parse_ok(&client, STATIONS, options()).await;
    assert_eq!(info(&events).dialect, pb::XmlDialect::Generic as i32);

    // A `title` directly under the root is the title.
    let titles = items_labelled(&events, pb::XmlItemLabel::Title);
    assert_eq!(texts(&titles), ["Sample monitoring stations"]);
    assert_eq!(titles[0].role, "title");

    assert_eq!(
        texts(&items_with_role(&events, "locationName")),
        ["Harbour Road", "Mill Lane & Park"]
    );
    let coords = items_with_role(&events, "x-coord");
    assert_eq!(texts(&coords), ["333898", "334272"]);
    assert_eq!(coords[1].path, "/monitoringStations/station[2]/x-coord");

    // Mixed content splits at the child, in reading order: the parent's runs
    // either side of the child, and the child's own text between them.
    let order: Vec<(&str, &str)> = text_items(&events)
        .iter()
        .skip_while(|item| item.role != "note")
        .map(|item| (item.role.as_str(), item.text.as_str()))
        .collect();
    // The station holds no text of its own, so its attributes are its item,
    // sent when it closes.
    assert_eq!(
        order,
        [
            ("note", "Moved in"),
            ("year", "2019"),
            ("note", "after works"),
            ("station", r#"id="s2""#),
        ]
    );
    assert_eq!(
        texts(&items_with_role(&events, "station")),
        [r#"id="s1" kind="automatic""#, r#"id="s2""#]
    );
    // Whitespace between elements is layout, not content, and an empty
    // element with no attributes has nothing to say.
    assert_eq!(texts(&items_with_role(&events, "empty")), NONE);
    assert_eq!(texts(&items_with_role(&events, "monitoringStations")), NONE);
    // Nothing was dropped, so nothing says it was.
    assert!(!warned(&events, pb::WarningCode::UnmappedElement));
}

#[tokio::test]
async fn generic_attributes_follow_the_attribute_option() {
    let client = client().await;
    let events = parse_ok(&client, STATIONS, options()).await;
    assert!(
        text_items(&events)
            .iter()
            .all(|item| item.attributes.is_empty())
    );

    // An element with both text and attributes yields only its text item;
    // the attributes ride on it when asked for, and are never a second item.
    let events = parse_ok(&client, WITH_ATTRS, with_attributes()).await;
    let records = items_with_role(&events, "record");
    assert_eq!(texts(&records), ["first", "second"]);
    let names: Vec<(&str, &str)> = records[0]
        .attributes
        .iter()
        .map(|a| (a.name.as_str(), a.value.as_str()))
        .collect();
    assert_eq!(names, [("id", "r1"), ("status", "ok")]);
    assert_eq!(records[0].element_id.as_deref(), Some("r1"));
    assert_eq!(records[1].attributes, []);
    let events = parse_ok(&client, WITH_ATTRS, options()).await;
    assert_eq!(texts(&text_items(&events)), ["first", "second"]);
}

/// An OpenOffice-style menu: all of the content is in attributes.
const MENU: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<menu:menupopup xmlns:menu="http://openoffice.org/2001/menu">
  <menu:menuitem menu:id=".uno:Cut"/>
  <menu:menuitem menu:id=".uno:Copy" menu:label="~Copy"/>
  <menu:menuseparator/>
</menu:menupopup>
"#;

#[tokio::test]
async fn an_element_with_only_attributes_yields_them_as_its_item() {
    let client = client().await;
    let events = parse_ok(&client, MENU, options()).await;
    assert_eq!(info(&events).dialect, pb::XmlDialect::Generic as i32);
    let items = text_items(&events);
    let pairs: Vec<(&str, &str, &str)> = items
        .iter()
        .map(|item| (item.role.as_str(), item.text.as_str(), item.path.as_str()))
        .collect();
    // The root's only attribute is a namespace declaration, and the
    // separator has none, so neither is an item.
    assert_eq!(
        pairs,
        [
            (
                "menuitem",
                r#"menu:id=".uno:Cut""#,
                "/menu:menupopup/menu:menuitem"
            ),
            (
                "menuitem",
                r#"menu:id=".uno:Copy" menu:label="~Copy""#,
                "/menu:menupopup/menu:menuitem[2]"
            ),
        ]
    );
    let first = items[0];
    assert_eq!(first.label, pb::XmlItemLabel::Paragraph as i32);
    assert_eq!(first.element_name, "menu:menuitem");
    assert_eq!(first.namespace, "http://openoffice.org/2001/menu");
    assert_eq!(
        first.source.as_ref().and_then(|s| s.model.as_deref()),
        Some("generic")
    );
    // The byte range is the whole element, here a self-closing tag.
    let start = usize::try_from(first.byte_start.unwrap()).unwrap();
    let end = usize::try_from(first.byte_end.unwrap()).unwrap();
    assert_eq!(&MENU[start..end], r#"<menu:menuitem menu:id=".uno:Cut"/>"#);
}

#[tokio::test]
async fn the_root_own_text_and_an_entity_reference_are_kept() {
    let client = client().await;
    let events = parse_ok(
        &client,
        "<note>Fish &amp; chips &#169; 2026</note>",
        options(),
    )
    .await;
    let items = text_items(&events);
    assert_eq!(texts(&items), ["Fish & chips \u{a9} 2026"]);
    assert_eq!(items[0].role, "note");
    assert_eq!(items[0].path, "/note");
}

#[tokio::test]
async fn generic_folds_into_a_document_named_by_its_title() {
    let client = client().await;
    let events = parse_ok(
        &client,
        STATIONS,
        pb::ParseOptions {
            emit_document: true,
            ..options()
        },
    )
    .await;
    let document: &doc::Document = events
        .iter()
        .find_map(|e| match e.event.as_ref() {
            Some(pb::parse_xml_response::Event::Document(document)) => Some(document),
            _ => None,
        })
        .expect("a document event");
    assert_eq!(integrity_errors(document), Vec::<String>::new());
    assert_eq!(document.name, "Sample monitoring stations");
    assert_eq!(document.texts.len(), text_items(&events).len());
}

#[tokio::test]
async fn generic_items_stream_before_the_upload_finishes() {
    const SENT_UP_FRONT: usize = 6;
    const TOTAL: usize = 200;
    let record = |n: usize| format!("  <record><value>value {n}</value></record>\n");

    let client = client().await;
    let mut parse = LiveParse::start(&client, options()).await;
    let mut head = String::from("<?xml version=\"1.0\"?>\n<dataset>\n");
    for n in 0..SENT_UP_FRONT {
        head.push_str(&record(n));
    }
    parse.send(&head).await;

    let first = parse.next().await;
    let Some(pb::parse_xml_response::Event::Info(info)) = first.event else {
        panic!("the first event is always XmlInfo");
    };
    assert_eq!(info.dialect, pb::XmlDialect::Generic as i32);
    // The document is still open and most of it unsent, so these items can
    // only be here if the server streams.
    for n in 0..SENT_UP_FRONT {
        let event = parse.next().await;
        let Some(pb::parse_xml_response::Event::TextItem(item)) = event.event else {
            panic!("expected the item for record {n}");
        };
        assert_eq!(item.text, format!("value {n}"));
        assert_eq!(item.role, "value");
    }

    let mut tail = String::new();
    for n in SENT_UP_FRONT..TOTAL {
        tail.push_str(&record(n));
    }
    let _ = writeln!(tail, "</dataset>");
    parse.send(&tail).await;
    let LiveParse {
        requests,
        mut events,
    } = parse;
    drop(requests);
    let mut rest = Vec::new();
    while let Some(event) = events.message().await.expect("stream error") {
        rest.push(event);
    }
    assert_eq!(text_items(&rest).len(), TOTAL - SENT_UP_FRONT);
    assert_eq!(
        status(&rest).counts.expect("counts").text_items,
        u64::try_from(TOTAL).unwrap()
    );
}

#[tokio::test]
async fn an_explicit_dialect_on_a_file_it_does_not_match_behaves_as_before() {
    let client = client().await;
    let events = parse_ok(
        &client,
        STATIONS,
        pb::ParseOptions {
            dialect: pb::XmlDialect::Jats as i32,
            ..options()
        },
    )
    .await;
    let info = info(&events);
    assert_eq!(info.dialect, pb::XmlDialect::Jats as i32);
    assert_eq!(info.evidence, pb::DialectEvidence::Requested as i32);
    // The JATS rules have nothing for this vocabulary: the text is dropped
    // and the trailer says so, exactly as before the fallback existed.
    assert_eq!(texts(&text_items(&events)), NONE);
    assert!(warned(&events, pb::WarningCode::UnmappedElement));
}

#[tokio::test]
async fn generic_can_be_requested_for_a_document_a_dialect_would_claim() {
    let client = client().await;
    let events = parse_ok(
        &client,
        JATS,
        pb::ParseOptions {
            dialect: pb::XmlDialect::Generic as i32,
            ..options()
        },
    )
    .await;
    let info = info(&events);
    assert_eq!(info.dialect, pb::XmlDialect::Generic as i32);
    assert_eq!(info.evidence, pb::DialectEvidence::Requested as i32);
    // Under the generic rules the JATS title is just an element with text.
    assert_eq!(
        texts(&items_with_role(&events, "article-title"))[0],
        "Streaming XML Without a DOM"
    );
}

#[tokio::test]
async fn generic_keeps_the_entity_policy() {
    let client = client().await;
    let bomb = r#"<?xml version="1.0"?>
<!DOCTYPE lolz [<!ENTITY lol "lol"><!ENTITY lol2 "&lol;&lol;&lol;">]>
<lolz>&lol2;</lolz>"#;
    let error = common::parse(&client, bomb, options())
        .await
        .expect_err("an internal subset is refused whatever the dialect");
    assert_eq!(error.code(), tonic::Code::InvalidArgument, "{error}");
}
