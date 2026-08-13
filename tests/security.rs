// SPDX-License-Identifier: Apache-2.0

//! The hostile-input suite: entity expansion, external entities, truncation,
//! malformed markup, byte caps and dialect refusals, all over the wire.
//!
//! These are the tests that decide whether this service can face a network.
//! Each one asserts the gRPC code the fleet rules fix, because a parser that
//! refuses the right documents with the wrong status is still a parser a
//! coordinator cannot route around.

mod common;

use std::fmt::Write as _;
use std::time::{Duration, Instant};

use common::{JATS, XBRL, client, client_with, options, parse, status, text_items, warned};
use grpc_xml::proto::v1 as pb;
use grpc_xml::service::XmlGrpc;
use tonic::Code;

/// The classic XXE payload from design.md, verbatim.
const XXE_SYSTEM_DOCTYPE: &str = r#"<?xml version="1.0"?>
<!DOCTYPE article SYSTEM "file:///etc/passwd">
<article xmlns="http://jats.nlm.nih.gov/ns/archiving/1.3/">
  <body><sec><title>T</title><p>text</p></sec></body>
</article>"#;

/// XXE by declared external entity: the DOCTYPE declares an entity that reads
/// a file, and the body references it.
const XXE_ENTITY: &str = r#"<?xml version="1.0"?>
<!DOCTYPE article [
  <!ENTITY xxe SYSTEM "file:///etc/passwd">
]>
<article xmlns="http://jats.nlm.nih.gov/ns/archiving/1.3/">
  <body><sec><title>T</title><p>&xxe;</p></sec></body>
</article>"#;

/// XXE over the network, which would turn the parser into an SSRF gadget.
const XXE_HTTP: &str = r#"<?xml version="1.0"?>
<!DOCTYPE article [
  <!ENTITY probe SYSTEM "http://169.254.169.254/latest/meta-data/">
]>
<article xmlns="http://jats.nlm.nih.gov/ns/archiving/1.3/">
  <body><sec><title>T</title><p>&probe;</p></sec></body>
</article>"#;

/// Billion laughs: ten levels of tenfold expansion, 10^9 characters if a
/// parser is foolish enough to expand it.
fn billion_laughs() -> String {
    let mut document = String::from("<?xml version=\"1.0\"?>\n<!DOCTYPE lolz [\n");
    document.push_str("  <!ENTITY lol \"lol\">\n");
    for level in 1..=9 {
        let inner = format!("&lol{};", level - 1).replace("lol0;", "lol;");
        let _ = writeln!(document, "  <!ENTITY lol{level} \"{}\">", inner.repeat(10));
    }
    document.push_str("]>\n<article xmlns=\"http://jats.nlm.nih.gov/ns/archiving/1.3/\">\n");
    document.push_str("  <body><sec><title>T</title><p>&lol9;</p></sec></body>\n</article>");
    document
}

/// A quadratic blowup variant: one large entity referenced many times, which
/// defeats naive depth limits.
fn quadratic_blowup() -> String {
    let filler = "A".repeat(50_000);
    let mut document =
        format!("<?xml version=\"1.0\"?>\n<!DOCTYPE article [\n  <!ENTITY big \"{filler}\">\n]>\n");
    document.push_str("<article xmlns=\"http://jats.nlm.nih.gov/ns/archiving/1.3/\">\n  <body><sec><title>T</title><p>");
    for _ in 0..2_000 {
        document.push_str("&big;");
    }
    document.push_str("</p></sec></body>\n</article>");
    document
}

// ------------------------------------------------------------ entity bombs

#[tokio::test]
async fn billion_laughs_is_refused_quickly_and_expands_nothing() {
    let client = client().await;
    let started = Instant::now();
    let error = parse(&client, &billion_laughs(), options())
        .await
        .expect_err("an entity bomb must not parse");
    let elapsed = started.elapsed();

    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
    assert!(
        error.message().contains("declares entities"),
        "the refusal must name its reason: {}",
        error.message()
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "the refusal must be immediate, not the result of surviving the expansion; took {elapsed:?}"
    );
}

#[tokio::test]
async fn quadratic_entity_blowup_is_refused_at_the_declaration() {
    let client = client().await;
    let started = Instant::now();
    let error = parse(&client, &quadratic_blowup(), options())
        .await
        .expect_err("a quadratic blowup must not parse");
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn an_undeclared_entity_reference_is_preserved_and_never_expanded() {
    // Without a DOCTYPE there is nothing to refuse, and the reference still
    // must not be resolved. This is the property that makes the bomb
    // impossible in the first place, asserted directly: the parser has no
    // entity table, so the reference survives as text and is reported.
    let document = r#"<?xml version="1.0"?>
<article xmlns="http://jats.nlm.nih.gov/ns/archiving/1.3/">
  <body><sec><title>T</title><p>before &mystery; after &amp; done &#65;</p></sec></body>
</article>"#;
    let client = client().await;
    let events = parse(&client, document, options()).await.expect("parses");
    let paragraph = text_items(&events)
        .into_iter()
        .find(|i| i.label == pb::XmlItemLabel::Paragraph as i32)
        .expect("the paragraph");
    assert_eq!(
        paragraph.text, "before &mystery; after & done A",
        "predefined and character references resolve; a named one is kept verbatim"
    );
    assert!(warned(&events, pb::WarningCode::UnexpandedEntity));
}

// -------------------------------------------------------------------- XXE

#[tokio::test]
async fn a_file_system_identifier_is_refused_and_nothing_is_read() {
    let client = client().await;
    let error = parse(&client, XXE_SYSTEM_DOCTYPE, options())
        .await
        .expect_err("an external system identifier must not parse");
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
    assert!(
        error.message().contains("file:///etc/passwd"),
        "{}",
        error.message()
    );
    assert!(
        error.message().contains("fetches no external resource"),
        "{}",
        error.message()
    );
    assert!(
        !error.message().contains("root:"),
        "the refusal must not contain file contents"
    );
}

#[tokio::test]
async fn a_declared_external_entity_is_refused_before_it_is_referenced() {
    let client = client().await;
    for document in [XXE_ENTITY, XXE_HTTP] {
        let error = parse(&client, document, options())
            .await
            .expect_err("a declared external entity must not parse");
        assert_eq!(error.code(), Code::InvalidArgument, "{error}");
        assert!(
            error.message().contains("declares entities"),
            "{}",
            error.message()
        );
    }
}

#[tokio::test]
async fn a_relative_dtd_reference_is_allowed_recorded_and_not_fetched() {
    // The USPTO corpus depends on this: refusing every DOCTYPE would refuse
    // the real documents, so the policy splits on retrievability instead.
    let document = r#"<?xml version="1.0"?>
<!DOCTYPE us-patent-grant PUBLIC "-//USPTO//DTD ICE Patent Grant V4.5 2014//EN" "us-patent-grant-v45-2014-04-03.dtd">
<us-patent-grant><abstract><p>Body.</p></abstract></us-patent-grant>"#;
    let client = client().await;
    let events = parse(&client, document, options()).await.expect("parses");
    let info = common::info(&events);
    assert_eq!(
        info.system_id.as_deref(),
        Some("us-patent-grant-v45-2014-04-03.dtd")
    );
    assert!(warned(&events, pb::WarningCode::ExternalIdIgnored));
}

// ------------------------------------------------------- malformed input

#[tokio::test]
async fn a_truncated_document_is_invalid_argument_not_a_short_success() {
    let client = client().await;
    // Cut on a tag boundary deep in the body: everything sent is well-formed
    // XML and several items have already streamed, so a parser that streams
    // could easily call this a success. It is not one — the tree never
    // closed.
    let cut = JATS.find("<table-wrap").expect("fixture landmark");
    let truncated = &JATS[..cut];
    let error = parse(&client, truncated, options())
        .await
        .expect_err("a truncated document must not report success");
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
    assert!(
        error.message().contains("still open"),
        "{}",
        error.message()
    );
}

#[tokio::test]
async fn truncation_inside_a_tag_is_also_invalid_argument() {
    let document = "<?xml version=\"1.0\"?>\n<article xmlns=\"http://jats.nlm.nih.gov/ns/archiving/1.3/\"><body><sec><title>Cut here</ti";
    let client = client().await;
    let error = parse(&client, document, options())
        .await
        .expect_err("a document cut inside a tag must not parse");
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
}

#[tokio::test]
async fn mismatched_end_tags_are_rejected_rather_than_repaired() {
    let document = r#"<?xml version="1.0"?>
<article xmlns="http://jats.nlm.nih.gov/ns/archiving/1.3/">
  <body><sec><title>T</title><p>text</sec></p></body>
</article>"#;
    let client = client().await;
    let error = parse(&client, document, options())
        .await
        .expect_err("mismatched nesting must not parse");
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
}

#[tokio::test]
async fn an_empty_document_is_invalid_argument() {
    let client = client().await;
    let error = parse(&client, "", options())
        .await
        .expect_err("no bytes is not a document");
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
}

#[tokio::test]
async fn a_request_that_does_not_start_with_options_is_rejected() {
    let mut client = client().await;
    let frames = vec![pb::ParseXmlRequest {
        payload: Some(pb::parse_xml_request::Payload::Chunk(b"<a/>".to_vec())),
    }];
    let error = client
        .parse_xml(tokio_stream::iter(frames))
        .await
        .expect_err("the first frame must be options");
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
}

// ------------------------------------------------------------- dialects

#[tokio::test]
async fn an_unrecognized_root_is_unimplemented_not_a_guess() {
    let client = client().await;
    let error = parse(&client, "<root/>", options())
        .await
        .expect_err("a bare root has no dialect");
    assert_eq!(
        error.code(),
        Code::Unimplemented,
        "an unmapped dialect is UNIMPLEMENTED: {error}"
    );
    assert!(
        error.message().contains("does not map arbitrary XML"),
        "{}",
        error.message()
    );
}

#[tokio::test]
async fn disagreeing_dialect_signals_are_invalid_argument_with_both_names() {
    let document = r#"<?xml version="1.0"?>
<!DOCTYPE article PUBLIC "-//USPTO//DTD ICE Patent Grant V4.5 2014//EN" "grant.dtd">
<article xmlns="http://jats.nlm.nih.gov/ns/archiving/1.3/"><body/></article>"#;
    let client = client().await;
    let error = parse(&client, document, options())
        .await
        .expect_err("two signals that disagree must fail closed");
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
    assert!(error.message().contains("jats"), "{}", error.message());
    assert!(error.message().contains("uspto"), "{}", error.message());
}

#[tokio::test]
async fn an_explicit_dialect_overrides_a_root_that_would_sniff_otherwise() {
    let client = client().await;
    let events = parse(
        &client,
        XBRL,
        pb::ParseOptions {
            dialect: pb::XmlDialect::Doclang as i32,
            ..options()
        },
    )
    .await
    .expect("an explicit dialect is obeyed, not second-guessed");
    let info = common::info(&events);
    assert_eq!(info.dialect, pb::XmlDialect::Doclang as i32);
    assert_eq!(info.evidence, pb::DialectEvidence::Requested as i32);
    assert_eq!(status(&events).dialect, pb::XmlDialect::Doclang as i32);
}

// ------------------------------------------------------------- byte caps

#[tokio::test]
async fn a_document_over_the_request_cap_is_resource_exhausted() {
    let client = client().await;
    let document = common::big_jats(20_000);
    assert!(
        document.len() > 1024 * 1024,
        "the fixture must exceed 1 MiB"
    );
    let error = parse(
        &client,
        &document,
        pb::ParseOptions {
            max_document_mib: 1,
            ..options()
        },
    )
    .await
    .expect_err("a document over the cap must not parse");
    assert_eq!(error.code(), Code::ResourceExhausted, "{error}");
    assert!(error.message().contains("byte cap"), "{}", error.message());
}

#[tokio::test]
async fn the_cap_trips_during_the_upload_rather_than_after_it() {
    // The distinction matters: a cap enforced after the upload has already
    // let the client spend the server's memory. This asserts the server had
    // read far less than the whole document when it refused.
    let client = client_with(XmlGrpc::new().with_ceiling_max_document_mib(1)).await;
    let document = common::big_jats(60_000);
    let error = parse(
        &client,
        &document,
        pb::ParseOptions {
            max_document_mib: 1,
            ..options()
        },
    )
    .await
    .expect_err("over the cap");
    assert_eq!(error.code(), Code::ResourceExhausted, "{error}");
    assert!(
        document.len() > 4 * 1024 * 1024,
        "the fixture must be several times the cap so 'stopped early' is meaningful"
    );
}

#[tokio::test]
async fn a_document_under_the_cap_parses_and_reports_the_bytes_it_read() {
    let client = client().await;
    let events = parse(&client, JATS, options()).await.expect("parses");
    let status = status(&events);
    assert_eq!(
        status.bytes_consumed,
        JATS.len() as u64,
        "the trailer accounts for every byte the parser was handed"
    );
    assert!(status.counts.as_ref().unwrap().elements_visited > 20);
}

// -------------------------------------------------------- admission control

#[tokio::test]
async fn parses_past_the_concurrency_limit_are_refused_not_queued() {
    let client = client_with(XmlGrpc::new().with_max_concurrent_parses(1)).await;
    // Hold one slot open by starting a parse and never finishing the upload.
    let held = common::LiveParse::start(&client, options()).await;
    held.send(common::BIG_JATS_HEAD).await;
    // Drain the info event so the parse is demonstrably running.
    let mut held = held;
    let _ = held.next().await;

    let error = parse(&client, JATS, options())
        .await
        .expect_err("the second parse has no slot");
    assert_eq!(error.code(), Code::ResourceExhausted, "{error}");
    assert!(
        error.message().contains("concurrent parses"),
        "{}",
        error.message()
    );
}
