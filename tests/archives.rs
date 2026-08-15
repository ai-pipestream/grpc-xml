// SPDX-License-Identifier: Apache-2.0

//! Golden tests for the two archive dialects, over a real server.
//!
//! Every archive is constructed in the test with the same crates the server
//! reads them with, so no binary fixture is committed and each fixture is
//! readable next to its assertions. The `.dclx` fixtures follow the OPC
//! layout docling-core's `save_as_doclang_archive` writes — `document.xml`
//! at the root, `assets/`, `[Content_Types].xml`, `_rels/.rels` — and the
//! METS fixtures follow the manifest shape docling's `MetsGbsDocumentBackend`
//! reads: `PROFILE="gbs"`, `fileGrp USE` naming the per-page files,
//! `div TYPE="page" ORDER` sequencing them.

mod common;

use std::fmt::Write as _;
use std::io::Write as _;

use common::{
    DOCLANG, client, info, items_with_role, options, parse_bytes, parse_bytes_ok, status, texts,
    warned,
};
use flate2::Compression;
use flate2::write::GzEncoder;
use grpc_xml::proto::v1 as pb;
use tonic::Code;

// ----------------------------------------------------------- fixture makers

/// A ZIP with the given members, deflated, in order.
fn zip_of(members: &[(&str, &[u8])]) -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    for (name, bytes) in members {
        writer
            .start_file(*name, zip::write::SimpleFileOptions::default())
            .expect("start zip member");
        writer.write_all(bytes).expect("write zip member");
    }
    writer.finish().expect("finish zip").into_inner()
}

/// A gzipped tar with the given members, in order.
fn targz_of(members: &[(&str, &[u8])]) -> Vec<u8> {
    let encoder = GzEncoder::new(Vec::new(), Compression::default());
    let mut builder = tar::Builder::new(encoder);
    for (name, bytes) in members {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, name, *bytes)
            .expect("append tar member");
    }
    builder
        .into_inner()
        .expect("finish tar")
        .finish()
        .expect("finish gzip")
}

/// A complete `.dclx`: the document plus the OPC furniture and an asset the
/// server must leave compressed and unread.
fn dclx_of(document: &str) -> Vec<u8> {
    zip_of(&[
        ("[Content_Types].xml", b"<Types/>".as_slice()),
        ("_rels/.rels", b"<Relationships/>".as_slice()),
        ("document.xml", document.as_bytes()),
        ("assets/figure-1.png", b"not a real png".as_slice()),
    ])
}

/// The METS manifest of a two-page export: images and coordOCR per page,
/// pages ordered by the structMap.
const GBS_METS: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<mets:mets xmlns:mets="http://www.loc.gov/METS/"
           xmlns:xlink="http://www.w3.org/1999/xlink" PROFILE="gbs">
  <mets:fileSec>
    <mets:fileGrp USE="image">
      <mets:file ID="IMG1" MIMETYPE="image/jp2"><mets:FLocat xlink:href="00000001.jp2"/></mets:file>
      <mets:file ID="IMG2" MIMETYPE="image/jp2"><mets:FLocat xlink:href="00000002.jp2"/></mets:file>
    </mets:fileGrp>
    <mets:fileGrp USE="coordOCR">
      <mets:file ID="OCR1" MIMETYPE="text/html"><mets:FLocat xlink:href="00000001.html"/></mets:file>
      <mets:file ID="OCR2" MIMETYPE="text/html"><mets:FLocat xlink:href="00000002.html"/></mets:file>
    </mets:fileGrp>
  </mets:fileSec>
  <mets:structMap>
    <mets:div TYPE="volume">
      <mets:div TYPE="page" ORDER="1">
        <mets:fptr FILEID="IMG1"/><mets:fptr FILEID="OCR1"/>
      </mets:div>
      <mets:div TYPE="page" ORDER="2">
        <mets:fptr FILEID="IMG2"/><mets:fptr FILEID="OCR2"/>
      </mets:div>
    </mets:div>
  </mets:structMap>
</mets:mets>
"#;

/// One hOCR page with two lines of two words each, in the shape Google Books
/// coordOCR files carry: `ocr_line` spans holding `ocrx_word` spans, with
/// `bbox` and `x_wconf` clauses in `title`.
fn hocr_page(lines: &[(&str, &str)]) -> String {
    let mut page = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <html xmlns=\"http://www.w3.org/1999/xhtml\"><body>\n\
         <div class=\"ocr_page\" id=\"page_1\" title=\"bbox 0 0 1000 1500\">\n",
    );
    for (n, (first, second)) in lines.iter().enumerate() {
        let top = 100 + 40 * n;
        let bottom = top + 30;
        let _ = write!(
            page,
            "<span class=\"ocr_line\" id=\"line_{n}\" title=\"bbox 100 {top} 900 {bottom}; x_wconf 96\">\n\
             <span class=\"ocrx_word\" title=\"bbox 100 {top} 400 {bottom}; x_wconf 97\">{first}</span>\n\
             <span class=\"ocrx_word\" title=\"bbox 420 {top} 900 {bottom}; x_wconf 95\">{second}</span>\n\
             </span>\n",
        );
    }
    page.push_str("</div></body></html>\n");
    page
}

/// A complete two-page export: manifest, two hOCR pages, two fake scans.
fn gbs_export() -> Vec<u8> {
    let page1 = hocr_page(&[("Chapter", "One"), ("It", "begins.")]);
    let page2 = hocr_page(&[("The", "middle"), ("It", "continues.")]);
    targz_of(&[
        ("UOM_39015012345678.mets.xml", GBS_METS.as_bytes()),
        ("00000001.html", page1.as_bytes()),
        ("00000002.html", page2.as_bytes()),
        ("00000001.jp2", b"not a real scan".as_slice()),
        ("00000002.jp2", b"not a real scan".as_slice()),
    ])
}

// ----------------------------------------------------------------- dclx

#[tokio::test]
async fn dclx_sniffs_from_zip_magic_and_maps_with_the_doclang_rules() {
    let client = client().await;
    let events = parse_bytes_ok(&client, &dclx_of(DOCLANG), options()).await;

    let info = info(&events);
    assert_eq!(info.dialect, pb::XmlDialect::Dclx as i32);
    assert_eq!(info.evidence, pb::DialectEvidence::ArchiveMagic as i32);
    assert_eq!(info.root_local_name, "doclang");

    // The inner document is the DOCLANG fixture, so the mapping must be the
    // one tests/dialects.rs pins for the plain dialect; one landmark per
    // item family is enough to prove the same rules ran.
    let titles: Vec<&pb::TextItem> = common::items_labelled(&events, pb::XmlItemLabel::Title);
    assert_eq!(texts(&titles), ["Quarterly Operations Review"]);
    for item in common::text_items(&events) {
        let source = item.source.as_ref().expect("every item is attributed");
        assert_eq!(source.model.as_deref(), Some("dclx"));
    }
    let status = status(&events);
    assert_eq!(status.dialect, pb::XmlDialect::Dclx as i32);
    assert_eq!(status.counts.as_ref().unwrap().tables, 1);
}

#[tokio::test]
async fn an_explicit_dclx_request_is_obeyed_and_reported_as_requested() {
    let client = client().await;
    let events = parse_bytes_ok(
        &client,
        &dclx_of(DOCLANG),
        pb::ParseOptions {
            dialect: pb::XmlDialect::Dclx as i32,
            ..options()
        },
    )
    .await;
    let info = info(&events);
    assert_eq!(info.dialect, pb::XmlDialect::Dclx as i32);
    assert_eq!(info.evidence, pb::DialectEvidence::Requested as i32);
}

#[tokio::test]
async fn a_zip_without_document_xml_is_not_a_doclang_archive() {
    let stray = zip_of(&[("readme.txt", b"just a zip".as_slice())]);
    let client = client().await;

    // Sniffed, the payload is merely a kind of archive this service does not
    // map; stated, the caller asserted a format the content is not.
    let error = parse_bytes(&client, &stray, options())
        .await
        .expect_err("an arbitrary zip has no dialect");
    assert_eq!(error.code(), Code::Unimplemented, "{error}");
    assert!(
        error.message().contains("document.xml"),
        "{}",
        error.message()
    );

    let error = parse_bytes(
        &client,
        &stray,
        pb::ParseOptions {
            dialect: pb::XmlDialect::Dclx as i32,
            ..options()
        },
    )
    .await
    .expect_err("a stated dclx must actually contain document.xml");
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
    assert!(
        error.message().contains("document.xml"),
        "{}",
        error.message()
    );
}

#[tokio::test]
async fn an_explicit_dclx_request_on_xml_bytes_fails_cleanly() {
    // The precedence doctrine: the explicit request wins and selects the
    // archive path, so a payload without the archive's magic is refused for
    // what it is not, never re-sniffed into the dialect it might be.
    let client = client().await;
    let error = parse_bytes(
        &client,
        DOCLANG.as_bytes(),
        pb::ParseOptions {
            dialect: pb::XmlDialect::Dclx as i32,
            ..options()
        },
    )
    .await
    .expect_err("plain XML is not a .dclx archive");
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
    assert!(error.message().contains("dclx"), "{}", error.message());
    assert!(error.message().contains("magic"), "{}", error.message());
}

#[tokio::test]
async fn an_explicit_xml_dialect_on_archive_bytes_fails_closed() {
    let client = client().await;
    let error = parse_bytes(
        &client,
        &dclx_of(DOCLANG),
        pb::ParseOptions {
            dialect: pb::XmlDialect::Jats as i32,
            ..options()
        },
    )
    .await
    .expect_err("two strong signals that disagree must fail closed");
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
    assert!(error.message().contains("jats"), "{}", error.message());
    assert!(error.message().contains("dclx"), "{}", error.message());
}

#[tokio::test]
async fn an_inflated_document_over_the_cap_is_resource_exhausted() {
    // The archive is a few kilobytes on the wire and megabytes inflated —
    // the exact shape a decompression bomb has — so a cap on uploaded bytes
    // alone would never trip. The cap must count inflated bytes.
    let mut document = String::from(
        "<?xml version=\"1.0\"?>\n<doclang xmlns=\"http://docling-project.org/ns/doclang/v1\">\n",
    );
    for _ in 0..30_000 {
        document.push_str("<paragraph>The same hundred bytes of text, deflated to nearly nothing, inflated to megabytes.</paragraph>\n");
    }
    document.push_str("</doclang>\n");
    let archive = zip_of(&[("document.xml", document.as_bytes())]);
    assert!(
        archive.len() < 1024 * 1024 && document.len() > 2 * 1024 * 1024,
        "the fixture must be small compressed and large inflated; got {} -> {}",
        archive.len(),
        document.len()
    );

    let client = client().await;
    let error = parse_bytes(
        &client,
        &archive,
        pb::ParseOptions {
            dialect: pb::XmlDialect::Dclx as i32,
            max_document_mib: 1,
            ..options()
        },
    )
    .await
    .expect_err("the inflated document exceeds the cap");
    assert_eq!(error.code(), Code::ResourceExhausted, "{error}");
    assert!(error.message().contains("byte cap"), "{}", error.message());
}

#[tokio::test]
async fn a_dclx_parse_folds_into_a_document_when_asked() {
    let client = client().await;
    let events = parse_bytes_ok(
        &client,
        &dclx_of(DOCLANG),
        pb::ParseOptions {
            emit_document: true,
            ..options()
        },
    )
    .await;
    let document = events
        .iter()
        .find_map(|e| match e.event.as_ref() {
            Some(pb::parse_xml_response::Event::Document(d)) => Some(d),
            _ => None,
        })
        .expect("emit_document produces a document event");
    assert_eq!(document.name, "Quarterly Operations Review");
    assert_eq!(
        document.origin.as_ref().map(|o| o.mimetype.as_str()),
        Some("application/zip"),
        "the origin names the payload the caller uploaded, which is the archive"
    );
}

// ------------------------------------------------------------------ mets-gbs

#[tokio::test]
async fn mets_gbs_sniffs_from_gzip_magic_and_streams_pages_in_manifest_order() {
    let client = client().await;
    let events = parse_bytes_ok(&client, &gbs_export(), options()).await;

    let info = info(&events);
    assert_eq!(info.dialect, pb::XmlDialect::MetsGbs as i32);
    assert_eq!(info.evidence, pb::DialectEvidence::ArchiveMagic as i32);
    assert_eq!(info.root_local_name, "mets");
    assert_eq!(info.root_namespace, "http://www.loc.gov/METS/");

    let lines = items_with_role(&events, "ocr-line");
    assert_eq!(
        texts(&lines),
        ["Chapter One", "It begins.", "The middle", "It continues.",]
    );
    assert_eq!(lines[0].path, "/page[1]/line[1]");
    assert_eq!(lines[3].path, "/page[2]/line[2]");
    for line in &lines {
        let source = line.source.as_ref().expect("every item is attributed");
        assert_eq!(source.model.as_deref(), Some("mets-gbs"));
        let confidence = source.confidence.expect("OCR carries its confidence");
        assert!((confidence - 0.96).abs() < 1e-9, "{confidence}");
    }

    let status = status(&events);
    assert_eq!(status.dialect, pb::XmlDialect::MetsGbs as i32);
    let counts = status.counts.as_ref().unwrap();
    assert_eq!(counts.pages, 2);
    assert_eq!(counts.text_items, 4);
    // The scans were inflated for the byte budget and never decoded, and the
    // trailer says so instead of leaving the omission silent.
    assert!(warned(&events, pb::WarningCode::ArchiveMemberIgnored));
}

#[tokio::test]
async fn an_explicit_mets_gbs_request_is_obeyed_and_reported_as_requested() {
    let client = client().await;
    let events = parse_bytes_ok(
        &client,
        &gbs_export(),
        pb::ParseOptions {
            dialect: pb::XmlDialect::MetsGbs as i32,
            ..options()
        },
    )
    .await;
    let info = info(&events);
    assert_eq!(info.dialect, pb::XmlDialect::MetsGbs as i32);
    assert_eq!(info.evidence, pb::DialectEvidence::Requested as i32);
}

#[tokio::test]
async fn a_targz_without_a_mets_manifest_is_not_a_google_books_export() {
    let stray = targz_of(&[("notes.txt", b"just a tarball".as_slice())]);
    let client = client().await;

    let error = parse_bytes(&client, &stray, options())
        .await
        .expect_err("an arbitrary tar.gz has no dialect");
    assert_eq!(error.code(), Code::Unimplemented, "{error}");
    assert!(error.message().contains("METS"), "{}", error.message());

    let error = parse_bytes(
        &client,
        &stray,
        pb::ParseOptions {
            dialect: pb::XmlDialect::MetsGbs as i32,
            ..options()
        },
    )
    .await
    .expect_err("a stated mets-gbs must actually contain a manifest");
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
    assert!(error.message().contains("METS"), "{}", error.message());
}

#[tokio::test]
async fn mets_inflation_over_the_cap_is_resource_exhausted() {
    // The bomb hides in a member the mapping would never read — a scan — so
    // this also pins that skipped members are charged while inflating, not
    // trusted and dropped.
    let bomb = vec![b'a'; 3 * 1024 * 1024];
    let archive = targz_of(&[
        ("UOM_39015012345678.mets.xml", GBS_METS.as_bytes()),
        ("00000001.jp2", bomb.as_slice()),
    ]);
    assert!(
        archive.len() < 1024 * 1024,
        "the fixture must be small on the wire; got {}",
        archive.len()
    );

    let client = client().await;
    let error = parse_bytes(
        &client,
        &archive,
        pb::ParseOptions {
            max_document_mib: 1,
            ..options()
        },
    )
    .await
    .expect_err("the inflated archive exceeds the cap");
    assert_eq!(error.code(), Code::ResourceExhausted, "{error}");
    assert!(error.message().contains("byte cap"), "{}", error.message());
}

#[tokio::test]
async fn a_page_without_coordocr_is_skipped_with_a_warning_not_a_failure() {
    // The same manifest, but page 2's hOCR member is missing from the tar:
    // the manifest references a member the archive does not contain.
    let page1 = hocr_page(&[("Only", "page.")]);
    let archive = targz_of(&[
        ("UOM_39015012345678.mets.xml", GBS_METS.as_bytes()),
        ("00000001.html", page1.as_bytes()),
    ]);
    let client = client().await;
    let events = parse_bytes_ok(&client, &archive, options()).await;
    assert_eq!(texts(&items_with_role(&events, "ocr-line")), ["Only page."]);
    let counts = status(&events).counts.unwrap();
    assert_eq!(counts.pages, 2, "the skipped page is still counted");
    assert_eq!(counts.text_items, 1);
    assert!(warned(&events, pb::WarningCode::ArchiveMemberIgnored));
}
