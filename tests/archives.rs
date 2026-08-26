// SPDX-License-Identifier: Apache-2.0

//! Golden tests for the two archive dialects, over a real server.
//!
//! Every archive is constructed in the test with the same crates the server
//! reads them with, so no binary fixture is committed and each fixture is
//! readable next to its assertions. The `.dclx` fixtures follow the OPC
//! layout of the format — `document.xml` at the root, `assets/`,
//! `[Content_Types].xml`, `_rels/.rels` — and the METS fixtures follow the
//! manifest shape of a Google Books export: `PROFILE="gbs"`,
//! `fileGrp USE` naming the per-page files, `div TYPE="page" ORDER`
//! sequencing them.

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
async fn a_stored_member_maps_exactly_like_a_deflated_one() {
    // The format does not require compression, and the reader must not: a
    // stored `document.xml` goes through the same budget accounting and the
    // same inner parse as a deflated one.
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer
        .start_file(
            "document.xml",
            zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored),
        )
        .expect("start zip member");
    writer
        .write_all(DOCLANG.as_bytes())
        .expect("write zip member");
    let archive = writer.finish().expect("finish zip").into_inner();

    let client = client().await;
    let events = parse_bytes_ok(&client, &archive, options()).await;
    assert_eq!(info(&events).dialect, pb::XmlDialect::Dclx as i32);
    let titles: Vec<&pb::TextItem> = common::items_labelled(&events, pb::XmlItemLabel::Title);
    assert_eq!(texts(&titles), ["Quarterly Operations Review"]);
}

#[tokio::test]
async fn a_nested_document_xml_does_not_count_as_the_root_member() {
    // The document lives at the archive root by definition; a member of the
    // same name inside a directory is some other archive's layout.
    let stray = zip_of(&[("inner/document.xml", DOCLANG.as_bytes())]);
    let client = client().await;
    let error = parse_bytes(&client, &stray, options())
        .await
        .expect_err("only a root document.xml names a DocLang archive");
    assert_eq!(error.code(), Code::Unimplemented, "{error}");
    assert!(
        error.message().contains("document.xml"),
        "{}",
        error.message()
    );
}

#[tokio::test]
async fn a_truncated_zip_is_the_callers_error_with_stable_wording() {
    let whole = dclx_of(DOCLANG);
    let cut = &whole[..whole.len() / 2];
    let client = client().await;
    let error = parse_bytes(&client, cut, options())
        .await
        .expect_err("half a central directory is not a readable archive");
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
    assert!(
        error.message().contains("not a readable ZIP archive"),
        "{}",
        error.message()
    );
}

/// A single-member ZIP whose `document.xml` claims a compression method this
/// build does not link, made by patching the method field of a stored
/// archive. Everything else about the bytes is a valid ZIP, which is exactly
/// the shape the error path under test must classify.
fn zip_with_unsupported_method(document: &[u8]) -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer
        .start_file(
            "document.xml",
            zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored),
        )
        .expect("start zip member");
    writer.write_all(document).expect("write zip member");
    let mut bytes = writer.finish().expect("finish zip").into_inner();
    // The 2-byte compression method sits 8 bytes after a local file header
    // signature and 10 bytes after a central directory one. 12 is bzip2,
    // which the deflate-only build cannot read.
    let mut i = 0;
    while i + 4 <= bytes.len() {
        match &bytes[i..i + 4] {
            b"PK\x03\x04" => {
                bytes[i + 8] = 12;
                bytes[i + 9] = 0;
                i += 4;
            }
            b"PK\x01\x02" => {
                bytes[i + 10] = 12;
                bytes[i + 11] = 0;
                i += 4;
            }
            _ => i += 1,
        }
    }
    bytes
}

#[tokio::test]
async fn an_unsupported_compression_method_is_invalid_argument_with_stable_wording() {
    let archive = zip_with_unsupported_method(DOCLANG.as_bytes());
    let client = client().await;
    let error = parse_bytes(&client, &archive, options())
        .await
        .expect_err("a member this build cannot decompress fails the parse");
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
    // The wording is pinned across zip crate upgrades: it reached the wire
    // under zip 7.x and stays byte-identical under 8.x.
    assert!(
        error
            .message()
            .contains("unsupported Zip archive: Compression method not supported"),
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
async fn mets_gbs_carries_the_page_geometry_the_hocr_states() {
    let client = client().await;
    let events = parse_bytes_ok(&client, &gbs_export(), options()).await;

    let pages: Vec<&pb::Page> = events
        .iter()
        .filter_map(|e| match e.event.as_ref() {
            Some(pb::parse_xml_response::Event::Page(page)) => Some(page),
            _ => None,
        })
        .collect();
    assert_eq!(pages.len(), 2, "one page event per manifest page");
    assert_eq!(pages[0].page_no, 1);
    assert_eq!(pages[1].page_no, 2);
    assert_eq!(pages[0].unit, "px", "hOCR counts image pixels");
    assert_eq!(pages[0].width, Some(1000.0));
    assert_eq!(pages[0].height, Some(1500.0));
    assert_eq!(pages[0].member.as_deref(), Some("00000001.html"));
    // The page opens before the lines on it.
    let first_page = events
        .iter()
        .position(|e| matches!(e.event, Some(pb::parse_xml_response::Event::Page(_))))
        .expect("a page event");
    let first_line = events
        .iter()
        .position(|e| matches!(e.event, Some(pb::parse_xml_response::Event::TextItem(_))))
        .expect("a line");
    assert!(first_page < first_line);

    let lines = items_with_role(&events, "ocr-line");
    for (n, line) in lines.iter().enumerate() {
        let bbox = line
            .bbox
            .as_ref()
            .expect("every line the parser kept had a bbox to keep it for");
        assert_eq!(line.page_no, Some(if n < 2 { 1 } else { 2 }));
        assert!((bbox.left - 100.0).abs() < f64::EPSILON, "{bbox:?}");
        assert!((bbox.right - 900.0).abs() < f64::EPSILON, "{bbox:?}");
        assert!(bbox.bottom > bbox.top, "the box is not inverted: {bbox:?}");
    }
    // Line 2 of a page sits below line 1: the boxes are the source's, not
    // a running count dressed up as geometry.
    let first = lines[0].bbox.as_ref().unwrap();
    let second = lines[1].bbox.as_ref().unwrap();
    assert!(second.top > first.top);
}

#[tokio::test]
async fn a_line_without_a_box_is_still_dropped() {
    // The box is now carried rather than only validated, and the rule that
    // a line without one is not a mapped line is unchanged.
    let page = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <html xmlns=\"http://www.w3.org/1999/xhtml\"><body>\n\
        <div class=\"ocr_page\" id=\"page_1\" title=\"bbox 0 0 640 480\">\n\
        <span class=\"ocr_line\" id=\"a\" title=\"x_wconf 90\">No box here.</span>\n\
        <span class=\"ocr_line\" id=\"b\" title=\"bbox 10 20 30 40\">Boxed.</span>\n\
        </div></body></html>\n";
    let archive = targz_of(&[
        ("UOM_39015012345678.mets.xml", GBS_METS.as_bytes()),
        ("00000001.html", page.as_bytes()),
    ]);
    let client = client().await;
    let events = parse_bytes_ok(&client, &archive, options()).await;
    assert_eq!(texts(&items_with_role(&events, "ocr-line")), ["Boxed."]);
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
async fn the_gbs_document_carries_pages_and_per_line_provenance() {
    let client = client().await;
    let events = parse_bytes_ok(
        &client,
        &gbs_export(),
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

    assert_eq!(
        document.pages.len(),
        2,
        "Document.pages had no writer in this crate before this run"
    );
    let page = document.pages.get(&1).expect("page 1");
    assert_eq!(page.page_no, 1);
    assert_eq!(page.unit.as_deref(), Some("px"));
    let size = page.size.as_ref().expect("the hOCR states the extent");
    assert!((size.width - 1000.0).abs() < f64::EPSILON);
    assert!((size.height - 1500.0).abs() < f64::EPSILON);

    let first = document.texts.first().expect("the first OCR line");
    let base = match first.item.as_ref().expect("a variant") {
        grpc_xml::document::v1::base_text_item::Item::Text(item) => {
            item.base.as_ref().expect("a base")
        }
        _ => panic!("an OCR line folds as a text item"),
    };
    let prov = base
        .prov
        .first()
        .expect("a line with a box has provenance to state");
    assert_eq!(prov.page_no, 1);
    let bbox = prov.bbox.as_ref().expect("the box");
    assert!((bbox.l - 100.0).abs() < f64::EPSILON);
    assert!(bbox.b > bbox.t, "top-left origin, bottom below top");
    let charspan = prov.charspan.as_ref().expect("the span the box bounds");
    assert_eq!(charspan.start, 0);
    assert_eq!(
        usize::try_from(charspan.end).expect("a span does not run backwards"),
        base.text.chars().count()
    );
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

#[tokio::test]
async fn every_ocr_word_carries_its_own_box_and_its_own_confidence() {
    // Multi-byte words, so a range that is right in bytes and wrong in
    // Unicode scalar values slices the wrong word rather than passing.
    let page = hocr_page(&[("Kapitel", "Über"), ("λόγος", "τέλος")]);
    let archive = targz_of(&[
        ("UOM_39015012345678.mets.xml", GBS_METS.as_bytes()),
        ("00000001.html", page.as_bytes()),
    ]);
    let client = client().await;
    let events = parse_bytes_ok(&client, &archive, options()).await;
    let lines = items_with_role(&events, "ocr-line");
    assert_eq!(texts(&lines), ["Kapitel Über", "λόγος τέλος"]);

    for (line, expected) in lines.iter().zip([["Kapitel", "Über"], ["λόγος", "τέλος"]]) {
        assert_eq!(line.words.len(), 2, "one entry per ocrx_word");
        let covered: Vec<String> = line
            .words
            .iter()
            .map(|word| {
                let range = word.range.as_ref().expect("a word states its range");
                line.text
                    .chars()
                    .skip(range.start as usize)
                    .take((range.end - range.start) as usize)
                    .collect()
            })
            .collect();
        assert_eq!(covered, expected, "the ranges count code points");
    }

    // The word boxes are the source's own, and they are not the line's: a
    // line's box bounds the line, which is the whole reason to carry these.
    let line = lines[0];
    let first = line.words[0].bbox.as_ref().expect("a word box");
    let second = line.words[1].bbox.as_ref().expect("a word box");
    assert!((first.right - 400.0).abs() < f64::EPSILON, "{first:?}");
    assert!((second.left - 420.0).abs() < f64::EPSILON, "{second:?}");
    assert!(second.left > first.right, "the words do not overlap");
    let line_box = line.bbox.as_ref().expect("the line box");
    assert!(first.right < line_box.right, "the word is inside the line");

    // Per-word confidence, which the line's own does not carry.
    assert!((line.words[0].confidence.expect("x_wconf 97") - 0.97).abs() < 1e-9);
    assert!((line.words[1].confidence.expect("x_wconf 95") - 0.95).abs() < 1e-9);
    assert!(
        (line.source.as_ref().unwrap().confidence.unwrap() - 0.96).abs() < 1e-9,
        "the line keeps its own"
    );
}

#[tokio::test]
async fn a_word_without_a_box_states_no_geometry_and_the_line_still_maps() {
    let page = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <html xmlns=\"http://www.w3.org/1999/xhtml\"><body>\n\
        <div class=\"ocr_page\" id=\"page_1\" title=\"bbox 0 0 640 480\">\n\
        <span class=\"ocr_line\" id=\"a\" title=\"bbox 10 20 300 40\">\n\
        <span class=\"ocrx_word\" title=\"x_wconf 90\">Ohne</span>\n\
        <span class=\"ocrx_word\" title=\"bbox 80 20 300 40\">Kästchen</span>\n\
        </span>\n\
        </div></body></html>\n";
    let archive = targz_of(&[
        ("UOM_39015012345678.mets.xml", GBS_METS.as_bytes()),
        ("00000001.html", page.as_bytes()),
    ]);
    let client = client().await;
    let events = parse_bytes_ok(&client, &archive, options()).await;
    let lines = items_with_role(&events, "ocr-line");
    assert_eq!(texts(&lines), ["Ohne Kästchen"]);
    assert_eq!(
        lines[0].words.len(),
        1,
        "a word with no box states no geometry rather than a fabricated one"
    );
    let range = lines[0].words[0].range.as_ref().expect("its range");
    let covered: String = lines[0]
        .text
        .chars()
        .skip(range.start as usize)
        .take((range.end - range.start) as usize)
        .collect();
    assert_eq!(covered, "Kästchen");
    assert_eq!(
        lines[0].words[0].confidence, None,
        "the source stated none for this word, so neither does the wire"
    );
}

#[tokio::test]
async fn the_document_locates_a_line_and_each_word_inside_it() {
    let page = hocr_page(&[("λόγος", "τέλος")]);
    let archive = targz_of(&[
        ("UOM_39015012345678.mets.xml", GBS_METS.as_bytes()),
        ("00000001.html", page.as_bytes()),
    ]);
    let client = client().await;
    let events = parse_bytes_ok(
        &client,
        &archive,
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
    let base = match document.texts[0].item.as_ref().expect("a variant") {
        grpc_xml::document::v1::base_text_item::Item::Text(item) => {
            item.base.as_ref().expect("a base")
        }
        _ => panic!("an OCR line folds as a text item"),
    };
    assert_eq!(base.text, "λόγος τέλος");
    assert_eq!(
        base.prov.len(),
        3,
        "the line's own box, then one entry per word"
    );
    let line = &base.prov[0];
    assert_eq!(
        line.charspan.as_ref().map(|s| (s.start, s.end)),
        Some((0, 11)),
        "the line's span is the whole line, in code points"
    );
    let words: Vec<(i32, i32)> = base.prov[1..]
        .iter()
        .map(|prov| {
            let span = prov.charspan.as_ref().expect("a word span");
            (span.start, span.end)
        })
        .collect();
    assert_eq!(
        words,
        [(0, 5), (6, 11)],
        "each word's span is the word, not the line"
    );
    for prov in &base.prov[1..] {
        assert_eq!(prov.page_no, 1);
        let bbox = prov.bbox.as_ref().expect("every word entry has its box");
        assert_eq!(
            bbox.coord_origin,
            Some(grpc_xml::document::v1::CoordOrigin::Topleft as i32)
        );
        assert!(bbox.b > bbox.t);
    }
    let first_word = base.prov[1].bbox.as_ref().unwrap();
    let second_word = base.prov[2].bbox.as_ref().unwrap();
    assert!(second_word.l > first_word.r, "the boxes are the source's");
}
