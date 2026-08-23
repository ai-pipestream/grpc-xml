// SPDX-License-Identifier: Apache-2.0

//! Character-encoding seams of the parse path, over a real server.
//!
//! The parser reads through a transcoding layer that converts every input
//! to UTF-8 before a byte of XML is parsed, and the declared encoding is
//! applied by scanning the head of the stream rather than by waiting for
//! the declaration event. These tests pin the wire behavior of that seam:
//! an ASCII-compatible encoding works even when its first non-ASCII byte
//! lands inside the reader's 64-byte detection prefix, UTF-16 is detected
//! from its BOM, and undecodable input is the caller's error — never the
//! server's.

mod common;

use common::{client, info, options, parse_bytes, parse_bytes_ok, texts};
use grpc_xml::proto::v1 as pb;
use tonic::Code;

/// Encode a string as ISO-8859-1, one byte per char. Panics on a char the
/// encoding cannot carry, which no fixture here uses.
fn latin1(text: &str) -> Vec<u8> {
    text.chars()
        .map(|c| {
            let cp = u32::from(c);
            assert!(cp <= 0xFF, "not Latin-1: {c:?}");
            u8::try_from(cp).expect("checked just above")
        })
        .collect()
}

/// Encode a string as UTF-16LE with a BOM.
fn utf16le(text: &str) -> Vec<u8> {
    let mut bytes = vec![0xFF, 0xFE];
    for unit in text.encode_utf16() {
        bytes.extend_from_slice(&unit.to_le_bytes());
    }
    bytes
}

/// A minimal `DocLang` document whose title carries non-ASCII text, with
/// the comment placing a non-ASCII byte inside the first 64 bytes of the
/// stream — the exact position where a decoder switched only at the
/// declaration event would already have misread it.
const ACCENTED: &str = "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><!-- caf\u{e9} -->\n\
     <doclang xmlns=\"http://docling-project.org/ns/doclang/v1\">\n\
     <title>R\u{e9}sum\u{e9} du caf\u{e9}</title>\n\
     </doclang>\n";

#[tokio::test]
async fn latin1_with_an_early_non_ascii_byte_parses_and_reports_its_label() {
    let payload = latin1(ACCENTED);
    assert!(
        payload[..64].iter().any(|b| *b >= 0x80),
        "the fixture must put a non-ASCII byte inside the 64-byte prefix"
    );
    let client = client().await;
    let events = parse_bytes_ok(&client, &payload, options()).await;
    assert_eq!(info(&events).encoding.as_deref(), Some("ISO-8859-1"));
    let titles = common::items_labelled(&events, pb::XmlItemLabel::Title);
    assert_eq!(texts(&titles), ["R\u{e9}sum\u{e9} du caf\u{e9}"]);
}

#[tokio::test]
async fn utf16le_with_a_bom_is_detected_and_parses() {
    let document = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\
         <doclang xmlns=\"http://docling-project.org/ns/doclang/v1\">\
         <title>R\u{e9}sum\u{e9} du caf\u{e9}</title>\
         </doclang>";
    let client = client().await;
    let events = parse_bytes_ok(&client, &utf16le(document), options()).await;
    assert_eq!(info(&events).encoding.as_deref(), Some("UTF-16"));
    let titles = common::items_labelled(&events, pb::XmlItemLabel::Title);
    assert_eq!(texts(&titles), ["R\u{e9}sum\u{e9} du caf\u{e9}"]);
}

#[tokio::test]
async fn a_utf8_bom_is_stripped_and_the_document_parses() {
    let mut payload = vec![0xEF, 0xBB, 0xBF];
    payload.extend_from_slice(common::DOCLANG.as_bytes());
    let client = client().await;
    let events = parse_bytes_ok(&client, &payload, options()).await;
    let titles = common::items_labelled(&events, pb::XmlItemLabel::Title);
    assert_eq!(texts(&titles), ["Quarterly Operations Review"]);
}

#[tokio::test]
async fn a_declaration_padded_past_the_scan_window_fails_as_the_callers_error() {
    // The encoding label sits beyond the 64 bytes the head scan reads, so
    // the non-ASCII byte later in the stream cannot be decoded. What the
    // contract fixes is whose error that is: INVALID_ARGUMENT, with the
    // parse task alive and well — never INTERNAL.
    let padding = " ".repeat(60);
    let document = format!(
        "<?xml version=\"1.0\"{padding}encoding=\"ISO-8859-1\"?>\
         <doclang xmlns=\"http://docling-project.org/ns/doclang/v1\">\
         <title>caf\u{e9}</title></doclang>"
    );
    let payload = latin1(&document);
    let client = client().await;
    let error = parse_bytes(&client, &payload, options())
        .await
        .expect_err("the label is out of scan range and the byte cannot decode");
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
}

#[tokio::test]
async fn undeclared_invalid_utf8_is_invalid_argument() {
    let mut payload =
        b"<doclang xmlns=\"http://docling-project.org/ns/doclang/v1\"><title>caf".to_vec();
    payload.push(0xE9);
    payload.extend_from_slice(b"</title></doclang>");
    let client = client().await;
    let error = parse_bytes(&client, &payload, options())
        .await
        .expect_err("a lone 0xE9 is not UTF-8 and no other encoding was declared");
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
}
