// SPDX-License-Identifier: Apache-2.0

//! Building the byte source a parse reads: the stream transcoded to UTF-8,
//! with the declared encoding applied before the first byte is decoded.

use std::io::{self, BufRead, Read};

use quick_xml::encoding::DecodingReader;

use super::{InputStats, ParseError, peek_bytes};

/// The byte source every XML parse runs over: the scanned head chained back
/// in front of the rest of the stream, transcoded to UTF-8.
pub(crate) type TranscodedReader<R> = DecodingReader<io::Chain<io::Cursor<Vec<u8>>, R>>;

/// Bytes of the stream head scanned for the declaration's encoding label.
///
/// This equals the detection prefix [`DecodingReader`] itself buffers before
/// serving a byte, so scanning it introduces no blocking the reader would
/// not: both wait for this many bytes (or EOF) before the first event.
const ENCODING_SCAN_BYTES: usize = 64;

/// Build the transcoding reader for one XML byte stream, with the declared
/// encoding already applied.
///
/// quick-xml reads through [`DecodingReader`], which converts the input to
/// UTF-8 and detects UTF-16 on its own from BOMs and byte patterns. An
/// ASCII-compatible encoding, though, is only knowable from the XML
/// declaration, and waiting for the parser to yield that declaration is too
/// late: the reader will already have decoded — or refused — non-ASCII
/// bytes near the head under its provisional UTF-8. So the declaration's
/// label is scanned here, bytewise, before any decoding starts, and the
/// decoder is switched up front. A label `encoding_rs` does not know keeps
/// UTF-8, exactly as quick-xml 0.41 kept its current decoder; a declaration
/// stretching past [`ENCODING_SCAN_BYTES`] keeps UTF-8 too, and a document
/// that then fails to decode is reported malformed.
pub(crate) fn decoding_reader<R: BufRead>(
    mut reader: R,
    input: &InputStats,
) -> Result<TranscodedReader<R>, ParseError> {
    let head = peek_bytes(&mut reader, input, ENCODING_SCAN_BYTES)?;
    let label = declared_encoding_label(&head);
    let mut decoding = DecodingReader::new(io::Cursor::new(head).chain(reader));
    if let Some(encoding) = label
        .as_deref()
        .and_then(|l| encoding_rs::Encoding::for_label(l.as_bytes()))
    {
        decoding.set_encoding(encoding);
    }
    Ok(decoding)
}

/// The encoding label of an XML declaration at the head of `head`, if one
/// can be read bytewise.
///
/// Only an ASCII-shaped head can match: a UTF-16 stream interleaves zero
/// bytes through `<?xml` and correctly falls through to the reader's own
/// byte-pattern detection. The label search is the same pseudo-attribute
/// scan the parser will repeat on the declaration event; disagreement is
/// impossible because both read the same bytes.
fn declared_encoding_label(head: &[u8]) -> Option<String> {
    let head = head.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(head);
    let decl = head.strip_prefix(b"<?xml")?;
    let end = decl
        .windows(2)
        .position(|w| w == b"?>")
        .unwrap_or(decl.len());
    let decl = String::from_utf8_lossy(&decl[..end]);
    let after = decl.split_once("encoding")?.1.trim_start();
    let after = after.strip_prefix('=')?.trim_start();
    let quote = after.chars().next().filter(|q| *q == '"' || *q == '\'')?;
    let value = &after[1..];
    Some(value[..value.find(quote)?].to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declared_labels_are_found_and_non_ascii_heads_are_not() {
        let label = declared_encoding_label(b"<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><d/>");
        assert_eq!(label.as_deref(), Some("ISO-8859-1"));
        let label = declared_encoding_label(b"\xEF\xBB\xBF<?xml version='1.0' encoding='utf-8'?>");
        assert_eq!(label.as_deref(), Some("utf-8"));
        assert_eq!(
            declared_encoding_label(b"<?xml version=\"1.0\"?><d/>"),
            None
        );
        assert_eq!(declared_encoding_label(b"<doc/>"), None);
        assert_eq!(declared_encoding_label(b"\xFF\xFE<\x00?\x00x\x00"), None);
        // An unterminated label never matches half a value.
        assert_eq!(declared_encoding_label(b"<?xml encoding=\"UTF-"), None);
    }
}
