// SPDX-License-Identifier: Apache-2.0

//! The `DocLang` archive (`.dclx`) driver: locate `document.xml` in the
//! ZIP, inflate it under the budget, and run the streaming XML driver over
//! it with the `DocLang` mapping.

use std::io::{self, BufRead};

use super::{budget_for, read_inflated, stream_error};
use crate::parse::{self, EmitFn, InputStats, ParseConfig, ParseError};
use crate::sniff::{Dialect, Evidence};

/// Render a zip error with the wording zip 7.x used.
///
/// zip 8.x moved the unsupported-compression case out of
/// `UnsupportedArchive` into its own `CompressionMethodNotSupported(u16)`
/// variant with a new display string. The message leaks to the wire through
/// `INVALID_ARGUMENT`, so the 7.x phrasing is pinned here; every other
/// variant kept its wording across the major bump.
fn zip_error_message(error: &zip::result::ZipError) -> String {
    match error {
        zip::result::ZipError::CompressionMethodNotSupported(_) => {
            "unsupported Zip archive: Compression method not supported".to_owned()
        }
        other => other.to_string(),
    }
}

// --------------------------------------------------------------------- dclx

/// Parse a `DocLang` archive: locate `document.xml`, inflate it under the
/// budget, and run the streaming XML driver over it with the `DocLang`
/// mapping and the resolution this module already made.
///
/// The other members — `assets/`, `pages/`, the OPC furniture — are left
/// compressed and unread: the document references its images by relative
/// `uri`, which the `DocLang` mapping already lifts into picture placeholder
/// items, and this service never decodes pixels.
pub(super) fn parse<R: BufRead>(
    reader: R,
    config: &ParseConfig,
    input: &InputStats,
    emit: EmitFn<'_>,
    evidence: Evidence,
    requested: bool,
) -> Result<Dialect, ParseError> {
    // ZIP needs random access to its central directory, so the compressed
    // archive is buffered whole; the request cap on uploaded bytes bounds it.
    let mut compressed = Vec::new();
    if let Err(e) = { reader }.read_to_end(&mut compressed) {
        return Err(stream_error(&e, input));
    }
    let mut budget = budget_for(input);
    let mut archive = zip::ZipArchive::new(io::Cursor::new(compressed))
        .map_err(|e| ParseError::Malformed(format!("not a readable ZIP archive: {e}")))?;
    let member = match archive.by_name("document.xml") {
        Ok(member) => member,
        Err(zip::result::ZipError::FileNotFound) => {
            return Err(if requested {
                ParseError::Malformed(
                    "the ZIP archive has no document.xml member; a DocLang archive (.dclx) \
                     carries its document there"
                        .to_owned(),
                )
            } else {
                ParseError::Unsupported(
                    "the ZIP archive has no document.xml member, so it is not a DocLang \
                     archive (.dclx); this service does not map arbitrary archives"
                        .to_owned(),
                )
            });
        }
        Err(e) => {
            return Err(ParseError::Malformed(format!(
                "unreadable ZIP archive: {}",
                zip_error_message(&e)
            )));
        }
    };
    let document = read_inflated(member, &mut budget, input, true)?;
    drop(archive);
    parse::parse_xml(
        io::Cursor::new(document),
        config,
        input,
        emit,
        Some((Dialect::Dclx, evidence)),
    )
}
