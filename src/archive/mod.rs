// SPDX-License-Identifier: Apache-2.0

//! The archive drivers: `DocLang` archives (`.dclx`) and Google Books METS
//! exports (`.tar.gz`).
//!
//! Neither payload is XML at byte 0, so [`crate::parse`] routes here on the
//! archive's magic bytes before the XML sniff ever runs. Each driver unpacks
//! in memory, never on disk, and then hands the XML it finds to the same
//! machinery the plain dialects use: a `DocLang` archive's `document.xml`
//! goes through the streaming XML driver with the `DocLang` mapping, and a
//! METS export's manifest and hOCR pages are walked with the same pull
//! parser and the same security policy.
//!
//! **Member layouts follow each format's own convention.** A `.dclx` is an
//! OPC ZIP: the document is the root member named exactly `document.xml`,
//! images live under `assets/` and `pages/`, and `[Content_Types].xml` /
//! `_rels/.rels` carry the OPC furniture; an archive without `document.xml`
//! is not a `DocLang` archive and is refused as such. A Google Books METS
//! export is a gzipped tar holding one `mets` manifest with
//! `PROFILE="gbs"`, whose `fileGrp` elements (`USE` of `image`, `OCR`,
//! `coordOCR`) name the per-page files and whose `div TYPE="page"
//! ORDER="N"` elements order them. Text comes from the `coordOCR` hOCR
//! files, one item per `ocr_line` span, in page order.
//!
//! **The byte cap counts inflated bytes.** A compressed archive is small by
//! construction, so the request cap on uploaded bytes is no bomb guard at
//! all. Every byte inflated out of an archive is counted against the same
//! cap while it is being inflated — the sizes a member header claims are
//! never trusted — and the first byte past the cap ends the parse with the
//! same `RESOURCE_EXHAUSTED` an oversized plain document gets.
//!
//! The module follows the two payloads: this file owns the magic routing
//! and the shared inflation budget, `dclx` reads the ZIP shape, and `mets`
//! walks the manifest-plus-pages shape.

mod dclx;
mod mets;

use std::io::{self, BufRead, Read};

use crate::parse::{self, EmitFn, InputStats, ParseConfig, ParseError};
use crate::sniff::{Dialect, Evidence};

/// Magic bytes of a ZIP local file header, the shape every `.dclx` starts
/// with.
pub const ZIP_MAGIC: &[u8] = b"PK\x03\x04";

/// Magic bytes of a gzip stream, the shape every `.tar.gz` starts with.
pub const GZIP_MAGIC: &[u8] = &[0x1f, 0x8b];

/// Namespace URI of the METS schema, the manifest vocabulary of a Google
/// Books export.
pub const NS_METS: &str = "http://www.loc.gov/METS/";

/// The archive family a payload's magic bytes name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveKind {
    /// ZIP: a `DocLang` archive.
    Zip,
    /// gzip: a Google Books METS export.
    Gzip,
}

impl ArchiveKind {
    /// The dialect this archive family carries.
    #[must_use]
    pub const fn dialect(self) -> Dialect {
        match self {
            Self::Zip => Dialect::Dclx,
            Self::Gzip => Dialect::MetsGbs,
        }
    }

    /// The magic's name, for error messages.
    #[must_use]
    pub const fn magic_name(self) -> &'static str {
        match self {
            Self::Zip => "ZIP archive",
            Self::Gzip => "gzip",
        }
    }
}

/// Match the payload's first bytes against the archive magics.
#[must_use]
pub fn sniff_magic(head: &[u8]) -> Option<ArchiveKind> {
    if head.starts_with(ZIP_MAGIC) {
        return Some(ArchiveKind::Zip);
    }
    if head.starts_with(GZIP_MAGIC) {
        return Some(ArchiveKind::Gzip);
    }
    None
}

/// Parse one archive payload, emitting events as they are produced.
///
/// `requested` records whether the caller stated the dialect: content that
/// contradicts a stated dialect is the caller's error (`INVALID_ARGUMENT`),
/// while a sniffed archive that turns out to be some other kind of ZIP or
/// tar is merely unsupported (`UNIMPLEMENTED`), matching how an unrecognized
/// XML root is refused.
///
/// # Errors
///
/// Any [`ParseError`], under the same contract as [`crate::parse::parse`].
pub fn parse<R: BufRead>(
    kind: ArchiveKind,
    reader: R,
    config: &ParseConfig,
    input: &InputStats,
    emit: EmitFn<'_>,
    requested: bool,
) -> Result<Dialect, ParseError> {
    let evidence = if requested {
        Evidence::Requested
    } else {
        Evidence::ArchiveMagic
    };
    match kind {
        ArchiveKind::Zip => dclx::parse(reader, config, input, emit, evidence, requested),
        ArchiveKind::Gzip => mets::parse(reader, config, input, emit, evidence, requested),
    }
}
// ------------------------------------------------------------------- budget

/// The decompressed-byte budget of one parse.
///
/// A zero request limit means no cap was configured — the service always
/// configures one, so this arises only when the driver is called directly —
/// and disables the budget rather than refusing every archive.
fn budget_for(input: &InputStats) -> u64 {
    if input.limit_bytes == 0 {
        u64::MAX
    } else {
        input.limit_bytes
    }
}

/// Read a stream to its end, charging every byte against `budget` as it
/// arrives.
///
/// This is the whole zip-bomb guard: the size a member header claims is
/// never consulted, only the bytes that actually inflate, and the byte that
/// crosses the budget ends the parse before the next one is produced. When
/// `keep` is false the bytes are counted and dropped, so an unmapped member
/// still spends the budget its inflation cost.
fn read_inflated<R: Read>(
    mut reader: R,
    budget: &mut u64,
    input: &InputStats,
    keep: bool,
) -> Result<Vec<u8>, ParseError> {
    let mut out = Vec::new();
    let mut buf = [0u8; 16 * 1024];
    loop {
        input.check()?;
        let n = match reader.read(&mut buf) {
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(stream_error(&e, input)),
        };
        if n == 0 {
            return Ok(out);
        }
        let n64 = n as u64;
        if n64 > *budget {
            return Err(ParseError::TooLarge {
                limit_bytes: input.limit_bytes,
            });
        }
        *budget -= n64;
        if keep {
            out.extend_from_slice(&buf[..n]);
        }
    }
}

/// A reader that charges every byte it yields against the inflation budget.
///
/// [`read_inflated`] meters one member's body, which leaves out whatever a
/// container format reads around its members: tar headers, padding, and the
/// bodies of entries that are not files, which the tar reader skips by
/// reading them out of the decompressor. Wrapping the decompressor itself
/// counts all of it, so a gzip of a million headers or of one device entry
/// declaring gigabytes of zeros spends the same budget as a file would.
struct Metered<'a, R> {
    inner: R,
    budget: u64,
    input: &'a InputStats,
}

impl<'a, R> Metered<'a, R> {
    fn new(inner: R, input: &'a InputStats) -> Self {
        Self {
            inner,
            budget: budget_for(input),
            input,
        }
    }
}

impl<R: Read> Read for Metered<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.input.check().is_err() {
            return Err(io::Error::other("grpc-xml: parse stopped"));
        }
        let n = self.inner.read(buf)?;
        let n64 = n as u64;
        if n64 > self.budget {
            // Recorded on the shared stats as well as in the message, so the
            // cap survives whatever wrapping the tar reader gives the error.
            self.input
                .capped
                .store(true, std::sync::atomic::Ordering::Relaxed);
            return Err(io::Error::other(parse::CAP_MARKER));
        }
        self.budget -= n64;
        Ok(n)
    }
}

/// Turn an I/O failure from an archive read into the fleet taxonomy.
///
/// The request-stream reader signals a tripped byte cap through an
/// `io::Error`, and that must stay `RESOURCE_EXHAUSTED` even when it
/// surfaces through a decompressor; everything else at this layer is the
/// archive's bytes being wrong.
fn stream_error(error: &io::Error, input: &InputStats) -> ParseError {
    if let Err(stopped) = input.check() {
        return stopped;
    }
    if input.capped.load(std::sync::atomic::Ordering::Relaxed)
        || error.to_string().contains(parse::CAP_MARKER)
    {
        return ParseError::TooLarge {
            limit_bytes: input.limit_bytes,
        };
    }
    ParseError::Malformed(format!("archive data cannot be read: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn magic_bytes_route_and_short_heads_do_not() {
        assert_eq!(sniff_magic(b"PK\x03\x04rest"), Some(ArchiveKind::Zip));
        assert_eq!(sniff_magic(&[0x1f, 0x8b, 0x08]), Some(ArchiveKind::Gzip));
        assert_eq!(sniff_magic(b"<?xm"), None);
        assert_eq!(sniff_magic(b"P"), None);
        assert_eq!(sniff_magic(&[0x1f]), None);
        assert_eq!(sniff_magic(b""), None);
    }
}
