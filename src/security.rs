// SPDX-License-Identifier: Apache-2.0

//! The XML security policy: what the parser refuses, and what it merely
//! records.
//!
//! An XML parser exposed on a network port is an attack surface with two
//! classic holes, and this module closes both without breaking the real
//! documents the collector exists for.
//!
//! **Entity expansion (billion laughs).** quick-xml declares no entities and
//! expands none: a general entity reference surfaces as
//! `Event::GeneralRef` and this server never looks up a replacement text, so
//! the exponential blow-up has nothing to recurse into. That is a property of
//! the parser rather than a setting, which is exactly why
//! [`Policy::refuses_entity_declarations`] also rejects the *declarations* up
//! front: a document that declares entities is asking for behaviour it will
//! not get, and silently dropping its content would be worse than refusing
//! it. The tests assert both halves.
//!
//! **External entities (XXE).** Nothing is ever dereferenced — not a DTD, not
//! a schema, not an `XInclude`, not an XBRL `schemaRef`. The subtlety is that
//! refusing every DOCTYPE would break the collector's own corpus: real USPTO
//! grants open with a DOCTYPE naming a relative DTD filename, and design.md
//! makes the DOCTYPE public identifier a sniffing input. So the policy splits
//! on the *shape* of the system identifier:
//!
//! - a system identifier with a URI scheme (`file:`, `http:`, …) or an
//!   absolute path is refused, because nothing legitimate in these four
//!   dialects needs one and it is the literal XXE payload;
//! - a bare relative filename is recorded on `XmlInfo`, reported as a
//!   warning, and never opened.

/// Scheme-and-path shapes that a DOCTYPE system identifier may not have.
///
/// Not an allowlist of schemes to fetch — there is no fetching. These are the
/// shapes that mean "go get this from somewhere", and their presence is what
/// makes a document an XXE attempt rather than a document with a DTD.
const REFUSED_PREFIXES: &[&str] = &[
    "file:", "http:", "https:", "ftp:", "ftps:", "jar:", "data:", "netdoc:", "gopher:", "php:",
    "expect:", "\\\\",
];

/// The parser policy applied to every document.
///
/// There is one instance of it and it has no knobs. It is a type rather than
/// free functions so `GetServiceInfo` can report the policy that is actually
/// compiled in, and so a future opt-out would have to be an explicit,
/// reviewable change to this struct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy;

impl Policy {
    /// The one policy this build enforces.
    pub const STRICT: Self = Self;

    /// Whether `<!ENTITY …>` declarations end the parse. Always true.
    #[must_use]
    pub const fn refuses_entity_declarations(self) -> bool {
        true
    }

    /// Whether general entity references are ever replaced by a definition.
    /// Always false: they are preserved verbatim and counted.
    #[must_use]
    pub const fn expands_entities(self) -> bool {
        false
    }

    /// Whether any identifier in the document is ever dereferenced. Always
    /// false, for DTDs, schemas, `XIncludes` and XBRL `schemaRef` alike.
    #[must_use]
    pub const fn fetches_external_resources(self) -> bool {
        false
    }
}

/// What a DOCTYPE declaration turned out to contain.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Doctype {
    /// Root element name the DOCTYPE declares.
    pub name: Option<String>,
    /// Public identifier, when the declaration has one.
    pub public_id: Option<String>,
    /// System identifier, when the declaration has one.
    pub system_id: Option<String>,
    /// True when the internal subset declares at least one entity.
    pub declares_entities: bool,
}

/// Why a DOCTYPE was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DoctypeRefusal {
    /// The internal subset declares entities. This is the billion-laughs
    /// shape, and the parser would not expand them anyway.
    EntityDeclaration,
    /// The system identifier points somewhere retrievable. This is the XXE
    /// shape.
    ExternalSystemId(String),
}

impl std::fmt::Display for DoctypeRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EntityDeclaration => f.write_str(
                "the DOCTYPE internal subset declares entities; this parser expands no entities \
                 and refuses documents that depend on it",
            ),
            Self::ExternalSystemId(id) => write!(
                f,
                "the DOCTYPE system identifier {id:?} is retrievable; this parser fetches no \
                 external resource and refuses documents that depend on it"
            ),
        }
    }
}

/// Parse the body of a `<!DOCTYPE …>` declaration.
///
/// `body` is what quick-xml hands over for `Event::DocType`: everything
/// between `<!DOCTYPE` and the matching `>`, internal subset included. The
/// grammar accepted here is deliberately loose — this is a security filter
/// reading a string that a hostile client wrote, not a DTD parser — but it is
/// loose only in the safe direction: anything it cannot make sense of leaves
/// the identifiers unset, and the entity check is a substring scan that no
/// amount of odd spacing evades.
#[must_use]
pub fn parse_doctype(body: &str) -> Doctype {
    let mut doctype = Doctype {
        declares_entities: declares_entities(body),
        ..Doctype::default()
    };

    // The internal subset can itself contain the words PUBLIC and SYSTEM, so
    // the external identifier is only ever read from the part before `[`.
    let head = body.split('[').next().unwrap_or(body);
    let mut tokens = QuotedTokens::new(head);
    doctype.name = tokens.next().map(|(text, _)| text);
    match tokens.next() {
        Some((keyword, false)) if keyword.eq_ignore_ascii_case("PUBLIC") => {
            doctype.public_id = tokens.next().map(|(text, _)| text);
            doctype.system_id = tokens.next().map(|(text, _)| text);
        }
        Some((keyword, false)) if keyword.eq_ignore_ascii_case("SYSTEM") => {
            doctype.system_id = tokens.next().map(|(text, _)| text);
        }
        _ => {}
    }
    doctype
}

/// True when the declaration contains an `<!ENTITY` in any casing.
///
/// XML keywords are case-sensitive, so `<!entity` is not a declaration at
/// all; it is matched anyway because a document containing that text has
/// nothing to lose by being refused and a parser that ever gained a
/// case-insensitive mode would otherwise gain a hole with it.
fn declares_entities(body: &str) -> bool {
    let bytes = body.as_bytes();
    bytes
        .windows(8)
        .any(|w| w.eq_ignore_ascii_case(b"<!ENTITY"))
}

/// Decide whether a parsed DOCTYPE may proceed.
///
/// # Errors
///
/// Returns the reason the document is refused; the caller turns it into
/// `INVALID_ARGUMENT`.
pub fn check_doctype(doctype: &Doctype) -> Result<(), DoctypeRefusal> {
    if doctype.declares_entities {
        return Err(DoctypeRefusal::EntityDeclaration);
    }
    if let Some(system_id) = &doctype.system_id
        && is_retrievable(system_id)
    {
        return Err(DoctypeRefusal::ExternalSystemId(system_id.clone()));
    }
    Ok(())
}

/// True when a system identifier names something outside the document.
///
/// A relative filename (`us-patent-grant-v45-2014-04-03.dtd`) is not
/// retrievable *by this server*, which has no base URI and no filesystem to
/// resolve it against, so it is allowed through and recorded. Anything with a
/// scheme, an absolute path, or a UNC prefix is refused.
#[must_use]
pub fn is_retrievable(system_id: &str) -> bool {
    let id = system_id.trim();
    if id.starts_with('/') {
        return true;
    }
    let lower = id.to_ascii_lowercase();
    if REFUSED_PREFIXES.iter().any(|p| lower.starts_with(p)) {
        return true;
    }
    // A Windows drive letter, `c:\…`, which is an absolute path wearing a
    // scheme's clothing.
    let drive = id.as_bytes();
    drive.len() > 2
        && drive[0].is_ascii_alphabetic()
        && drive[1] == b':'
        && matches!(drive[2], b'\\' | b'/')
}

/// Iterator over the whitespace- or quote-delimited tokens of a DOCTYPE head.
///
/// Yields `(text, was_quoted)`. Quoting matters: an unquoted `SYSTEM` is the
/// keyword, a quoted `"SYSTEM"` is an identifier that happens to spell it.
struct QuotedTokens<'a> {
    rest: &'a str,
}

impl<'a> QuotedTokens<'a> {
    fn new(input: &'a str) -> Self {
        Self { rest: input }
    }
}

impl Iterator for QuotedTokens<'_> {
    type Item = (String, bool);

    fn next(&mut self) -> Option<Self::Item> {
        let rest = self.rest.trim_start();
        let mut chars = rest.char_indices();
        let (_, first) = chars.next()?;
        if first == '"' || first == '\'' {
            let end = rest[1..].find(first)? + 1;
            self.rest = &rest[end + 1..];
            return Some((rest[1..end].to_owned(), true));
        }
        let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        self.rest = &rest[end..];
        Some((rest[..end].to_owned(), false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_doctype_has_no_identifiers() {
        let doctype = parse_doctype("article");
        assert_eq!(doctype.name.as_deref(), Some("article"));
        assert_eq!(doctype.public_id, None);
        assert_eq!(doctype.system_id, None);
        assert!(check_doctype(&doctype).is_ok());
    }

    #[test]
    fn uspto_public_doctype_is_recorded_and_allowed() {
        let doctype = parse_doctype(
            r#"us-patent-grant PUBLIC "-//USPTO//DTD ICE Patent Grant V4.5 2014//EN" "us-patent-grant-v45-2014-04-03.dtd""#,
        );
        assert_eq!(doctype.name.as_deref(), Some("us-patent-grant"));
        assert_eq!(
            doctype.public_id.as_deref(),
            Some("-//USPTO//DTD ICE Patent Grant V4.5 2014//EN")
        );
        assert_eq!(
            doctype.system_id.as_deref(),
            Some("us-patent-grant-v45-2014-04-03.dtd")
        );
        assert!(
            check_doctype(&doctype).is_ok(),
            "a relative DTD name is not retrievable"
        );
    }

    #[test]
    fn file_system_id_is_refused() {
        let doctype = parse_doctype(r#"foo SYSTEM "file:///etc/passwd""#);
        assert_eq!(doctype.system_id.as_deref(), Some("file:///etc/passwd"));
        assert!(matches!(
            check_doctype(&doctype),
            Err(DoctypeRefusal::ExternalSystemId(_))
        ));
    }

    #[test]
    fn absolute_and_unc_and_drive_paths_are_refused() {
        for id in ["/etc/passwd", r"\\host\share\a.dtd", r"c:\windows\a.dtd"] {
            assert!(is_retrievable(id), "{id} must be treated as retrievable");
        }
    }

    #[test]
    fn entity_declarations_are_refused_whatever_the_identifiers() {
        let doctype = parse_doctype(r#"lolz [ <!ENTITY lol "lol"> ]"#);
        assert!(doctype.declares_entities);
        assert!(matches!(
            check_doctype(&doctype),
            Err(DoctypeRefusal::EntityDeclaration)
        ));
    }

    #[test]
    fn internal_subset_cannot_smuggle_an_identifier_past_the_head_scan() {
        // The word SYSTEM inside the subset must not be read as the external
        // identifier of the document type itself.
        let doctype = parse_doctype(r#"foo [ <!NOTATION x SYSTEM "http://evil/x"> ]"#);
        assert_eq!(doctype.system_id, None);
    }
}
