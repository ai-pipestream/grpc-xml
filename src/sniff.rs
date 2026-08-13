// SPDX-License-Identifier: Apache-2.0

//! Resolving which dialect a document belongs to.
//!
//! The order is the one design.md fixes: an explicit request wins outright,
//! then the root namespace, then the DOCTYPE public identifier, then a
//! well-known root element name. The rule that matters is the last one in
//! that list is a *fallback*, not a vote: it is consulted only when neither
//! strong signal matched, so a namespaced document is never overruled by its
//! element name.
//!
//! Two strong signals that disagree are an error, not a tie broken by
//! precedence. A document whose namespace says JATS and whose public
//! identifier says USPTO is not a JATS document with a stale DOCTYPE as far
//! as this service can tell; it is a document nobody should be guessing
//! about, so it fails closed with both names in the message.

use crate::proto::v1 as pb;

/// The four families this service maps.
///
/// A Rust-side mirror of [`pb::XmlDialect`] minus its unspecified variant, so
/// the mappers can match exhaustively on a value that is known to be real.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Dialect {
    /// NISO JATS journal articles.
    Jats,
    /// USPTO patent grants and applications (ST.36 / ST.96).
    Uspto,
    /// XBRL instance documents.
    Xbrl,
    /// Docling `DocLang` documents.
    Doclang,
}

impl Dialect {
    /// The mapper name recorded in `CollectorSource.model`.
    #[must_use]
    pub const fn model(self) -> &'static str {
        match self {
            Self::Jats => "jats",
            Self::Uspto => "uspto",
            Self::Xbrl => "xbrl",
            Self::Doclang => "doclang",
        }
    }

    /// The wire enum value.
    #[must_use]
    pub const fn to_proto(self) -> pb::XmlDialect {
        match self {
            Self::Jats => pb::XmlDialect::Jats,
            Self::Uspto => pb::XmlDialect::Uspto,
            Self::Xbrl => pb::XmlDialect::Xbrl,
            Self::Doclang => pb::XmlDialect::Doclang,
        }
    }

    /// The dialect a request asked for, or `None` for "sniff it".
    #[must_use]
    pub const fn from_proto(value: pb::XmlDialect) -> Option<Self> {
        match value {
            pb::XmlDialect::Unspecified => None,
            pb::XmlDialect::Jats => Some(Self::Jats),
            pb::XmlDialect::Uspto => Some(Self::Uspto),
            pb::XmlDialect::Xbrl => Some(Self::Xbrl),
            pb::XmlDialect::Doclang => Some(Self::Doclang),
        }
    }

    /// Every dialect this build maps, in wire-enum order.
    #[must_use]
    pub const fn all() -> [Self; 4] {
        [Self::Jats, Self::Uspto, Self::Xbrl, Self::Doclang]
    }
}

/// Namespace URI of an XBRL 2.1 instance.
pub const NS_XBRL_INSTANCE: &str = "http://www.xbrl.org/2003/instance";
/// Namespace URI of the XBRL 2.1 linkbase vocabulary, used inside instances.
pub const NS_XBRL_LINKBASE: &str = "http://www.xbrl.org/2003/linkbase";
/// Namespace URI prefix shared by every JATS tag set and version.
pub const NS_JATS_PREFIX: &str = "http://jats.nlm.nih.gov";
/// Namespace URI of XHTML, the marker for an HTML island.
pub const NS_XHTML: &str = "http://www.w3.org/1999/xhtml";
/// Namespace URI this service recognizes for `DocLang` documents.
///
/// `DocLang` is Docling's own serialization and the collector accepts it
/// unqualified as well; the URI is matched when present so a producer that
/// namespaces its output is not forced to fall back to element-name
/// sniffing.
pub const NS_DOCLANG: &str = "http://docling-project.org/ns/doclang/v1";
/// Namespace URI fragment shared by the WIPO ST.96 patent schemas.
pub const NS_ST96_FRAGMENT: &str = "wipo.int/standards/xmlschema";

/// The signal a match came from, reported on `XmlInfo` so a surprising
/// mapping can be explained without re-running the sniff by hand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Evidence {
    /// The request stated the dialect.
    Requested,
    /// The root element's namespace URI matched.
    RootNamespace,
    /// The DOCTYPE public identifier matched.
    PublicId,
    /// A well-known root element name matched.
    RootElement,
}

impl Evidence {
    /// The wire enum value.
    #[must_use]
    pub const fn to_proto(self) -> pb::DialectEvidence {
        match self {
            Self::Requested => pb::DialectEvidence::Requested,
            Self::RootNamespace => pb::DialectEvidence::RootNamespace,
            Self::PublicId => pb::DialectEvidence::PublicId,
            Self::RootElement => pb::DialectEvidence::RootElement,
        }
    }
}

/// Why a document could not be assigned a dialect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SniffError {
    /// Two strong signals named different dialects. Maps to
    /// `INVALID_ARGUMENT`.
    Conflict {
        /// What the root namespace said.
        namespace: Dialect,
        /// What the DOCTYPE public identifier said.
        public_id: Dialect,
        /// The root namespace URI, quoted back to the caller.
        namespace_uri: String,
        /// The public identifier, quoted back to the caller.
        public_id_text: String,
    },
    /// Nothing matched. Maps to `UNIMPLEMENTED`: the document may be
    /// perfectly good XML, it is simply not one of the four families this
    /// service maps.
    Unrecognized {
        /// Root namespace URI, empty when the root is unqualified.
        namespace: String,
        /// Root element local name.
        local_name: String,
    },
}

impl std::fmt::Display for SniffError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conflict {
                namespace,
                public_id,
                namespace_uri,
                public_id_text,
            } => write!(
                f,
                "ambiguous dialect: root namespace {namespace_uri:?} indicates {} but DOCTYPE \
                 public identifier {public_id_text:?} indicates {}; state `dialect` on the \
                 request to resolve it",
                namespace.model(),
                public_id.model()
            ),
            Self::Unrecognized {
                namespace,
                local_name,
            } => write!(
                f,
                "unsupported XML dialect: root element {{{namespace}}}{local_name} is not JATS, \
                 USPTO, XBRL or DocLang; this service does not map arbitrary XML"
            ),
        }
    }
}

/// The prolog and root-element facts a sniff runs on.
#[derive(Debug, Default, Clone)]
pub struct Signals {
    /// Namespace URI bound to the root element, empty when unqualified.
    pub root_namespace: String,
    /// Local name of the root element.
    pub root_local_name: String,
    /// DOCTYPE public identifier, when the document declared one.
    pub public_id: Option<String>,
}

/// Resolve a dialect from an explicit request plus the observed signals.
///
/// # Errors
///
/// [`SniffError::Conflict`] when the namespace and the public identifier
/// disagree, [`SniffError::Unrecognized`] when nothing matches.
pub fn resolve(
    requested: Option<Dialect>,
    signals: &Signals,
) -> Result<(Dialect, Evidence), SniffError> {
    if let Some(dialect) = requested {
        return Ok((dialect, Evidence::Requested));
    }

    let by_namespace = from_namespace(&signals.root_namespace);
    let by_public_id = signals.public_id.as_deref().and_then(from_public_id);

    match (by_namespace, by_public_id) {
        (Some(ns), Some(pid)) if ns != pid => Err(SniffError::Conflict {
            namespace: ns,
            public_id: pid,
            namespace_uri: signals.root_namespace.clone(),
            public_id_text: signals.public_id.clone().unwrap_or_default(),
        }),
        (Some(ns), _) => Ok((ns, Evidence::RootNamespace)),
        (None, Some(pid)) => Ok((pid, Evidence::PublicId)),
        (None, None) => from_root_element(&signals.root_local_name)
            .map(|dialect| (dialect, Evidence::RootElement))
            .ok_or_else(|| SniffError::Unrecognized {
                namespace: signals.root_namespace.clone(),
                local_name: signals.root_local_name.clone(),
            }),
    }
}

/// Match a root namespace URI. Case- and version-tolerant: JATS ships a
/// namespace per tag set and version, and ST.96 ships one per schema module.
#[must_use]
pub fn from_namespace(namespace: &str) -> Option<Dialect> {
    if namespace.is_empty() {
        return None;
    }
    let lower = namespace.to_ascii_lowercase();
    if lower.starts_with(NS_JATS_PREFIX) {
        return Some(Dialect::Jats);
    }
    if lower == NS_XBRL_INSTANCE {
        return Some(Dialect::Xbrl);
    }
    if lower == NS_DOCLANG {
        return Some(Dialect::Doclang);
    }
    if lower.contains(NS_ST96_FRAGMENT) || lower.contains("uspto.gov") {
        return Some(Dialect::Uspto);
    }
    None
}

/// Match a DOCTYPE public identifier.
#[must_use]
pub fn from_public_id(public_id: &str) -> Option<Dialect> {
    let upper = public_id.to_ascii_uppercase();
    if upper.contains("//USPTO//")
        || upper.contains("PATENT GRANT")
        || upper.contains("PATENT APPLICATION")
    {
        return Some(Dialect::Uspto);
    }
    if upper.contains("//NLM//") || upper.contains("JATS") {
        return Some(Dialect::Jats);
    }
    if upper.contains("DOCLANG") {
        return Some(Dialect::Doclang);
    }
    None
}

/// Match a well-known root element local name. The weakest signal, used only
/// when nothing stronger matched.
#[must_use]
pub fn from_root_element(local_name: &str) -> Option<Dialect> {
    match local_name {
        "article" => Some(Dialect::Jats),
        "us-patent-grant"
        | "us-patent-application"
        | "patent-document"
        | "PatentDocument"
        | "sequence-cwu" => Some(Dialect::Uspto),
        "xbrl" => Some(Dialect::Xbrl),
        "doclang" | "docling-document" => Some(Dialect::Doclang),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signals(namespace: &str, local: &str, public_id: Option<&str>) -> Signals {
        Signals {
            root_namespace: namespace.to_owned(),
            root_local_name: local.to_owned(),
            public_id: public_id.map(str::to_owned),
        }
    }

    #[test]
    fn request_short_circuits_every_signal() {
        let s = signals(NS_XBRL_INSTANCE, "xbrl", None);
        assert_eq!(
            resolve(Some(Dialect::Jats), &s).unwrap(),
            (Dialect::Jats, Evidence::Requested)
        );
    }

    #[test]
    fn each_root_namespace_resolves() {
        for (ns, local, want) in [
            (
                "http://jats.nlm.nih.gov/ns/archiving/1.3/",
                "article",
                Dialect::Jats,
            ),
            (NS_XBRL_INSTANCE, "xbrl", Dialect::Xbrl),
            (NS_DOCLANG, "doclang", Dialect::Doclang),
            (
                "http://www.wipo.int/standards/XMLSchema/ST96/Patent",
                "PatentDocument",
                Dialect::Uspto,
            ),
        ] {
            let got = resolve(None, &signals(ns, local, None)).unwrap();
            assert_eq!(got, (want, Evidence::RootNamespace), "namespace {ns}");
        }
    }

    #[test]
    fn public_id_resolves_when_the_root_is_unqualified() {
        let s = signals(
            "",
            "us-patent-grant",
            Some("-//USPTO//DTD ICE Patent Grant V4.5 2014//EN"),
        );
        assert_eq!(
            resolve(None, &s).unwrap(),
            (Dialect::Uspto, Evidence::PublicId)
        );
    }

    #[test]
    fn root_element_is_the_last_resort() {
        let s = signals("", "us-patent-grant", None);
        assert_eq!(
            resolve(None, &s).unwrap(),
            (Dialect::Uspto, Evidence::RootElement)
        );
    }

    #[test]
    fn namespace_beats_a_root_element_that_would_say_otherwise() {
        // `article` alone means JATS, but the namespace is authoritative and
        // the element name is a fallback, so this is DocLang and not an error.
        let s = signals(NS_DOCLANG, "article", None);
        assert_eq!(
            resolve(None, &s).unwrap(),
            (Dialect::Doclang, Evidence::RootNamespace)
        );
    }

    #[test]
    fn disagreeing_strong_signals_fail_closed_with_both_names() {
        let s = signals(
            "http://jats.nlm.nih.gov/ns/archiving/1.3/",
            "article",
            Some("-//USPTO//DTD ICE Patent Grant V4.5 2014//EN"),
        );
        let err = resolve(None, &s).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("jats"), "{message}");
        assert!(message.contains("uspto"), "{message}");
    }

    #[test]
    fn a_bare_root_is_unrecognized_rather_than_guessed() {
        let err = resolve(None, &signals("", "root", None)).unwrap_err();
        assert!(matches!(err, SniffError::Unrecognized { .. }), "{err}");
    }
}
