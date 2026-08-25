// SPDX-License-Identifier: Apache-2.0

//! Per-dialect mapping rules: what each element means in the Document plane.
//!
//! The driver in [`crate::parse`] knows how to walk XML and how to emit
//! protobuf; it knows nothing about JATS or patents. Everything family
//! specific is a pure function from "an element just started, here is its
//! name and its ancestors" to an [`Action`]. That keeps the streaming machine
//! honest — it cannot special-case a dialect by buffering — and it keeps the
//! mappings reviewable next to the specifications they follow.
//!
//! Mappings follow Docling's declarative backends rather than the XML tree:
//! the goal is a Document that merges cleanly with a PDF collector's parse of
//! the same paper, not a lossless XML clone. What does not survive the
//! mapping is still visible on the wire — `role` carries the source's own
//! vocabulary, `path` carries the element's position, and
//! `ParseOptions.include_attributes` carries every attribute the rule did not
//! consume.

use crate::proto::v1 as pb;
use crate::sniff::{Dialect, NS_XBRL_INSTANCE, NS_XBRL_LINKBASE};

/// Attributes of one start tag, as written.
///
/// Keys keep their prefix (`xlink:href`) because that is what the document
/// said and what `Attribute` puts on the wire; [`Attrs::get`] also matches on
/// the local part, so a rule can ask for `href` without caring which prefix a
/// particular producer bound `XLink` to.
#[derive(Debug, Default, Clone)]
pub struct Attrs(pub Vec<(String, String)>);

impl Attrs {
    /// Look up an attribute by qualified name, falling back to local name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(key, _)| key == name)
            .or_else(|| {
                self.0
                    .iter()
                    .find(|(key, _)| key.rsplit(':').next() == Some(name))
            })
            .map(|(_, value)| value.as_str())
    }

    /// Look up an attribute and parse it as an unsigned ordinal, tolerating
    /// the zero-padded forms patents use (`00001`) and trailing punctuation.
    #[must_use]
    pub fn ordinal(&self, name: &str) -> Option<u64> {
        let raw = self.get(name)?;
        let digits: String = raw.chars().filter(char::is_ascii_digit).collect();
        digits.parse().ok()
    }
}

/// What the driver should do with an element it has just entered.
#[derive(Debug, Clone)]
pub enum Action {
    /// Nothing to emit here; keep walking into the children.
    Descend,
    /// Skip this element and everything under it. Used for metadata that
    /// would otherwise arrive as unlabelled text noise.
    Skip,
    /// Collect this element's descendant text and emit it as one `TextItem`
    /// when the element closes. Inline markup inside it is flattened, and
    /// nested rules do not fire until the capture closes.
    Capture(Capture),
    /// Emit a `TextItem` whose text is one of the element's own attributes.
    /// Used for elements that reference content instead of containing it,
    /// such as a JATS `graphic` or a patent drawing `img`.
    AttrText(AttrText),
    /// Collect this element's text as the caption of the table that follows
    /// it inside the same wrapper. Unconsumed captions are emitted as
    /// ordinary `CAPTION` items when the wrapper closes.
    Caption,
    /// Open a table: emit `TableStart`, then a `TableRow` per source row,
    /// then `TableEnd`.
    Table,
}

/// A text capture and the labelling to apply when it closes.
#[derive(Debug, Clone)]
pub struct Capture {
    /// Structural label for the item.
    pub label: pb::XmlItemLabel,
    /// Dialect vocabulary refinement, or empty.
    pub role: String,
    /// Explicit heading level from the source. `None` lets the driver count
    /// section ancestors instead.
    pub level: Option<u32>,
    /// Explicit ordinal from the source, such as a claim number.
    pub ordinal: Option<u64>,
}

impl Capture {
    /// A capture with no level and no ordinal.
    #[must_use]
    pub fn new(label: pb::XmlItemLabel, role: &str) -> Self {
        Self {
            label,
            role: role.to_owned(),
            level: None,
            ordinal: None,
        }
    }

    /// Attach an ordinal read from the source.
    #[must_use]
    pub fn with_ordinal(mut self, ordinal: Option<u64>) -> Self {
        self.ordinal = ordinal;
        self
    }

    /// Attach a heading level read from the source.
    #[must_use]
    pub fn with_level(mut self, level: Option<u32>) -> Self {
        self.level = level;
        self
    }
}

/// An item whose text comes from an attribute rather than from content.
#[derive(Debug, Clone)]
pub struct AttrText {
    /// Structural label for the item.
    pub label: pb::XmlItemLabel,
    /// Dialect vocabulary refinement.
    pub role: String,
    /// Attribute to read, by local name.
    pub attr: &'static str,
}

/// What an element inside an open capture contributes beyond its text.
///
/// A capture flattens its descendants into one string, which is the right
/// shape for the Document plane and the wrong shape for everything the
/// markup said about *where* in that string something happened. An `Inline`
/// is that missing half: the driver keeps flattening and records a parallel
/// run for every element a dialect recognizes here.
#[derive(Debug, Default, Clone)]
pub struct Inline {
    /// Character styles the element applies to its run.
    pub styles: Vec<pb::SpanStyle>,
    /// External link target read from an attribute.
    pub hyperlink: Option<String>,
    /// Scheme to prefix the run's own text with when the element carries no
    /// link attribute, because the address *is* the text: `""` for a bare
    /// URI, `"mailto:"` for an email address.
    pub link_from_text: Option<&'static str>,
    /// Identifiers inside this document the element points at. A source may
    /// name several in one reference, which is why this is a list.
    pub references: Vec<String>,
    /// What `references` points at, as the source states it.
    pub reference_kind: pb::ReferenceKind,
}

impl Inline {
    /// A run that only carries styles.
    fn styled(style: pb::SpanStyle) -> Self {
        Self {
            styles: vec![style],
            ..Self::default()
        }
    }

    /// A run that links out, reading the target from the first attribute
    /// that carries one and falling back to the run's own text.
    fn link(attrs: &Attrs, names: &[&str], scheme: &'static str) -> Self {
        Self {
            hyperlink: names
                .iter()
                .find_map(|name| attrs.get(name))
                .map(str::to_owned),
            link_from_text: Some(scheme),
            ..Self::default()
        }
    }

    /// A run that points at other items of the same document. Whitespace
    /// separates the identifiers, as both JATS and USPTO write them.
    fn reference(attrs: &Attrs, attr: &str, kind: pb::ReferenceKind) -> Self {
        Self {
            references: attrs
                .get(attr)
                .map(|raw| raw.split_whitespace().map(str::to_owned).collect())
                .unwrap_or_default(),
            reference_kind: kind,
            ..Self::default()
        }
    }
}

/// Decide what an element inside an open capture contributes, or `None` when
/// it contributes only its text.
///
/// This is the counterpart to [`action`] for the elements that rule never
/// sees: once a capture is open the driver flattens everything under it, so
/// the inline vocabulary of every dialect lives here instead.
#[must_use]
pub fn inline(dialect: Dialect, ctx: &ElementCtx<'_>) -> Option<Inline> {
    match dialect {
        Dialect::Jats => jats_inline(ctx),
        Dialect::Uspto => uspto_inline(ctx),
        Dialect::Doclang | Dialect::Dclx => doclang_inline(ctx),
        // An XBRL instance has no prose, and a METS export's hOCR is walked
        // by the archive driver rather than by a capture.
        Dialect::Xbrl | Dialect::MetsGbs => None,
    }
}

/// JATS inline vocabulary: emphasis, `ext-link`, `uri`, `email` and the
/// `xref` cross-reference graph.
fn jats_inline(ctx: &ElementCtx<'_>) -> Option<Inline> {
    match ctx.local {
        "bold" => Some(Inline::styled(pb::SpanStyle::Bold)),
        "italic" | "em" => Some(Inline::styled(pb::SpanStyle::Italic)),
        "underline" => Some(Inline::styled(pb::SpanStyle::Underline)),
        "strike" => Some(Inline::styled(pb::SpanStyle::Strikethrough)),
        "monospace" => Some(Inline::styled(pb::SpanStyle::Monospace)),
        "sc" => Some(Inline::styled(pb::SpanStyle::SmallCaps)),
        "sup" => Some(Inline::styled(pb::SpanStyle::Superscript)),
        "sub" => Some(Inline::styled(pb::SpanStyle::Subscript)),
        // Nested inside a captured paragraph these cannot become their own
        // FORMULA item, so the run says the text is notation instead.
        "inline-formula" | "tex-math" | "math" => Some(Inline::styled(pb::SpanStyle::Math)),
        "ext-link" | "self-uri" | "uri" => Some(Inline::link(ctx.attrs, &["href"], "")),
        "email" => Some(Inline::link(ctx.attrs, &["href"], "mailto:")),
        "xref" => Some(Inline::reference(
            ctx.attrs,
            "rid",
            jats_reference_kind(ctx.attrs.get("ref-type")),
        )),
        _ => None,
    }
}

/// The JATS `ref-type` vocabulary, as the source states it.
fn jats_reference_kind(ref_type: Option<&str>) -> pb::ReferenceKind {
    match ref_type {
        Some("bibr") => pb::ReferenceKind::Citation,
        Some("fn") => pb::ReferenceKind::Footnote,
        Some("fig") => pb::ReferenceKind::Figure,
        Some("table") => pb::ReferenceKind::Table,
        Some("disp-formula") => pb::ReferenceKind::Equation,
        Some("sec") => pb::ReferenceKind::Section,
        _ => pb::ReferenceKind::CrossRef,
    }
}

/// USPTO inline vocabulary. `claim-ref` is the one that matters most: claim
/// dependency is the defining structure of a patent's claim set, and it is
/// stated nowhere else in the document.
fn uspto_inline(ctx: &ElementCtx<'_>) -> Option<Inline> {
    match ctx.local {
        "b" => Some(Inline::styled(pb::SpanStyle::Bold)),
        "i" => Some(Inline::styled(pb::SpanStyle::Italic)),
        "u" => Some(Inline::styled(pb::SpanStyle::Underline)),
        "smallcaps" => Some(Inline::styled(pb::SpanStyle::SmallCaps)),
        "sup" | "sup2" => Some(Inline::styled(pb::SpanStyle::Superscript)),
        "sub" | "sub2" => Some(Inline::styled(pb::SpanStyle::Subscript)),
        "maths" | "math" => Some(Inline::styled(pb::SpanStyle::Math)),
        "claim-ref" | "ClaimReference" => Some(Inline::reference(
            ctx.attrs,
            "idref",
            pb::ReferenceKind::Claim,
        )),
        "figref" => Some(Inline::reference(
            ctx.attrs,
            "idref",
            pb::ReferenceKind::Figure,
        )),
        "crossref" => Some(Inline::reference(
            ctx.attrs,
            "idref",
            pb::ReferenceKind::CrossRef,
        )),
        _ => None,
    }
}

/// `DocLang` inline vocabulary. The element names mirror the labels, so an
/// element that would be its own item at the top level becomes a styled run
/// when it appears inside one.
fn doclang_inline(ctx: &ElementCtx<'_>) -> Option<Inline> {
    match ctx.local {
        "bold" | "b" | "strong" => Some(Inline::styled(pb::SpanStyle::Bold)),
        "italic" | "i" | "em" => Some(Inline::styled(pb::SpanStyle::Italic)),
        "underline" | "u" => Some(Inline::styled(pb::SpanStyle::Underline)),
        "strike" | "s" => Some(Inline::styled(pb::SpanStyle::Strikethrough)),
        "code" | "monospace" => Some(Inline::styled(pb::SpanStyle::Monospace)),
        "sup" => Some(Inline::styled(pb::SpanStyle::Superscript)),
        "sub" => Some(Inline::styled(pb::SpanStyle::Subscript)),
        "formula" | "math" => Some(Inline::styled(pb::SpanStyle::Math)),
        "link" | "a" => Some(Inline::link(ctx.attrs, &["href", "uri"], "")),
        "ref" | "xref" => Some(Inline::reference(
            ctx.attrs,
            "target",
            pb::ReferenceKind::CrossRef,
        )),
        _ => None,
    }
}

/// Context passed to a mapping rule.
pub struct ElementCtx<'a> {
    /// Resolved namespace URI, empty when the element is unqualified.
    pub namespace: &'a str,
    /// Element local name.
    pub local: &'a str,
    /// Local names of the ancestors, root first, excluding this element.
    pub ancestors: &'a [String],
    /// Attributes of this start tag.
    pub attrs: &'a Attrs,
}

impl ElementCtx<'_> {
    /// Local name of the immediate parent, or empty at the root.
    #[must_use]
    pub fn parent(&self) -> &str {
        self.ancestors.last().map_or("", String::as_str)
    }

    /// True when any ancestor has this local name.
    #[must_use]
    pub fn inside(&self, name: &str) -> bool {
        self.ancestors.iter().any(|a| a == name)
    }
}

/// Element local names that open a table row, across all dialects.
///
/// The name sets are shared rather than per dialect because the two table
/// grammars in play (XHTML-style in JATS and `DocLang`, CALS in patent XML) do
/// not collide: no dialect uses `row` to mean something other than a table
/// row, so a union costs nothing and one table walker serves all four.
pub const ROW_ELEMENTS: &[&str] = &["tr", "row"];

/// Element local names that open a table cell.
pub const CELL_ELEMENTS: &[&str] = &["td", "th", "entry", "cell"];

/// Cell element names that are always header cells.
pub const HEADER_CELL_ELEMENTS: &[&str] = &["th"];

/// Element local names whose rows are header rows.
pub const HEADER_SECTION_ELEMENTS: &[&str] = &["thead"];

/// Element local names that may hold a caption for a table that follows.
const CAPTION_WRAPPERS: &[&str] = &["table-wrap", "fig", "figure", "table-container"];

/// Element local names that nest to imply a heading level.
#[must_use]
pub const fn section_containers(dialect: Dialect) -> &'static [&'static str] {
    match dialect {
        Dialect::Jats => &["sec"],
        Dialect::Uspto => &["description", "section"],
        Dialect::Xbrl | Dialect::MetsGbs => &[],
        Dialect::Doclang | Dialect::Dclx => &["section", "group"],
    }
}

/// Decide what an element means. The whole of the family-specific mapping.
#[must_use]
pub fn action(dialect: Dialect, ctx: &ElementCtx<'_>) -> Action {
    match dialect {
        Dialect::Jats => jats(ctx),
        Dialect::Uspto => uspto(ctx),
        // Neither of these is element-mapped. An XBRL instance is contexts,
        // units and facts, which the driver reads directly; a METS-GBS
        // export is read whole by the archive driver in `crate::archive`,
        // which never consults these rules.
        Dialect::Xbrl | Dialect::MetsGbs => Action::Descend,
        // A DocLang archive's `document.xml` member is a DocLang document,
        // so the archive dialect maps with the same rules.
        Dialect::Doclang | Dialect::Dclx => doclang(ctx),
    }
}

/// True when this element is an XBRL structural element rather than a fact.
#[must_use]
pub fn is_xbrl_infrastructure(namespace: &str) -> bool {
    namespace == NS_XBRL_INSTANCE || namespace == NS_XBRL_LINKBASE
}

/// NISO JATS: title, contributors, abstract, sections, paragraphs, tables,
/// figures, formulas and the reference list.
fn jats(ctx: &ElementCtx<'_>) -> Action {
    match ctx.local {
        // ---- front matter -------------------------------------------------
        "article-title" if ctx.inside("title-group") => {
            Action::Capture(Capture::new(pb::XmlItemLabel::Title, ""))
        }
        "article-title" => Action::Capture(Capture::new(pb::XmlItemLabel::Text, "cited-title")),
        "subtitle" => Action::Capture(Capture::new(pb::XmlItemLabel::Text, "subtitle")),
        "contrib" => {
            let role = ctx
                .attrs
                .get("contrib-type")
                .unwrap_or("contributor")
                .to_owned();
            Action::Capture(Capture {
                label: pb::XmlItemLabel::Text,
                role,
                level: None,
                ordinal: None,
            })
        }
        "aff" => Action::Capture(Capture::new(pb::XmlItemLabel::Text, "affiliation")),
        "journal-title" => Action::Capture(Capture::new(pb::XmlItemLabel::Text, "journal-title")),
        "publisher-name" => Action::Capture(Capture::new(pb::XmlItemLabel::Text, "publisher")),
        "article-id" => {
            let kind = ctx.attrs.get("pub-id-type").unwrap_or("id");
            Action::Capture(Capture::new(
                pb::XmlItemLabel::Text,
                &format!("article-id:{kind}"),
            ))
        }
        "kwd" => Action::Capture(Capture::new(pb::XmlItemLabel::Text, "keyword")),
        // Dates, counts, permissions and funding are structured metadata that
        // flattens into meaningless text; the Document plane has no home for
        // them in v1, so they are skipped rather than emitted as noise.
        "pub-date" | "history" | "counts" | "permissions" | "funding-group" | "author-notes"
        | "article-categories" | "issn" | "journal-id" => Action::Skip,

        // ---- body ---------------------------------------------------------
        "title" if ctx.parent() == "sec" => {
            Action::Capture(Capture::new(pb::XmlItemLabel::SectionHeader, ""))
        }
        // Inside a figure or table wrapper, the title and the caption both
        // caption whatever follows; the bare `label` there is the "Table 1"
        // ordinal, which the Document plane numbers for itself.
        "title" | "caption" if CAPTION_WRAPPERS.contains(&ctx.parent()) => Action::Caption,
        "label" if CAPTION_WRAPPERS.contains(&ctx.parent()) => Action::Skip,
        "caption" => Action::Capture(Capture::new(pb::XmlItemLabel::Caption, "")),
        "list-item" => Action::Capture(Capture::new(pb::XmlItemLabel::ListItem, "")),
        "p" if ctx.inside("abstract") => {
            Action::Capture(Capture::new(pb::XmlItemLabel::Paragraph, "abstract"))
        }
        "p" => Action::Capture(Capture::new(pb::XmlItemLabel::Paragraph, "")),
        "disp-quote" => Action::Capture(Capture::new(pb::XmlItemLabel::Paragraph, "quote")),
        "disp-formula" | "inline-formula" | "tex-math" | "mml:math" => {
            Action::Capture(Capture::new(pb::XmlItemLabel::Formula, ""))
        }
        "code" | "preformat" => Action::Capture(Capture::new(pb::XmlItemLabel::Code, "")),
        "fn" => Action::Capture(Capture::new(pb::XmlItemLabel::Footnote, "")),
        "graphic" | "inline-graphic" => Action::AttrText(AttrText {
            label: pb::XmlItemLabel::Picture,
            role: "graphic".to_owned(),
            attr: "href",
        }),
        "table" => Action::Table,

        // ---- back matter ---------------------------------------------------
        "ref" if ctx.inside("ref-list") => Action::Capture(
            Capture::new(pb::XmlItemLabel::Reference, "").with_ordinal(ctx.attrs.ordinal("id")),
        ),
        _ => Action::Descend,
    }
}

/// USPTO ST.36 / ST.96 grants and applications: bibliographic identity,
/// abstract, numbered claims, description and drawings.
fn uspto(ctx: &ElementCtx<'_>) -> Action {
    match ctx.local {
        // ST.96 spells element names in UpperCamelCase; both are matched so
        // one mapper covers the grant families Docling already handles.
        "invention-title" | "InventionTitle" => {
            Action::Capture(Capture::new(pb::XmlItemLabel::Title, ""))
        }
        "inventor" | "Inventor" => {
            Action::Capture(Capture::new(pb::XmlItemLabel::Text, "inventor"))
        }
        "applicant" | "Applicant" => {
            Action::Capture(Capture::new(pb::XmlItemLabel::Text, "applicant"))
        }
        "assignee" | "Assignee" => {
            Action::Capture(Capture::new(pb::XmlItemLabel::Text, "assignee"))
        }
        "doc-number" | "DocumentNumber" => {
            let role = if ctx.inside("application-reference") {
                "application-number"
            } else {
                "document-number"
            };
            Action::Capture(Capture::new(pb::XmlItemLabel::Text, role))
        }
        // Correspondence blocks are addresses, and dates, country codes,
        // kind codes and the classification blocks are codes rather than
        // prose; all of them flatten into digit soup.
        "agent"
        | "correspondence-address"
        | "date"
        | "country"
        | "kind"
        | "classification-national"
        | "classifications-cpc"
        | "classifications-ipcr"
        | "us-related-documents"
        | "us-field-of-classification-search"
        | "us-references-cited"
        | "citation" => Action::Skip,

        "claim" | "Claim" => Action::Capture(
            Capture::new(pb::XmlItemLabel::Text, "claim").with_ordinal(ctx.attrs.ordinal("num")),
        ),
        "heading" => Action::Capture(
            Capture::new(pb::XmlItemLabel::SectionHeader, "")
                .with_level(ctx.attrs.get("level").and_then(|l| l.parse().ok())),
        ),
        "p" if ctx.inside("abstract") => {
            Action::Capture(Capture::new(pb::XmlItemLabel::Paragraph, "abstract"))
        }
        "p" if ctx.inside("description-of-drawings") => Action::Capture(Capture::new(
            pb::XmlItemLabel::Paragraph,
            "drawing-description",
        )),
        "p" if ctx.inside("description") => {
            Action::Capture(Capture::new(pb::XmlItemLabel::Paragraph, "description"))
        }
        "p" => Action::Capture(Capture::new(pb::XmlItemLabel::Paragraph, "")),
        "img" => Action::AttrText(AttrText {
            label: pb::XmlItemLabel::Picture,
            role: "drawing".to_owned(),
            attr: "file",
        }),
        "table" => Action::Table,
        _ => Action::Descend,
    }
}

/// `DocLang`: a document tree that is already shaped like the Document plane,
/// so the mapping is a typed decode rather than an interpretation.
///
/// Two spellings are accepted. An element named for its label (`paragraph`,
/// `section-header`) carries the label in its name; a generic `item` carries
/// it in a `label` attribute holding a `DocItemLabel` short name
/// (`section_header`). The attribute form wins where both are present,
/// because it is the one a serializer emits mechanically.
fn doclang(ctx: &ElementCtx<'_>) -> Action {
    if let Some(raw) = ctx.attrs.get("label")
        && let Some(label) = label_from_raw(raw)
    {
        return Action::Capture(
            Capture::new(label, "")
                .with_level(ctx.attrs.get("level").and_then(|l| l.parse().ok()))
                .with_ordinal(ctx.attrs.ordinal("ordinal")),
        );
    }
    match ctx.local {
        "title" | "doc-title" => Action::Capture(Capture::new(pb::XmlItemLabel::Title, "")),
        "section-header" | "heading" => Action::Capture(
            Capture::new(pb::XmlItemLabel::SectionHeader, "")
                .with_level(ctx.attrs.get("level").and_then(|l| l.parse().ok())),
        ),
        "paragraph" | "p" | "text" => {
            Action::Capture(Capture::new(pb::XmlItemLabel::Paragraph, ""))
        }
        "list-item" | "li" => Action::Capture(
            Capture::new(pb::XmlItemLabel::ListItem, "").with_ordinal(ctx.attrs.ordinal("ordinal")),
        ),
        "caption" => Action::Caption,
        "code" => Action::Capture(Capture::new(pb::XmlItemLabel::Code, "")),
        "formula" => Action::Capture(Capture::new(pb::XmlItemLabel::Formula, "")),
        "footnote" => Action::Capture(Capture::new(pb::XmlItemLabel::Footnote, "")),
        "reference" => Action::Capture(Capture::new(pb::XmlItemLabel::Reference, "")),
        "picture" | "figure" => Action::AttrText(AttrText {
            label: pb::XmlItemLabel::Picture,
            role: "picture".to_owned(),
            attr: "uri",
        }),
        "table" => Action::Table,
        "metadata" | "provenance" => Action::Skip,
        _ => Action::Descend,
    }
}

/// Decode a `DocItemLabel` short name as `DocLang` writes it.
///
/// Both the `snake_case` form a Python serializer produces and the `kebab-case`
/// form an XML author would write are accepted, because the `DocLang` schema is
/// not pinned by a published DTD this service can point at.
#[must_use]
pub fn label_from_raw(raw: &str) -> Option<pb::XmlItemLabel> {
    let normalized = raw.trim().to_ascii_lowercase().replace('-', "_");
    Some(match normalized.as_str() {
        "title" | "document_title" => pb::XmlItemLabel::Title,
        "section_header" | "heading" => pb::XmlItemLabel::SectionHeader,
        "paragraph" => pb::XmlItemLabel::Paragraph,
        "text" => pb::XmlItemLabel::Text,
        "list_item" => pb::XmlItemLabel::ListItem,
        "caption" => pb::XmlItemLabel::Caption,
        "reference" => pb::XmlItemLabel::Reference,
        "footnote" => pb::XmlItemLabel::Footnote,
        "code" => pb::XmlItemLabel::Code,
        "formula" => pb::XmlItemLabel::Formula,
        "picture" => pb::XmlItemLabel::Picture,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx<'a>(local: &'a str, ancestors: &'a [String], attrs: &'a Attrs) -> ElementCtx<'a> {
        ElementCtx {
            namespace: "",
            local,
            ancestors,
            attrs,
        }
    }

    fn path(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| (*n).to_owned()).collect()
    }

    #[test]
    fn attrs_match_on_prefix_or_local_name() {
        let attrs = Attrs(vec![("xlink:href".to_owned(), "fig1.png".to_owned())]);
        assert_eq!(attrs.get("xlink:href"), Some("fig1.png"));
        assert_eq!(attrs.get("href"), Some("fig1.png"));
        assert_eq!(attrs.get("src"), None);
    }

    #[test]
    fn zero_padded_claim_numbers_parse() {
        let attrs = Attrs(vec![("num".to_owned(), "00012".to_owned())]);
        assert_eq!(attrs.ordinal("num"), Some(12));
    }

    #[test]
    fn jats_abstract_paragraphs_are_role_tagged() {
        let ancestors = path(&["article", "front", "article-meta", "abstract"]);
        let attrs = Attrs::default();
        let Action::Capture(capture) = jats(&ctx("p", &ancestors, &attrs)) else {
            panic!("abstract p must be captured");
        };
        assert_eq!(capture.label, pb::XmlItemLabel::Paragraph);
        assert_eq!(capture.role, "abstract");
    }

    #[test]
    fn jats_body_paragraphs_have_no_role() {
        let ancestors = path(&["article", "body", "sec"]);
        let attrs = Attrs::default();
        let Action::Capture(capture) = jats(&ctx("p", &ancestors, &attrs)) else {
            panic!("body p must be captured");
        };
        assert_eq!(capture.role, "");
    }

    #[test]
    fn uspto_claims_carry_their_number() {
        let ancestors = path(&["us-patent-grant", "claims"]);
        let attrs = Attrs(vec![("num".to_owned(), "00003".to_owned())]);
        let Action::Capture(capture) = uspto(&ctx("claim", &ancestors, &attrs)) else {
            panic!("claim must be captured");
        };
        assert_eq!(capture.ordinal, Some(3));
        assert_eq!(capture.role, "claim");
    }

    #[test]
    fn a_jats_link_yields_its_href_and_a_jats_xref_its_targets() {
        let ancestors = path(&["article", "body", "sec", "p"]);
        let attrs = Attrs(vec![(
            "xlink:href".to_owned(),
            "https://example.org/spec".to_owned(),
        )]);
        let link = jats_inline(&ctx("ext-link", &ancestors, &attrs)).expect("a link is inline");
        assert_eq!(link.hyperlink.as_deref(), Some("https://example.org/spec"));
        assert!(link.references.is_empty());

        // A JATS xref may name several targets in one attribute, and each is
        // its own edge of the reference graph.
        let attrs = Attrs(vec![
            ("ref-type".to_owned(), "bibr".to_owned()),
            ("rid".to_owned(), "b1 b2".to_owned()),
        ]);
        let xref = jats_inline(&ctx("xref", &ancestors, &attrs)).expect("an xref is inline");
        assert_eq!(xref.references, ["b1", "b2"]);
        assert_eq!(xref.reference_kind, pb::ReferenceKind::Citation);
    }

    #[test]
    fn an_unmarked_jats_xref_is_a_cross_reference_not_a_guess() {
        let ancestors = path(&["article", "body", "sec", "p"]);
        let attrs = Attrs(vec![("rid".to_owned(), "s2".to_owned())]);
        let xref = jats_inline(&ctx("xref", &ancestors, &attrs)).expect("an xref is inline");
        assert_eq!(xref.reference_kind, pb::ReferenceKind::CrossRef);
    }

    #[test]
    fn a_uspto_claim_reference_carries_the_claim_it_depends_on() {
        let ancestors = path(&["us-patent-grant", "claims", "claim", "claim-text"]);
        let attrs = Attrs(vec![("idref".to_owned(), "CLM-00001".to_owned())]);
        let inline =
            uspto_inline(&ctx("claim-ref", &ancestors, &attrs)).expect("a claim-ref is inline");
        assert_eq!(inline.references, ["CLM-00001"]);
        assert_eq!(inline.reference_kind, pb::ReferenceKind::Claim);
    }

    #[test]
    fn emphasis_maps_to_styles_and_ordinary_elements_stay_invisible() {
        let ancestors = path(&["article", "body", "sec", "p"]);
        let attrs = Attrs::default();
        let italic = jats_inline(&ctx("italic", &ancestors, &attrs)).expect("italic is inline");
        assert_eq!(italic.styles, [pb::SpanStyle::Italic]);
        assert!(jats_inline(&ctx("named-content", &ancestors, &attrs)).is_none());
        // An XBRL instance has no prose, so nothing in it is an inline run.
        assert!(inline(Dialect::Xbrl, &ctx("italic", &ancestors, &attrs)).is_none());
    }

    #[test]
    fn doclang_label_attribute_beats_the_element_name() {
        let ancestors = path(&["doclang"]);
        let attrs = Attrs(vec![("label".to_owned(), "section_header".to_owned())]);
        let Action::Capture(capture) = doclang(&ctx("item", &ancestors, &attrs)) else {
            panic!("labelled item must be captured");
        };
        assert_eq!(capture.label, pb::XmlItemLabel::SectionHeader);
    }
}
