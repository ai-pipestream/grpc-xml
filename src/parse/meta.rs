// SPDX-License-Identifier: Apache-2.0

//! Structured metadata subtrees: publication dates, licence terms, funding,
//! classification codes and cited-reference entries.
//!
//! These are the subtrees the item mapping walks past, because flattening a
//! `<pub-date><year>2026</year><month>02</month></pub-date>` into `"2026 02"`
//! produces noise rather than content. Read as a small tree and decoded into
//! the shapes in `xml.proto` they are among the most valuable fields a
//! journal article or a patent carries, so the driver reads them here
//! instead of consuming them.
//!
//! Each rule covers one leaf structure rather than a whole metadata block:
//! `pub-date`, not `front`. That keeps every tree read here bounded by the
//! shape of one record, and it keeps the decoders below flat.

use std::io::BufRead;

use super::{Driver, ParseError, Step, collapse};
use crate::dialect::{Attrs, MetaShape};
use crate::proto::v1 as pb;

/// Upper bound on elements read from one metadata subtree.
///
/// A metadata record is a handful of elements; a document that nests
/// thousands under one `pub-date` is not describing a date. Past the bound
/// the rest of the subtree is consumed and what was read is decoded.
const MAX_META_ELEMENTS: usize = 256;

/// One metadata subtree, read whole and flattened by element name.
///
/// Metadata records are shallow and their meaning is carried by element
/// names rather than by nesting, so a flat list of named leaves decodes
/// every shape below without a per-shape walker.
pub(super) struct MetaTree {
    /// Attributes of the element that opened the subtree.
    attrs: Attrs,
    /// One entry per descendant element that had text: its local name, its
    /// own attributes, and its collapsed text, in document order.
    leaves: Vec<(String, Attrs, String)>,
    /// Every descendant's text, flattened, for the shapes that are prose.
    text: String,
}

impl MetaTree {
    /// The text of the first descendant with this local name.
    fn leaf(&self, local: &str) -> Option<&str> {
        self.leaves
            .iter()
            .find(|(name, _, _)| name == local)
            .map(|(_, _, text)| text.as_str())
    }

    /// The texts of every descendant with this local name, in order.
    fn leaves_named<'a>(&'a self, local: &'a str) -> impl Iterator<Item = &'a str> {
        self.leaves
            .iter()
            .filter(move |(name, _, _)| name == local)
            .map(|(_, _, text)| text.as_str())
    }

    /// An attribute of the first descendant with this local name.
    fn leaf_attr(&self, local: &str, attr: &str) -> Option<&str> {
        self.leaves
            .iter()
            .find(|(name, _, _)| name == local)
            .and_then(|(_, attrs, _)| attrs.get(attr))
    }

    /// A leaf's text, or `None` when it is absent or empty.
    fn owned(&self, local: &str) -> Option<String> {
        self.leaf(local)
            .filter(|t| !t.is_empty())
            .map(str::to_owned)
    }

    /// The whole subtree's text, or `None` when it has none.
    fn prose(&self) -> Option<String> {
        (!self.text.is_empty()).then(|| self.text.clone())
    }
}

impl<R: BufRead> Driver<'_, R> {
    /// Read a metadata subtree, decode it, and send it.
    ///
    /// The subtree is consumed either way: the caller has already decided
    /// this element is not content, and leaving it open would put the
    /// driver back in the middle of a record it does not map.
    pub(super) fn emit_meta(&mut self, shape: &MetaShape, attrs: &Attrs) -> Result<(), ParseError> {
        let path = self.path();
        let tree = self.read_meta_tree(attrs)?;
        let value = decode(shape, &tree);
        let item = pb::MetaItem {
            path,
            value: Some(value),
            source: Some(self.source.clone()),
        };
        self.counts.meta_items += 1;
        self.send(pb::parse_xml_response::Event::MetaItem(item))
    }

    /// Read a subtree whole, from just after its start tag through its
    /// matching end tag.
    fn read_meta_tree(&mut self, attrs: &Attrs) -> Result<MetaTree, ParseError> {
        let mut tree = MetaTree {
            attrs: attrs.clone(),
            leaves: Vec::new(),
            text: String::new(),
        };
        let mut open: Vec<(String, Attrs, String)> = Vec::new();
        let mut depth = 1usize;
        let mut elements = 0usize;
        loop {
            match self.next_step()? {
                Step::Start { local, attrs, .. } => {
                    depth += 1;
                    elements += 1;
                    self.counts.elements_visited += 1;
                    if elements <= MAX_META_ELEMENTS {
                        open.push((local, attrs, String::new()));
                    }
                }
                Step::End => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(tree);
                    }
                    if let Some((local, attrs, text)) = open.pop() {
                        let text = collapse(&text);
                        if !text.is_empty() {
                            push_text(&mut tree.text, &text);
                        }
                        tree.leaves.push((local, attrs, text));
                    }
                }
                Step::Text(chunk) => {
                    if let Some((_, _, text)) = open.last_mut() {
                        text.push_str(&chunk);
                    } else {
                        let chunk = collapse(&chunk);
                        if !chunk.is_empty() {
                            push_text(&mut tree.text, &chunk);
                        }
                    }
                }
                Step::GeneralRef { resolved, .. } => {
                    if let (Some(resolved), Some((_, _, text))) = (resolved, open.last_mut()) {
                        text.push_str(&resolved);
                    }
                }
                Step::Ignorable => {}
                Step::Declaration { .. } | Step::DocType(_) => {
                    return Err(ParseError::Malformed(
                        "a declaration inside an element".to_owned(),
                    ));
                }
                Step::Eof => {
                    return Err(ParseError::Truncated(
                        "input ended inside a metadata element".to_owned(),
                    ));
                }
            }
        }
    }
}

/// Append a word to a flattened run, separating it from what came before.
fn push_text(into: &mut String, text: &str) {
    if !into.is_empty() {
        into.push(' ');
    }
    into.push_str(text);
}

/// Decode one subtree into the shape the rule asked for.
fn decode(shape: &MetaShape, tree: &MetaTree) -> pb::meta_item::Value {
    match shape {
        MetaShape::Date => pb::meta_item::Value::Date(date(tree)),
        MetaShape::Identifier(kind) => pb::meta_item::Value::Identifier(identifier(kind, tree)),
        MetaShape::Classification(scheme) => {
            pb::meta_item::Value::Classification(classification(scheme, tree))
        }
        MetaShape::License => pb::meta_item::Value::License(license(tree)),
        MetaShape::Funding => pb::meta_item::Value::Funding(funding(tree)),
        MetaShape::Citation => pb::meta_item::Value::Citation(citation(tree)),
    }
}

/// A publication or history date.
///
/// The parts are reported as the source states them; `iso_date` is filled
/// only when the source writes the whole date itself or names all three
/// parts, because a `pub-date` with only a year means the year, not its
/// first day.
fn date(tree: &MetaTree) -> pb::MetaDate {
    let kind = tree
        .attrs
        .get("pub-type")
        .or_else(|| tree.attrs.get("date-type"))
        .or_else(|| tree.attrs.get("type"))
        .unwrap_or_default()
        .to_ascii_lowercase();
    let year = tree.leaf("year").and_then(|v| v.parse().ok());
    let month = tree.leaf("month").and_then(|v| v.parse().ok());
    let day = tree.leaf("day").and_then(|v| v.parse().ok());
    let iso_date = tree
        .attrs
        .get("iso-8601-date")
        .map(str::to_owned)
        .or_else(|| match (year, month, day) {
            (Some(y), Some(m), Some(d)) => Some(format!("{y:04}-{m:02}-{d:02}")),
            _ => None,
        });
    pb::MetaDate {
        kind,
        year,
        month,
        day,
        iso_date,
    }
}

/// An identifier of the document or of the work containing it.
fn identifier(default_kind: &str, tree: &MetaTree) -> pb::MetaIdentifier {
    let kind = tree
        .attrs
        .get("journal-id-type")
        .or_else(|| tree.attrs.get("pub-id-type"))
        .unwrap_or(default_kind)
        .to_ascii_lowercase();
    pb::MetaIdentifier {
        kind,
        value: tree.text.clone(),
        scope: tree.attrs.get("pub-type").map(str::to_owned),
    }
}

/// A classification code.
///
/// The patent schemes spell one code across child elements (`section`,
/// `class`, `subclass`, `main-group`, `subgroup`), so the code is joined
/// from those when they are present and taken as the element's own text
/// otherwise. The join is the code's own notation, not a list.
fn classification(scheme: &str, tree: &MetaTree) -> pb::MetaClassification {
    const PARTS: [&str; 4] = ["section", "class", "subclass", "main-group"];
    let mut joined: String = PARTS
        .iter()
        .filter_map(|part| tree.leaf(part))
        .collect::<Vec<_>>()
        .concat();
    // CPC and IPC write the subgroup after a solidus, which is part of the
    // notation rather than a separator this decoder invented.
    if let Some(subgroup) = tree.leaf("subgroup").filter(|s| !s.is_empty()) {
        joined.push('/');
        joined.push_str(subgroup);
    }
    let code = if joined.is_empty() {
        tree.text.clone()
    } else {
        joined
    };
    pb::MetaClassification {
        scheme: scheme.to_owned(),
        code,
        edition: tree
            .owned("classification-value")
            .or_else(|| tree.owned("edition"))
            .or_else(|| tree.owned("classification-data-source")),
        office: tree.owned("office").or_else(|| tree.owned("country")),
    }
}

/// Licence and copyright terms.
fn license(tree: &MetaTree) -> pb::MetaLicense {
    pb::MetaLicense {
        type_uri: tree
            .leaf_attr("license", "href")
            .or_else(|| tree.leaf_attr("license", "license-type"))
            .map(str::to_owned),
        statement: tree
            .owned("license-p")
            .or_else(|| tree.owned("license-statement")),
        copyright_statement: tree.owned("copyright-statement"),
        copyright_year: tree.owned("copyright-year"),
        copyright_holder: tree.owned("copyright-holder"),
    }
}

/// A funding award or a funding statement.
fn funding(tree: &MetaTree) -> pb::MetaFunding {
    pb::MetaFunding {
        funder: tree
            .owned("institution")
            .or_else(|| tree.owned("funding-source")),
        award_id: tree.owned("award-id"),
        statement: tree
            .owned("funding-statement")
            .or_else(|| tree.prose().filter(|_| tree.leaves.is_empty())),
    }
}

/// One entry of a cited-reference block.
fn citation(tree: &MetaTree) -> pb::MetaCitation {
    pb::MetaCitation {
        element_id: tree.attrs.get("id").map(str::to_owned),
        ordinal: tree.attrs.ordinal("num").or_else(|| {
            tree.leaves_named("sequence")
                .next()
                .and_then(|v| v.parse().ok())
        }),
        text: tree.text.clone(),
    }
}
