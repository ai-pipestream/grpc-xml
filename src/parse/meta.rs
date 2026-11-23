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
    /// Local name of the element that opened the subtree.
    root: String,
    /// Attributes of the element that opened the subtree.
    attrs: Attrs,
    /// One entry per descendant element, in the order their end tags close.
    leaves: Vec<Leaf>,
    /// Every descendant's text, flattened, for the shapes that are prose.
    text: String,
    /// The subtree's text read as running prose: every text node in
    /// document order, inline markup included, collapsed once.
    prose_text: String,
}

/// One descendant element of a metadata subtree.
struct Leaf {
    name: String,
    attrs: Attrs,
    /// The element's own text, without the text of its child elements.
    text: String,
    /// All of the element's text, its children's included, in document
    /// order: what a paragraph or a statement with inline markup says.
    prose: String,
}

/// An element of a metadata subtree whose end tag has not arrived yet.
struct OpenLeaf {
    name: String,
    attrs: Attrs,
    text: String,
    /// Raw text of the element and every descendant, in document order.
    prose: String,
    /// Nesting depth of the element within the subtree, so only its own end
    /// tag closes it.
    depth: usize,
}

impl MetaTree {
    /// The text of the first descendant with this local name.
    fn leaf(&self, local: &str) -> Option<&str> {
        self.leaves
            .iter()
            .find(|leaf| leaf.name == local)
            .map(|leaf| leaf.text.as_str())
    }

    /// The texts of every descendant with this local name, in order.
    fn leaves_named<'a>(&'a self, local: &'a str) -> impl Iterator<Item = &'a str> {
        self.leaves
            .iter()
            .filter(move |leaf| leaf.name == local)
            .map(|leaf| leaf.text.as_str())
    }

    /// The prose of every descendant with this local name that has any, in
    /// order, inline markup's text included.
    fn prose_named<'a>(&'a self, local: &'a str) -> impl Iterator<Item = String> + 'a {
        self.leaves
            .iter()
            .filter(move |leaf| leaf.name == local && !leaf.prose.is_empty())
            .map(|leaf| leaf.prose.clone())
    }

    /// An attribute of the first descendant with this local name.
    fn leaf_attr(&self, local: &str, attr: &str) -> Option<&str> {
        self.leaves
            .iter()
            .find(|leaf| leaf.name == local)
            .and_then(|leaf| leaf.attrs.get(attr))
    }

    /// A leaf's text, or `None` when it is absent or empty.
    fn owned(&self, local: &str) -> Option<String> {
        self.leaf(local)
            .filter(|t| !t.is_empty())
            .map(str::to_owned)
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
        let root = self
            .stack
            .last()
            .map(|frame| frame.local.clone())
            .unwrap_or_default();
        let tree = self.read_meta_tree(root, attrs)?;
        let value = decode(shape, &tree);
        let item = pb::MetaItem {
            path,
            value: Some(value),
            source: Some(self.source.clone()),
        };
        self.counts.meta_items += 1;
        self.send(pb::parse_xml_response::Event::MetaItem(item))
    }

    /// Report a metadata record that held more elements than
    /// [`MAX_META_ELEMENTS`].
    fn warn_meta_cap(&mut self) {
        self.warn(
            pb::WarningCode::LimitReached,
            &format!(
                "a metadata record held more than {MAX_META_ELEMENTS} elements: the text of the \
                 rest is kept in the element around them and their names are not recorded"
            ),
        );
    }

    /// Read a subtree whole, from just after its start tag through its
    /// matching end tag.
    fn read_meta_tree(&mut self, root: String, attrs: &Attrs) -> Result<MetaTree, ParseError> {
        let mut tree = MetaTree {
            root,
            attrs: attrs.clone(),
            leaves: Vec::new(),
            text: String::new(),
            prose_text: String::new(),
        };
        let mut open: Vec<OpenLeaf> = Vec::new();
        // The root's raw prose: every text node in the subtree, in order.
        let mut root_prose = String::new();
        let mut depth = 1usize;
        let mut elements = 0usize;
        // Text that sits in no open child: the subtree's own prose. A
        // reference arrives as an event of its own between two text chunks,
        // so the run is gathered whole and collapsed once, at the next tag.
        let mut loose = String::new();
        loop {
            let step = self.next_step()?;
            if matches!(step, Step::Start { .. } | Step::End) {
                let run = collapse(&std::mem::take(&mut loose));
                if !run.is_empty() {
                    push_text(&mut tree.text, &run);
                }
            }
            match step {
                Step::Start { local, attrs, .. } => {
                    depth += 1;
                    elements += 1;
                    self.counts.elements_visited += 1;
                    if elements <= MAX_META_ELEMENTS {
                        open.push(OpenLeaf {
                            name: local,
                            attrs,
                            text: String::new(),
                            prose: String::new(),
                            depth,
                        });
                    } else if elements == MAX_META_ELEMENTS + 1 {
                        // Past the bound an element is not recorded, and its
                        // text runs into the nearest one that is.
                        self.warn_meta_cap();
                    }
                }
                Step::End => {
                    let closing = depth;
                    depth -= 1;
                    if depth == 0 {
                        tree.prose_text = collapse(&root_prose);
                        return Ok(tree);
                    }
                    // Only the end tag of a recorded element closes one: an
                    // element past the bound was never pushed, and its end
                    // tag closing a recorded ancestor would end that
                    // ancestor early and drop the rest of its text.
                    if open.last().is_some_and(|leaf| leaf.depth == closing)
                        && let Some(done) = open.pop()
                    {
                        let text = collapse(&done.text);
                        if !text.is_empty() {
                            push_text(&mut tree.text, &text);
                        }
                        // The child's prose is part of its parent's: a
                        // paragraph's text runs through its inline markup.
                        if let Some(parent) = open.last_mut() {
                            parent.prose.push_str(&done.prose);
                        }
                        tree.leaves.push(Leaf {
                            name: done.name,
                            attrs: done.attrs,
                            text,
                            prose: collapse(&done.prose),
                        });
                    }
                }
                Step::Text(chunk) | Step::CData(chunk) => {
                    root_prose.push_str(&chunk);
                    if let Some(leaf) = open.last_mut() {
                        leaf.text.push_str(&chunk);
                        leaf.prose.push_str(&chunk);
                    } else {
                        loose.push_str(&chunk);
                    }
                }
                Step::GeneralRef { name, resolved } => {
                    let reference = self.reference_text(&name, resolved.as_deref());
                    root_prose.push_str(&reference);
                    if let Some(leaf) = open.last_mut() {
                        leaf.text.push_str(&reference);
                        leaf.prose.push_str(&reference);
                    } else {
                        loose.push_str(&reference);
                    }
                }
                // A comment or a processing instruction inside one of these
                // records is not part of the record.
                Step::Ignorable | Step::ProcessingInstruction(_) => {}
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
///
/// A licence is often several paragraphs, each with links or emphasis in
/// it: every paragraph is kept, with the text inside its markup.
fn license(tree: &MetaTree) -> pb::MetaLicense {
    let paragraphs: Vec<String> = tree.prose_named("license-p").collect();
    let statement = if paragraphs.is_empty() {
        tree.prose_named("license-statement").next()
    } else {
        Some(paragraphs.join("\n\n"))
    };
    pb::MetaLicense {
        type_uri: tree
            .leaf_attr("license", "href")
            .or_else(|| tree.leaf_attr("license", "license-type"))
            .map(str::to_owned),
        statement,
        copyright_statement: tree.owned("copyright-statement"),
        copyright_year: tree.owned("copyright-year"),
        copyright_holder: tree.owned("copyright-holder"),
        paragraphs,
    }
}

/// A funding award or a funding statement.
///
/// An award group may name several funders and several awards, and a
/// statement is prose that may mark its funder up inline: every funder and
/// award is kept, and a statement keeps its whole sentence.
fn funding(tree: &MetaTree) -> pb::MetaFunding {
    let mut funders: Vec<String> = tree
        .leaves
        .iter()
        .filter(|leaf| leaf.name == "institution" && !leaf.text.is_empty())
        .map(|leaf| leaf.text.clone())
        .collect();
    if funders.is_empty() {
        funders = tree.prose_named("funding-source").collect();
    }
    let award_ids: Vec<String> = tree
        .leaves_named("award-id")
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
        .collect();
    let statement = if tree.root == "funding-statement" {
        (!tree.prose_text.is_empty()).then(|| tree.prose_text.clone())
    } else {
        tree.prose_named("funding-statement").next()
    };
    pb::MetaFunding {
        funder: funders.first().cloned(),
        award_id: award_ids.first().cloned(),
        statement,
        funders,
        award_ids,
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
