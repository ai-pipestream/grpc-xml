// SPDX-License-Identifier: Apache-2.0

//! The merge contract stated as a check: every ref in a folded fragment must
//! be dense, unique and reciprocated, or the coordinator's additive merge
//! corrupts a document this fold never sees.

use std::collections::{BTreeMap, BTreeSet};

use super::{BODY_REF, FURNITURE_REF};
use crate::document::v1 as doc;

/// Everything wrong with a folded fragment, as messages, empty when it is
/// sound.
///
/// This is the merge contract stated as a check, the same one gRParse's
/// coordinator applies before merging: the coordinator merges fragments
/// additively by renumbering refs, which only works if every ref is dense,
/// unique and reciprocated. Every fold test asserts this is empty, because a
/// fragment that fails it corrupts a document the fold never sees.
#[must_use]
pub fn integrity_errors(document: &doc::Document) -> Vec<String> {
    let mut walk = Walk::default();
    walk.roots(document);
    walk.arenas(document);
    walk.finish(document)
}

/// One pass over a document's refs, gathering what the three checks need:
/// which refs exist, who lists whom as a child, and who claims whom as a
/// parent.
#[derive(Default)]
struct Walk {
    refs: BTreeSet<String>,
    children: BTreeMap<String, BTreeSet<String>>,
    parents: Vec<(String, String)>,
    errors: Vec<String>,
}

impl Walk {
    /// The two groups that exist before any item does.
    fn roots(&mut self, document: &doc::Document) {
        for (name, root) in [
            (BODY_REF, document.body.as_ref()),
            (FURNITURE_REF, document.furniture.as_ref()),
        ] {
            self.refs.insert(name.to_owned());
            let entry = self.children.entry(name.to_owned()).or_default();
            if let Some(root) = root {
                for child in &root.children {
                    entry.insert(child.r#ref.clone());
                }
            }
        }
    }

    /// Every arena, in the order the refs number them.
    fn arenas(&mut self, document: &doc::Document) {
        for (index, group) in document.groups.iter().enumerate() {
            let expected = format!("#/groups/{index}");
            self.item(
                &group.self_ref,
                &expected,
                &group.children,
                group.parent.as_ref(),
            );
        }
        for (index, item) in document.texts.iter().enumerate() {
            let expected = format!("#/texts/{index}");
            match item.item.as_ref() {
                // CodeItem inlines its base fields, so it is walked directly
                // rather than through `text_base`.
                Some(doc::base_text_item::Item::Code(code)) => {
                    self.item(
                        &code.self_ref,
                        &expected,
                        &code.children,
                        code.parent.as_ref(),
                    );
                }
                Some(other) => match text_base(other) {
                    Some(base) => {
                        self.item(
                            &base.self_ref,
                            &expected,
                            &base.children,
                            base.parent.as_ref(),
                        );
                    }
                    None => self
                        .errors
                        .push(format!("text item {expected} has no base")),
                },
                None => self
                    .errors
                    .push(format!("text item {expected} has no variant set")),
            }
        }
        for (index, picture) in document.pictures.iter().enumerate() {
            let expected = format!("#/pictures/{index}");
            self.item(
                &picture.self_ref,
                &expected,
                &picture.children,
                picture.parent.as_ref(),
            );
        }
        for (index, table) in document.tables.iter().enumerate() {
            let expected = format!("#/tables/{index}");
            self.item(
                &table.self_ref,
                &expected,
                &table.children,
                table.parent.as_ref(),
            );
        }
    }

    /// One item: its ref must be present, unique and exactly its position.
    fn item(
        &mut self,
        self_ref: &str,
        expected: &str,
        children: &[doc::RefItem],
        parent: Option<&doc::RefItem>,
    ) {
        if self_ref.is_empty() {
            self.errors
                .push(format!("item at {expected} has an empty self_ref"));
            return;
        }
        if self_ref != expected {
            self.errors.push(format!(
                "self_ref {self_ref} does not match its arena position {expected}"
            ));
        }
        if !self.refs.insert(self_ref.to_owned()) {
            self.errors.push(format!("duplicate self_ref {self_ref}"));
        }
        let entry = self.children.entry(self_ref.to_owned()).or_default();
        for child in children {
            entry.insert(child.r#ref.clone());
        }
        if let Some(parent) = parent {
            self.parents
                .push((self_ref.to_owned(), parent.r#ref.clone()));
        }
    }

    /// Resolve every ref gathered, and every caption a table points at.
    fn finish(mut self, document: &doc::Document) -> Vec<String> {
        for (parent, listed) in &self.children {
            for child in listed {
                if !self.refs.contains(child) {
                    self.errors
                        .push(format!("child {child} of {parent} does not resolve"));
                }
            }
        }
        for (child, parent) in &self.parents {
            if !self.refs.contains(parent) {
                self.errors
                    .push(format!("parent {parent} of {child} does not resolve"));
                continue;
            }
            if !self
                .children
                .get(parent)
                .is_some_and(|listed| listed.contains(child))
            {
                self.errors
                    .push(format!("parent {parent} does not list {child} as a child"));
            }
        }
        let captions = document
            .tables
            .iter()
            .map(|table| (table.self_ref.as_str(), table.captions.as_slice()))
            .chain(
                document
                    .pictures
                    .iter()
                    .map(|picture| (picture.self_ref.as_str(), picture.captions.as_slice())),
            );
        for (owner, listed) in captions {
            for caption in listed {
                if !self.refs.contains(&caption.r#ref) {
                    self.errors.push(format!(
                        "caption {} of {owner} does not resolve",
                        caption.r#ref
                    ));
                }
            }
        }
        self.errors
    }
}

/// The shared base of every text variant that has one. `CodeItem` has none:
/// its base fields are inlined.
fn text_base(item: &doc::base_text_item::Item) -> Option<&doc::TextItemBase> {
    match item {
        doc::base_text_item::Item::Title(i) => i.base.as_ref(),
        doc::base_text_item::Item::SectionHeader(i) => i.base.as_ref(),
        doc::base_text_item::Item::ListItem(i) => i.base.as_ref(),
        doc::base_text_item::Item::Formula(i) => i.base.as_ref(),
        doc::base_text_item::Item::Text(i) => i.base.as_ref(),
        doc::base_text_item::Item::FieldHeading(i) => i.base.as_ref(),
        doc::base_text_item::Item::FieldValue(i) => i.base.as_ref(),
        doc::base_text_item::Item::Code(_) => None,
    }
}
