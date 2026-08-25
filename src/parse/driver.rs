// SPDX-License-Identifier: Apache-2.0

//! The driver core: the forward pass over the pull parser, the prolog, the
//! open-element stack, text captures, and the machinery every mapped shape
//! shares. The shapes with state of their own live next door: tables in
//! [`super::table`], XHTML islands in [`super::island`], XBRL instances in
//! [`super::xbrl`].

use std::collections::HashMap;
use std::io::BufRead;

use quick_xml::events::{BytesText, Event};
use quick_xml::name::ResolveResult;

use super::{
    Capture, Frame, MAX_INLINE_SPANS, MAX_WARNING_KINDS, ParseError, PendingCaption, SpanBuild,
    Step, attribute_value, collapse, collapse_positions, collapsed_range, convert_error,
    resolve_reference,
};
use crate::dialect::{self, Action, Attrs, ElementCtx};
use crate::proto::v1 as pb;
use crate::security;
use crate::sniff::{self, Dialect, NS_XHTML, Signals, SniffError};
use crate::{COLLECTOR, VERSION};

pub(super) use super::Driver;

impl<R: BufRead> Driver<'_, R> {
    /// Prolog, then content, then the trailer. Returns the dialect the
    /// document was mapped with, for the process counters.
    pub(super) fn run(&mut self) -> Result<Dialect, ParseError> {
        self.read_prolog()?;
        self.content_loop()?;
        self.emit_status()?;
        Ok(self.dialect)
    }

    // ---------------------------------------------------------------- prolog

    /// Read up to and including the root start tag, resolve the dialect, and
    /// emit `XmlInfo`.
    pub(super) fn read_prolog(&mut self) -> Result<(), ParseError> {
        let mut version = None;
        let mut encoding = None;
        let mut doctype = security::Doctype::default();
        loop {
            match self.next_step()? {
                Step::Declaration {
                    version: v,
                    encoding: e,
                } => {
                    version = v;
                    encoding = e;
                }
                Step::DocType(body) => {
                    doctype = security::parse_doctype(&body);
                    match security::check_doctype(&doctype) {
                        Ok(()) => {}
                        // Both refusals are the caller's document asking for
                        // something the policy does not do, and both carry
                        // their own explanation.
                        Err(refusal) => return Err(ParseError::Refused(refusal.to_string())),
                    }
                    if doctype.system_id.is_some() || doctype.public_id.is_some() {
                        self.warn(
                            pb::WarningCode::ExternalIdIgnored,
                            "DOCTYPE external identifier recorded and not dereferenced",
                        );
                    }
                }
                Step::Text(text) if text.trim().is_empty() => {}
                Step::Text(_) => {
                    return Err(ParseError::Malformed(
                        "character data before the root element".to_owned(),
                    ));
                }
                Step::GeneralRef { name, .. } => {
                    return Err(ParseError::Malformed(format!(
                        "entity reference &{name}; before the root element"
                    )));
                }
                Step::Ignorable => {}
                Step::End => {
                    return Err(ParseError::Malformed(
                        "end tag before the root element".to_owned(),
                    ));
                }
                Step::Eof => {
                    return Err(ParseError::Truncated(
                        "the document has no root element".to_owned(),
                    ));
                }
                Step::Start {
                    namespace,
                    local,
                    qname,
                    attrs,
                } => {
                    let signals = Signals {
                        root_namespace: namespace.clone(),
                        root_local_name: local.clone(),
                        public_id: doctype.public_id.clone(),
                    };
                    let (dialect, evidence) = self.resolve_dialect(&signals)?;
                    self.dialect = dialect;
                    self.source = pb::CollectorSource {
                        collector: COLLECTOR.to_owned(),
                        model: Some(dialect.model().to_owned()),
                        version: Some(VERSION.to_owned()),
                        confidence: None,
                    };
                    if self.config.taxonomy_supplied {
                        self.warn(
                            pb::WarningCode::TaxonomyIgnored,
                            "taxonomy bytes accepted but unused: label linkbase resolution is \
                             not implemented, labels are concept local names",
                        );
                    }
                    self.push_frame(&local, &qname);
                    self.counts.elements_visited += 1;
                    let info = pb::XmlInfo {
                        dialect: dialect.to_proto() as i32,
                        evidence: evidence.to_proto() as i32,
                        root_namespace: namespace,
                        root_local_name: local,
                        doctype_name: doctype.name.clone(),
                        public_id: doctype.public_id.clone(),
                        system_id: doctype.system_id.clone(),
                        // The title is a body event; XmlInfo goes out before
                        // the parser has reached it, which is the point.
                        title: None,
                        encoding,
                        xml_version: version,
                        root_attributes: root_attributes(&attrs),
                        namespaces: namespace_bindings(&attrs),
                        schema_locations: schema_locations(&attrs),
                        language: attrs.get("lang").map(str::to_owned),
                    };
                    self.send(pb::parse_xml_response::Event::Info(info))?;
                    return Ok(());
                }
            }
        }
    }

    /// Resolve the dialect for this document: the resolution the archive
    /// driver already made when there is one, the sniff otherwise.
    pub(super) fn resolve_dialect(
        &self,
        signals: &Signals,
    ) -> Result<(Dialect, sniff::Evidence), ParseError> {
        if let Some(resolution) = self.forced {
            return Ok(resolution);
        }
        sniff::resolve(self.config.dialect, signals).map_err(|e| match e {
            SniffError::Conflict { .. } => ParseError::Ambiguous(e.to_string()),
            SniffError::Unrecognized { .. } => ParseError::Unsupported(e.to_string()),
        })
    }

    // ---------------------------------------------------------------- content

    /// The forward pass over the document body.
    pub(super) fn content_loop(&mut self) -> Result<(), ParseError> {
        loop {
            match self.next_step()? {
                Step::Start {
                    namespace,
                    local,
                    qname,
                    attrs,
                } => self.on_start(&namespace, &local, &qname, &attrs)?,
                Step::End => {
                    if self.on_end()? {
                        return Ok(());
                    }
                }
                Step::Text(text) => self.on_text(&text),
                Step::GeneralRef { name, resolved } => {
                    self.on_general_ref(&name, resolved.as_deref());
                }
                Step::Declaration { .. } => {
                    return Err(ParseError::Malformed(
                        "an XML declaration may only appear before the root element".to_owned(),
                    ));
                }
                Step::DocType(_) => {
                    return Err(ParseError::Malformed(
                        "a DOCTYPE may only appear before the root element".to_owned(),
                    ));
                }
                Step::Ignorable => {}
                Step::Eof => {
                    let open = self
                        .stack
                        .iter()
                        .map(|f| f.qname.as_str())
                        .collect::<Vec<_>>()
                        .join("/");
                    return Err(ParseError::Truncated(format!(
                        "input ended with {} element(s) still open: {open}",
                        self.stack.len()
                    )));
                }
            }
        }
    }

    pub(super) fn on_start(
        &mut self,
        namespace: &str,
        local: &str,
        qname: &str,
        attrs: &Attrs,
    ) -> Result<(), ParseError> {
        self.counts.elements_visited += 1;

        // An island swallows its whole subtree verbatim.
        if self.island.is_some() {
            self.write_island_start(qname, attrs);
            self.push_frame(local, qname);
            return Ok(());
        }
        // A capture flattens everything under it into one string. Structural
        // rules still do not fire inside one — a paragraph does not sprout a
        // nested paragraph — but the inline vocabulary does, so emphasis,
        // links and cross-references are recorded as runs over the string
        // the flattening is already building.
        if self.capture.is_some() {
            self.capture_child_boundary();
            if self.config.emit_inline_spans {
                self.open_inline_span(namespace, local, qname, attrs);
            }
            self.push_frame(local, qname);
            return Ok(());
        }
        if self.fact.is_some() {
            // XBRL facts hold simple content; any markup inside one is
            // flattened the same way a capture flattens inline markup.
            self.push_frame(local, qname);
            return Ok(());
        }
        if self.table.is_some() {
            self.table_start(local, qname, attrs);
            return Ok(());
        }
        if self.dialect == Dialect::Xbrl {
            return self.xbrl_start(namespace, local, qname, attrs);
        }
        if self.config.emit_html_islands && namespace == NS_XHTML {
            self.begin_island(namespace, local, qname, attrs);
            return Ok(());
        }

        let ancestors: Vec<String> = self.stack.iter().map(|f| f.local.clone()).collect();
        let ctx = ElementCtx {
            namespace,
            local,
            ancestors: &ancestors,
            attrs,
        };
        match dialect::action(self.dialect, &ctx) {
            Action::Descend => self.push_frame(local, qname),
            Action::Skip => self.skip_subtree(local, qname)?,
            Action::Meta(shape) => {
                if self.config.emit_source_metadata {
                    self.push_frame(local, qname);
                    let result = self.emit_meta(&shape, attrs);
                    self.stack.pop();
                    result?;
                } else {
                    self.skip_subtree(local, qname)?;
                }
            }
            Action::Table => {
                self.push_frame(local, qname);
                self.begin_table(attrs)?;
            }
            Action::Caption => {
                self.push_frame(local, qname);
                self.begin_capture(
                    dialect::Capture::new(pb::XmlItemLabel::Caption, ""),
                    attrs,
                    true,
                );
            }
            Action::Capture(spec) => {
                self.push_frame(local, qname);
                self.begin_capture(spec, attrs, false);
            }
            Action::AttrText(spec) => {
                self.push_frame(local, qname);
                if let Some(value) = attrs.get(spec.attr) {
                    let text = collapse(value);
                    if !text.is_empty() {
                        let item = self.text_item(spec.label, &spec.role, text, None, None, attrs);
                        self.send(pb::parse_xml_response::Event::TextItem(item))?;
                        self.counts.text_items += 1;
                    }
                }
            }
        }
        Ok(())
    }

    /// Handle an end tag. Returns true when the root element closed.
    pub(super) fn on_end(&mut self) -> Result<bool, ParseError> {
        let depth = self.stack.len();

        if let Some(island) = self.island.as_mut() {
            if depth == island.depth {
                self.finish_island()?;
            } else {
                self.write_island_end();
            }
            self.stack.pop();
            return Ok(self.stack.is_empty());
        }
        if let Some(capture) = self.capture.as_mut() {
            if depth == capture.depth {
                self.finish_capture()?;
            } else {
                // The innermost run still open at this depth is the one this
                // end tag closes; anything deeper closed before it.
                let chars = capture.chars;
                if let Some(span) = capture
                    .spans
                    .iter_mut()
                    .rev()
                    .find(|span| span.end.is_none() && span.depth == depth)
                {
                    span.end = Some(chars);
                }
                capture.after_child = true;
            }
            self.stack.pop();
            return Ok(self.stack.is_empty());
        }
        if let Some(fact) = self.fact.as_mut() {
            if depth == fact.depth {
                self.finish_fact()?;
            }
            self.stack.pop();
            return Ok(self.stack.is_empty());
        }
        if self.table.is_some() {
            self.table_end()?;
            self.stack.pop();
            return Ok(self.stack.is_empty());
        }
        if let Some(pending) = self.pending_caption.as_ref()
            && depth == pending.wrapper_depth
        {
            self.flush_pending_caption()?;
        }
        self.stack.pop();
        Ok(self.stack.is_empty())
    }

    pub(super) fn on_text(&mut self, text: &str) {
        if let Some(island) = self.island.as_mut() {
            let _ = island.writer.write_event(Event::Text(BytesText::new(text)));
            return;
        }
        if let Some(capture) = self.capture.as_mut() {
            capture.text.push_str(text);
            capture.chars += text.chars().count();
            capture.after_child = false;
            return;
        }
        if let Some(fact) = self.fact.as_mut() {
            fact.text.push_str(text);
            return;
        }
        if let Some(table) = self.table.as_mut()
            && let Some(cell) = table.cell.as_mut()
        {
            cell.text.push_str(text);
            return;
        }
        if !text.trim().is_empty() {
            let element = self.stack.last().map_or("document", |f| f.qname.as_str());
            self.warn(
                pb::WarningCode::UnmappedElement,
                &format!("character data in <{element}> has no mapping and was dropped"),
            );
        }
    }

    /// A general entity reference in content.
    ///
    /// `resolved` is `Some` only for character references and the five
    /// predefined entities. Everything else is a reference to a definition
    /// this parser refuses to have, so it is preserved exactly as written —
    /// dropping it would lose text, and expanding it is the vulnerability.
    pub(super) fn on_general_ref(&mut self, name: &str, resolved: Option<&str>) {
        let text = if let Some(text) = resolved {
            text.to_owned()
        } else {
            self.warn(
                pb::WarningCode::UnexpandedEntity,
                &format!(
                    "entity reference &{name}; was preserved verbatim; this parser declares \
                     and expands no entities"
                ),
            );
            format!("&{name};")
        };
        if let Some(island) = self.island.as_mut() {
            let _ = island
                .writer
                .write_event(Event::Text(BytesText::from_escaped(text)));
            return;
        }
        if let Some(capture) = self.capture.as_mut() {
            capture.text.push_str(&text);
            capture.chars += text.chars().count();
            capture.after_child = false;
            return;
        }
        if let Some(fact) = self.fact.as_mut() {
            fact.text.push_str(&text);
            return;
        }
        if let Some(table) = self.table.as_mut()
            && let Some(cell) = table.cell.as_mut()
        {
            cell.text.push_str(&text);
        }
    }

    // --------------------------------------------------------------- captures

    pub(super) fn begin_capture(
        &mut self,
        spec: dialect::Capture,
        attrs: &Attrs,
        is_caption: bool,
    ) {
        let level = spec.level.or_else(|| {
            (spec.label == pb::XmlItemLabel::SectionHeader).then(|| self.section_level())
        });
        self.capture = Some(Capture {
            depth: self.stack.len(),
            spec: dialect::Capture { level, ..spec },
            text: String::new(),
            chars: 0,
            path: self.path(),
            element_id: attrs.get("id").map(str::to_owned),
            attributes: self.reportable_attributes(attrs),
            spans: Vec::new(),
            is_caption,
            after_child: false,
        });
    }

    /// Insert the word boundary a new child element implies, and clear the
    /// flag that asked for it.
    ///
    /// Two adjacent sibling elements are two words, not one, and the space
    /// that says so is part of the capture's text, so it counts toward the
    /// offsets the inline runs are measured at.
    fn capture_child_boundary(&mut self) {
        let Some(capture) = self.capture.as_mut() else {
            return;
        };
        if capture.after_child
            && !capture.text.is_empty()
            && !capture.text.ends_with(char::is_whitespace)
        {
            capture.text.push(' ');
            capture.chars += 1;
        }
        capture.after_child = false;
    }

    /// Record an inline run for an element the dialect recognizes, opening
    /// at the capture's current length.
    ///
    /// The run's end is filled in by the matching end tag; a run whose
    /// element never closes cannot happen, because the reader rejects an
    /// unbalanced tree before the driver sees it.
    fn open_inline_span(&mut self, namespace: &str, local: &str, qname: &str, attrs: &Attrs) {
        let ancestors: Vec<String> = self.stack.iter().map(|f| f.local.clone()).collect();
        let ctx = ElementCtx {
            namespace,
            local,
            ancestors: &ancestors,
            attrs,
        };
        let Some(inline) = dialect::inline(self.dialect, &ctx) else {
            return;
        };
        let attributes = self.reportable_attributes(attrs);
        // The frame for this element is pushed by the caller after this
        // returns, so its depth is one deeper than the stack stands now.
        let depth = self.stack.len() + 1;
        if let Some(capture) = self.capture.as_mut() {
            if capture.spans.len() >= MAX_INLINE_SPANS {
                return;
            }
            capture.spans.push(SpanBuild {
                depth,
                start: capture.chars,
                end: None,
                inline,
                element_name: qname.to_owned(),
                namespace: namespace.to_owned(),
                attributes,
            });
        }
    }

    /// Translate the runs recorded against the raw captured string onto the
    /// collapsed text the item carries.
    fn finish_spans(spans: Vec<SpanBuild>, text: &str, map: &[Option<u32>]) -> Vec<pb::InlineSpan> {
        spans
            .into_iter()
            .filter_map(|span| {
                let range = collapsed_range(map, span.start, span.end.unwrap_or(map.len()))?;
                let mut hyperlink = span.inline.hyperlink;
                if hyperlink.is_none()
                    && let Some(scheme) = span.inline.link_from_text
                {
                    // The address is the run's own text: a bare `<uri>` or
                    // an `<email>` with no href attribute.
                    let run: String = text
                        .chars()
                        .skip(range.start as usize)
                        .take((range.end - range.start) as usize)
                        .collect();
                    if !run.is_empty() {
                        hyperlink = Some(format!("{scheme}{run}"));
                    }
                }
                Some(pb::InlineSpan {
                    range: Some(range),
                    styles: span.inline.styles.iter().map(|s| *s as i32).collect(),
                    hyperlink,
                    references: span.inline.references,
                    reference_kind: span.inline.reference_kind as i32,
                    element_name: span.element_name,
                    namespace: span.namespace,
                    attributes: span.attributes,
                })
            })
            .collect()
    }

    pub(super) fn finish_capture(&mut self) -> Result<(), ParseError> {
        let Some(capture) = self.capture.take() else {
            return Ok(());
        };
        let (text, positions) = collapse_positions(&capture.text);
        if text.is_empty() {
            return Ok(());
        }
        let spans = Self::finish_spans(capture.spans, &text, &positions);
        if capture.is_caption {
            // The caption belongs to the table that follows it inside the
            // same wrapper; `wrapper_depth` is where it gives up waiting.
            self.pending_caption = Some(PendingCaption {
                text,
                path: capture.path,
                element_id: capture.element_id,
                spans,
                wrapper_depth: capture.depth.saturating_sub(1),
            });
            return Ok(());
        }
        let item = pb::TextItem {
            index: self.next_index(),
            label: capture.spec.label as i32,
            role: capture.spec.role,
            text,
            level: capture.spec.level,
            ordinal: capture.spec.ordinal,
            path: capture.path,
            element_id: capture.element_id,
            attributes: capture.attributes,
            source: Some(self.source.clone()),
            // A single XML document has no pages and no boxes; the archive
            // driver is the only one in this service with geometry to state.
            bbox: None,
            page_no: None,
            spans,
        };
        self.counts.text_items += 1;
        self.send(pb::parse_xml_response::Event::TextItem(item))
    }

    pub(super) fn flush_pending_caption(&mut self) -> Result<(), ParseError> {
        let Some(pending) = self.pending_caption.take() else {
            return Ok(());
        };
        let item = pb::TextItem {
            index: self.next_index(),
            label: pb::XmlItemLabel::Caption as i32,
            role: String::new(),
            text: pending.text,
            level: None,
            ordinal: None,
            path: pending.path,
            element_id: pending.element_id,
            attributes: Vec::new(),
            source: Some(self.source.clone()),
            // A single XML document has no pages and no boxes; the archive
            // driver is the only one in this service with geometry to state.
            bbox: None,
            page_no: None,
            spans: pending.spans,
        };
        self.counts.text_items += 1;
        self.send(pb::parse_xml_response::Event::TextItem(item))
    }

    // -------------------------------------------------------------- machinery

    /// Drop an element and everything under it, and say so on the trailer.
    ///
    /// `WARNING_CODE_UNMAPPED_ELEMENT` is documented as meaning exactly
    /// this, and the skip used to be silent: publication dates, licence
    /// terms, funding, classification codes and cited-reference lists left
    /// no trace at all. The message names the element so the trailer says
    /// which mapping is missing rather than only that one is.
    fn skip_subtree(&mut self, local: &str, qname: &str) -> Result<(), ParseError> {
        self.count_child(local, qname);
        self.warn(
            pb::WarningCode::UnmappedElement,
            &format!("<{qname}> has no mapping and its subtree was skipped"),
        );
        self.consume_subtree()
    }

    /// Consume the current element's subtree, from just after its start tag
    /// through its matching end tag.
    pub(super) fn consume_subtree(&mut self) -> Result<(), ParseError> {
        let mut depth = 1usize;
        loop {
            match self.next_step()? {
                Step::Start { .. } => {
                    depth += 1;
                    self.counts.elements_visited += 1;
                }
                Step::End => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(());
                    }
                }
                Step::Eof => {
                    return Err(ParseError::Truncated(
                        "input ended inside a skipped element".to_owned(),
                    ));
                }
                _ => {}
            }
        }
    }

    /// Read one XML event and copy it into owned data.
    pub(super) fn next_step(&mut self) -> Result<Step, ParseError> {
        self.buf.clear();
        let (resolved, event) = self
            .xml
            .read_resolved_event_into(&mut self.buf)
            .map_err(|e| convert_error(&e, self.input))?;
        let step = match event {
            Event::Start(start) => {
                let qname = start.name().as_ref().to_owned();
                let local = start.local_name().as_ref().to_owned();
                let namespace = match resolved {
                    ResolveResult::Bound(ns) => ns.as_ref().to_owned(),
                    ResolveResult::Unbound | ResolveResult::Unknown(_) => String::new(),
                };
                let mut attrs = Vec::new();
                for attribute in start.attributes() {
                    let attribute = attribute.map_err(|e| ParseError::Malformed(e.to_string()))?;
                    let key = attribute.key.as_ref().to_owned();
                    let value = attribute_value(&attribute);
                    attrs.push((key, value));
                }
                Step::Start {
                    namespace,
                    local,
                    qname,
                    attrs: Attrs(attrs),
                }
            }
            // `expand_empty_elements` turns `<a/>` into Start + End, so an
            // Empty event never reaches here.
            Event::Empty(_) => unreachable!("empty elements are expanded"),
            Event::End(_) => Step::End,
            Event::Text(text) => Step::Text(text.xml10_content().into_owned()),
            Event::CData(cdata) => Step::Text(cdata.into_inner().into_owned()),
            Event::GeneralRef(reference) => {
                let name = reference.into_inner().into_owned();
                let resolved = resolve_reference(&name);
                Step::GeneralRef { name, resolved }
            }
            Event::Decl(decl) => {
                let version = decl.version().ok().map(std::borrow::Cow::into_owned);
                let encoding = decl
                    .encoding()
                    .and_then(Result::ok)
                    .map(std::borrow::Cow::into_owned);
                Step::Declaration { version, encoding }
            }
            Event::DocType(doctype) => Step::DocType(doctype.into_inner().into_owned()),
            Event::Comment(_) | Event::PI(_) => Step::Ignorable,
            Event::Eof => Step::Eof,
        };
        Ok(step)
    }

    pub(super) fn push_frame(&mut self, local: &str, qname: &str) {
        let position = self.count_child(local, qname);
        self.stack.push(Frame {
            local: local.to_owned(),
            qname: qname.to_owned(),
            position,
            children: HashMap::new(),
        });
    }

    /// Record that a child with this name was seen and return its 1-based
    /// position among same-named siblings.
    ///
    /// Called for skipped subtrees too, so a later sibling's positional path
    /// stays correct even when what came before it produced no events.
    pub(super) fn count_child(&mut self, _local: &str, qname: &str) -> usize {
        let Some(parent) = self.stack.last_mut() else {
            return 1;
        };
        let counter = parent.children.entry(qname.to_owned()).or_insert(0);
        *counter += 1;
        *counter
    }

    /// Positional path of the element on top of the stack.
    pub(super) fn path(&self) -> String {
        let mut path = String::new();
        for frame in &self.stack {
            path.push('/');
            path.push_str(&frame.qname);
            if frame.position > 1 {
                path.push('[');
                path.push_str(&frame.position.to_string());
                path.push(']');
            }
        }
        path
    }

    /// Heading depth implied by how many section containers are open.
    pub(super) fn section_level(&self) -> u32 {
        let containers = dialect::section_containers(self.dialect);
        let depth = self
            .stack
            .iter()
            .filter(|f| containers.contains(&f.local.as_str()))
            .count();
        u32::try_from(depth).unwrap_or(u32::MAX).max(1)
    }

    /// Attributes to report on an item, if the caller asked for them.
    ///
    /// Namespace declarations are dropped: they are not content, they are on
    /// every root element of every document, and the resolved namespace is
    /// already reported where it matters.
    pub(super) fn reportable_attributes(&self, attrs: &Attrs) -> Vec<pb::Attribute> {
        if !self.config.include_attributes {
            return Vec::new();
        }
        attrs
            .0
            .iter()
            .filter(|(key, _)| key != "xmlns" && !key.starts_with("xmlns:"))
            .map(|(name, value)| pb::Attribute {
                name: name.clone(),
                value: value.clone(),
            })
            .collect()
    }

    pub(super) fn text_item(
        &mut self,
        label: pb::XmlItemLabel,
        role: &str,
        text: String,
        level: Option<u32>,
        ordinal: Option<u64>,
        attrs: &Attrs,
    ) -> pb::TextItem {
        pb::TextItem {
            index: self.next_index(),
            label: label as i32,
            role: role.to_owned(),
            text,
            level,
            ordinal,
            path: self.path(),
            element_id: attrs.get("id").map(str::to_owned),
            attributes: self.reportable_attributes(attrs),
            source: Some(self.source.clone()),
            bbox: None,
            page_no: None,
            // An item whose text came from an attribute has no inline
            // markup by construction: there is no element content to mark.
            spans: Vec::new(),
        }
    }

    pub(super) fn next_index(&mut self) -> u64 {
        let index = self.index;
        self.index += 1;
        index
    }

    pub(super) fn warn(&mut self, code: pb::WarningCode, message: &str) {
        let key = (code as i32, message.to_owned());
        if let Some(count) = self.warnings.get_mut(&key) {
            *count += 1;
        } else if self.warnings.len() < MAX_WARNING_KINDS {
            self.warnings.insert(key, 1);
        }
    }

    pub(super) fn send(&mut self, event: pb::parse_xml_response::Event) -> Result<(), ParseError> {
        if (self.emit)(pb::ParseXmlResponse { event: Some(event) }) {
            Ok(())
        } else {
            Err(ParseError::ConsumerGone)
        }
    }

    pub(super) fn emit_status(&mut self) -> Result<(), ParseError> {
        let warnings = self
            .warnings
            .iter()
            .map(|((code, message), count)| pb::ParseWarning {
                code: *code,
                message: message.clone(),
                count: *count,
            })
            .collect();
        let status = pb::ParseStatus {
            dialect: self.dialect.to_proto() as i32,
            counts: Some(self.counts),
            warnings,
            bytes_consumed: self.input.bytes(),
            elapsed_millis: u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX),
        };
        self.send(pb::parse_xml_response::Event::Status(status))
    }
}

/// Every attribute of the root element except its namespace declarations.
///
/// The root never becomes an item, so `reportable_attributes` never sees it
/// and `ParseOptions.include_attributes` cannot reach it. These are carried
/// unconditionally instead: there is one root element per document, and the
/// attributes on it are the document's own statement about itself.
pub(crate) fn root_attributes(attrs: &Attrs) -> Vec<pb::Attribute> {
    attrs
        .0
        .iter()
        .filter(|(key, _)| key != "xmlns" && !key.starts_with("xmlns:"))
        .map(|(name, value)| pb::Attribute {
            name: name.clone(),
            value: value.clone(),
        })
        .collect()
}

/// The namespace declarations of the root element, decoded into bindings.
///
/// These are filtered out of every item's attributes as "not content", which
/// is true and is also why a `TextItem.path` written in qualified names had
/// nothing to resolve those names against.
pub(crate) fn namespace_bindings(attrs: &Attrs) -> Vec<pb::NamespaceBinding> {
    attrs
        .0
        .iter()
        .filter_map(|(key, value)| {
            let prefix = if key == "xmlns" {
                ""
            } else {
                key.strip_prefix("xmlns:")?
            };
            Some(pb::NamespaceBinding {
                prefix: prefix.to_owned(),
                uri: value.clone(),
            })
        })
        .collect()
}

/// The schema documents the root associates with the instance.
///
/// `xsi:schemaLocation` is a whitespace-separated sequence of alternating
/// namespace URIs and locations; a trailing namespace with no location is
/// malformed and is dropped rather than paired with an empty string.
/// `xsi:noNamespaceSchemaLocation` is a bare location for unqualified
/// content, which is the empty-namespace pair.
pub(crate) fn schema_locations(attrs: &Attrs) -> Vec<pb::SchemaLocation> {
    let mut locations = Vec::new();
    if let Some(raw) = attrs.get("schemaLocation") {
        let mut tokens = raw.split_whitespace();
        while let (Some(namespace), Some(location)) = (tokens.next(), tokens.next()) {
            locations.push(pb::SchemaLocation {
                namespace: namespace.to_owned(),
                location: location.to_owned(),
            });
        }
    }
    if let Some(location) = attrs.get("noNamespaceSchemaLocation") {
        locations.push(pb::SchemaLocation {
            namespace: String::new(),
            location: location.to_owned(),
        });
    }
    locations
}
