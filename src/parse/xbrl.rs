// SPDX-License-Identifier: Apache-2.0

//! XBRL instances: contexts and units read whole, facts streamed one event
//! each as their end tags close.

use std::io::BufRead;

use super::{Driver, ParseError, Step, collapse};
use crate::dialect::{self, Attrs};
use crate::proto::v1 as pb;
use crate::sniff::NS_XBRL_INSTANCE;

/// An XBRL fact whose value is still being read.
pub(super) struct PendingFact {
    pub(super) depth: usize,
    pub(super) fact: pb::Fact,
    pub(super) text: String,
}

impl<R: BufRead> Driver<'_, R> {
    // ------------------------------------------------------------------- XBRL

    /// XBRL instances are not a document tree: they are a flat list of
    /// contexts, units and facts under the root. Contexts and units are read
    /// whole because a fact is meaningless without them, and they always
    /// precede the facts that use them in a conformant instance; facts stream
    /// one event each.
    pub(super) fn xbrl_start(
        &mut self,
        namespace: &str,
        local: &str,
        qname: &str,
        attrs: &Attrs,
    ) -> Result<(), ParseError> {
        if namespace == NS_XBRL_INSTANCE && local == "context" {
            self.count_child(local, qname);
            let id = attrs.get("id").unwrap_or_default().to_owned();
            let context = self.read_context(id)?;
            self.counts.contexts += 1;
            self.contexts.insert(context.id.clone(), context);
            return Ok(());
        }
        if namespace == NS_XBRL_INSTANCE && local == "unit" {
            self.count_child(local, qname);
            let id = attrs.get("id").unwrap_or_default().to_owned();
            let unit = self.read_unit(id)?;
            self.counts.units += 1;
            self.units.insert(unit.id.clone(), unit);
            return Ok(());
        }
        if let Some(context_ref) = attrs.get("contextRef") {
            self.push_frame(local, qname);
            self.begin_fact(namespace, local, qname, context_ref, attrs);
            return Ok(());
        }
        if dialect::is_xbrl_infrastructure(namespace) {
            if local == "schemaRef" {
                self.warn(
                    pb::WarningCode::ExternalIdIgnored,
                    "linkbase schemaRef recorded and not dereferenced",
                );
            }
            self.count_child(local, qname);
            self.consume_subtree()?;
            return Ok(());
        }
        self.push_frame(local, qname);
        Ok(())
    }

    pub(super) fn begin_fact(
        &mut self,
        namespace: &str,
        local: &str,
        qname: &str,
        context_ref: &str,
        attrs: &Attrs,
    ) {
        let prefix = qname.split_once(':').map_or("", |(p, _)| p).to_owned();
        let unit_ref = attrs.get("unitRef").map(str::to_owned);
        let context = self.contexts.get(context_ref).cloned();
        let unit = unit_ref
            .as_deref()
            .and_then(|id| self.units.get(id))
            .cloned();
        if context.is_none() {
            self.warn(
                pb::WarningCode::DanglingReference,
                &format!("fact references undeclared context {context_ref:?}"),
            );
        }
        if unit_ref.is_some() && unit.is_none() {
            self.warn(
                pb::WarningCode::DanglingReference,
                "fact references a unit that was not declared before it",
            );
        }
        let fact = pb::Fact {
            index: self.next_index(),
            concept_namespace: namespace.to_owned(),
            concept_local_name: local.to_owned(),
            concept_prefix: prefix,
            // v1 has no linkbase resolution, so the label is the local name.
            // `WARNING_CODE_TAXONOMY_IGNORED` says so on the trailer whenever
            // a caller supplied a taxonomy expecting otherwise.
            label: local.to_owned(),
            context_ref: context_ref.to_owned(),
            context,
            unit_ref,
            unit,
            value: String::new(),
            decimals: attrs.get("decimals").map(str::to_owned),
            precision: attrs.get("precision").map(str::to_owned),
            is_nil: attrs.get("nil").is_some_and(|v| v == "true" || v == "1"),
            sign: attrs.get("sign").map(str::to_owned),
            path: self.path(),
            source: Some(self.source.clone()),
        };
        self.fact = Some(PendingFact {
            depth: self.stack.len(),
            fact,
            text: String::new(),
        });
    }

    pub(super) fn finish_fact(&mut self) -> Result<(), ParseError> {
        let Some(mut pending) = self.fact.take() else {
            return Ok(());
        };
        pending.fact.value = collapse(&pending.text);
        self.counts.facts += 1;
        self.send(pb::parse_xml_response::Event::Fact(pending.fact))
    }

    /// Read a `context` element whole, from just after its start tag to its
    /// end tag.
    pub(super) fn read_context(&mut self, id: String) -> Result<pb::XbrlContext, ParseError> {
        let mut context = pb::XbrlContext {
            id,
            period: Some(pb::XbrlPeriod::default()),
            ..pb::XbrlContext::default()
        };
        let mut path: Vec<String> = Vec::new();
        let mut text = String::new();
        let mut dimension: Option<pb::XbrlDimension> = None;
        let mut depth = 1usize;
        loop {
            match self.next_step()? {
                Step::Start { local, attrs, .. } => {
                    depth += 1;
                    text.clear();
                    match local.as_str() {
                        "identifier" => {
                            context.entity_scheme = attrs.get("scheme").map(str::to_owned);
                        }
                        "explicitMember" | "typedMember" => {
                            dimension = Some(pb::XbrlDimension {
                                dimension: attrs.get("dimension").unwrap_or_default().to_owned(),
                                member: None,
                                typed_value: None,
                                is_scenario: path.iter().any(|p| p == "scenario"),
                            });
                        }
                        _ => {}
                    }
                    path.push(local);
                }
                Step::End => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(context);
                    }
                    let closing = path.pop().unwrap_or_default();
                    let value = collapse(&text);
                    text.clear();
                    match closing.as_str() {
                        "identifier" => context.entity_identifier = Some(value),
                        "instant" => {
                            context.period.get_or_insert_default().instant = Some(value);
                        }
                        "startDate" => {
                            context.period.get_or_insert_default().start_date = Some(value);
                        }
                        "endDate" => {
                            context.period.get_or_insert_default().end_date = Some(value);
                        }
                        "forever" => context.period.get_or_insert_default().forever = true,
                        "explicitMember" => {
                            if let Some(mut d) = dimension.take() {
                                d.member = Some(value);
                                context.dimensions.push(d);
                            }
                        }
                        "typedMember" => {
                            if let Some(mut d) = dimension.take() {
                                d.typed_value = Some(value);
                                context.dimensions.push(d);
                            }
                        }
                        _ => {}
                    }
                }
                Step::Text(chunk) => text.push_str(&chunk),
                Step::GeneralRef { resolved, .. } => {
                    if let Some(resolved) = resolved {
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
                        "input ended inside an XBRL context".to_owned(),
                    ));
                }
            }
        }
    }

    /// Read a `unit` element whole.
    pub(super) fn read_unit(&mut self, id: String) -> Result<pb::XbrlUnit, ParseError> {
        let mut unit = pb::XbrlUnit {
            id,
            ..pb::XbrlUnit::default()
        };
        let mut path: Vec<String> = Vec::new();
        let mut text = String::new();
        let mut depth = 1usize;
        loop {
            match self.next_step()? {
                Step::Start { local, .. } => {
                    depth += 1;
                    text.clear();
                    path.push(local);
                }
                Step::End => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(unit);
                    }
                    let closing = path.pop().unwrap_or_default();
                    let value = collapse(&text);
                    text.clear();
                    if closing == "measure" {
                        if path.iter().any(|p| p == "unitNumerator") {
                            unit.numerator_measures.push(value);
                        } else if path.iter().any(|p| p == "unitDenominator") {
                            unit.denominator_measures.push(value);
                        } else {
                            unit.measures.push(value);
                        }
                    }
                }
                Step::Text(chunk) => text.push_str(&chunk),
                Step::GeneralRef { resolved, .. } => {
                    if let Some(resolved) = resolved {
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
                        "input ended inside an XBRL unit".to_owned(),
                    ));
                }
            }
        }
    }
}
