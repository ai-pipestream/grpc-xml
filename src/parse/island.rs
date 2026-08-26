// SPDX-License-Identifier: Apache-2.0

//! XHTML island capture: a subtree under the XHTML namespace re-serialized
//! verbatim for the HTML collector, when the caller asked for islands.

use std::io::BufRead;

use quick_xml::Writer;
use quick_xml::events::attributes::Attribute as XmlAttribute;
use quick_xml::events::{BytesEnd, BytesStart, Event};

use super::{Driver, ParseError};
use crate::dialect::Attrs;
use crate::proto::v1 as pb;

/// An XHTML subtree being re-serialized for the HTML collector.
pub(super) struct Island {
    pub(super) depth: usize,
    pub(super) writer: Writer<Vec<u8>>,
    pub(super) path: String,
    pub(super) element_id: Option<String>,
    pub(super) namespace: String,
    /// The island's character data as it is written, element boundaries
    /// already separated, for the text half of the placeholder.
    pub(super) text: String,
}

impl<R: BufRead> Driver<'_, R> {
    // ---------------------------------------------------------------- islands

    pub(super) fn begin_island(
        &mut self,
        namespace: &str,
        local: &str,
        qname: &str,
        attrs: &Attrs,
    ) {
        let mut writer = Writer::new(Vec::new());
        let mut start = BytesStart::new(qname);
        for (key, value) in &attrs.0 {
            start.push_attribute(XmlAttribute::from((key.as_str(), value.as_str())));
        }
        let _ = writer.write_event(Event::Start(start));
        self.push_frame(local, qname);
        self.island = Some(Island {
            depth: self.stack.len(),
            writer,
            path: self.path(),
            element_id: attrs.get("id").map(str::to_owned),
            namespace: namespace.to_owned(),
            text: String::new(),
        });
    }

    pub(super) fn write_island_start(&mut self, qname: &str, attrs: &Attrs) {
        let Some(island) = self.island.as_mut() else {
            return;
        };
        let mut start = BytesStart::new(qname);
        for (key, value) in &attrs.0 {
            start.push_attribute(XmlAttribute::from((key.as_str(), value.as_str())));
        }
        let _ = island.writer.write_event(Event::Start(start));
        // An element boundary is a word boundary: without this a `<p>a</p>`
        // beside a `<p>b</p>` would read as one word.
        island.text.push(' ');
    }

    pub(super) fn write_island_end(&mut self) {
        let Some(qname) = self.stack.last().map(|f| f.qname.clone()) else {
            return;
        };
        if let Some(island) = self.island.as_mut() {
            let _ = island.writer.write_event(Event::End(BytesEnd::new(qname)));
            island.text.push(' ');
        }
    }

    /// Record character data the island carries, for its text half.
    pub(super) fn write_island_text(&mut self, text: &str) {
        if let Some(island) = self.island.as_mut() {
            island.text.push_str(text);
        }
    }

    pub(super) fn finish_island(&mut self) -> Result<(), ParseError> {
        self.write_island_end();
        let Some(island) = self.island.take() else {
            return Ok(());
        };
        let event = pb::HtmlIsland {
            index: self.next_index(),
            path: island.path,
            element_id: island.element_id,
            namespace: island.namespace,
            html: island.writer.into_inner(),
            source: Some(self.source.clone()),
            text: super::collapse(&island.text),
        };
        self.counts.html_islands += 1;
        self.send(pb::parse_xml_response::Event::HtmlIsland(event))
    }
}
