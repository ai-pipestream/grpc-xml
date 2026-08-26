// SPDX-License-Identifier: Apache-2.0

//! Shared harness and fixtures for the integration tests.
//!
//! Every fixture in this file is a string literal, and the one large document
//! is synthesized in a loop. Nothing is read from disk, nothing is fetched,
//! and no corpus is committed: the tests are the specification of what these
//! dialects look like as far as this repository is concerned, so they have
//! to be readable next to the assertions that consume them. The archive
//! fixtures live in `tests/archives.rs`, built there with the same crates
//! the server reads them with.
//!
//! The harness runs a real tonic server on an ephemeral port and drives it
//! with the generated client, because the properties under test — that events
//! arrive before the upload finishes, that a cap trips mid-stream, that a
//! refusal is a status and not an event — are properties of the wire and
//! cannot be observed by calling the driver directly.

#![allow(dead_code)]

use std::fmt::Write as _;
use std::time::Duration;

use grpc_xml::proto::v1 as pb;
use grpc_xml::proto::v1::xml_parse_service_client::XmlParseServiceClient;
use grpc_xml::service::XmlGrpc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Channel;

/// How long a test waits for an event before declaring the stream stalled.
pub const EVENT_TIMEOUT: Duration = Duration::from_secs(10);

/// Start a server on an ephemeral localhost port and connect a client.
pub async fn client() -> XmlParseServiceClient<Channel> {
    client_with(XmlGrpc::new()).await
}

/// Start a server built from a specific service configuration.
pub async fn client_with(service: XmlGrpc) -> XmlParseServiceClient<Channel> {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().unwrap();
    let service = service.into_service();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("server");
    });
    // The listener is bound before the task starts, so connecting cannot race
    // the serve call.
    XmlParseServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("connect")
}

/// Default options: sniff the dialect, server byte cap, flatten XHTML.
pub fn options() -> pb::ParseOptions {
    pb::ParseOptions::default()
}

/// Build the request frames for a whole document in one go.
pub fn frames(
    document: &str,
    options: pb::ParseOptions,
    chunk_size: usize,
) -> Vec<pb::ParseXmlRequest> {
    byte_frames(document.as_bytes(), options, chunk_size)
}

/// Build the request frames for a binary payload, the shape the archive
/// dialects upload.
pub fn byte_frames(
    document: &[u8],
    options: pb::ParseOptions,
    chunk_size: usize,
) -> Vec<pb::ParseXmlRequest> {
    let mut frames = vec![pb::ParseXmlRequest {
        payload: Some(pb::parse_xml_request::Payload::Options(options)),
    }];
    frames.extend(
        document
            .chunks(chunk_size)
            .map(|chunk| pb::ParseXmlRequest {
                payload: Some(pb::parse_xml_request::Payload::Chunk(chunk.to_vec())),
            }),
    );
    frames
}

/// Parse a document end to end and return every event, or the status that
/// ended the stream.
pub async fn parse(
    client: &XmlParseServiceClient<Channel>,
    document: &str,
    options: pb::ParseOptions,
) -> Result<Vec<pb::ParseXmlResponse>, tonic::Status> {
    let mut client = client.clone();
    let requests = frames(document, options, 8 * 1024);
    let mut stream = client
        .parse_xml(tokio_stream::iter(requests))
        .await?
        .into_inner();
    let mut events = Vec::new();
    while let Some(event) = stream.message().await? {
        events.push(event);
    }
    Ok(events)
}

/// Parse a binary payload end to end and return every event, or the status
/// that ended the stream.
pub async fn parse_bytes(
    client: &XmlParseServiceClient<Channel>,
    document: &[u8],
    options: pb::ParseOptions,
) -> Result<Vec<pb::ParseXmlResponse>, tonic::Status> {
    let mut client = client.clone();
    let requests = byte_frames(document, options, 8 * 1024);
    let mut stream = client
        .parse_xml(tokio_stream::iter(requests))
        .await?
        .into_inner();
    let mut events = Vec::new();
    while let Some(event) = stream.message().await? {
        events.push(event);
    }
    Ok(events)
}

/// Parse a binary payload that is expected to succeed, with the same
/// envelope assertions as [`parse_ok`].
pub async fn parse_bytes_ok(
    client: &XmlParseServiceClient<Channel>,
    document: &[u8],
    options: pb::ParseOptions,
) -> Vec<pb::ParseXmlResponse> {
    let events = parse_bytes(client, document, options)
        .await
        .expect("parse succeeds");
    assert_envelope(&events);
    events
}

/// Parse a document that is expected to succeed, asserting the stream
/// envelope: exactly one `info` first, exactly one `status` last.
pub async fn parse_ok(
    client: &XmlParseServiceClient<Channel>,
    document: &str,
    options: pb::ParseOptions,
) -> Vec<pb::ParseXmlResponse> {
    let events = parse(client, document, options)
        .await
        .expect("parse succeeds");
    assert_envelope(&events);
    events
}

/// Assert the stream envelope of a successful parse.
fn assert_envelope(events: &[pb::ParseXmlResponse]) {
    assert!(
        events.len() >= 2,
        "a successful parse is at least info + status"
    );
    assert!(
        matches!(
            events.first().and_then(|e| e.event.as_ref()),
            Some(pb::parse_xml_response::Event::Info(_))
        ),
        "the first event must be XmlInfo"
    );
    assert!(
        matches!(
            events.last().and_then(|e| e.event.as_ref()),
            Some(pb::parse_xml_response::Event::Status(_))
        ),
        "the last event must be ParseStatus"
    );
    let infos = events
        .iter()
        .filter(|e| matches!(e.event, Some(pb::parse_xml_response::Event::Info(_))))
        .count();
    let statuses = events
        .iter()
        .filter(|e| matches!(e.event, Some(pb::parse_xml_response::Event::Status(_))))
        .count();
    assert_eq!(infos, 1, "XmlInfo is sent exactly once");
    assert_eq!(statuses, 1, "ParseStatus is sent exactly once");
}

/// A parse whose request stream the test feeds by hand, so it can hold bytes
/// back and observe what the server has already produced.
pub struct LiveParse {
    /// Sink for further request frames. Dropping it ends the upload.
    pub requests: mpsc::Sender<pb::ParseXmlRequest>,
    /// The server's event stream.
    pub events: tonic::Streaming<pb::ParseXmlResponse>,
}

impl LiveParse {
    /// Start a parse, sending only the options frame.
    pub async fn start(client: &XmlParseServiceClient<Channel>, options: pb::ParseOptions) -> Self {
        let mut client = client.clone();
        let (tx, rx) = mpsc::channel(4);
        tx.send(pb::ParseXmlRequest {
            payload: Some(pb::parse_xml_request::Payload::Options(options)),
        })
        .await
        .expect("send options");
        let events = client
            .parse_xml(ReceiverStream::new(rx))
            .await
            .expect("open stream")
            .into_inner();
        Self {
            requests: tx,
            events,
        }
    }

    /// Send one chunk of document bytes.
    pub async fn send(&self, chunk: &str) {
        self.requests
            .send(pb::ParseXmlRequest {
                payload: Some(pb::parse_xml_request::Payload::Chunk(
                    chunk.as_bytes().to_vec(),
                )),
            })
            .await
            .expect("send chunk");
    }

    /// Wait for the next event, failing the test if none arrives in time.
    pub async fn next(&mut self) -> pb::ParseXmlResponse {
        tokio::time::timeout(EVENT_TIMEOUT, self.events.message())
            .await
            .expect("the server produced no event before the deadline")
            .expect("stream error")
            .expect("stream ended early")
    }

    /// Wait for the next event, returning `None` when the stream ends.
    pub async fn try_next(&mut self) -> Option<Result<pb::ParseXmlResponse, tonic::Status>> {
        tokio::time::timeout(EVENT_TIMEOUT, self.events.message())
            .await
            .expect("the server produced no event before the deadline")
            .transpose()
    }
}

// ---------------------------------------------------------------- selectors

/// Every `TextItem` in a stream.
pub fn text_items(events: &[pb::ParseXmlResponse]) -> Vec<&pb::TextItem> {
    events
        .iter()
        .filter_map(|e| match e.event.as_ref() {
            Some(pb::parse_xml_response::Event::TextItem(item)) => Some(item),
            _ => None,
        })
        .collect()
}

/// Every `TextItem` carrying a given label.
pub fn items_labelled(
    events: &[pb::ParseXmlResponse],
    label: pb::XmlItemLabel,
) -> Vec<&pb::TextItem> {
    text_items(events)
        .into_iter()
        .filter(|item| item.label == label as i32)
        .collect()
}

/// Every `TextItem` carrying a given role.
pub fn items_with_role<'a>(
    events: &'a [pb::ParseXmlResponse],
    role: &str,
) -> Vec<&'a pb::TextItem> {
    text_items(events)
        .into_iter()
        .filter(|item| item.role == role)
        .collect()
}

/// The texts of a set of items, for golden comparison.
pub fn texts(items: &[&pb::TextItem]) -> Vec<String> {
    items.iter().map(|i| i.text.clone()).collect()
}

/// Every `XbrlNote` in a stream.
pub fn xbrl_notes(events: &[pb::ParseXmlResponse]) -> Vec<&pb::XbrlNote> {
    events
        .iter()
        .filter_map(|e| match e.event.as_ref() {
            Some(pb::parse_xml_response::Event::XbrlNote(note)) => Some(note),
            _ => None,
        })
        .collect()
}

/// Every `Fact` in a stream.
pub fn facts(events: &[pb::ParseXmlResponse]) -> Vec<&pb::Fact> {
    events
        .iter()
        .filter_map(|e| match e.event.as_ref() {
            Some(pb::parse_xml_response::Event::Fact(fact)) => Some(fact),
            _ => None,
        })
        .collect()
}

/// Every `TableRow` in a stream.
pub fn table_rows(events: &[pb::ParseXmlResponse]) -> Vec<&pb::TableRow> {
    events
        .iter()
        .filter_map(|e| match e.event.as_ref() {
            Some(pb::parse_xml_response::Event::TableRow(row)) => Some(row),
            _ => None,
        })
        .collect()
}

/// Every `HtmlIsland` in a stream.
pub fn islands(events: &[pb::ParseXmlResponse]) -> Vec<&pb::HtmlIsland> {
    events
        .iter()
        .filter_map(|e| match e.event.as_ref() {
            Some(pb::parse_xml_response::Event::HtmlIsland(island)) => Some(island),
            _ => None,
        })
        .collect()
}

/// The `XmlInfo` header of a stream.
pub fn info(events: &[pb::ParseXmlResponse]) -> &pb::XmlInfo {
    events
        .iter()
        .find_map(|e| match e.event.as_ref() {
            Some(pb::parse_xml_response::Event::Info(info)) => Some(info),
            _ => None,
        })
        .expect("stream has an XmlInfo")
}

/// The `ParseStatus` trailer of a stream.
pub fn status(events: &[pb::ParseXmlResponse]) -> &pb::ParseStatus {
    events
        .iter()
        .find_map(|e| match e.event.as_ref() {
            Some(pb::parse_xml_response::Event::Status(status)) => Some(status),
            _ => None,
        })
        .expect("stream has a ParseStatus")
}

/// Every `TableEnd` in a stream.
pub fn table_ends(events: &[pb::ParseXmlResponse]) -> Vec<&pb::TableEnd> {
    events
        .iter()
        .filter_map(|e| match e.event.as_ref() {
            Some(pb::parse_xml_response::Event::TableEnd(end)) => Some(end),
            _ => None,
        })
        .collect()
}

/// The text one run covers, sliced by code points because that is what the
/// contract says a range counts. A fixture with multi-byte text and a range
/// that is right in bytes yields the wrong string here rather than passing.
pub fn run_of(text: &str, span: &pb::InlineSpan) -> String {
    let range = span.range.as_ref().expect("every span carries its range");
    text.chars()
        .skip(range.start as usize)
        .take((range.end - range.start) as usize)
        .collect()
}

/// The inline spans of an item, by the text it covers, so an assertion
/// reads as the source does rather than as a pair of integers.
pub fn span_text(item: &pb::TextItem, span: &pb::InlineSpan) -> String {
    run_of(&item.text, span)
}

/// Every `MetaItem` in a stream.
pub fn meta_items(events: &[pb::ParseXmlResponse]) -> Vec<&pb::MetaItem> {
    events
        .iter()
        .filter_map(|e| match e.event.as_ref() {
            Some(pb::parse_xml_response::Event::MetaItem(item)) => Some(item),
            _ => None,
        })
        .collect()
}

/// True when the trailer carries at least one warning of this code.
pub fn warned(events: &[pb::ParseXmlResponse], code: pb::WarningCode) -> bool {
    status(events)
        .warnings
        .iter()
        .any(|w| w.code == code as i32)
}

// ----------------------------------------------------------------- fixtures

/// A JATS journal article exercising title, contributors, abstract, nested
/// sections, a captioned table, a figure and a reference list.
pub const JATS: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<article xmlns="http://jats.nlm.nih.gov/ns/archiving/1.3/"
         xmlns:xlink="http://www.w3.org/1999/xlink"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://jats.nlm.nih.gov/ns/archiving/1.3/ JATS-archivearticle1.xsd"
         xml:lang="en"
         article-type="research-article"
         dtd-version="1.3">
  <front>
    <journal-meta>
      <journal-title>Journal of Streaming Parsers</journal-title>
      <journal-id journal-id-type="nlm-ta">J Stream Parse</journal-id>
      <issn pub-type="epub">1234-5678</issn>
    </journal-meta>
    <article-meta>
      <article-id pub-id-type="doi">10.1234/jsp.2026.001</article-id>
      <title-group>
        <article-title>Streaming XML Without a DOM</article-title>
      </title-group>
      <contrib-group>
        <contrib contrib-type="author">
          <name><surname>Rivera</surname><given-names>Ana</given-names></name>
        </contrib>
        <contrib contrib-type="author">
          <name><surname>Okafor</surname><given-names>Chidi</given-names></name>
        </contrib>
      </contrib-group>
      <aff>Institute for Pull Parsing</aff>
      <abstract>
        <p>A collector need not build a tree to produce a document.</p>
      </abstract>
      <kwd-group><kwd>streaming</kwd><kwd>xml</kwd></kwd-group>
      <pub-date pub-type="epub"><year>2026</year><month>02</month></pub-date>
      <history><date date-type="revised"><day>04</day><month>03</month><year>2026</year></date></history>
      <permissions>
        <copyright-statement>(c) 2026 The Authors</copyright-statement>
        <copyright-year>2026</copyright-year>
        <license xlink:href="https://creativecommons.org/licenses/by/4.0/">
          <license-p>Distributed under CC BY 4.0.</license-p>
        </license>
      </permissions>
      <funding-group>
        <award-group>
          <funding-source><institution>National Science Foundation</institution></funding-source>
          <award-id>NSF-1234567</award-id>
        </award-group>
      </funding-group>
      <article-categories>
        <subj-group subj-group-type="heading"><subject>Research Article</subject></subj-group>
      </article-categories>
    </article-meta>
  </front>
  <body>
    <sec id="intro">
      <title>Introduction</title>
      <p>A pull parser yields events in document order.</p>
      <p>Each item is forwarded as soon as its end tag is read.</p>
      <sec id="scope">
        <title>Scope</title>
        <p>Four dialects are in scope.</p>
      </sec>
    </sec>
    <sec id="results">
      <title>Results</title>
      <table-wrap id="t1">
        <label>Table 1</label>
        <caption><p>Throughput by dialect.</p></caption>
        <table>
          <thead><tr><th>Dialect</th><th>MB/s</th></tr></thead>
          <tbody>
            <tr><td>JATS</td><td>180</td></tr>
            <tr><td>XBRL</td><td>240</td></tr>
          </tbody>
        </table>
      </table-wrap>
      <fig id="f1">
        <caption><p>The event pipeline.</p></caption>
        <graphic xlink:href="pipeline.png"/>
      </fig>
      <p>Throughput scales with the number of <italic>concurrent</italic> streams.</p>
      <p>See <xref ref-type="bibr" rid="b1">Rivera</xref> and the <ext-link ext-link-type="uri" xlink:href="https://example.org/spec">specification</ext-link>.</p>
    </sec>
  </body>
  <back>
    <ref-list>
      <ref id="b1"><mixed-citation>Rivera A. Parsers. 2025.</mixed-citation></ref>
      <ref id="b2"><mixed-citation>Okafor C. Streams. 2026.</mixed-citation></ref>
    </ref-list>
  </back>
</article>
"#;

/// A USPTO grant in the ST.36 shape Docling maps, DOCTYPE included so the
/// public-identifier sniff and the relative-system-identifier policy are both
/// exercised by the same fixture.
pub const USPTO: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE us-patent-grant PUBLIC "-//USPTO//DTD ICE Patent Grant V4.5 2014//EN" "us-patent-grant-v45-2014-04-03.dtd">
<us-patent-grant lang="EN" country="US" date-produced="20260210">
  <us-bibliographic-data-grant>
    <publication-reference>
      <document-id>
        <country>US</country><doc-number>11999999</doc-number>
        <kind>B2</kind><date>20260210</date>
      </document-id>
    </publication-reference>
    <application-reference appl-type="utility">
      <document-id><doc-number>17123456</doc-number><date>20240401</date></document-id>
    </application-reference>
    <invention-title id="d2e43">Method for streaming structured documents</invention-title>
    <us-parties>
      <inventors>
        <inventor sequence="001">
          <addressbook><last-name>Rivera</last-name><first-name>Ana</first-name></addressbook>
        </inventor>
      </inventors>
      <assignees>
        <assignee><orgname>Acme Streaming Corp</orgname></assignee>
      </assignees>
    </us-parties>
    <classifications-cpc>
      <main-cpc>
        <classification-cpc>
          <cpc-version-indicator><date>20260101</date></cpc-version-indicator>
          <section>G</section><class>06</class><subclass>F</subclass>
          <main-group>16</main-group><subgroup>93</subgroup>
        </classification-cpc>
      </main-cpc>
    </classifications-cpc>
    <us-references-cited>
      <citation id="cit-0001">
        <patcit num="00001"><document-id><country>US</country><doc-number>9876543</doc-number></document-id></patcit>
      </citation>
    </us-references-cited>
  </us-bibliographic-data-grant>
  <abstract id="abstract"><p id="p-0001">A method streams document items as they are parsed.</p></abstract>
  <description id="description">
    <heading id="h-0001" level="1">BACKGROUND</heading>
    <p id="p-0002">Prior systems buffered the entire document before emitting it.</p>
    <description-of-drawings>
      <p id="p-0003">FIG. 1 is a block diagram of the pipeline.</p>
    </description-of-drawings>
    <heading id="h-0002" level="1">DETAILED DESCRIPTION</heading>
    <p id="p-0004">The parser emits an event per completed element.</p>
    <img id="img-0001" file="US11999999-20260210-D00001.TIF" wi="60" he="40"/>
  </description>
  <claims id="claims">
    <claim id="CLM-00001" num="00001"><claim-text>1. A method comprising streaming items.</claim-text></claim>
    <claim id="CLM-00002" num="00002"><claim-text>2. The method of <claim-ref idref="CLM-00001">claim 1</claim-ref>, wherein items are ordered.</claim-text></claim>
    <claim id="CLM-00003" num="00003"><claim-text>3. The method of claim 1, further comprising a trailer.</claim-text></claim>
  </claims>
</us-patent-grant>
"#;

/// An XBRL instance with a duration context, an instant context, a unit, a
/// dimensioned context and four facts, one of them nil.
pub const XBRL: &str = r##"<?xml version="1.0" encoding="UTF-8"?>
<xbrl xmlns="http://www.xbrl.org/2003/instance"
      xmlns:link="http://www.xbrl.org/2003/linkbase"
      xmlns:xlink="http://www.w3.org/1999/xlink"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
      xmlns:us-gaap="http://fasb.org/us-gaap/2026">
  <link:schemaRef xlink:type="simple" xlink:href="acme-20261231.xsd"/>
  <context id="D2026">
    <entity><identifier scheme="http://www.sec.gov/CIK">0000123456</identifier></entity>
    <period><startDate>2026-01-01</startDate><endDate>2026-12-31</endDate></period>
  </context>
  <context id="I2026">
    <entity><identifier scheme="http://www.sec.gov/CIK">0000123456</identifier></entity>
    <period><instant>2026-12-31</instant></period>
  </context>
  <context id="D2026-NA">
    <entity>
      <identifier scheme="http://www.sec.gov/CIK">0000123456</identifier>
      <segment>
        <xbrldi:explicitMember dimension="us-gaap:StatementGeographicalAxis">us-gaap:NorthAmericaMember</xbrldi:explicitMember>
      </segment>
    </entity>
    <period><startDate>2026-01-01</startDate><endDate>2026-12-31</endDate></period>
  </context>
  <unit id="usd"><measure>iso4217:USD</measure></unit>
  <unit id="usd-per-share">
    <divide>
      <unitNumerator><measure>iso4217:USD</measure></unitNumerator>
      <unitDenominator><measure>xbrli:shares</measure></unitDenominator>
    </divide>
  </unit>
  <us-gaap:Assets id="f-assets" contextRef="I2026" unitRef="usd" decimals="-6">1234000000</us-gaap:Assets>
  <us-gaap:Revenues contextRef="D2026" unitRef="usd" decimals="-6">987000000</us-gaap:Revenues>
  <us-gaap:Revenues contextRef="D2026-NA" unitRef="usd" decimals="-6">412000000</us-gaap:Revenues>
  <us-gaap:EarningsPerShareBasic contextRef="D2026" unitRef="usd-per-share" decimals="2" xsi:nil="true"/>
  <link:footnoteLink xlink:type="extended" xlink:role="http://www.xbrl.org/2003/role/link">
    <link:loc xlink:type="locator" xlink:href="#f-assets" xlink:label="fact-assets"/>
    <link:footnoteArc xlink:type="arc" xlink:arcrole="http://www.xbrl.org/2003/arcrole/fact-footnote" xlink:from="fact-assets" xlink:to="fn-1"/>
    <link:footnote xlink:type="resource" xlink:label="fn-1" xlink:role="http://www.xbrl.org/2003/role/footnote" xml:lang="en">Includes restricted cash of 12 million.</link:footnote>
  </link:footnoteLink>
</xbrl>
"##;

/// A `DocLang` document in both spellings: elements named for their label, and
/// a generic `item` carrying a `DocItemLabel` short name.
pub const DOCLANG: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<doclang xmlns="http://docling-project.org/ns/doclang/v1" version="1">
  <metadata><origin>quarterly.pdf</origin></metadata>
  <title>Quarterly Operations Review</title>
  <section level="1">
    <section-header level="1">Summary</section-header>
    <paragraph>Throughput rose in every region.</paragraph>
    <list-item ordinal="1">North America up 12 percent.</list-item>
    <list-item ordinal="2">EMEA up 4 percent.</list-item>
    <caption>Regional throughput.</caption>
    <table>
      <row><cell>Region</cell><cell>Delta</cell></row>
      <row><cell>NA</cell><cell>+12%</cell></row>
      <row><cell>EMEA</cell><cell>+4%</cell></row>
    </table>
    <item label="footnote">Figures are unaudited.</item>
    <section level="2">
      <section-header>Outlook</section-header>
      <paragraph>Guidance is unchanged.</paragraph>
    </section>
  </section>
</doclang>
"#;

/// A JATS article whose table is in the OASIS CALS model the standard also
/// admits: declared column geometry, per-cell alignment, and cell text that
/// carries emphasis and a citation into the reference list.
///
/// Every cell that carries a run carries multi-byte text as well, so a span
/// range that is right in bytes and wrong in Unicode scalar values fails
/// here rather than passing quietly.
pub const JATS_CALS_TABLE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<article xmlns="http://jats.nlm.nih.gov/ns/archiving/1.3/"
         xmlns:xlink="http://www.w3.org/1999/xlink">
  <front><article-meta><title-group><article-title>Durchsatz</article-title></title-group></article-meta></front>
  <body>
    <sec id="messung">
      <title>Messung</title>
      <table-wrap id="t1">
        <caption><p>Durchsatz je Dialekt.</p></caption>
        <table>
          <tgroup cols="2">
            <colspec colname="dialekt" colwidth="2*" align="left"/>
            <colspec colname="wert" colwidth="1*" align="right" valign="bottom"/>
            <thead>
              <row><entry>Dialekt</entry><entry>MB/s</entry></row>
            </thead>
            <tbody>
              <row>
                <entry align="center">Für <italic>λόγος</italic> gemessen</entry>
                <entry>180</entry>
              </row>
              <row>
                <entry>Wie <xref ref-type="bibr" rid="b1">Rivera</xref> für μ zeigt</entry>
                <entry valign="top">240</entry>
              </row>
            </tbody>
          </tgroup>
        </table>
      </table-wrap>
    </sec>
  </body>
  <back>
    <ref-list>
      <ref id="b1"><mixed-citation>Rivera A. Parsers. 2025.</mixed-citation></ref>
    </ref-list>
  </back>
</article>
"#;

/// A `DocLang` document whose lists nest, so the depth the wire reports and
/// the groups the fold builds have something to be right about.
///
/// The bulleted list inside the ordered one is a sibling of the item it
/// follows, which is where a container-nested list is written; a nested list
/// written *inside* an item is flattened into that item's text, as anything
/// inside a capture is.
pub const DOCLANG_NESTED_LISTS: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<doclang xmlns="http://docling-project.org/ns/doclang/v1" version="1">
  <title>Ablauf</title>
  <paragraph>Vor der Liste.</paragraph>
  <list type="order">
    <list-item>Erste Stufe</list-item>
    <list-item>Zweite Stufe</list-item>
    <list>
      <list-item>Untereintrag α</list-item>
      <list-item>Untereintrag β</list-item>
    </list>
    <list-item>Dritte Stufe</list-item>
  </list>
  <paragraph>Nach der Liste.</paragraph>
  <ul>
    <li>Ein zweiter Lauf</li>
  </ul>
</doclang>
"#;

/// A JATS article carrying an XHTML island, for the hand-off path.
pub const JATS_WITH_ISLAND: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<article xmlns="http://jats.nlm.nih.gov/ns/archiving/1.3/"
         xmlns:xhtml="http://www.w3.org/1999/xhtml">
  <front><article-meta><title-group><article-title>Islands</article-title></title-group></article-meta></front>
  <body>
    <sec id="s1">
      <title>Embedded markup</title>
      <p>Text before the island.</p>
      <xhtml:div id="widget" class="callout">
        <xhtml:p>Rendu par le collecteur <xhtml:em>HTML</xhtml:em>.</xhtml:p>
        <xhtml:p>Fin de l&#8217;encart&#160;: &#955;.</xhtml:p>
      </xhtml:div>
      <p>Text after the island.</p>
    </sec>
  </body>
</article>
"#;

/// Build a JATS article with `paragraphs` body paragraphs.
///
/// Synthesized rather than committed: the streaming tests need a document
/// larger than one chunk and larger than the parser's read buffer, and a
/// multi-megabyte fixture in git would be exactly the "huge binary" the
/// guidelines forbid.
pub fn big_jats(paragraphs: usize) -> String {
    let mut document = String::with_capacity(paragraphs * 200 + 512);
    document.push_str(BIG_JATS_HEAD);
    for n in 0..paragraphs {
        let _ = writeln!(
            document,
            "      <p id=\"p{n}\">Paragraph {n} of a synthetic article, long enough that a \
             chunk boundary lands inside the body rather than in the prolog.</p>"
        );
    }
    document.push_str(BIG_JATS_TAIL);
    document
}

/// Everything in [`big_jats`] before the generated paragraphs. Exposed so the
/// live-stream tests can send a prefix that is a complete, parsable head of a
/// document without being a complete document.
pub const BIG_JATS_HEAD: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<article xmlns="http://jats.nlm.nih.gov/ns/archiving/1.3/">
  <front><article-meta>
    <title-group><article-title>A Long Article</article-title></title-group>
    <abstract><p>Synthesized so the body outgrows a single chunk.</p></abstract>
  </article-meta></front>
  <body>
    <sec id="body">
      <title>Body</title>
"#;

/// Everything in [`big_jats`] after the generated paragraphs.
pub const BIG_JATS_TAIL: &str = "    </sec>\n  </body>\n</article>\n";
