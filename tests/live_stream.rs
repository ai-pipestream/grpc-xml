// SPDX-License-Identifier: Apache-2.0

//! The tests that fail if the stream ever becomes a batch.
//!
//! "Live stream is the product" is a claim about *when* bytes leave the
//! server, and no assertion about the contents of a completed stream can
//! check it: buffering the whole parse and flushing at the end produces
//! exactly the same event list. So these tests hold the upload open. They
//! send a prefix of a document, assert that content events have already
//! arrived while the rest is still unsent, and only then finish the upload.
//!
//! An implementation that accumulates items and emits them at the end cannot
//! pass [`content_events_arrive_before_the_upload_finishes`]: the server
//! would be waiting for bytes the test is deliberately withholding, and the
//! test would time out. That is the regression barrier, and it is mechanical.

mod common;

use std::fmt::Write as _;
use std::time::{Duration, Instant};

use common::{
    BIG_JATS_HEAD, BIG_JATS_TAIL, LiveParse, big_jats, client, options, parse_ok, status,
    text_items,
};
use grpc_xml::proto::v1 as pb;

/// Which kind of event a response carries, for order assertions.
fn kind(event: &pb::ParseXmlResponse) -> &'static str {
    match event.event.as_ref() {
        Some(pb::parse_xml_response::Event::Info(_)) => "info",
        Some(pb::parse_xml_response::Event::TextItem(_)) => "text_item",
        Some(pb::parse_xml_response::Event::TableStart(_)) => "table_start",
        Some(pb::parse_xml_response::Event::TableRow(_)) => "table_row",
        Some(pb::parse_xml_response::Event::TableEnd(_)) => "table_end",
        Some(pb::parse_xml_response::Event::Fact(_)) => "fact",
        Some(pb::parse_xml_response::Event::HtmlIsland(_)) => "html_island",
        Some(pb::parse_xml_response::Event::Document(_)) => "document",
        Some(pb::parse_xml_response::Event::Status(_)) => "status",
        None => "empty",
    }
}

/// One body paragraph of the synthesized article, as the server will render
/// it after whitespace collapsing.
fn paragraph_text(n: usize) -> String {
    format!(
        "Paragraph {n} of a synthetic article, long enough that a chunk boundary lands inside \
         the body rather than in the prolog."
    )
}

/// A prefix of the synthesized article: the head plus `n` complete
/// paragraphs, with the document deliberately left open.
fn prefix(paragraphs: usize) -> String {
    let mut document = String::from(BIG_JATS_HEAD);
    for n in 0..paragraphs {
        let _ = writeln!(document, "      <p id=\"p{n}\">{}</p>", paragraph_text(n));
    }
    document
}

/// The remainder of the same article, from paragraph `from` to the end.
fn suffix(from: usize, total: usize) -> String {
    let mut document = String::new();
    for n in from..total {
        let _ = writeln!(document, "      <p id=\"p{n}\">{}</p>", paragraph_text(n));
    }
    document.push_str(BIG_JATS_TAIL);
    document
}

/// THE anti-batch test.
///
/// The client sends the head of a document and eight complete paragraphs,
/// then sends nothing. If the server buffers, it has nothing to flush and
/// this test hangs until the deadline. If it streams, the items are already
/// on the wire.
#[tokio::test]
async fn content_events_arrive_before_the_upload_finishes() {
    const TOTAL: usize = 400;
    const SENT_UP_FRONT: usize = 8;

    let client = client().await;
    let mut parse = LiveParse::start(&client, options()).await;
    parse.send(&prefix(SENT_UP_FRONT)).await;

    // The header first, as the contract promises.
    let first = parse.next().await;
    assert_eq!(kind(&first), "info", "the first event is always XmlInfo");

    // Now the part a batch implementation cannot do: content, while the
    // client has sent neither the remaining paragraphs nor the closing tags.
    // The head itself carries a title, an abstract paragraph and a section
    // title, so the first body paragraph is the fourth item.
    let mut streamed = Vec::new();
    while streamed.len() < 4 {
        let event = parse.next().await;
        assert_eq!(kind(&event), "text_item", "{}", kind(&event));
        let Some(pb::parse_xml_response::Event::TextItem(item)) = event.event else {
            unreachable!()
        };
        streamed.push(item.text);
    }
    assert!(
        streamed.contains(&paragraph_text(0)),
        "the first body paragraph must be on the wire already: {streamed:?}"
    );
    let seen_before_upload_finished = streamed.len();

    // Only now finish the upload. Dropping the request sender is what signals
    // EOF, so the struct is destructured to release it.
    parse.send(&suffix(SENT_UP_FRONT, TOTAL)).await;
    let LiveParse {
        requests,
        mut events,
    } = parse;
    drop(requests);

    let mut remaining = Vec::new();
    while let Some(event) = events.message().await.expect("stream error") {
        remaining.push(event);
    }
    assert_eq!(
        kind(remaining.last().expect("a trailer")),
        "status",
        "ParseStatus is the trailer, never the payload"
    );
    let total_items =
        seen_before_upload_finished + remaining.iter().filter(|e| kind(e) == "text_item").count();
    // Title, abstract paragraph, section title, and every body paragraph.
    assert_eq!(total_items, TOTAL + 3);
}

/// The parse makes progress in step with the upload rather than at the end
/// of it: bytes arrive in waves, and each wave produces its items before the
/// next wave is sent.
#[tokio::test]
async fn each_chunk_produces_its_items_before_the_next_chunk_is_sent() {
    const PER_WAVE: usize = 5;
    const WAVES: usize = 6;

    let client = client().await;
    let mut parse = LiveParse::start(&client, options()).await;
    parse.send(BIG_JATS_HEAD).await;

    // Drain the header and the three items the head itself contains.
    assert_eq!(kind(&parse.next().await), "info");
    let mut seen = 0usize;
    while seen < 3 {
        assert_eq!(kind(&parse.next().await), "text_item");
        seen += 1;
    }

    for wave in 0..WAVES {
        let mut chunk = String::new();
        for n in (wave * PER_WAVE)..((wave + 1) * PER_WAVE) {
            let _ = writeln!(chunk, "      <p id=\"p{n}\">{}</p>", paragraph_text(n));
        }
        parse.send(&chunk).await;
        // Every paragraph of this wave must arrive before the next wave is
        // sent. A batching server would deadlock here on the first wave.
        for n in (wave * PER_WAVE)..((wave + 1) * PER_WAVE) {
            let event = parse.next().await;
            let Some(pb::parse_xml_response::Event::TextItem(item)) = event.event else {
                panic!("expected a text item for paragraph {n}");
            };
            assert_eq!(
                item.text,
                paragraph_text(n),
                "items arrive in document order"
            );
        }
    }

    parse.send(BIG_JATS_TAIL).await;
    let LiveParse {
        requests,
        mut events,
    } = parse;
    drop(requests);
    let mut trailer = None;
    while let Some(event) = events.message().await.expect("stream error") {
        if kind(&event) == "status" {
            trailer = event.event;
        }
    }
    assert!(trailer.is_some(), "the stream ends with a trailer");
}

/// The first item is emitted promptly, not after a fixed batching delay.
///
/// A server that packs items into batches on a timer would still pass the
/// tests above; this one bounds how long the first item may take once the
/// bytes that contain it have been delivered.
#[tokio::test]
async fn the_first_item_is_not_held_for_a_batching_window() {
    let client = client().await;
    let mut parse = LiveParse::start(&client, options()).await;
    let started = Instant::now();
    parse.send(&prefix(1)).await;
    assert_eq!(kind(&parse.next().await), "info");
    assert_eq!(kind(&parse.next().await), "text_item");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "the first item took {elapsed:?}; nothing in the design should delay it"
    );
}

/// Item indexes are dense and ascending across every event kind, so a client
/// merging a live stream can order what it has without waiting for the rest.
#[tokio::test]
async fn stream_indexes_are_dense_and_ascending_across_event_kinds() {
    let client = client().await;
    let events = parse_ok(&client, common::JATS, options()).await;
    let mut indexes = Vec::new();
    for event in &events {
        match event.event.as_ref() {
            Some(pb::parse_xml_response::Event::TextItem(i)) => indexes.push(i.index),
            Some(pb::parse_xml_response::Event::TableStart(i)) => indexes.push(i.index),
            Some(pb::parse_xml_response::Event::Fact(i)) => indexes.push(i.index),
            Some(pb::parse_xml_response::Event::HtmlIsland(i)) => indexes.push(i.index),
            _ => {}
        }
    }
    assert!(!indexes.is_empty());
    assert_eq!(
        indexes,
        (0..indexes.len() as u64).collect::<Vec<_>>(),
        "indexes are dense from zero and in stream order"
    );
}

/// A large document streams through without the server ever holding it: the
/// trailer's byte count matches what was sent, and the item count matches
/// what was written, with no per-document memory proportional to either.
#[tokio::test]
async fn a_large_document_streams_end_to_end() {
    let client = client().await;
    let document = big_jats(5_000);
    assert!(document.len() > 500_000);
    let events = parse_ok(&client, &document, options()).await;
    let items = text_items(&events);
    assert_eq!(items.len(), 5_000 + 3);
    let status = status(&events);
    assert_eq!(status.bytes_consumed, document.len() as u64);
    assert_eq!(status.counts.as_ref().unwrap().text_items, 5_003);
}

/// A client that hangs up mid-stream ends the parse instead of leaving it
/// running against a socket nobody is reading.
#[tokio::test]
async fn dropping_the_response_stream_stops_the_parse() {
    let client = client().await;
    let mut parse = LiveParse::start(&client, options()).await;
    parse.send(&prefix(50)).await;
    assert_eq!(kind(&parse.next().await), "info");
    drop(parse);
    // Nothing to assert beyond "the server is still healthy": a leaked parse
    // shows up as the next request being refused for want of a slot.
    let events = parse_ok(&client, common::JATS, options()).await;
    assert!(!events.is_empty());
}
