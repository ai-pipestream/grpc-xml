// SPDX-License-Identifier: Apache-2.0

//! Process counters and the periodic stdout line.
//!
//! Deliberately not a metrics framework. The fleet convention is a set of
//! monotonic counters plus one line on an interval, which is enough to tell a
//! server that is working from one that is refusing everything, costs no
//! dependency, and survives a container with no scrape target pointed at it.
//! A Prometheus endpoint, if one is ever wanted, reads these same atomics.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::sniff::Dialect;

/// Every counter the server keeps. All are monotonic and all are read
/// without synchronization, so a printed line can be a few events stale.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Parses admitted.
    pub parses_started: AtomicU64,
    /// Parses that reached their `ParseStatus` trailer.
    pub parses_ok: AtomicU64,
    /// Parses that ended with a gRPC error status.
    pub parses_failed: AtomicU64,
    /// Parses refused because the concurrency limit was reached.
    pub parses_refused: AtomicU64,
    /// Parses that ended because the document exceeded the byte cap.
    pub parses_capped: AtomicU64,
    /// Document bytes read from request streams.
    pub bytes_in: AtomicU64,
    /// Events written to response streams, `ParseStatus` included.
    pub events_out: AtomicU64,
    /// Successful parses per dialect, indexed by [`Dialect`] declaration
    /// order.
    pub by_dialect: [AtomicU64; 6],
}

impl Metrics {
    /// A fresh counter set.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Record a successful parse of a document in this dialect.
    pub fn record_success(&self, dialect: Dialect, bytes: u64, events: u64) {
        self.parses_ok.fetch_add(1, Ordering::Relaxed);
        self.bytes_in.fetch_add(bytes, Ordering::Relaxed);
        self.events_out.fetch_add(events, Ordering::Relaxed);
        let slot = match dialect {
            Dialect::Jats => 0,
            Dialect::Uspto => 1,
            Dialect::Xbrl => 2,
            Dialect::Doclang => 3,
            Dialect::Dclx => 4,
            Dialect::MetsGbs => 5,
        };
        self.by_dialect[slot].fetch_add(1, Ordering::Relaxed);
    }

    /// One line summarizing everything, in the order the fields are
    /// declared.
    #[must_use]
    pub fn line(&self) -> String {
        let get = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        format!(
            "grpc-xml metrics started={} ok={} failed={} refused={} capped={} bytes_in={} \
             events_out={} jats={} uspto={} xbrl={} doclang={} dclx={} mets_gbs={}",
            get(&self.parses_started),
            get(&self.parses_ok),
            get(&self.parses_failed),
            get(&self.parses_refused),
            get(&self.parses_capped),
            get(&self.bytes_in),
            get(&self.events_out),
            get(&self.by_dialect[0]),
            get(&self.by_dialect[1]),
            get(&self.by_dialect[2]),
            get(&self.by_dialect[3]),
            get(&self.by_dialect[4]),
            get(&self.by_dialect[5]),
        )
    }
}

/// Print [`Metrics::line`] every `interval` until the process exits.
///
/// A zero interval disables reporting entirely, which is what a deployment
/// scraping the counters some other way wants.
pub fn spawn_reporter(metrics: Arc<Metrics>, interval: Duration) {
    if interval.is_zero() {
        return;
    }
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // The first tick fires immediately; skip it so startup does not print
        // a line of zeroes.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            println!("{}", metrics.line());
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_line_reports_every_counter() {
        let metrics = Metrics::new();
        metrics.parses_started.fetch_add(3, Ordering::Relaxed);
        metrics.record_success(Dialect::Xbrl, 1024, 7);
        let line = metrics.line();
        assert!(line.contains("started=3"), "{line}");
        assert!(line.contains("ok=1"), "{line}");
        assert!(line.contains("bytes_in=1024"), "{line}");
        assert!(line.contains("events_out=7"), "{line}");
        assert!(line.contains("xbrl=1"), "{line}");
        assert!(line.contains("jats=0"), "{line}");
    }
}
