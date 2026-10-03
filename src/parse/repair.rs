// SPDX-License-Identifier: Apache-2.0

//! Opt-in repair of text a generator forgot to escape.
//!
//! `ParseOptions.repair_unescaped_text` exists for markup written by
//! something that does not escape its text, such as a vision-language model
//! emitting `DocLang`, where one bare `&` or `p <0.05` would otherwise fail
//! the whole document.
//!
//! The repair is split by what can do it soundly:
//!
//! - **A bare `&`** is quick-xml's own `allow_dangling_amp`: the reader
//!   already knows where a reference can start and hands an unterminated one
//!   back as text. The driver counts those.
//! - **A `<` that cannot start markup**, and **a control character XML 1.0
//!   forbids**, are this module: a byte filter between the transcoder and
//!   the reader, because quick-xml has already failed by the time it sees
//!   either.
//!
//! A whole-string repair would use three regular expressions. The filter
//! here is the same rule as a streaming state machine, so the
//! parse stays live: it holds at most the one byte after a `<` back, never a
//! document. It also skips comments as well as CDATA sections, and leaves a
//! `<` followed by any non-ASCII byte alone, because XML names may start
//! with a non-ASCII letter and an ASCII-only pattern would escape `<日本>`
//! into text.
//!
//! What the filter can produce is the security argument. It only ever
//! *removes* a byte or replaces a `<` with `&lt;`, the predefined entity for
//! that same character: it can turn would-be markup into text and never
//! text into markup. It cannot write a `<!ENTITY`, a DOCTYPE, or a reference
//! to anything but `lt`, so entity declarations are still refused, undeclared
//! references are still preserved verbatim, and nothing is fetched. Output
//! grows by at most three bytes per input byte, and the byte cap is enforced
//! on the input underneath it.

use std::io::{self, BufRead, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Repairs made during one parse, shared by the filter and the driver so
/// the trailer can report one count.
#[derive(Debug, Default, Clone)]
pub(crate) struct RepairTally(Arc<AtomicU64>);

impl RepairTally {
    /// Record `n` repairs.
    pub(crate) fn add(&self, n: u64) {
        if n > 0 {
            self.0.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Repairs recorded so far.
    pub(crate) fn count(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Where in the markup the filter is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Character data, or the inside of a tag, a declaration or an
    /// instruction: anywhere a `<` is checked.
    Text,
    /// A `<` was read and is held back until the next byte says whether it
    /// starts markup.
    Lt,
    /// `<!` and then this many bytes of `opener`, which decides whether a
    /// CDATA section or a comment is opening.
    Bang { opener: Opener, matched: usize },
    /// Inside a CDATA section; `run` counts the `]` just read.
    Cdata { run: u8 },
    /// Inside a comment; `run` counts the `-` just read.
    Comment { run: u8 },
}

/// The two `<!` constructs whose content the filter must not touch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Opener {
    Cdata,
    Comment,
}

impl Opener {
    /// What follows `<!` to open this construct.
    const fn bytes(self) -> &'static [u8] {
        match self {
            Self::Cdata => b"[CDATA[",
            Self::Comment => b"--",
        }
    }
}

/// A control character XML 1.0 does not allow anywhere in a document. Tab,
/// line feed and carriage return are the three C0 characters it does allow.
const fn is_forbidden_control(byte: u8) -> bool {
    matches!(byte, 0x00..=0x08 | 0x0B | 0x0C | 0x0E..=0x1F)
}

/// A byte after `<` that means markup: a name start, an end tag, a
/// declaration or an instruction. Any non-ASCII byte counts, because a name
/// may start with a non-ASCII letter and the filter does not guess which.
const fn starts_markup(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || matches!(byte, b'_' | b':' | b'/' | b'!' | b'?') || byte >= 0x80
}

/// The repairing byte filter. Reads UTF-8 from `inner`, which in a UTF-8
/// stream never has an ASCII byte inside a multi-byte sequence, so a
/// bytewise filter cannot split a character.
pub(crate) struct RepairReader<R> {
    inner: R,
    state: State,
    out: Vec<u8>,
    pos: usize,
    tally: RepairTally,
    eof: bool,
}

impl<R: BufRead> RepairReader<R> {
    /// Wrap a reader, counting repairs into `tally`.
    pub(crate) fn new(inner: R, tally: RepairTally) -> Self {
        Self {
            inner,
            state: State::Text,
            out: Vec::with_capacity(8 * 1024),
            pos: 0,
            tally,
            eof: false,
        }
    }

    /// Run one input byte through the state machine.
    fn step(&mut self, byte: u8, repairs: &mut u64) {
        if is_forbidden_control(byte) {
            // Dropping the character must not splice a held `<` onto what
            // follows it: `<\x01b>` is a stray `<`, not a `<b>` start tag.
            if self.state == State::Lt {
                self.out.extend_from_slice(b"&lt;");
                self.state = State::Text;
                *repairs += 1;
            }
            *repairs += 1;
            return;
        }
        match self.state {
            State::Text => self.text(byte),
            State::Lt => {
                self.state = State::Text;
                if byte == b'!' {
                    self.out.extend_from_slice(b"<!");
                    self.state = State::Bang {
                        opener: Opener::Cdata,
                        matched: 0,
                    };
                } else if starts_markup(byte) {
                    self.out.push(b'<');
                    self.out.push(byte);
                } else {
                    self.out.extend_from_slice(b"&lt;");
                    *repairs += 1;
                    self.text(byte);
                }
            }
            State::Bang { opener, matched } => {
                // The first byte after `<!` picks the only opener it can
                // still be; after that each byte must continue it.
                let opener = match (matched, byte) {
                    (0, b'-') => Opener::Comment,
                    (0, _) => Opener::Cdata,
                    _ => opener,
                };
                let expected = opener.bytes();
                if expected[matched] == byte {
                    self.out.push(byte);
                    self.state = if matched + 1 == expected.len() {
                        match opener {
                            Opener::Cdata => State::Cdata { run: 0 },
                            Opener::Comment => State::Comment { run: 0 },
                        }
                    } else {
                        State::Bang {
                            opener,
                            matched: matched + 1,
                        }
                    };
                } else {
                    // A DOCTYPE or another declaration: ordinary markup.
                    self.state = State::Text;
                    self.text(byte);
                }
            }
            State::Cdata { run } => {
                self.out.push(byte);
                self.state = match byte {
                    b']' => State::Cdata {
                        run: run.saturating_add(1).min(2),
                    },
                    b'>' if run >= 2 => State::Text,
                    _ => State::Cdata { run: 0 },
                };
            }
            State::Comment { run } => {
                self.out.push(byte);
                self.state = match byte {
                    b'-' => State::Comment {
                        run: run.saturating_add(1).min(2),
                    },
                    b'>' if run >= 2 => State::Text,
                    _ => State::Comment { run: 0 },
                };
            }
        }
    }

    /// A byte in the text state: hold a `<` back, pass anything else.
    fn text(&mut self, byte: u8) {
        if byte == b'<' {
            self.state = State::Lt;
        } else {
            self.out.push(byte);
        }
    }
}

impl<R: BufRead> BufRead for RepairReader<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        while self.pos == self.out.len() && !self.eof {
            self.out.clear();
            self.pos = 0;
            let mut repairs = 0;
            let input = self.inner.fill_buf()?;
            if input.is_empty() {
                self.eof = true;
                // A `<` at the very end cannot start markup either.
                if self.state == State::Lt {
                    self.out.extend_from_slice(b"&lt;");
                    self.state = State::Text;
                    repairs += 1;
                }
            } else {
                // Copied out because `step` needs `self` mutably; the inner
                // buffer is bounded by the transcoder's own.
                let chunk = input.to_vec();
                self.inner.consume(chunk.len());
                for byte in chunk {
                    self.step(byte, &mut repairs);
                }
            }
            self.tally.add(repairs);
        }
        Ok(&self.out[self.pos..])
    }

    fn consume(&mut self, amount: usize) {
        self.pos = (self.pos + amount).min(self.out.len());
    }
}

impl<R: BufRead> Read for RepairReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let available = self.fill_buf()?;
        let n = available.len().min(buf.len());
        buf[..n].copy_from_slice(&available[..n]);
        self.consume(n);
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A reader that hands out one byte per `fill_buf`, so every state
    /// boundary also falls on a buffer boundary.
    struct Trickle<'a>(&'a [u8]);

    impl Read for Trickle<'_> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let n = self.fill_buf()?.len().min(buf.len());
            buf[..n].copy_from_slice(&self.0[..n]);
            self.consume(n);
            Ok(n)
        }
    }

    impl BufRead for Trickle<'_> {
        fn fill_buf(&mut self) -> io::Result<&[u8]> {
            Ok(&self.0[..self.0.len().min(1)])
        }

        fn consume(&mut self, amount: usize) {
            self.0 = &self.0[amount..];
        }
    }

    /// Repair a string whole and byte by byte, check both agree, and return
    /// the result with its repair count.
    fn repair(source: &str) -> (String, u64) {
        let whole_tally = RepairTally::default();
        let mut whole = String::new();
        RepairReader::new(source.as_bytes(), whole_tally.clone())
            .read_to_string(&mut whole)
            .expect("in memory");
        let trickle_tally = RepairTally::default();
        let mut trickled = String::new();
        RepairReader::new(Trickle(source.as_bytes()), trickle_tally.clone())
            .read_to_string(&mut trickled)
            .expect("in memory");
        assert_eq!(whole, trickled, "buffer boundaries change nothing");
        assert_eq!(whole_tally.count(), trickle_tally.count());
        (whole, whole_tally.count())
    }

    #[test]
    fn a_less_than_that_cannot_start_markup_becomes_text() {
        assert_eq!(
            repair("<t>$q < 0$ and p <0.05 and $a<\\ln b$</t>"),
            (
                "<t>$q &lt; 0$ and p &lt;0.05 and $a&lt;\\ln b$</t>".to_owned(),
                3
            )
        );
        assert_eq!(repair("<t>a<<b/></t>"), ("<t>a&lt;<b/></t>".to_owned(), 1));
        assert_eq!(repair("<t/>x<"), ("<t/>x&lt;".to_owned(), 1));
    }

    #[test]
    fn markup_is_never_touched() {
        for source in [
            "<?xml version=\"1.0\"?><!DOCTYPE d [<!ENTITY e \"x\">]><d a=\"1\">&e;</d>",
            "<doclang><text>a &amp; b</text><_x/><ns:y/></doclang>",
            "<d>\u{65e5}<\u{672c}>x</\u{672c}></d>",
            "<d><![CDATA[a < b ]] ]]]></d>",
        ] {
            assert_eq!(repair(source), (source.to_owned(), 0), "{source}");
        }
    }

    #[test]
    fn cdata_and_comments_are_left_as_written() {
        assert_eq!(
            repair("<d><![CDATA[1 < 2 ]]> < <!-- 3 < 4 --> <</d>"),
            (
                "<d><![CDATA[1 < 2 ]]> &lt; <!-- 3 < 4 --> &lt;</d>".to_owned(),
                2
            )
        );
        // `]]>` closes a section only when it is two brackets and a `>`.
        assert_eq!(
            repair("<d><![CDATA[a]>]]><</d>"),
            ("<d><![CDATA[a]>]]>&lt;</d>".to_owned(), 1)
        );
    }

    #[test]
    fn forbidden_control_characters_are_dropped_and_allowed_ones_kept() {
        assert_eq!(
            repair("<d>a\u{18}b\u{0}\t\n\r</d>"),
            ("<d>ab\t\n\r</d>".to_owned(), 2)
        );
        // Dropped inside a CDATA section too: XML 1.0 forbids them there.
        assert_eq!(
            repair("<d><![CDATA[a\u{1}b]]></d>"),
            ("<d><![CDATA[ab]]></d>".to_owned(), 1)
        );
    }

    #[test]
    fn the_filter_can_never_write_a_declaration_or_a_reference() {
        // Every `<` and `&` in the output is either one the input had in
        // markup position, or the `&lt;` the filter wrote for a stray `<`.
        let (out, repairs) = repair("< !ENTITY x> <\u{1}!DOCTYPE <\u{1}b> <!-- --> & &lt;");
        assert_eq!(out, "&lt; !ENTITY x> &lt;!DOCTYPE &lt;b> <!-- --> & &lt;");
        assert_eq!(repairs, 5);
    }
}
