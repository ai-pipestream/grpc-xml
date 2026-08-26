// SPDX-License-Identifier: Apache-2.0

//! Table streaming: `TableStart` when the table element opens, one
//! `TableRow` per closed row, `TableEnd` with the counts and the declared
//! column geometry when it closes.
//!
//! A cell is a capture like any other: its markup flattens into one string,
//! and the inline vocabulary of the dialect leaves a run over that string
//! rather than being deleted with the tags. The runs are measured against the
//! raw text the cell is accumulating and translated onto the collapsed text
//! the cell carries, exactly as [`super::driver`] does for a `TextItem`.

use std::io::BufRead;

use super::{Driver, MAX_INLINE_SPANS, ParseError, SpanBuild, collapse, collapse_positions};
use crate::dialect::{
    Attrs, CELL_ELEMENTS, COLUMN_SPEC_ELEMENTS, HEADER_CELL_ELEMENTS, HEADER_SECTION_ELEMENTS,
    ROW_ELEMENTS,
};
use crate::proto::v1 as pb;

/// A table being streamed.
pub(super) struct Table {
    pub(super) depth: usize,
    /// Identifier the row and end events carry, matching `TableStart`.
    pub(super) reference: String,
    pub(super) row_index: u32,
    pub(super) column_count: u32,
    pub(super) header_sections: usize,
    pub(super) row: Option<Row>,
    pub(super) cell: Option<Cell>,
    /// The column geometry the source declares, in the order it declares it.
    /// It rides on `TableEnd`: a `colspec` is a child of the table, so the
    /// table has already opened by the time the source states one.
    pub(super) columns: Vec<pb::ColumnSpec>,
}

/// A table row being assembled.
pub(super) struct Row {
    pub(super) is_header: bool,
    pub(super) cells: Vec<pb::TableCell>,
    pub(super) next_column: u32,
}

/// A table cell being assembled.
pub(super) struct Cell {
    pub(super) depth: usize,
    pub(super) text: String,
    /// Code points appended to `text` so far, kept alongside it so a span
    /// boundary costs a read rather than a walk of the whole string.
    pub(super) chars: usize,
    pub(super) column_index: u32,
    pub(super) column_span: u32,
    pub(super) row_span: u32,
    pub(super) is_header: bool,
    pub(super) align: pb::Alignment,
    pub(super) valign: pb::VerticalAlignment,
    /// Inline runs recognized inside this cell, in the order they opened.
    pub(super) spans: Vec<SpanBuild>,
    /// True when the previous child event was an element end, which is where
    /// a word boundary between two sibling elements belongs.
    pub(super) after_child: bool,
}

impl<R: BufRead> Driver<'_, R> {
    // ----------------------------------------------------------------- tables

    pub(super) fn begin_table(&mut self, attrs: &Attrs) -> Result<(), ParseError> {
        let table_ref = format!("t{}", self.counts.tables + 1);
        let caption = self.pending_caption.take().map(|c| c.text);
        let start = pb::TableStart {
            index: self.next_index(),
            table_ref: table_ref.clone(),
            caption,
            path: self.path(),
            element_id: attrs.get("id").map(str::to_owned),
            source: Some(self.source.clone()),
        };
        self.table = Some(Table {
            depth: self.stack.len(),
            reference: table_ref,
            row_index: 0,
            column_count: 0,
            header_sections: 0,
            row: None,
            cell: None,
            columns: Vec::new(),
        });
        self.counts.tables += 1;
        self.send(pb::parse_xml_response::Event::TableStart(start))
    }

    pub(super) fn table_start(&mut self, namespace: &str, local: &str, qname: &str, attrs: &Attrs) {
        let Some(table) = self.table.as_mut() else {
            return;
        };
        if table.cell.is_some() {
            // Markup inside a cell flattens into the cell's text, and the
            // inline vocabulary leaves a run over the part of that text it
            // covered.
            self.cell_child_boundary();
            if self.config.emit_inline_spans {
                self.open_cell_span(namespace, local, qname, attrs);
            }
            self.push_frame(local, qname);
            return;
        }
        if COLUMN_SPEC_ELEMENTS.contains(&local) {
            let spec = column_spec(attrs);
            // An XHTML `col` may stand for several columns at once; CALS
            // repeats the element instead. Expanding here means index N of
            // the list is column N either way.
            let span = attrs
                .get("span")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(1)
                .clamp(1, MAX_COLUMN_SPECS);
            for _ in 0..span {
                if table.columns.len() >= MAX_COLUMN_SPECS {
                    break;
                }
                table.columns.push(spec.clone());
            }
            self.push_frame(local, qname);
            return;
        }
        if HEADER_SECTION_ELEMENTS.contains(&local) {
            table.header_sections += 1;
        } else if ROW_ELEMENTS.contains(&local) {
            table.row = Some(Row {
                is_header: table.header_sections > 0,
                cells: Vec::new(),
                next_column: 0,
            });
        } else if CELL_ELEMENTS.contains(&local) {
            let column_span = attrs
                .get("colspan")
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(1)
                .max(1);
            // CALS spells a vertical span as `morerows`, counting the extra
            // rows rather than the total.
            let row_span = attrs
                .get("rowspan")
                .and_then(|v| v.parse::<u32>().ok())
                .or_else(|| {
                    attrs
                        .get("morerows")
                        .and_then(|v| v.parse::<u32>().ok().map(|m| m + 1))
                })
                .unwrap_or(1)
                .max(1);
            let is_header = HEADER_CELL_ELEMENTS.contains(&local) || table.header_sections > 0;
            let column_index = table.row.as_ref().map_or(0, |r| r.next_column);
            table.cell = Some(Cell {
                // The frame for this cell is pushed at the end of this
                // function, and `on_end` compares against the stack as it
                // stands before the pop, so the cell's depth is one deeper
                // than the stack is right now.
                depth: self.stack.len() + 1,
                text: String::new(),
                chars: 0,
                column_index,
                column_span,
                row_span,
                is_header,
                align: horizontal(attrs.get("align")),
                valign: vertical(attrs.get("valign")),
                spans: Vec::new(),
                after_child: false,
            });
        }
        self.push_frame(local, qname);
    }

    /// Insert the word boundary a new child element implies inside a cell,
    /// and clear the flag that asked for it.
    ///
    /// Two adjacent sibling elements are two words, and the space that says
    /// so is part of the cell's text, so it counts toward the offsets the
    /// inline runs are measured at.
    fn cell_child_boundary(&mut self) {
        let Some(cell) = self.table.as_mut().and_then(|table| table.cell.as_mut()) else {
            return;
        };
        if cell.after_child && !cell.text.is_empty() && !cell.text.ends_with(char::is_whitespace) {
            cell.text.push(' ');
            cell.chars += 1;
        }
        cell.after_child = false;
    }

    /// Record an inline run inside the open cell, at its current length.
    fn open_cell_span(&mut self, namespace: &str, local: &str, qname: &str, attrs: &Attrs) {
        let Some(start) = self
            .table
            .as_ref()
            .and_then(|table| table.cell.as_ref())
            .map(|cell| cell.chars)
        else {
            return;
        };
        let Some(span) = self.build_inline_span(namespace, local, qname, attrs, start) else {
            return;
        };
        if let Some(cell) = self.table.as_mut().and_then(|table| table.cell.as_mut())
            && cell.spans.len() < MAX_INLINE_SPANS
        {
            cell.spans.push(span);
        }
    }

    /// Close the innermost inline run of the open cell that this end tag
    /// ends. Returns true when a cell is open, so the caller knows the end
    /// tag belonged to the cell's content rather than to the table's
    /// structure.
    fn close_cell_span(&mut self, depth: usize) -> bool {
        let Some(cell) = self.table.as_mut().and_then(|table| table.cell.as_mut()) else {
            return false;
        };
        let chars = cell.chars;
        if let Some(span) = cell
            .spans
            .iter_mut()
            .rev()
            .find(|span| span.end.is_none() && span.depth == depth)
        {
            span.end = Some(chars);
        }
        cell.after_child = true;
        true
    }

    pub(super) fn table_end(&mut self) -> Result<(), ParseError> {
        let depth = self.stack.len();
        // An end tag inside a cell closes one of the cell's inline runs,
        // never the cell and never a row. This runs before the table is
        // borrowed so the run bookkeeping can own the cell for its own call.
        let inside_cell = self
            .table
            .as_ref()
            .and_then(|table| table.cell.as_ref())
            .is_some_and(|cell| depth != cell.depth);
        if inside_cell {
            self.close_cell_span(depth);
            return Ok(());
        }
        let Some(table) = self.table.as_mut() else {
            return Ok(());
        };
        if table.cell.is_some() {
            let cell = table.cell.take().expect("checked just above");
            let (text, positions) = collapse_positions(&cell.text);
            debug_assert_eq!(text, collapse(&cell.text));
            let spans = Self::finish_spans(cell.spans, &text, &positions);
            if let Some(row) = table.row.as_mut() {
                row.next_column = cell.column_index + cell.column_span;
                row.cells.push(pb::TableCell {
                    column_index: cell.column_index,
                    text,
                    column_span: cell.column_span,
                    row_span: cell.row_span,
                    is_header: cell.is_header,
                    spans,
                    align: cell.align as i32,
                    valign: cell.valign as i32,
                });
            }
            return Ok(());
        }
        // Which structural element closed is decided by what is open, since
        // the stack frame is still the one that is about to be popped.
        let closing = self
            .stack
            .last()
            .map_or("", |f| f.local.as_str())
            .to_owned();
        if HEADER_SECTION_ELEMENTS.contains(&closing.as_str()) {
            table.header_sections = table.header_sections.saturating_sub(1);
            return Ok(());
        }
        if ROW_ELEMENTS.contains(&closing.as_str())
            && let Some(row) = table.row.take()
        {
            if row.cells.is_empty() {
                return Ok(());
            }
            let width = row
                .cells
                .iter()
                .map(|c| c.column_index + c.column_span)
                .max()
                .unwrap_or(0);
            table.column_count = table.column_count.max(width);
            let is_header = row.is_header || row.cells.iter().all(|c| c.is_header);
            let event = pb::TableRow {
                table_ref: table.reference.clone(),
                row_index: table.row_index,
                is_header,
                cells: row.cells,
            };
            table.row_index += 1;
            self.counts.table_rows += 1;
            return self.send(pb::parse_xml_response::Event::TableRow(event));
        }
        if depth == table.depth {
            let end = pb::TableEnd {
                table_ref: table.reference.clone(),
                row_count: table.row_index,
                column_count: table.column_count,
                columns: std::mem::take(&mut table.columns),
            };
            self.table = None;
            return self.send(pb::parse_xml_response::Event::TableEnd(end));
        }
        Ok(())
    }
}

/// Upper bound on declared columns kept for one table.
///
/// An XHTML `col span="4000000000"` is one attribute asking for unbounded
/// memory; a real table's column count is orders of magnitude below this.
const MAX_COLUMN_SPECS: usize = 4096;

/// One `colspec` or `col` element as the geometry it declares.
///
/// CALS names the width `colwidth` and XHTML names it `width`; both are kept
/// exactly as written, because a CALS `2*` is a share of the table and a
/// `30%` is a share of something else again, and resolving either here would
/// be inventing a page this service never sees.
fn column_spec(attrs: &Attrs) -> pb::ColumnSpec {
    pb::ColumnSpec {
        name: attrs.get("colname").unwrap_or_default().to_owned(),
        width: attrs
            .get("colwidth")
            .or_else(|| attrs.get("width"))
            .unwrap_or_default()
            .to_owned(),
        align: horizontal(attrs.get("align")) as i32,
        valign: vertical(attrs.get("valign")) as i32,
    }
}

/// The horizontal alignment an `align` attribute states, matched
/// case-insensitively because CALS and XHTML both admit either casing.
fn horizontal(value: Option<&str>) -> pb::Alignment {
    match value
        .map(str::trim)
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "left" => pb::Alignment::Left,
        "center" | "centre" => pb::Alignment::Center,
        "right" => pb::Alignment::Right,
        "justify" => pb::Alignment::Justify,
        "char" => pb::Alignment::Char,
        _ => pb::Alignment::Unspecified,
    }
}

/// The vertical alignment a `valign` attribute states.
fn vertical(value: Option<&str>) -> pb::VerticalAlignment {
    match value
        .map(str::trim)
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "top" => pb::VerticalAlignment::Top,
        "middle" | "center" => pb::VerticalAlignment::Middle,
        "bottom" => pb::VerticalAlignment::Bottom,
        _ => pb::VerticalAlignment::Unspecified,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attrs(pairs: &[(&str, &str)]) -> Attrs {
        Attrs(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        )
    }

    #[test]
    fn a_cals_colspec_and_an_xhtml_col_decode_to_the_same_geometry() {
        let cals = column_spec(&attrs(&[
            ("colname", "c1"),
            ("colwidth", "2*"),
            ("align", "right"),
            ("valign", "bottom"),
        ]));
        assert_eq!(cals.name, "c1");
        assert_eq!(cals.width, "2*", "a proportional width is not a length");
        assert_eq!(cals.align, pb::Alignment::Right as i32);
        assert_eq!(cals.valign, pb::VerticalAlignment::Bottom as i32);

        let xhtml = column_spec(&attrs(&[("width", "30%"), ("align", "CENTER")]));
        assert_eq!(xhtml.name, "", "an unnamed column declares no name");
        assert_eq!(xhtml.width, "30%");
        assert_eq!(xhtml.align, pb::Alignment::Center as i32);
        assert_eq!(xhtml.valign, pb::VerticalAlignment::Unspecified as i32);
    }

    #[test]
    fn an_alignment_the_source_did_not_state_is_unspecified_not_a_guess() {
        assert_eq!(horizontal(None), pb::Alignment::Unspecified);
        assert_eq!(horizontal(Some("")), pb::Alignment::Unspecified);
        assert_eq!(horizontal(Some("sideways")), pb::Alignment::Unspecified);
        // CALS really does declare it, so it is read rather than flattened.
        assert_eq!(horizontal(Some("char")), pb::Alignment::Char);
        assert_eq!(vertical(Some("top")), pb::VerticalAlignment::Top);
        assert_eq!(
            vertical(Some("baseline")),
            pb::VerticalAlignment::Unspecified
        );
    }
}
