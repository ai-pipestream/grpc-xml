// SPDX-License-Identifier: Apache-2.0

//! Table streaming: `TableStart` when the table element opens, one
//! `TableRow` per closed row, `TableEnd` with the counts when it closes.

use std::io::BufRead;

use super::{Driver, ParseError, collapse};
use crate::dialect::{
    Attrs, CELL_ELEMENTS, HEADER_CELL_ELEMENTS, HEADER_SECTION_ELEMENTS, ROW_ELEMENTS,
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
    pub(super) column_index: u32,
    pub(super) column_span: u32,
    pub(super) row_span: u32,
    pub(super) is_header: bool,
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
        });
        self.counts.tables += 1;
        self.send(pb::parse_xml_response::Event::TableStart(start))
    }

    pub(super) fn table_start(&mut self, local: &str, qname: &str, attrs: &Attrs) {
        let Some(table) = self.table.as_mut() else {
            return;
        };
        if table.cell.is_some() {
            // Markup inside a cell is flattened into the cell's text.
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
                column_index,
                column_span,
                row_span,
                is_header,
            });
        }
        self.push_frame(local, qname);
    }

    pub(super) fn table_end(&mut self) -> Result<(), ParseError> {
        let depth = self.stack.len();
        let Some(table) = self.table.as_mut() else {
            return Ok(());
        };
        if let Some(cell) = table.cell.as_ref()
            && depth == cell.depth
        {
            let cell = table.cell.take().expect("checked just above");
            if let Some(row) = table.row.as_mut() {
                row.next_column = cell.column_index + cell.column_span;
                row.cells.push(pb::TableCell {
                    column_index: cell.column_index,
                    text: collapse(&cell.text),
                    column_span: cell.column_span,
                    row_span: cell.row_span,
                    is_header: cell.is_header,
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
            };
            self.table = None;
            return self.send(pb::parse_xml_response::Event::TableEnd(end));
        }
        Ok(())
    }
}
