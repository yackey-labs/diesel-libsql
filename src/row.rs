//! Row types for the LibSql backend.

use std::sync::Arc;

use diesel::backend::Backend;
use diesel::row::{Field, IntoOwnedRow, PartialRow, Row, RowIndex, RowSealed};

use crate::backend::LibSql;
use crate::value::LibSqlValue;

/// An owned row of values from a libsql query result.
#[derive(Debug)]
pub struct LibSqlRow {
    pub(crate) values: Vec<Option<LibSqlValue>>,
    pub(crate) column_names: Arc<[Option<String>]>,
}

impl RowSealed for LibSqlRow {}

impl<'a> Row<'a, LibSql> for LibSqlRow {
    type Field<'field>
        = LibSqlField<'field>
    where
        'a: 'field,
        Self: 'field;
    type InnerPartialRow = Self;

    fn field_count(&self) -> usize {
        self.values.len()
    }

    fn get<'field, I>(&'field self, idx: I) -> Option<Self::Field<'field>>
    where
        'a: 'field,
        Self: RowIndex<I>,
    {
        let idx = self.idx(idx)?;
        Some(LibSqlField {
            row: self,
            col_idx: idx,
        })
    }

    fn partial_row(&self, range: std::ops::Range<usize>) -> PartialRow<'_, Self::InnerPartialRow> {
        PartialRow::new(self, range)
    }
}

impl RowIndex<usize> for LibSqlRow {
    fn idx(&self, idx: usize) -> Option<usize> {
        if idx < self.values.len() {
            Some(idx)
        } else {
            None
        }
    }
}

impl<'idx> RowIndex<&'idx str> for LibSqlRow {
    fn idx(&self, field_name: &'idx str) -> Option<usize> {
        self.column_names
            .iter()
            .position(|n| n.as_ref().map(|s| s as &str) == Some(field_name))
    }
}

impl<'a> IntoOwnedRow<'a, LibSql> for LibSqlRow {
    type OwnedRow = LibSqlRow;
    type Cache = ();

    fn into_owned(self, _cache: &mut Self::Cache) -> Self::OwnedRow {
        // LibSqlRow is already fully owned — no conversion needed.
        self
    }
}

/// A field within a LibSql row.
#[allow(missing_debug_implementations)]
pub struct LibSqlField<'row> {
    row: &'row LibSqlRow,
    col_idx: usize,
}

impl<'row> Field<'row, LibSql> for LibSqlField<'row> {
    fn field_name(&self) -> Option<&str> {
        self.row
            .column_names
            .get(self.col_idx)
            .and_then(|o| o.as_ref().map(|s| s.as_ref()))
    }

    fn is_null(&self) -> bool {
        match self.row.values.get(self.col_idx) {
            Some(Some(v)) => v.is_null(),
            _ => true,
        }
    }

    fn value(&self) -> Option<<LibSql as Backend>::RawValue<'row>> {
        self.row.values.get(self.col_idx).and_then(|v| {
            v.as_ref().and_then(|val| {
                if val.is_null() {
                    None
                } else {
                    Some(val.clone())
                }
            })
        })
    }
}
