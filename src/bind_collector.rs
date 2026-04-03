//! Bind collector for the LibSql backend.

use diesel::query_builder::{BindCollector, MoveableBindCollector};
use diesel::serialize::{IsNull, Output};
use diesel::sql_types::HasSqlType;
use diesel::sqlite::SqliteType;
use diesel::QueryResult;

use crate::backend::LibSql;

/// A bind value for LibSql statements.
///
/// This type wraps values that can be converted to `libsql::Value`.
#[derive(Debug)]
pub struct LibSqlBindValue<'a> {
    pub(crate) inner: InternalBindValue<'a>,
}

#[derive(Debug)]
pub(crate) enum InternalBindValue<'a> {
    Null,
    I32(i32),
    I64(i64),
    F64(f64),
    BorrowedString(&'a str),
    String(Box<str>),
    BorrowedBinary(&'a [u8]),
    Binary(Box<[u8]>),
}

impl std::fmt::Display for InternalBindValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            InternalBindValue::Null => "Null",
            InternalBindValue::I32(_) | InternalBindValue::I64(_) => "Integer",
            InternalBindValue::F64(_) => "Float",
            InternalBindValue::BorrowedString(_) | InternalBindValue::String(_) => "Text",
            InternalBindValue::BorrowedBinary(_) | InternalBindValue::Binary(_) => "Binary",
        };
        f.write_str(name)
    }
}

impl InternalBindValue<'_> {
    /// Convert to an owned `libsql::Value`.
    pub(crate) fn to_libsql_value(&self) -> libsql::Value {
        match self {
            InternalBindValue::Null => libsql::Value::Null,
            InternalBindValue::I32(i) => libsql::Value::Integer(*i as i64),
            InternalBindValue::I64(i) => libsql::Value::Integer(*i),
            InternalBindValue::F64(f) => libsql::Value::Real(*f),
            InternalBindValue::BorrowedString(s) => libsql::Value::Text((*s).to_string()),
            InternalBindValue::String(s) => libsql::Value::Text(s.to_string()),
            InternalBindValue::BorrowedBinary(b) => libsql::Value::Blob(b.to_vec()),
            InternalBindValue::Binary(b) => libsql::Value::Blob(b.to_vec()),
        }
    }
}

// From impls for LibSqlBindValue (mirrors SqliteBindValue)
impl From<i32> for LibSqlBindValue<'_> {
    fn from(i: i32) -> Self {
        Self {
            inner: InternalBindValue::I32(i),
        }
    }
}

impl From<i64> for LibSqlBindValue<'_> {
    fn from(i: i64) -> Self {
        Self {
            inner: InternalBindValue::I64(i),
        }
    }
}

impl From<f64> for LibSqlBindValue<'_> {
    fn from(f: f64) -> Self {
        Self {
            inner: InternalBindValue::F64(f),
        }
    }
}

impl<'a> From<&'a str> for LibSqlBindValue<'a> {
    fn from(s: &'a str) -> Self {
        Self {
            inner: InternalBindValue::BorrowedString(s),
        }
    }
}

impl From<String> for LibSqlBindValue<'_> {
    fn from(s: String) -> Self {
        Self {
            inner: InternalBindValue::String(s.into_boxed_str()),
        }
    }
}

impl<'a> From<&'a [u8]> for LibSqlBindValue<'a> {
    fn from(b: &'a [u8]) -> Self {
        Self {
            inner: InternalBindValue::BorrowedBinary(b),
        }
    }
}

impl From<Vec<u8>> for LibSqlBindValue<'_> {
    fn from(b: Vec<u8>) -> Self {
        Self {
            inner: InternalBindValue::Binary(b.into_boxed_slice()),
        }
    }
}

impl<'a, T> From<Option<T>> for LibSqlBindValue<'a>
where
    T: Into<LibSqlBindValue<'a>>,
{
    fn from(o: Option<T>) -> Self {
        match o {
            Some(v) => v.into(),
            None => Self {
                inner: InternalBindValue::Null,
            },
        }
    }
}

/// Bind parameter collector for LibSql queries.
#[derive(Debug, Default)]
pub struct LibSqlBindCollector<'a> {
    pub(crate) binds: Vec<(InternalBindValue<'a>, SqliteType)>,
}

impl<'a> BindCollector<'a, LibSql> for LibSqlBindCollector<'a> {
    type Buffer = LibSqlBindValue<'a>;

    fn push_bound_value<T, U>(&mut self, bind: &'a U, metadata_lookup: &mut ()) -> QueryResult<()>
    where
        LibSql: HasSqlType<T>,
        U: diesel::serialize::ToSql<T, LibSql> + ?Sized,
    {
        let value = LibSqlBindValue {
            inner: InternalBindValue::Null,
        };
        let mut to_sql_output = Output::new(value, metadata_lookup);
        let is_null = bind
            .to_sql(&mut to_sql_output)
            .map_err(diesel::result::Error::SerializationError)?;
        let bind = to_sql_output.into_inner();
        let metadata = <LibSql as HasSqlType<T>>::metadata(metadata_lookup);
        self.binds.push((
            match is_null {
                IsNull::No => bind.inner,
                IsNull::Yes => InternalBindValue::Null,
            },
            metadata,
        ));
        Ok(())
    }

    fn push_null_value(&mut self, metadata: SqliteType) -> QueryResult<()> {
        self.binds.push((InternalBindValue::Null, metadata));
        Ok(())
    }
}

// Owned version for MoveableBindCollector
#[derive(Debug, Clone)]
enum OwnedBindValue {
    Null,
    I32(i32),
    I64(i64),
    F64(f64),
    String(Box<str>),
    Binary(Box<[u8]>),
}

impl From<&InternalBindValue<'_>> for OwnedBindValue {
    fn from(value: &InternalBindValue<'_>) -> Self {
        match value {
            InternalBindValue::Null => OwnedBindValue::Null,
            InternalBindValue::I32(v) => OwnedBindValue::I32(*v),
            InternalBindValue::I64(v) => OwnedBindValue::I64(*v),
            InternalBindValue::F64(v) => OwnedBindValue::F64(*v),
            InternalBindValue::BorrowedString(s) => {
                OwnedBindValue::String(String::from(*s).into_boxed_str())
            }
            InternalBindValue::String(s) => OwnedBindValue::String(s.clone()),
            InternalBindValue::BorrowedBinary(b) => {
                OwnedBindValue::Binary(Vec::from(*b).into_boxed_slice())
            }
            InternalBindValue::Binary(b) => OwnedBindValue::Binary(b.clone()),
        }
    }
}

impl From<&OwnedBindValue> for InternalBindValue<'_> {
    fn from(value: &OwnedBindValue) -> Self {
        match value {
            OwnedBindValue::Null => InternalBindValue::Null,
            OwnedBindValue::I32(v) => InternalBindValue::I32(*v),
            OwnedBindValue::I64(v) => InternalBindValue::I64(*v),
            OwnedBindValue::F64(v) => InternalBindValue::F64(*v),
            OwnedBindValue::String(s) => InternalBindValue::String(s.clone()),
            OwnedBindValue::Binary(b) => InternalBindValue::Binary(b.clone()),
        }
    }
}

/// Owned bind data for [`MoveableBindCollector`] support.
///
/// Stores a snapshot of bind parameters that can be sent across threads.
#[derive(Debug)]
pub struct LibSqlBindCollectorData {
    binds: Vec<(OwnedBindValue, SqliteType)>,
}

impl MoveableBindCollector<LibSql> for LibSqlBindCollector<'_> {
    type BindData = LibSqlBindCollectorData;

    fn moveable(&self) -> Self::BindData {
        LibSqlBindCollectorData {
            binds: self
                .binds
                .iter()
                .map(|(bind, tpe)| (OwnedBindValue::from(bind), *tpe))
                .collect(),
        }
    }

    fn append_bind_data(&mut self, from: &Self::BindData) {
        self.binds.reserve_exact(from.binds.len());
        self.binds.extend(
            from.binds
                .iter()
                .map(|(bind, tpe)| (InternalBindValue::from(bind), *tpe)),
        );
    }

    fn push_debug_binds<'a, 'b>(
        bind_data: &Self::BindData,
        f: &'a mut Vec<Box<dyn std::fmt::Debug + 'b>>,
    ) {
        f.extend(
            bind_data
                .binds
                .iter()
                .map(|(b, _)| Box::new(b.clone()) as Box<dyn std::fmt::Debug>),
        );
    }
}
