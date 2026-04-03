//! Owned value type extracted from libsql rows.

/// An eagerly-extracted, owned value from a libsql row.
///
/// Unlike `diesel::sqlite::SqliteValue` which wraps C pointers,
/// this type owns all its data.
#[derive(Debug, Clone)]
pub enum LibSqlValue {
    /// SQL NULL
    Null,
    /// SQL INTEGER (i64)
    Integer(i64),
    /// SQL REAL (f64)
    Real(f64),
    /// SQL TEXT (owned String)
    Text(String),
    /// SQL BLOB (owned bytes)
    Blob(Vec<u8>),
}

impl LibSqlValue {
    /// Read as text. Panics if not Text.
    pub fn read_text(&self) -> &str {
        match self {
            LibSqlValue::Text(s) => s.as_str(),
            _ => panic!("Expected Text value, got {:?}", self),
        }
    }

    /// Read as i32 (from integer, truncating).
    pub fn read_integer(&self) -> i32 {
        match self {
            LibSqlValue::Integer(i) => *i as i32,
            _ => panic!("Expected Integer value, got {:?}", self),
        }
    }

    /// Read as i64.
    pub fn read_long(&self) -> i64 {
        match self {
            LibSqlValue::Integer(i) => *i,
            _ => panic!("Expected Integer value, got {:?}", self),
        }
    }

    /// Read as f64.
    pub fn read_double(&self) -> f64 {
        match self {
            LibSqlValue::Real(f) => *f,
            LibSqlValue::Integer(i) => *i as f64,
            _ => panic!("Expected Real value, got {:?}", self),
        }
    }

    /// Read as blob.
    pub fn read_blob(&self) -> &[u8] {
        match self {
            LibSqlValue::Blob(b) => b.as_slice(),
            _ => panic!("Expected Blob value, got {:?}", self),
        }
    }

    /// Returns true if this value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, LibSqlValue::Null)
    }
}
