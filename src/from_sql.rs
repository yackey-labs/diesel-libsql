//! FromSql implementations for the LibSql backend.

use diesel::deserialize::{self, FromSql};
use diesel::sql_types;

use crate::backend::LibSql;
use crate::value::LibSqlValue;

impl FromSql<sql_types::SmallInt, LibSql> for i16 {
    fn from_sql(value: LibSqlValue) -> deserialize::Result<Self> {
        Ok(value.read_integer() as i16)
    }
}

impl FromSql<sql_types::Integer, LibSql> for i32 {
    fn from_sql(value: LibSqlValue) -> deserialize::Result<Self> {
        Ok(value.read_integer())
    }
}

impl FromSql<sql_types::BigInt, LibSql> for i64 {
    fn from_sql(value: LibSqlValue) -> deserialize::Result<Self> {
        Ok(value.read_long())
    }
}

impl FromSql<sql_types::Float, LibSql> for f32 {
    fn from_sql(value: LibSqlValue) -> deserialize::Result<Self> {
        Ok(value.read_double() as f32)
    }
}

impl FromSql<sql_types::Double, LibSql> for f64 {
    fn from_sql(value: LibSqlValue) -> deserialize::Result<Self> {
        Ok(value.read_double())
    }
}

impl FromSql<sql_types::Bool, LibSql> for bool {
    fn from_sql(value: LibSqlValue) -> deserialize::Result<Self> {
        Ok(value.read_integer() != 0)
    }
}

impl FromSql<sql_types::Text, LibSql> for String {
    fn from_sql(value: LibSqlValue) -> deserialize::Result<Self> {
        Ok(value.read_text().to_string())
    }
}

impl FromSql<sql_types::Binary, LibSql> for Vec<u8> {
    fn from_sql(value: LibSqlValue) -> deserialize::Result<Self> {
        Ok(value.read_blob().to_vec())
    }
}
