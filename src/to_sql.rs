//! ToSql implementations for the LibSql backend.

use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types;

use crate::backend::LibSql;

impl ToSql<sql_types::Bool, LibSql> for bool {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, LibSql>) -> serialize::Result {
        let int_value = if *self { &1i32 } else { &0i32 };
        <i32 as ToSql<sql_types::Integer, LibSql>>::to_sql(int_value, out)
    }
}

impl ToSql<sql_types::SmallInt, LibSql> for i16 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, LibSql>) -> serialize::Result {
        out.set_value(*self as i32);
        Ok(IsNull::No)
    }
}

impl ToSql<sql_types::Integer, LibSql> for i32 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, LibSql>) -> serialize::Result {
        out.set_value(*self);
        Ok(IsNull::No)
    }
}

impl ToSql<sql_types::BigInt, LibSql> for i64 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, LibSql>) -> serialize::Result {
        out.set_value(*self);
        Ok(IsNull::No)
    }
}

impl ToSql<sql_types::Float, LibSql> for f32 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, LibSql>) -> serialize::Result {
        out.set_value(*self as f64);
        Ok(IsNull::No)
    }
}

impl ToSql<sql_types::Double, LibSql> for f64 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, LibSql>) -> serialize::Result {
        out.set_value(*self);
        Ok(IsNull::No)
    }
}

impl ToSql<sql_types::Text, LibSql> for str {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, LibSql>) -> serialize::Result {
        out.set_value(self);
        Ok(IsNull::No)
    }
}

impl ToSql<sql_types::Binary, LibSql> for [u8] {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, LibSql>) -> serialize::Result {
        out.set_value(self);
        Ok(IsNull::No)
    }
}
