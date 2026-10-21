//! chrono support (feature `chrono`).
//!
//! Values are stored as TEXT in exactly the formats Diesel's SQLite backend writes, and every
//! format Diesel's SQLite backend accepts is accepted when reading, including SQLite julian-day
//! numbers, so a database is interchangeable between `diesel::sqlite` and `diesel-libsql`.

use chrono::{DateTime, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use diesel::deserialize::{self, FromSql};
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types::{Date, Time, Timestamp, TimestamptzSqlite};
use diesel::sqlite::SqliteType;

use crate::backend::LibSql;
use crate::value::LibSqlValue;

const DATE_FORMAT: &str = "%F";

const ENCODE_TIME_FORMAT: &str = "%T%.f";

const TIME_FORMATS: [&str; 9] = [
    "%T%.f", "%T", "%R", "%RZ", "%R%:z", "%TZ", "%T%:z", "%T%.fZ", "%T%.f%:z",
];

const ENCODE_NAIVE_DATETIME_FORMAT: &str = "%F %T%.f";

const ENCODE_DATETIME_FORMAT: &str = "%F %T%.f%:z";

const NAIVE_DATETIME_FORMATS: [&str; 18] = [
    "%F %T%.f",
    "%F %T%.f%:z",
    "%F %T",
    "%F %T%:z",
    "%F %R",
    "%F %RZ",
    "%F %R%:z",
    "%F %TZ",
    "%F %T%.fZ",
    "%FT%R",
    "%FT%RZ",
    "%FT%R%:z",
    "%FT%T",
    "%FT%TZ",
    "%FT%T%:z",
    "%FT%T%.f",
    "%FT%T%.fZ",
    "%FT%T%.f%:z",
];

const DATETIME_FORMATS: [&str; 12] = [
    "%F %T%.f%:z",
    "%F %T%:z",
    "%F %RZ",
    "%F %R%:z",
    "%F %TZ",
    "%F %T%.fZ",
    "%FT%RZ",
    "%FT%R%:z",
    "%FT%TZ",
    "%FT%T%:z",
    "%FT%T%.fZ",
    "%FT%T%.f%:z",
];

impl diesel::sql_types::HasSqlType<TimestamptzSqlite> for LibSql {
    fn metadata(_: &mut ()) -> SqliteType {
        SqliteType::Text
    }
}

fn text(value: &LibSqlValue) -> deserialize::Result<&str> {
    match value {
        LibSqlValue::Text(text) => Ok(text),
        other => Err(format!("expected a TEXT date/time value, got {other:?}").into()),
    }
}

/// SQLite julian-day numbers (e.g. from `julianday()`), stored as REAL or INTEGER.
fn julian(value: &LibSqlValue) -> Option<NaiveDateTime> {
    let days = match value {
        LibSqlValue::Real(days) => *days,
        LibSqlValue::Integer(days) => *days as f64,
        LibSqlValue::Text(text) => text.parse::<f64>().ok()?,
        _ => return None,
    };
    const EPOCH_IN_JULIAN_DAYS: f64 = 2_440_587.5;
    const SECONDS_IN_DAY: f64 = 86400.0;
    let timestamp = (days - EPOCH_IN_JULIAN_DAYS) * SECONDS_IN_DAY;
    #[allow(clippy::cast_possible_truncation)]
    let seconds = timestamp.trunc() as i64;
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let nanos = (timestamp.fract() * 1E9) as u32;
    DateTime::from_timestamp(seconds, nanos).map(|dt| dt.naive_utc())
}

fn naive_date_time(value: &LibSqlValue) -> deserialize::Result<NaiveDateTime> {
    if let LibSqlValue::Text(text) = value {
        for format in NAIVE_DATETIME_FORMATS {
            if let Ok(dt) = NaiveDateTime::parse_from_str(text, format) {
                return Ok(dt);
            }
        }
    }
    julian(value).ok_or_else(|| format!("invalid datetime {value:?}").into())
}

impl FromSql<Date, LibSql> for NaiveDate {
    fn from_sql(value: LibSqlValue) -> deserialize::Result<Self> {
        Ok(NaiveDate::parse_from_str(text(&value)?, DATE_FORMAT)?)
    }
}

impl ToSql<Date, LibSql> for NaiveDate {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, LibSql>) -> serialize::Result {
        out.set_value(self.format(DATE_FORMAT).to_string());
        Ok(IsNull::No)
    }
}

impl FromSql<Time, LibSql> for NaiveTime {
    fn from_sql(value: LibSqlValue) -> deserialize::Result<Self> {
        let text = text(&value)?;
        for format in TIME_FORMATS {
            if let Ok(time) = NaiveTime::parse_from_str(text, format) {
                return Ok(time);
            }
        }
        Err(format!("invalid time {text}").into())
    }
}

impl ToSql<Time, LibSql> for NaiveTime {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, LibSql>) -> serialize::Result {
        out.set_value(self.format(ENCODE_TIME_FORMAT).to_string());
        Ok(IsNull::No)
    }
}

impl FromSql<Timestamp, LibSql> for NaiveDateTime {
    fn from_sql(value: LibSqlValue) -> deserialize::Result<Self> {
        naive_date_time(&value)
    }
}

impl ToSql<Timestamp, LibSql> for NaiveDateTime {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, LibSql>) -> serialize::Result {
        out.set_value(self.format(ENCODE_NAIVE_DATETIME_FORMAT).to_string());
        Ok(IsNull::No)
    }
}

impl FromSql<TimestamptzSqlite, LibSql> for NaiveDateTime {
    fn from_sql(value: LibSqlValue) -> deserialize::Result<Self> {
        naive_date_time(&value)
    }
}

impl ToSql<TimestamptzSqlite, LibSql> for NaiveDateTime {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, LibSql>) -> serialize::Result {
        out.set_value(self.format(ENCODE_NAIVE_DATETIME_FORMAT).to_string());
        Ok(IsNull::No)
    }
}

fn date_time_with_offset(value: &LibSqlValue) -> Option<DateTime<chrono::FixedOffset>> {
    let LibSqlValue::Text(text) = value else {
        return None;
    };
    DATETIME_FORMATS
        .iter()
        .find_map(|format| DateTime::parse_from_str(text, format).ok())
}

impl FromSql<TimestamptzSqlite, LibSql> for DateTime<Utc> {
    fn from_sql(value: LibSqlValue) -> deserialize::Result<Self> {
        if let Some(dt) = date_time_with_offset(&value) {
            return Ok(dt.with_timezone(&Utc));
        }
        // No offset in the text: assume UTC, as Diesel's SQLite backend does.
        Ok(Utc.from_utc_datetime(&naive_date_time(&value)?))
    }
}

impl FromSql<TimestamptzSqlite, LibSql> for DateTime<Local> {
    fn from_sql(value: LibSqlValue) -> deserialize::Result<Self> {
        if let Some(dt) = date_time_with_offset(&value) {
            return Ok(dt.with_timezone(&Local));
        }
        Ok(Local.from_utc_datetime(&naive_date_time(&value)?))
    }
}

impl<TZ: TimeZone> ToSql<TimestamptzSqlite, LibSql> for DateTime<TZ> {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, LibSql>) -> serialize::Result {
        // Converting to UTC keeps stored values comparable as text.
        let utc = self.with_timezone(&Utc);
        out.set_value(utc.format(ENCODE_DATETIME_FORMAT).to_string());
        Ok(IsNull::No)
    }
}
