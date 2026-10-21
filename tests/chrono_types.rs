//! chrono date/time support, stored as TEXT in the same formats Diesel's SQLite backend uses so
//! that databases are interchangeable between `diesel::sqlite` and `diesel-libsql`.
#![cfg(feature = "chrono")]

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use diesel::prelude::*;
use diesel_libsql::LibSqlConnection;

diesel::table! {
    events (id) {
        id -> Integer,
        day -> Date,
        at -> Timestamp,
        clock -> Time,
        moment -> TimestamptzSqlite,
        maybe_day -> Nullable<Date>,
    }
}

#[derive(Debug, PartialEq, Queryable, Selectable, Insertable)]
#[diesel(table_name = events)]
struct Event {
    id: i32,
    day: NaiveDate,
    at: NaiveDateTime,
    clock: NaiveTime,
    moment: DateTime<Utc>,
    maybe_day: Option<NaiveDate>,
}

fn setup() -> LibSqlConnection {
    let mut conn = LibSqlConnection::establish(":memory:").expect("in-memory libSQL database");
    diesel::sql_query(
        "CREATE TABLE events (
            id INTEGER PRIMARY KEY,
            day TEXT NOT NULL,
            at TEXT NOT NULL,
            clock TEXT NOT NULL,
            moment TEXT NOT NULL,
            maybe_day TEXT
        )",
    )
    .execute(&mut conn)
    .expect("create table");
    conn
}

fn sample() -> Event {
    let day = NaiveDate::from_ymd_opt(2026, 10, 21).expect("valid date");
    let clock = NaiveTime::from_hms_micro_opt(13, 45, 7, 123_456).expect("valid time");
    Event {
        id: 1,
        day,
        at: day.and_time(clock),
        clock,
        moment: day.and_time(clock).and_utc(),
        maybe_day: None,
    }
}

#[test]
fn chrono_values_round_trip() {
    let mut conn = setup();
    let event = sample();
    diesel::insert_into(events::table)
        .values(&event)
        .execute(&mut conn)
        .expect("insert chrono values");

    let loaded = events::table
        .select(Event::as_select())
        .first(&mut conn)
        .expect("load chrono values");
    assert_eq!(loaded, event);
}

#[test]
fn chrono_values_use_diesel_sqlite_text_formats() {
    let mut conn = setup();
    diesel::insert_into(events::table)
        .values(&sample())
        .execute(&mut conn)
        .expect("insert chrono values");

    #[allow(clippy::redundant_field_names)] // emitted by the derive expansion
    #[derive(QueryableByName)]
    struct Raw {
        #[diesel(sql_type = diesel::sql_types::Text)]
        day: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        at: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        clock: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        moment: String,
    }
    let raw = diesel::sql_query("SELECT day, at, clock, moment FROM events")
        .get_result::<Raw>(&mut conn)
        .expect("raw text");
    assert_eq!(raw.day, "2026-10-21");
    assert_eq!(raw.at, "2026-10-21 13:45:07.123456");
    assert_eq!(raw.clock, "13:45:07.123456");
    assert_eq!(raw.moment, "2026-10-21 13:45:07.123456+00:00");
}

#[test]
fn chrono_values_written_by_sqlite_functions_are_readable() {
    let mut conn = setup();
    diesel::sql_query(
        "INSERT INTO events (id, day, at, clock, moment) VALUES
            (1, date('2026-10-21'), '2026-10-21T13:45:07', '13:45', '2026-10-21T13:45:07Z')",
    )
    .execute(&mut conn)
    .expect("insert SQLite-formatted text");

    let loaded = events::table
        .select(Event::as_select())
        .first(&mut conn)
        .expect("parse SQLite-formatted text");
    let day = NaiveDate::from_ymd_opt(2026, 10, 21).expect("valid date");
    assert_eq!(loaded.day, day);
    assert_eq!(loaded.at, day.and_hms_opt(13, 45, 7).expect("valid time"));
    assert_eq!(
        loaded.clock,
        NaiveTime::from_hms_opt(13, 45, 0).expect("valid time")
    );
    assert_eq!(
        loaded.moment,
        day.and_hms_opt(13, 45, 7).expect("valid time").and_utc()
    );
}
