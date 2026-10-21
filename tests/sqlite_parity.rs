//! Insert semantics that must match Diesel's own SQLite backend, since libSQL shares SQLite's SQL
//! dialect: defaultable (`Option`) columns and multi-row (batch) inserts.

use diesel::prelude::*;
use diesel_libsql::LibSqlConnection;

diesel::table! {
    notes (id) {
        id -> Integer,
        title -> Text,
        label -> Text,
        priority -> Integer,
    }
}

#[derive(Insertable)]
#[diesel(table_name = notes)]
struct NewNote<'a> {
    title: &'a str,
    label: Option<&'a str>,
    priority: Option<i32>,
}

#[derive(Insertable)]
#[diesel(table_name = notes)]
struct PlainNote<'a> {
    title: &'a str,
    label: &'a str,
    priority: i32,
}

fn setup() -> LibSqlConnection {
    let mut conn = LibSqlConnection::establish(":memory:").expect("in-memory libSQL database");
    diesel::sql_query(
        "CREATE TABLE notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            label TEXT NOT NULL DEFAULT 'inbox',
            priority INTEGER NOT NULL DEFAULT 3
        )",
    )
    .execute(&mut conn)
    .expect("create table");
    conn
}

fn all(conn: &mut LibSqlConnection) -> Vec<(i32, String, String, i32)> {
    notes::table
        .order(notes::id)
        .load::<(i32, String, String, i32)>(conn)
        .expect("load notes")
}

#[test]
fn single_insert_omits_none_columns_so_sql_defaults_apply() {
    let mut conn = setup();
    diesel::insert_into(notes::table)
        .values(&NewNote {
            title: "a",
            label: None,
            priority: Some(1),
        })
        .execute(&mut conn)
        .expect("SQLite has no DEFAULT keyword in VALUES; None columns must be omitted");

    assert_eq!(all(&mut conn), vec![(1, "a".into(), "inbox".into(), 1)]);
}

#[test]
fn batch_insert_without_defaultable_values() {
    let mut conn = setup();
    let rows = vec![
        PlainNote {
            title: "a",
            label: "x",
            priority: 1,
        },
        PlainNote {
            title: "b",
            label: "y",
            priority: 2,
        },
    ];
    let inserted = diesel::insert_into(notes::table)
        .values(&rows)
        .execute(&mut conn)
        .expect("multi-row insert");

    assert_eq!(inserted, 2);
    assert_eq!(
        all(&mut conn),
        vec![
            (1, "a".into(), "x".into(), 1),
            (2, "b".into(), "y".into(), 2)
        ]
    );
}

#[test]
fn batch_insert_with_mixed_defaultable_values() {
    let mut conn = setup();
    let rows = vec![
        NewNote {
            title: "a",
            label: None,
            priority: Some(5),
        },
        NewNote {
            title: "b",
            label: Some("work"),
            priority: None,
        },
    ];
    let inserted = diesel::insert_into(notes::table)
        .values(&rows)
        .execute(&mut conn)
        .expect("multi-row insert where rows omit different columns");

    assert_eq!(inserted, 2);
    assert_eq!(
        all(&mut conn),
        vec![
            (1, "a".into(), "inbox".into(), 5),
            (2, "b".into(), "work".into(), 3)
        ]
    );
}

#[test]
fn batch_insert_is_atomic() {
    let mut conn = setup();
    diesel::sql_query("CREATE UNIQUE INDEX notes_title ON notes(title)")
        .execute(&mut conn)
        .expect("unique index");
    let rows = vec![
        NewNote {
            title: "dup",
            label: None,
            priority: None,
        },
        NewNote {
            title: "dup",
            label: None,
            priority: None,
        },
    ];
    let result = diesel::insert_into(notes::table)
        .values(&rows)
        .execute(&mut conn);

    assert!(result.is_err(), "second row violates the unique index");
    assert!(
        all(&mut conn).is_empty(),
        "a failed batch must not leave partial rows"
    );
}
