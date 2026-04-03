//! Integration tests against a local Turso dev server (HTTP mode).
//!
//! Start the server with: `turso dev --port 8081`
//! Then run: `cargo test --test server`
//!
//! These tests verify that diesel-libsql works in "remote" mode
//! against a real sqld server, not just local SQLite files.

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel_libsql::LibSqlConnection;

// Each test uses its own table to avoid parallel test conflicts.

diesel::table! {
    crud_users (id) {
        id -> Integer,
        name -> Text,
    }
}

diesel::table! {
    txn_users (id) {
        id -> Integer,
        name -> Text,
    }
}

diesel::table! {
    srv_typed (id) {
        id -> Integer,
        int_val -> Integer,
        bigint_val -> BigInt,
        real_val -> Double,
        text_val -> Text,
        blob_val -> Binary,
        bool_val -> Bool,
    }
}

diesel::table! {
    async_users (id) {
        id -> Integer,
        name -> Text,
    }
}

const SERVER_URL: &str = "http://127.0.0.1:8081";

fn connect() -> LibSqlConnection {
    std::env::set_var("LIBSQL_AUTH_TOKEN", "dev-token");
    LibSqlConnection::establish(SERVER_URL).expect("Failed to connect to turso dev server")
}

/// CRUD against the turso dev server
#[test]
fn test_server_crud() {
    let mut conn = connect();

    conn.batch_execute(
        "DROP TABLE IF EXISTS crud_users;
         CREATE TABLE crud_users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL);",
    )
    .expect("Failed to setup table");

    // Insert
    diesel::sql_query("INSERT INTO crud_users (name) VALUES ('alice')")
        .execute(&mut conn)
        .expect("Failed to insert");

    // Select
    let results: Vec<(i32, String)> = crud_users::table
        .select((crud_users::id, crud_users::name))
        .load(&mut conn)
        .expect("Failed to select");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, "alice");

    // Update
    diesel::update(crud_users::table.filter(crud_users::name.eq("alice")))
        .set(crud_users::name.eq("bob"))
        .execute(&mut conn)
        .expect("Failed to update");

    let results: Vec<(i32, String)> = crud_users::table
        .select((crud_users::id, crud_users::name))
        .load(&mut conn)
        .expect("Failed to select after update");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, "bob");

    // Delete
    diesel::delete(crud_users::table.filter(crud_users::name.eq("bob")))
        .execute(&mut conn)
        .expect("Failed to delete");

    let results: Vec<(i32, String)> = crud_users::table
        .select((crud_users::id, crud_users::name))
        .load(&mut conn)
        .expect("Failed to select after delete");

    assert!(results.is_empty());
}

/// Transaction support over HTTP
#[test]
fn test_server_transaction() {
    let mut conn = connect();

    conn.batch_execute(
        "DROP TABLE IF EXISTS txn_users;
         CREATE TABLE txn_users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL);",
    )
    .expect("Failed to setup table");

    // Transaction that commits
    conn.transaction::<_, diesel::result::Error, _>(|conn| {
        diesel::sql_query("INSERT INTO txn_users (name) VALUES ('committed')").execute(conn)?;
        Ok(())
    })
    .expect("Transaction failed");

    let results: Vec<(i32, String)> = txn_users::table
        .select((txn_users::id, txn_users::name))
        .load(&mut conn)
        .expect("Failed to select");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, "committed");

    // Transaction that rolls back
    let _ = conn.transaction::<(), diesel::result::Error, _>(|conn| {
        diesel::sql_query("INSERT INTO txn_users (name) VALUES ('rolled_back')").execute(conn)?;
        Err(diesel::result::Error::RollbackTransaction)
    });

    let results: Vec<(i32, String)> = txn_users::table
        .select((txn_users::id, txn_users::name))
        .load(&mut conn)
        .expect("Failed to select after rollback");
    assert_eq!(results.len(), 1); // still just the committed row
}

/// Multiple types round-trip over HTTP
#[test]
fn test_server_types() {
    let mut conn = connect();

    conn.batch_execute(
        "DROP TABLE IF EXISTS srv_typed;
         CREATE TABLE srv_typed (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             int_val INTEGER NOT NULL,
             bigint_val INTEGER NOT NULL,
             real_val REAL NOT NULL,
             text_val TEXT NOT NULL,
             blob_val BLOB NOT NULL,
             bool_val INTEGER NOT NULL
         );",
    )
    .expect("Failed to setup table");

    diesel::sql_query(
        "INSERT INTO srv_typed (int_val, bigint_val, real_val, text_val, blob_val, bool_val)
         VALUES (42, 9999999999, 3.14, 'hello server', X'DEADBEEF', 1)",
    )
    .execute(&mut conn)
    .expect("Failed to insert");

    let results: Vec<(i32, i32, i64, f64, String, Vec<u8>, bool)> = srv_typed::table
        .select((
            srv_typed::id,
            srv_typed::int_val,
            srv_typed::bigint_val,
            srv_typed::real_val,
            srv_typed::text_val,
            srv_typed::blob_val,
            srv_typed::bool_val,
        ))
        .load(&mut conn)
        .expect("Failed to load");

    assert_eq!(results.len(), 1);
    let row = &results[0];
    assert_eq!(row.1, 42);
    assert_eq!(row.2, 9999999999i64);
    assert!((row.3 - 3.14).abs() < 0.001);
    assert_eq!(row.4, "hello server");
    assert_eq!(row.5, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    assert!(row.6);
}

/// Async CRUD against the server
#[cfg(feature = "async")]
mod async_server {
    use super::*;
    use diesel_async::AsyncConnection;
    use diesel_async::RunQueryDsl;
    use diesel_libsql::AsyncLibSqlConnection;

    async fn async_connect() -> AsyncLibSqlConnection {
        std::env::set_var("LIBSQL_AUTH_TOKEN", "dev-token");
        AsyncLibSqlConnection::establish(SERVER_URL)
            .await
            .expect("Failed to async connect to turso dev server")
    }

    #[tokio::test]
    async fn test_async_server_crud() {
        let mut conn = async_connect().await;

        // Split DDL into separate statements — remote HTTP mode may not
        // support multi-statement strings via execute()
        diesel::sql_query("DROP TABLE IF EXISTS async_users")
            .execute(&mut conn)
            .await
            .expect("Failed to drop table");

        diesel::sql_query(
            "CREATE TABLE async_users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
        )
        .execute(&mut conn)
        .await
        .expect("Failed to create table");

        diesel::sql_query("INSERT INTO async_users (name) VALUES ('async_alice')")
            .execute(&mut conn)
            .await
            .expect("Failed to insert");

        let results: Vec<(i32, String)> = async_users::table
            .select((async_users::id, async_users::name))
            .load(&mut conn)
            .await
            .expect("Failed to select");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, "async_alice");

        diesel::sql_query("DROP TABLE async_users")
            .execute(&mut conn)
            .await
            .expect("Failed to cleanup");
    }
}
