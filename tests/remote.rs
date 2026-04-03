//! Integration tests for diesel-libsql remote (Turso) connections.
//!
//! These tests are `#[ignore]` by default since they require Turso credentials.
//! Run them with:
//!
//! ```sh
//! LIBSQL_URL=libsql://your-db.turso.io LIBSQL_AUTH_TOKEN=your-token cargo test -- --ignored
//! ```

use diesel::prelude::*;
use diesel_libsql::LibSqlConnection;

diesel::table! {
    users (id) {
        id -> Integer,
        name -> Text,
    }
}

fn remote_url() -> String {
    std::env::var("LIBSQL_URL").expect("LIBSQL_URL must be set for remote tests")
}

fn auth_token() -> String {
    std::env::var("LIBSQL_AUTH_TOKEN").expect("LIBSQL_AUTH_TOKEN must be set for remote tests")
}

/// Remote CRUD test (same as M1 local CRUD but against a Turso database).
#[test]
#[ignore]
fn test_remote_crud() {
    let url = format!("{}?authToken={}", remote_url(), auth_token());
    let mut conn = LibSqlConnection::establish(&url).expect("Failed to connect to remote");

    // Create table (idempotent)
    diesel::sql_query(
        "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    // Clean up from previous runs
    diesel::sql_query("DELETE FROM users")
        .execute(&mut conn)
        .expect("Failed to clean up");

    // Insert
    diesel::sql_query("INSERT INTO users (name) VALUES ('remote_alice')")
        .execute(&mut conn)
        .expect("Failed to insert");

    // Select back
    let results: Vec<(i32, String)> = users::table
        .select((users::id, users::name))
        .filter(users::name.eq("remote_alice"))
        .load(&mut conn)
        .expect("Failed to select");

    assert!(!results.is_empty());
    assert_eq!(results[0].1, "remote_alice");

    // Update
    diesel::update(users::table.filter(users::name.eq("remote_alice")))
        .set(users::name.eq("remote_bob"))
        .execute(&mut conn)
        .expect("Failed to update");

    let results: Vec<(i32, String)> = users::table
        .select((users::id, users::name))
        .filter(users::name.eq("remote_bob"))
        .load(&mut conn)
        .expect("Failed to select after update");

    assert!(!results.is_empty());
    assert_eq!(results[0].1, "remote_bob");

    // Delete
    diesel::delete(users::table.filter(users::name.eq("remote_bob")))
        .execute(&mut conn)
        .expect("Failed to delete");

    let results: Vec<(i32, String)> = users::table
        .select((users::id, users::name))
        .filter(users::name.eq("remote_bob"))
        .load(&mut conn)
        .expect("Failed to select after delete");

    assert!(results.is_empty());
}

/// Test establishing a remote connection with auth token from env var.
#[test]
#[ignore]
fn test_remote_auth_from_env() {
    // Set the env var and connect with URL only (no ?authToken=)
    std::env::set_var("LIBSQL_AUTH_TOKEN", auth_token());

    let url = remote_url();
    let mut conn = LibSqlConnection::establish(&url).expect("Failed to connect with env token");

    // Simple smoke test: execute a SELECT 1
    diesel::sql_query("SELECT 1")
        .execute(&mut conn)
        .expect("Failed to execute query");
}
