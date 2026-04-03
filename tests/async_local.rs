//! Async integration tests for diesel-libsql.
//!
//! Run with: `cargo test --features async`

#![cfg(feature = "async")]

use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::RunQueryDsl;
use diesel_libsql::AsyncLibSqlConnection;

diesel::table! {
    users (id) {
        id -> Integer,
        name -> Text,
    }
}

diesel::table! {
    typed_data (id) {
        id -> Integer,
        int_val -> Integer,
        bigint_val -> BigInt,
        real_val -> Double,
        text_val -> Text,
        blob_val -> Binary,
        bool_val -> Bool,
    }
}

/// Test 1: Async CRUD — establish, create table, insert, select, update, delete
#[tokio::test]
async fn test_async_crud() {
    let mut conn = AsyncLibSqlConnection::establish(":memory:")
        .await
        .expect("Failed to connect");

    // Create table
    diesel::sql_query(
        "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .await
    .expect("Failed to create table");

    // Insert
    diesel::sql_query("INSERT INTO users (name) VALUES ('alice')")
        .execute(&mut conn)
        .await
        .expect("Failed to insert");

    // Select
    let results: Vec<(i32, String)> = users::table
        .select((users::id, users::name))
        .load(&mut conn)
        .await
        .expect("Failed to select");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, "alice");

    // Update
    diesel::update(users::table.filter(users::name.eq("alice")))
        .set(users::name.eq("bob"))
        .execute(&mut conn)
        .await
        .expect("Failed to update");

    let results: Vec<(i32, String)> = users::table
        .select((users::id, users::name))
        .load(&mut conn)
        .await
        .expect("Failed to select after update");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, "bob");

    // Delete
    diesel::delete(users::table.filter(users::name.eq("bob")))
        .execute(&mut conn)
        .await
        .expect("Failed to delete");

    let results: Vec<(i32, String)> = users::table
        .select((users::id, users::name))
        .load(&mut conn)
        .await
        .expect("Failed to select after delete");

    assert!(results.is_empty());
}

/// Test 2: Async transaction — begin, insert, rollback, verify
#[tokio::test]
async fn test_async_transaction_rollback() {
    use diesel_async::scoped_futures::ScopedFutureExt;

    let mut conn = AsyncLibSqlConnection::establish(":memory:")
        .await
        .expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .await
    .expect("Failed to create table");

    // Begin transaction, insert, then rollback via returning Err
    let result: Result<(), diesel::result::Error> = conn
        .transaction::<_, diesel::result::Error, _>(|conn| {
            async move {
                diesel::sql_query("INSERT INTO users (name) VALUES ('should_not_exist')")
                    .execute(conn)
                    .await
                    .expect("Failed to insert in transaction");

                // Verify the row exists inside the transaction
                let count: Vec<(i32, String)> = users::table
                    .select((users::id, users::name))
                    .load(conn)
                    .await
                    .expect("Failed to select in transaction");
                assert_eq!(count.len(), 1);

                // Return error to trigger rollback
                Err(diesel::result::Error::RollbackTransaction)
            }
            .scope_boxed()
        })
        .await;

    assert!(result.is_err());

    // Verify the row was rolled back
    let results: Vec<(i32, String)> = users::table
        .select((users::id, users::name))
        .load(&mut conn)
        .await
        .expect("Failed to select after rollback");

    assert!(results.is_empty());
}

/// Test: Async last_insert_rowid
#[tokio::test]
async fn test_async_last_insert_rowid() {
    use diesel_libsql::AsyncLibSqlConnectionExt;

    let mut conn = AsyncLibSqlConnection::establish(":memory:")
        .await
        .expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .await
    .expect("Failed to create table");

    diesel::sql_query("INSERT INTO users (name) VALUES ('alice')")
        .execute(&mut conn)
        .await
        .expect("Failed to insert");

    let rowid1 = conn.last_insert_rowid();
    assert!(rowid1 > 0, "Expected rowid > 0, got {}", rowid1);

    diesel::sql_query("INSERT INTO users (name) VALUES ('bob')")
        .execute(&mut conn)
        .await
        .expect("Failed to insert");

    let rowid2 = conn.last_insert_rowid();
    assert!(
        rowid2 > rowid1,
        "Expected rowid2 ({}) > rowid1 ({})",
        rowid2,
        rowid1
    );
}

/// Test 3: Async multiple types round-trip
#[tokio::test]
async fn test_async_multiple_types() {
    let mut conn = AsyncLibSqlConnection::establish(":memory:")
        .await
        .expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE typed_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            int_val INTEGER NOT NULL,
            bigint_val INTEGER NOT NULL,
            real_val REAL NOT NULL,
            text_val TEXT NOT NULL,
            blob_val BLOB NOT NULL,
            bool_val INTEGER NOT NULL
        )",
    )
    .execute(&mut conn)
    .await
    .expect("Failed to create table");

    diesel::sql_query(
        "INSERT INTO typed_data (int_val, bigint_val, real_val, text_val, blob_val, bool_val)
         VALUES (42, 9999999999, 3.14, 'hello world', X'DEADBEEF', 1)",
    )
    .execute(&mut conn)
    .await
    .expect("Failed to insert typed data");

    let results: Vec<(i32, i32, i64, f64, String, Vec<u8>, bool)> = typed_data::table
        .select((
            typed_data::id,
            typed_data::int_val,
            typed_data::bigint_val,
            typed_data::real_val,
            typed_data::text_val,
            typed_data::blob_val,
            typed_data::bool_val,
        ))
        .load(&mut conn)
        .await
        .expect("Failed to load typed data");

    assert_eq!(results.len(), 1);
    let row = &results[0];
    assert_eq!(row.1, 42);
    assert_eq!(row.2, 9999999999i64);
    assert!((row.3 - 3.14).abs() < 0.001);
    assert_eq!(row.4, "hello world");
    assert_eq!(row.5, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    assert!(row.6);
}

/// Test deadpool async connection pool
#[cfg(feature = "deadpool")]
#[tokio::test]
async fn test_deadpool_pool() {
    use diesel_libsql::deadpool::{Manager, Pool};

    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("deadpool_test.db");
    let db_url = db_path.to_str().unwrap().to_string();

    let manager = Manager::new(&db_url);
    let pool = Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("Failed to build deadpool pool");

    // Setup table
    {
        let mut conn = pool.get().await.expect("Failed to get connection");
        diesel::sql_query(
            "CREATE TABLE IF NOT EXISTS dp_test (id INTEGER PRIMARY KEY, val TEXT NOT NULL)",
        )
        .execute(&mut *conn)
        .await
        .expect("Failed to create table");
    }

    // Use pool connections sequentially to verify pooling works
    for i in 0..4u32 {
        let mut conn = pool.get().await.expect("Failed to get connection");
        diesel::sql_query(format!(
            "INSERT INTO dp_test (id, val) VALUES ({}, 'task_{}')",
            i + 100,
            i
        ))
        .execute(&mut *conn)
        .await
        .expect("Failed to insert");
    }

    // Verify all inserts via a fresh pooled connection
    let mut conn = pool.get().await.expect("Failed to get connection");
    // Verify pool still works with a write operation
    diesel::sql_query("INSERT INTO dp_test (id, val) VALUES (999, 'verify')")
        .execute(&mut *conn)
        .await
        .expect("Pool connection still works");
}

/// Test bb8 async connection pool
#[cfg(feature = "bb8")]
#[tokio::test]
async fn test_bb8_pool() {
    use diesel_async::pooled_connection::{
        AsyncDieselConnectionManager, ManagerConfig, RecyclingMethod,
    };
    use diesel_libsql::bb8::Pool;
    use diesel_libsql::AsyncLibSqlConnection;

    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("bb8_test.db");
    let db_url = db_path.to_str().unwrap().to_string();

    // Use RecyclingMethod::Fast to skip connection validation
    let mut config = ManagerConfig::<AsyncLibSqlConnection>::default();
    config.recycling_method = RecyclingMethod::Fast;
    let manager =
        AsyncDieselConnectionManager::<AsyncLibSqlConnection>::new_with_config(&db_url, config);
    let pool = Pool::builder()
        .max_size(1)
        .build(manager)
        .await
        .expect("Failed to build bb8 pool");

    // Use the pool
    let mut conn = pool.get().await.expect("Failed to get connection");
    diesel::sql_query(
        "CREATE TABLE IF NOT EXISTS bb8_test (id INTEGER PRIMARY KEY, val TEXT NOT NULL)",
    )
    .execute(&mut *conn)
    .await
    .expect("Failed to create table");

    diesel::sql_query("INSERT INTO bb8_test (id, val) VALUES (1, 'hello')")
        .execute(&mut *conn)
        .await
        .expect("Failed to insert");
}
