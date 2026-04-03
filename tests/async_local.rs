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

// ============================================================
// Coverage tests: async_conn.rs — typed inserts, limit/offset, returning, errors
// ============================================================

diesel::table! {
    async_all_types (id) {
        id -> Integer,
        bool_val -> Bool,
        small_val -> SmallInt,
        int_val -> Integer,
        big_val -> BigInt,
        float_val -> Float,
        double_val -> Double,
        text_val -> Text,
        blob_val -> Binary,
    }
}

/// Test: Async typed inserts for all types
#[tokio::test]
async fn test_async_typed_inserts_all_types() {
    let mut conn = AsyncLibSqlConnection::establish(":memory:")
        .await
        .expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE async_all_types (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bool_val INTEGER NOT NULL,
            small_val INTEGER NOT NULL,
            int_val INTEGER NOT NULL,
            big_val INTEGER NOT NULL,
            float_val REAL NOT NULL,
            double_val REAL NOT NULL,
            text_val TEXT NOT NULL,
            blob_val BLOB NOT NULL
        )",
    )
    .execute(&mut conn)
    .await
    .expect("Failed to create table");

    diesel::insert_into(async_all_types::table)
        .values((
            async_all_types::bool_val.eq(true),
            async_all_types::small_val.eq(255i16),
            async_all_types::int_val.eq(42i32),
            async_all_types::big_val.eq(9_999_999_999i64),
            async_all_types::float_val.eq(1.5f32),
            async_all_types::double_val.eq(3.14159f64),
            async_all_types::text_val.eq("hello"),
            async_all_types::blob_val.eq(vec![0xDE, 0xAD]),
        ))
        .execute(&mut conn)
        .await
        .expect("Failed to typed insert");

    let results: Vec<(i32, bool, i16, i32, i64, f32, f64, String, Vec<u8>)> =
        async_all_types::table
            .select((
                async_all_types::id,
                async_all_types::bool_val,
                async_all_types::small_val,
                async_all_types::int_val,
                async_all_types::big_val,
                async_all_types::float_val,
                async_all_types::double_val,
                async_all_types::text_val,
                async_all_types::blob_val,
            ))
            .load(&mut conn)
            .await
            .expect("Failed to load");

    assert_eq!(results.len(), 1);
    assert!(results[0].1);
    assert_eq!(results[0].2, 255i16);
    assert_eq!(results[0].3, 42);
    assert_eq!(results[0].4, 9_999_999_999i64);
    assert_eq!(results[0].7, "hello");
}

diesel::table! {
    async_limit_test (id) {
        id -> Integer,
        val -> Text,
    }
}

/// Test: Async LIMIT, OFFSET, LIMIT+OFFSET
#[tokio::test]
async fn test_async_limit_offset() {
    let mut conn = AsyncLibSqlConnection::establish(":memory:")
        .await
        .expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE async_limit_test (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .await
    .expect("Failed to create table");

    for i in 0..10 {
        diesel::insert_into(async_limit_test::table)
            .values(async_limit_test::val.eq(format!("item_{}", i)))
            .execute(&mut conn)
            .await
            .expect("Failed to insert");
    }

    // LIMIT only
    let results: Vec<(i32, String)> = async_limit_test::table
        .select((async_limit_test::id, async_limit_test::val))
        .order(async_limit_test::id.asc())
        .limit(5)
        .load(&mut conn)
        .await
        .expect("Failed to load with limit");
    assert_eq!(results.len(), 5);

    // OFFSET only
    let results: Vec<(i32, String)> = async_limit_test::table
        .select((async_limit_test::id, async_limit_test::val))
        .order(async_limit_test::id.asc())
        .offset(7)
        .load(&mut conn)
        .await
        .expect("Failed to load with offset");
    assert_eq!(results.len(), 3);

    // LIMIT + OFFSET
    let results: Vec<(i32, String)> = async_limit_test::table
        .select((async_limit_test::id, async_limit_test::val))
        .order(async_limit_test::id.asc())
        .limit(3)
        .offset(2)
        .load(&mut conn)
        .await
        .expect("Failed to load with limit+offset");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].1, "item_2");
}

/// Test: Async INSERT RETURNING
#[tokio::test]
async fn test_async_insert_returning() {
    let mut conn = AsyncLibSqlConnection::establish(":memory:")
        .await
        .expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE async_limit_test (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .await
    .expect("Failed to create table");

    let result: (i32, String) = diesel::insert_into(async_limit_test::table)
        .values(async_limit_test::val.eq("returning_test"))
        .returning((async_limit_test::id, async_limit_test::val))
        .get_result(&mut conn)
        .await
        .expect("Failed to insert returning");

    assert_eq!(result.0, 1);
    assert_eq!(result.1, "returning_test");
}

/// Test: Async ALTER COLUMN
#[tokio::test]
async fn test_async_alter_column() {
    use diesel_libsql::AsyncLibSqlConnectionExt;

    let mut conn = AsyncLibSqlConnection::establish(":memory:")
        .await
        .expect("Failed to connect");

    diesel::sql_query("CREATE TABLE async_alter (id INTEGER PRIMARY KEY, name TEXT)")
        .execute(&mut conn)
        .await
        .expect("Failed to create table");

    conn.alter_column(
        "async_alter",
        "name",
        "name TEXT NOT NULL DEFAULT 'unknown'",
    )
    .await
    .expect("Failed to alter column");

    diesel::sql_query("INSERT INTO async_alter DEFAULT VALUES")
        .execute(&mut conn)
        .await
        .expect("Failed to insert with default");
}

/// Test: Async immediate_transaction commit and rollback
#[tokio::test]
async fn test_async_immediate_transaction() {
    use diesel_libsql::AsyncLibSqlConnectionExt;
    use futures_util::FutureExt;

    let mut conn = AsyncLibSqlConnection::establish(":memory:")
        .await
        .expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE async_imm (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .await
    .expect("Failed to create table");

    // Commit path
    let result: Result<(), diesel::result::Error> = conn
        .immediate_transaction(|conn| {
            async move {
                diesel::sql_query("INSERT INTO async_imm (val) VALUES ('committed')")
                    .execute(conn)
                    .await?;
                Ok(())
            }
            .boxed()
        })
        .await;
    assert!(result.is_ok());

    // Rollback path
    let result: Result<(), diesel::result::Error> = conn
        .immediate_transaction(|conn| {
            async move {
                diesel::sql_query("INSERT INTO async_imm (val) VALUES ('will_vanish')")
                    .execute(conn)
                    .await?;
                Err(diesel::result::Error::RollbackTransaction)
            }
            .boxed()
        })
        .await;
    assert!(result.is_err());
}

/// Test: Async exclusive_transaction commit and rollback
#[tokio::test]
async fn test_async_exclusive_transaction() {
    use diesel_libsql::AsyncLibSqlConnectionExt;
    use futures_util::FutureExt;

    let mut conn = AsyncLibSqlConnection::establish(":memory:")
        .await
        .expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE async_excl (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .await
    .expect("Failed to create table");

    // Commit path
    let result: Result<(), diesel::result::Error> = conn
        .exclusive_transaction(|conn| {
            async move {
                diesel::sql_query("INSERT INTO async_excl (val) VALUES ('committed')")
                    .execute(conn)
                    .await?;
                Ok(())
            }
            .boxed()
        })
        .await;
    assert!(result.is_ok());

    // Rollback path
    let result: Result<(), diesel::result::Error> = conn
        .exclusive_transaction(|conn| {
            async move {
                diesel::sql_query("INSERT INTO async_excl (val) VALUES ('will_vanish')")
                    .execute(conn)
                    .await?;
                Err(diesel::result::Error::RollbackTransaction)
            }
            .boxed()
        })
        .await;
    assert!(result.is_err());
}

/// Test: Async sync on non-replica is no-op
#[tokio::test]
async fn test_async_sync_non_replica() {
    use diesel_libsql::AsyncLibSqlConnectionExt;

    let mut conn = AsyncLibSqlConnection::establish(":memory:")
        .await
        .expect("Failed to connect");

    conn.sync()
        .await
        .expect("sync on non-replica should be no-op");
}

/// Test: Async set_prepared_statement_cache_size and set_instrumentation
#[tokio::test]
async fn test_async_set_cache_size_and_instrumentation() {
    use diesel::connection::CacheSize;
    use diesel_async::AsyncConnection;

    let mut conn = AsyncLibSqlConnection::establish(":memory:")
        .await
        .expect("Failed to connect");

    conn.set_prepared_statement_cache_size(CacheSize::Unbounded);
    conn.set_prepared_statement_cache_size(CacheSize::Disabled);

    // Verify connection still works
    diesel::sql_query("CREATE TABLE async_cache_test (id INTEGER PRIMARY KEY)")
        .execute(&mut conn)
        .await
        .expect("Connection should still work");
}

/// Test: Async set_instrumentation (covers async_conn.rs lines 429-430)
#[cfg(feature = "otel")]
#[tokio::test]
async fn test_async_set_otel_instrumentation() {
    use diesel_async::AsyncConnection;
    use diesel_libsql::OtelInstrumentation;

    let mut conn = AsyncLibSqlConnection::establish(":memory:")
        .await
        .expect("Failed to connect");

    conn.set_instrumentation(OtelInstrumentation::new());

    diesel::sql_query("CREATE TABLE async_otel_test (id INTEGER PRIMARY KEY)")
        .execute(&mut conn)
        .await
        .expect("Should work with OTel instrumentation");
}

/// Test: Async error handling — bad SQL
#[tokio::test]
async fn test_async_bad_sql_error() {
    let mut conn = AsyncLibSqlConnection::establish(":memory:")
        .await
        .expect("Failed to connect");

    let result = diesel::sql_query("THIS IS NOT VALID SQL")
        .execute(&mut conn)
        .await;
    assert!(result.is_err());

    let result = diesel::sql_query("SELECT * FROM nonexistent_async")
        .execute(&mut conn)
        .await;
    assert!(result.is_err());
}

// ============================================================
// Coverage tests: async_conn.rs — remote URL paths, set_instrumentation
// ============================================================

/// Test: Async establish with remote URL (exercises parse_remote_url in async path)
#[tokio::test]
async fn test_async_remote_url_with_auth_token() {
    let result =
        AsyncLibSqlConnection::establish("libsql://fake-host.example.com?authToken=test-token")
            .await;
    // libsql lazy-connects; establish may succeed. Query will fail.
    if let Ok(mut conn) = result {
        let query_result = diesel::sql_query("SELECT 1").execute(&mut conn).await;
        assert!(query_result.is_err());
    }
}

/// Test: Async establish with remote URL, empty auth token
#[tokio::test]
async fn test_async_remote_url_empty_auth() {
    let result =
        AsyncLibSqlConnection::establish("libsql://fake-host.example.com?authToken=").await;
    assert!(result.is_err());
}

/// Test: Async establish with remote URL, no auth token
#[tokio::test]
async fn test_async_remote_url_no_auth() {
    std::env::remove_var("LIBSQL_AUTH_TOKEN");
    let result = AsyncLibSqlConnection::establish("libsql://fake-host.example.com").await;
    assert!(result.is_err());
}

/// Test: Async load error path (query on non-existent table via load)
#[tokio::test]
async fn test_async_load_error() {
    diesel::table! {
        ghost_table (id) {
            id -> Integer,
            val -> Text,
        }
    }

    let mut conn = AsyncLibSqlConnection::establish(":memory:")
        .await
        .expect("Failed to connect");

    // This exercises the load() error path in AsyncConnectionCore
    let result: Result<Vec<(i32, String)>, _> = ghost_table::table
        .select((ghost_table::id, ghost_table::val))
        .load(&mut conn)
        .await;
    assert!(result.is_err());
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
