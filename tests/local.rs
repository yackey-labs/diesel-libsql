//! Integration tests for diesel-libsql using local SQLite databases.

use diesel::prelude::*;
use diesel::result::Error;
use diesel_libsql::LibSqlConnection;

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

diesel::table! {
    nullable_data (id) {
        id -> Integer,
        text_val -> Nullable<Text>,
        small_val -> Nullable<SmallInt>,
        float_val -> Nullable<Float>,
    }
}

diesel::table! {
    items (id) {
        id -> Integer,
        title -> Text,
    }
}

/// Test 1: In-memory CRUD operations
#[test]
fn test_in_memory_crud() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    // Create table
    diesel::sql_query(
        "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    // Insert
    diesel::sql_query("INSERT INTO users (name) VALUES ('alice')")
        .execute(&mut conn)
        .expect("Failed to insert");

    // Select back
    let results: Vec<(i32, String)> = users::table
        .select((users::id, users::name))
        .load(&mut conn)
        .expect("Failed to select");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, "alice");

    // Update
    diesel::update(users::table.filter(users::name.eq("alice")))
        .set(users::name.eq("bob"))
        .execute(&mut conn)
        .expect("Failed to update");

    let results: Vec<(i32, String)> = users::table
        .select((users::id, users::name))
        .load(&mut conn)
        .expect("Failed to select after update");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, "bob");

    // Delete
    diesel::delete(users::table.filter(users::name.eq("bob")))
        .execute(&mut conn)
        .expect("Failed to delete");

    let results: Vec<(i32, String)> = users::table
        .select((users::id, users::name))
        .load(&mut conn)
        .expect("Failed to select after delete");

    assert!(results.is_empty());
}

/// Test 2: Transaction rollback
#[test]
fn test_transaction_rollback() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    // Begin transaction, insert, then rollback via returning Err
    let result: Result<(), Error> = conn.transaction::<_, Error, _>(|conn| {
        diesel::sql_query("INSERT INTO users (name) VALUES ('should_not_exist')")
            .execute(conn)
            .expect("Failed to insert in transaction");

        // Verify the row exists inside the transaction
        let count: Vec<(i32, String)> = users::table
            .select((users::id, users::name))
            .load(conn)
            .expect("Failed to select in transaction");
        assert_eq!(count.len(), 1);

        // Return error to trigger rollback
        Err(Error::RollbackTransaction)
    });

    assert!(result.is_err());

    // Verify the row was rolled back
    let results: Vec<(i32, String)> = users::table
        .select((users::id, users::name))
        .load(&mut conn)
        .expect("Failed to select after rollback");

    assert!(results.is_empty());
}

/// Test 3: File persistence
#[test]
fn test_file_persistence() {
    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("test.db");
    let db_url = db_path.to_str().unwrap();

    // Create table and insert data
    {
        let mut conn = LibSqlConnection::establish(db_url).expect("Failed to connect");

        diesel::sql_query(
            "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
        )
        .execute(&mut conn)
        .expect("Failed to create table");

        diesel::sql_query("INSERT INTO users (name) VALUES ('persistent_alice')")
            .execute(&mut conn)
            .expect("Failed to insert");
    }
    // Connection dropped here

    // Re-establish and verify data persisted
    {
        let mut conn = LibSqlConnection::establish(db_url).expect("Failed to reconnect");

        let results: Vec<(i32, String)> = users::table
            .select((users::id, users::name))
            .load(&mut conn)
            .expect("Failed to select after reconnect");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, "persistent_alice");
    }
}

/// Test 4: Multiple types round-trip
#[test]
fn test_multiple_types() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

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
    .expect("Failed to create table");

    // Insert typed data using raw SQL to avoid needing Insertable derives
    diesel::sql_query(
        "INSERT INTO typed_data (int_val, bigint_val, real_val, text_val, blob_val, bool_val)
         VALUES (42, 9999999999, 3.14, 'hello world', X'DEADBEEF', 1)",
    )
    .execute(&mut conn)
    .expect("Failed to insert typed data");

    // Read back using diesel's typed query
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
        .expect("Failed to load typed data");

    assert_eq!(results.len(), 1);
    let row = &results[0];
    assert_eq!(row.1, 42); // int_val
    assert_eq!(row.2, 9999999999i64); // bigint_val
    assert!((row.3 - 3.14).abs() < 0.001); // real_val
    assert_eq!(row.4, "hello world"); // text_val
    assert_eq!(row.5, vec![0xDE, 0xAD, 0xBE, 0xEF]); // blob_val
    assert!(row.6); // bool_val
}

/// Test 5: Nullable types round-trip (Option<String>, Option<i16>, Option<f32>)
#[test]
fn test_nullable_types() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE nullable_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text_val TEXT,
            small_val INTEGER,
            float_val REAL
        )",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    // Insert a row with all values present
    diesel::sql_query(
        "INSERT INTO nullable_data (text_val, small_val, float_val) VALUES ('hello', 42, 2.5)",
    )
    .execute(&mut conn)
    .expect("Failed to insert row with values");

    // Insert a row with all NULLs
    diesel::sql_query(
        "INSERT INTO nullable_data (text_val, small_val, float_val) VALUES (NULL, NULL, NULL)",
    )
    .execute(&mut conn)
    .expect("Failed to insert row with NULLs");

    let results: Vec<(i32, Option<String>, Option<i16>, Option<f32>)> = nullable_data::table
        .select((
            nullable_data::id,
            nullable_data::text_val,
            nullable_data::small_val,
            nullable_data::float_val,
        ))
        .order(nullable_data::id.asc())
        .load(&mut conn)
        .expect("Failed to load nullable data");

    assert_eq!(results.len(), 2);

    // First row: values present
    assert_eq!(results[0].1, Some("hello".to_string()));
    assert_eq!(results[0].2, Some(42i16));
    assert!((results[0].3.unwrap() - 2.5f32).abs() < 0.001);

    // Second row: all NULL
    assert_eq!(results[1].1, None);
    assert_eq!(results[1].2, None);
    assert_eq!(results[1].3, None);
}

/// Test 6: Typed inserts for i16 and f32 via Diesel bind parameters
#[test]
fn test_typed_inserts_i16_f32() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE nullable_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text_val TEXT,
            small_val INTEGER,
            float_val REAL
        )",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    // Use Diesel's typed insert with bind params
    diesel::insert_into(nullable_data::table)
        .values((
            nullable_data::text_val.eq(Some("typed")),
            nullable_data::small_val.eq(Some(127i16)),
            nullable_data::float_val.eq(Some(1.5f32)),
        ))
        .execute(&mut conn)
        .expect("Failed to typed insert");

    let results: Vec<(i32, Option<String>, Option<i16>, Option<f32>)> = nullable_data::table
        .select((
            nullable_data::id,
            nullable_data::text_val,
            nullable_data::small_val,
            nullable_data::float_val,
        ))
        .load(&mut conn)
        .expect("Failed to load");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, Some("typed".to_string()));
    assert_eq!(results[0].2, Some(127i16));
    assert!((results[0].3.unwrap() - 1.5f32).abs() < 0.001);
}

/// Test 7: ALTER COLUMN (libSQL-specific)
#[test]
fn test_alter_column() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    // Create table with nullable name column
    diesel::sql_query("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
        .execute(&mut conn)
        .expect("Failed to create table");

    // Alter column to add NOT NULL DEFAULT
    conn.alter_column("users", "name", "name TEXT NOT NULL DEFAULT 'unknown'")
        .expect("Failed to alter column");

    // Insert a row without specifying name — should use default
    diesel::sql_query("INSERT INTO users DEFAULT VALUES")
        .execute(&mut conn)
        .expect("Failed to insert with default");

    let results: Vec<(i32, String)> = users::table
        .select((users::id, users::name))
        .load(&mut conn)
        .expect("Failed to select");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, "unknown");

    // Alter again to remove default constraint
    conn.alter_column("users", "name", "name TEXT NOT NULL")
        .expect("Failed to alter column again");

    // Insert with explicit name should still work
    diesel::sql_query("INSERT INTO users (name) VALUES ('alice')")
        .execute(&mut conn)
        .expect("Failed to insert after second alter");

    let results: Vec<(i32, String)> = users::table
        .select((users::id, users::name))
        .order(users::id.asc())
        .load(&mut conn)
        .expect("Failed to select");

    assert_eq!(results.len(), 2);
    assert_eq!(results[1].1, "alice");
}

/// Test 8: Diesel migrations support
#[test]
fn test_migrations() {
    use diesel_migrations::{embed_migrations, HarnessWithOutput, MigrationHarness};

    const MIGRATIONS: diesel_migrations::EmbeddedMigrations = embed_migrations!("tests/migrations");

    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    // Run pending migrations
    let mut harness = HarnessWithOutput::write_to_stdout(&mut conn);
    let applied = harness
        .run_pending_migrations(MIGRATIONS)
        .expect("Failed to run migrations");

    assert_eq!(applied.len(), 1);
    drop(harness);

    // Verify the items table exists by inserting and querying
    diesel::sql_query("INSERT INTO items (title) VALUES ('test item')")
        .execute(&mut conn)
        .expect("Failed to insert into items");

    let results: Vec<(i32, String)> = items::table
        .select((items::id, items::title))
        .load(&mut conn)
        .expect("Failed to select items");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, "test item");

    // No pending migrations remain
    assert!(!conn
        .has_pending_migration(MIGRATIONS)
        .expect("Failed to check pending"));

    // Revert the migration
    let mut harness = HarnessWithOutput::write_to_stdout(&mut conn);
    harness
        .revert_last_migration(MIGRATIONS)
        .expect("Failed to revert migration");
    drop(harness);

    // After reverting, querying items should fail (table dropped)
    let result = diesel::sql_query("SELECT * FROM items").execute(&mut conn);
    assert!(result.is_err());
}

/// Test: Immediate transaction — insert committed on success
#[test]
fn test_immediate_transaction() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    let result: Result<(), Error> = conn.immediate_transaction(|conn| {
        diesel::sql_query("INSERT INTO users (name) VALUES ('immediate_alice')")
            .execute(conn)
            .expect("Failed to insert");
        Ok(())
    });
    assert!(result.is_ok());

    let results: Vec<(i32, String)> = users::table
        .select((users::id, users::name))
        .load(&mut conn)
        .expect("Failed to select");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, "immediate_alice");
}

/// Test: Exclusive transaction — insert committed on success
#[test]
fn test_exclusive_transaction() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    let result: Result<(), Error> = conn.exclusive_transaction(|conn| {
        diesel::sql_query("INSERT INTO users (name) VALUES ('exclusive_bob')")
            .execute(conn)
            .expect("Failed to insert");
        Ok(())
    });
    assert!(result.is_ok());

    let results: Vec<(i32, String)> = users::table
        .select((users::id, users::name))
        .load(&mut conn)
        .expect("Failed to select");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, "exclusive_bob");
}

/// Test: last_insert_rowid returns correct values
#[test]
fn test_last_insert_rowid() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    diesel::sql_query("INSERT INTO users (name) VALUES ('alice')")
        .execute(&mut conn)
        .expect("Failed to insert");

    let rowid1 = conn.last_insert_rowid();
    assert!(rowid1 > 0, "Expected rowid > 0, got {}", rowid1);

    diesel::sql_query("INSERT INTO users (name) VALUES ('bob')")
        .execute(&mut conn)
        .expect("Failed to insert");

    let rowid2 = conn.last_insert_rowid();
    assert!(
        rowid2 > rowid1,
        "Expected rowid2 ({}) > rowid1 ({})",
        rowid2,
        rowid1
    );
}

/// Test: Encrypted database — write, reopen with same key, verify; wrong key fails
#[test]
#[cfg(feature = "encryption")]
fn test_establish_encrypted() {
    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("encrypted.db");
    let db_url = db_path.to_str().unwrap();

    let key: Vec<u8> = (0..32).collect(); // 32-byte key

    // Create encrypted DB, insert data
    {
        let mut conn =
            LibSqlConnection::establish_encrypted(db_url, key.clone()).expect("Failed to connect");

        diesel::sql_query(
            "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
        )
        .execute(&mut conn)
        .expect("Failed to create table");

        diesel::sql_query("INSERT INTO users (name) VALUES ('secret_alice')")
            .execute(&mut conn)
            .expect("Failed to insert");
    }

    // Reopen with same key — data should be there
    {
        let mut conn = LibSqlConnection::establish_encrypted(db_url, key.clone())
            .expect("Failed to reconnect");

        let results: Vec<(i32, String)> = users::table
            .select((users::id, users::name))
            .load(&mut conn)
            .expect("Failed to select after reconnect");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, "secret_alice");
    }

    // Reopen with wrong key — should fail
    {
        let wrong_key: Vec<u8> = (32..64).collect();
        let result = LibSqlConnection::establish_encrypted(db_url, wrong_key);
        // Opening with wrong key may succeed but querying should fail,
        // or the connection itself may fail. Either way, we can't read the data.
        if let Ok(mut conn) = result {
            let query_result: Result<Vec<(i32, String)>, _> = users::table
                .select((users::id, users::name))
                .load(&mut conn);
            assert!(
                query_result.is_err(),
                "Expected error when reading with wrong key"
            );
        }
        // If establish itself failed, that's also acceptable.
    }
}

/// Test 9: r2d2 connection pool
#[test]
#[cfg(feature = "r2d2")]
fn test_connection_pool() {
    use diesel::connection::SimpleConnection;
    use diesel_libsql::r2d2::LibSqlConnectionManager;

    let manager = LibSqlConnectionManager::new("file::memory:?cache=shared");
    let pool = r2d2::Pool::builder()
        .max_size(4)
        .build(manager)
        .expect("Failed to create pool");

    // Set up a table using one connection
    {
        let mut conn = pool.get().expect("Failed to get setup connection");
        conn.batch_execute(
            "CREATE TABLE IF NOT EXISTS pool_test (id INTEGER PRIMARY KEY, val TEXT)",
        )
        .expect("Failed to create table");
    }

    // Spawn 4 threads, each gets a connection and runs SELECT 1
    let handles: Vec<_> = (0..4)
        .map(|i| {
            let pool = pool.clone();
            std::thread::spawn(move || {
                let mut conn = pool.get().expect("Failed to get connection in thread");
                diesel::sql_query(format!(
                    "INSERT INTO pool_test (id, val) VALUES ({}, 'thread_{}')",
                    i, i
                ))
                .execute(&mut *conn)
                .expect("Failed to insert in thread");

                // Verify the connection works
                conn.batch_execute("SELECT 1")
                    .expect("SELECT 1 failed in thread");
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify connection still works after all threads complete
    let mut conn = pool.get().expect("Failed to get final connection");
    conn.batch_execute("SELECT 1")
        .expect("Final connection check failed");
}
