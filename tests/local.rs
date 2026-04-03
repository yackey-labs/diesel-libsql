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

// ============================================================
// Coverage tests: to_sql.rs — typed inserts for all types
// ============================================================

diesel::table! {
    all_types (id) {
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

/// Test: Typed Diesel inserts/selects for EVERY type (bool, i16, i32, i64, f32, f64, String, Vec<u8>)
/// Covers to_sql.rs lines 9-11, 23-25, 30-32, 44-46, 58-60 and from_sql.rs equivalents.
#[test]
fn test_typed_inserts_all_types() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE all_types (
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
    .expect("Failed to create table");

    // Use Diesel typed insert with bind params for every type
    diesel::insert_into(all_types::table)
        .values((
            all_types::bool_val.eq(true),
            all_types::small_val.eq(255i16),
            all_types::int_val.eq(42i32),
            all_types::big_val.eq(9_999_999_999i64),
            all_types::float_val.eq(1.5f32),
            all_types::double_val.eq(3.14159f64),
            all_types::text_val.eq("hello world"),
            all_types::blob_val.eq(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        ))
        .execute(&mut conn)
        .expect("Failed to typed insert all types");

    // Insert a second row with false bool
    diesel::insert_into(all_types::table)
        .values((
            all_types::bool_val.eq(false),
            all_types::small_val.eq(-1i16),
            all_types::int_val.eq(-100i32),
            all_types::big_val.eq(0i64),
            all_types::float_val.eq(0.0f32),
            all_types::double_val.eq(-1.0f64),
            all_types::text_val.eq(""),
            all_types::blob_val.eq(vec![]),
        ))
        .execute(&mut conn)
        .expect("Failed to typed insert second row");

    let results: Vec<(i32, bool, i16, i32, i64, f32, f64, String, Vec<u8>)> = all_types::table
        .select((
            all_types::id,
            all_types::bool_val,
            all_types::small_val,
            all_types::int_val,
            all_types::big_val,
            all_types::float_val,
            all_types::double_val,
            all_types::text_val,
            all_types::blob_val,
        ))
        .order(all_types::id.asc())
        .load(&mut conn)
        .expect("Failed to load all types");

    assert_eq!(results.len(), 2);

    // First row
    assert!(results[0].1); // bool true
    assert_eq!(results[0].2, 255i16);
    assert_eq!(results[0].3, 42i32);
    assert_eq!(results[0].4, 9_999_999_999i64);
    assert!((results[0].5 - 1.5f32).abs() < 0.001);
    assert!((results[0].6 - 3.14159f64).abs() < 0.00001);
    assert_eq!(results[0].7, "hello world");
    assert_eq!(results[0].8, vec![0xDE, 0xAD, 0xBE, 0xEF]);

    // Second row
    assert!(!results[1].1); // bool false
    assert_eq!(results[1].2, -1i16);
    assert_eq!(results[1].3, -100i32);
    assert_eq!(results[1].4, 0i64);
    assert!((results[1].5 - 0.0f32).abs() < 0.001);
    assert!((results[1].6 - (-1.0f64)).abs() < 0.00001);
    assert_eq!(results[1].7, "");
    assert!(results[1].8.is_empty());
}

// ============================================================
// Coverage tests: query_builder.rs — LIMIT, OFFSET, LIMIT+OFFSET, RETURNING
// ============================================================

diesel::table! {
    limit_test (id) {
        id -> Integer,
        val -> Text,
    }
}

/// Test: SELECT with LIMIT only
/// Covers query_builder.rs lines 112-114
#[test]
fn test_query_limit() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE limit_test (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    for i in 0..10 {
        diesel::insert_into(limit_test::table)
            .values(limit_test::val.eq(format!("item_{}", i)))
            .execute(&mut conn)
            .expect("Failed to insert");
    }

    let results: Vec<(i32, String)> = limit_test::table
        .select((limit_test::id, limit_test::val))
        .order(limit_test::id.asc())
        .limit(5)
        .load(&mut conn)
        .expect("Failed to load with limit");

    assert_eq!(results.len(), 5);
    assert_eq!(results[0].1, "item_0");
    assert_eq!(results[4].1, "item_4");
}

/// Test: SELECT with OFFSET only (no limit)
/// Covers query_builder.rs lines 122-127 (LIMIT -1 OFFSET)
#[test]
fn test_query_offset_only() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE limit_test (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    for i in 0..10 {
        diesel::insert_into(limit_test::table)
            .values(limit_test::val.eq(format!("item_{}", i)))
            .execute(&mut conn)
            .expect("Failed to insert");
    }

    let results: Vec<(i32, String)> = limit_test::table
        .select((limit_test::id, limit_test::val))
        .order(limit_test::id.asc())
        .offset(7)
        .load(&mut conn)
        .expect("Failed to load with offset only");

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].1, "item_7");
    assert_eq!(results[2].1, "item_9");
}

/// Test: SELECT with LIMIT and OFFSET
/// Covers query_builder.rs lines 136-139
#[test]
fn test_query_limit_and_offset() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE limit_test (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    for i in 0..10 {
        diesel::insert_into(limit_test::table)
            .values(limit_test::val.eq(format!("item_{}", i)))
            .execute(&mut conn)
            .expect("Failed to insert");
    }

    let results: Vec<(i32, String)> = limit_test::table
        .select((limit_test::id, limit_test::val))
        .order(limit_test::id.asc())
        .limit(3)
        .offset(2)
        .load(&mut conn)
        .expect("Failed to load with limit+offset");

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].1, "item_2");
    assert_eq!(results[2].1, "item_4");
}

/// Test: INSERT ... RETURNING
/// Covers query_builder.rs lines 229-233 (ReturningClause impl)
#[test]
fn test_insert_returning() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE limit_test (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    let result: (i32, String) = diesel::insert_into(limit_test::table)
        .values(limit_test::val.eq("returning_test"))
        .returning((limit_test::id, limit_test::val))
        .get_result(&mut conn)
        .expect("Failed to insert returning");

    assert_eq!(result.0, 1);
    assert_eq!(result.1, "returning_test");

    // Insert another and verify the id increments
    let result2: (i32, String) = diesel::insert_into(limit_test::table)
        .values(limit_test::val.eq("second"))
        .returning((limit_test::id, limit_test::val))
        .get_result(&mut conn)
        .expect("Failed to insert returning second");

    assert_eq!(result2.0, 2);
    assert_eq!(result2.1, "second");
}

// ============================================================
// Coverage tests: Boxed queries with LIMIT/OFFSET
// ============================================================

/// Test: Boxed query with LIMIT + OFFSET
/// Covers query_builder.rs lines 146-155 (BoxedLimitOffsetClause)
#[test]
fn test_boxed_query_limit_offset() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE limit_test (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    for i in 0..10 {
        diesel::insert_into(limit_test::table)
            .values(limit_test::val.eq(format!("item_{}", i)))
            .execute(&mut conn)
            .expect("Failed to insert");
    }

    // Boxed query with limit + offset
    let results: Vec<(i32, String)> = limit_test::table
        .select((limit_test::id, limit_test::val))
        .order(limit_test::id.asc())
        .limit(3)
        .offset(2)
        .into_boxed()
        .load(&mut conn)
        .expect("Failed to load boxed limit+offset");

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].1, "item_2");

    // Boxed query with limit only
    let results: Vec<(i32, String)> = limit_test::table
        .select((limit_test::id, limit_test::val))
        .order(limit_test::id.asc())
        .limit(2)
        .into_boxed()
        .load(&mut conn)
        .expect("Failed to load boxed limit only");

    assert_eq!(results.len(), 2);

    // Boxed query with offset only
    let results: Vec<(i32, String)> = limit_test::table
        .select((limit_test::id, limit_test::val))
        .order(limit_test::id.asc())
        .offset(8)
        .into_boxed()
        .load(&mut conn)
        .expect("Failed to load boxed offset only");

    assert_eq!(results.len(), 2);

    // Boxed query with no limit/offset
    let results: Vec<(i32, String)> = limit_test::table
        .select((limit_test::id, limit_test::val))
        .into_boxed()
        .load(&mut conn)
        .expect("Failed to load boxed no limit");

    assert_eq!(results.len(), 10);
}

// ============================================================
// Coverage tests: row.rs — RowIndex<&str>, field_name, is_null, value
// ============================================================

/// Test: sql_query with named columns exercises RowIndex<&str> and field methods
/// Covers row.rs lines 60-63 (RowIndex<&str>), 84-95 (Field impl)
#[test]
fn test_sql_query_named_columns() {
    use diesel::sql_types::{Integer, Nullable, Text};

    #[derive(QueryableByName, Debug)]
    struct NamedRow {
        #[diesel(sql_type = Integer)]
        id: i32,
        #[diesel(sql_type = Text)]
        name: String,
        #[diesel(sql_type = Nullable<Text>)]
        bio: Option<String>,
    }

    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE named_test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, bio TEXT)",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    diesel::sql_query("INSERT INTO named_test (id, name, bio) VALUES (1, 'alice', 'dev')")
        .execute(&mut conn)
        .expect("Failed to insert");

    diesel::sql_query("INSERT INTO named_test (id, name, bio) VALUES (2, 'bob', NULL)")
        .execute(&mut conn)
        .expect("Failed to insert");

    let results: Vec<NamedRow> =
        diesel::sql_query("SELECT id, name, bio FROM named_test ORDER BY id")
            .load(&mut conn)
            .expect("Failed to load named");

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].id, 1);
    assert_eq!(results[0].name, "alice");
    assert_eq!(results[0].bio, Some("dev".to_string()));
    assert_eq!(results[1].id, 2);
    assert_eq!(results[1].name, "bob");
    assert_eq!(results[1].bio, None);
}

// ============================================================
// Coverage tests: value.rs — Integer→f64 conversion in read_double
// ============================================================

diesel::table! {
    int_as_double (id) {
        id -> Integer,
        int_col -> Integer,
        double_col -> Double,
    }
}

/// Test: Insert INTEGER, select as Double to hit Integer→f64 path in read_double
/// Covers value.rs line 50-51
#[test]
fn test_integer_read_as_double() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE int_as_double (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            int_col INTEGER NOT NULL,
            double_col REAL NOT NULL
        )",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    // Insert an integer value into int_col, and also a real into double_col
    diesel::sql_query("INSERT INTO int_as_double (int_col, double_col) VALUES (42, 42)")
        .execute(&mut conn)
        .expect("Failed to insert");

    // Select the int_col as a Double type — forces read_double on an Integer value
    // SQLite stores 42 as INTEGER, but we're asking Diesel to read it as f64
    // The double_col was stored as integer 42 (no decimal point).
    // SQLite may store it as Integer internally. Selecting via typed column
    // forces read_double on what may be an Integer value internally.
    let results: Vec<(i32, i32, f64)> = int_as_double::table
        .select((
            int_as_double::id,
            int_as_double::int_col,
            int_as_double::double_col,
        ))
        .load(&mut conn)
        .expect("Failed to load");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, 42);
    assert!((results[0].2 - 42.0).abs() < 0.001);
}

// ============================================================
// Coverage tests: connection.rs — error handling, set_prepared_statement_cache_size
// ============================================================

/// Test: Execute bad SQL to verify error handling
/// Covers connection.rs error paths in batch_execute, execute_returning_count
#[test]
fn test_bad_sql_error() {
    use diesel::connection::SimpleConnection;

    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    // Bad execute_returning_count path (via sql_query().execute())
    let result = diesel::sql_query("THIS IS NOT VALID SQL").execute(&mut conn);
    assert!(result.is_err());

    // Bad select on non-existent table
    let result = diesel::sql_query("SELECT * FROM nonexistent_table").execute(&mut conn);
    assert!(result.is_err());

    // Bad batch_execute (SimpleConnection::batch_execute error path)
    let result = conn.batch_execute("INVALID SQL BATCH");
    assert!(result.is_err());

    // Bad load query (exercises run_query error path)
    diesel::table! {
        ghost_table (id) {
            id -> Integer,
            val -> Text,
        }
    }
    let result: Result<Vec<(i32, String)>, _> = ghost_table::table
        .select((ghost_table::id, ghost_table::val))
        .load(&mut conn);
    assert!(result.is_err());
}

/// Test: set_prepared_statement_cache_size is a no-op but should not panic
/// Covers connection.rs line 431-433
#[test]
fn test_set_prepared_statement_cache_size() {
    use diesel::connection::CacheSize;

    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");
    conn.set_prepared_statement_cache_size(CacheSize::Unbounded);
    conn.set_prepared_statement_cache_size(CacheSize::Disabled);

    // Should still work after changing cache size
    use diesel::connection::SimpleConnection;
    conn.batch_execute("SELECT 1")
        .expect("Connection should still work");
}

// ============================================================
// Coverage tests: bind_collector.rs — MoveableBindCollector via repeated queries
// ============================================================

/// Test: Run the same parameterized query multiple times to exercise MoveableBindCollector
/// Covers bind_collector.rs lines 182-208, 224-251
#[test]
fn test_moveable_bind_collector_via_cache() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE cache_test (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT NOT NULL, num INTEGER NOT NULL)",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    diesel::table! {
        cache_test (id) {
            id -> Integer,
            val -> Text,
            num -> Integer,
        }
    }

    // Run the same shaped query many times to trigger caching
    for i in 0..10 {
        diesel::insert_into(cache_test::table)
            .values((
                cache_test::val.eq(format!("item_{}", i)),
                cache_test::num.eq(i),
            ))
            .execute(&mut conn)
            .expect("Failed to insert");
    }

    // Run the same select query multiple times with different bind params
    for i in 0..10 {
        let results: Vec<(i32, String, i32)> = cache_test::table
            .select((cache_test::id, cache_test::val, cache_test::num))
            .filter(cache_test::num.eq(i))
            .load(&mut conn)
            .expect("Failed to load");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].2, i);
    }
}

// ============================================================
// Coverage tests: connection.rs — immediate/exclusive transaction rollback paths
// ============================================================

/// Test: Immediate transaction that rolls back
/// Covers connection.rs lines 192-195 (rollback path in immediate_transaction)
#[test]
fn test_immediate_transaction_rollback() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    let result: Result<(), Error> = conn.immediate_transaction(|conn| {
        diesel::sql_query("INSERT INTO users (name) VALUES ('should_vanish')")
            .execute(conn)
            .expect("Failed to insert");
        Err(Error::RollbackTransaction)
    });
    assert!(result.is_err());

    let results: Vec<(i32, String)> = users::table
        .select((users::id, users::name))
        .load(&mut conn)
        .expect("Failed to select");
    assert!(results.is_empty());
}

/// Test: Exclusive transaction that rolls back
/// Covers connection.rs lines 213-215 (rollback path in exclusive_transaction)
#[test]
fn test_exclusive_transaction_rollback() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    let result: Result<(), Error> = conn.exclusive_transaction(|conn| {
        diesel::sql_query("INSERT INTO users (name) VALUES ('should_vanish')")
            .execute(conn)
            .expect("Failed to insert");
        Err(Error::RollbackTransaction)
    });
    assert!(result.is_err());

    let results: Vec<(i32, String)> = users::table
        .select((users::id, users::name))
        .load(&mut conn)
        .expect("Failed to select");
    assert!(results.is_empty());
}

// ============================================================
// Coverage tests: Typed inserts with nullable types via bind params
// ============================================================

/// Test: Typed insert of NULL values via diesel bind params
/// Covers null handling paths in bind_collector and to_sql
#[test]
fn test_typed_insert_nulls() {
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

    // Insert with explicit None values through diesel typed insert
    diesel::insert_into(nullable_data::table)
        .values((
            nullable_data::text_val.eq(None::<String>),
            nullable_data::small_val.eq(None::<i16>),
            nullable_data::float_val.eq(None::<f32>),
        ))
        .execute(&mut conn)
        .expect("Failed to typed insert nulls");

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
    assert_eq!(results[0].1, None);
    assert_eq!(results[0].2, None);
    assert_eq!(results[0].3, None);
}

// ============================================================
// Coverage tests: connection.rs — sync on non-replica is a no-op
// ============================================================

/// Test: sync() on a non-replica connection is a no-op
#[test]
fn test_sync_non_replica_noop() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");
    conn.sync().expect("sync on non-replica should be no-op");
}

// ============================================================
// Coverage tests: Multiple RETURNING columns
// ============================================================

/// Test: UPDATE ... RETURNING
#[test]
fn test_update_returning() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE limit_test (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    diesel::insert_into(limit_test::table)
        .values(limit_test::val.eq("original"))
        .execute(&mut conn)
        .expect("Failed to insert");

    let result: (i32, String) = diesel::update(limit_test::table.filter(limit_test::id.eq(1)))
        .set(limit_test::val.eq("updated"))
        .returning((limit_test::id, limit_test::val))
        .get_result(&mut conn)
        .expect("Failed to update returning");

    assert_eq!(result.0, 1);
    assert_eq!(result.1, "updated");
}

/// Test: DELETE ... RETURNING
#[test]
fn test_delete_returning() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    diesel::sql_query(
        "CREATE TABLE limit_test (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    diesel::insert_into(limit_test::table)
        .values(limit_test::val.eq("to_delete"))
        .execute(&mut conn)
        .expect("Failed to insert");

    let result: (i32, String) = diesel::delete(limit_test::table.filter(limit_test::id.eq(1)))
        .returning((limit_test::id, limit_test::val))
        .get_result(&mut conn)
        .expect("Failed to delete returning");

    assert_eq!(result.0, 1);
    assert_eq!(result.1, "to_delete");

    // Verify it's actually deleted
    let count: Vec<(i32, String)> = limit_test::table
        .select((limit_test::id, limit_test::val))
        .load(&mut conn)
        .expect("Failed to select");
    assert!(count.is_empty());
}

// ============================================================
// Coverage tests: connection.rs — remote URL parsing (via establish)
// ============================================================

/// Test: Establish with libsql:// URL containing authToken in query string
/// Exercises parse_remote_url with ?authToken= path. Connection succeeds
/// (libsql lazy-connects), then a query will fail at network level.
#[test]
fn test_remote_url_with_auth_token_query_param() {
    let result = LibSqlConnection::establish("libsql://fake-host.example.com?authToken=test-token");
    // libsql creates the connection lazily; establish may succeed.
    // Either outcome exercises the parse_remote_url code.
    if let Ok(mut conn) = result {
        let query_result = diesel::sql_query("SELECT 1").execute(&mut conn);
        assert!(query_result.is_err());
    }
}

/// Test: Establish with libsql:// URL with &authToken= (not first param)
#[test]
fn test_remote_url_with_ampersand_auth_token() {
    let result =
        LibSqlConnection::establish("libsql://fake-host.example.com?foo=bar&authToken=my-token");
    if let Ok(mut conn) = result {
        let query_result = diesel::sql_query("SELECT 1").execute(&mut conn);
        assert!(query_result.is_err());
    }
}

/// Test: Establish with libsql:// URL with authToken that has trailing &
#[test]
fn test_remote_url_with_auth_token_and_trailing_params() {
    let result = LibSqlConnection::establish(
        "libsql://fake-host.example.com?authToken=my-token&other=value",
    );
    if let Ok(mut conn) = result {
        let query_result = diesel::sql_query("SELECT 1").execute(&mut conn);
        assert!(query_result.is_err());
    }
}

/// Test: Establish with libsql:// URL with empty authToken
#[test]
fn test_remote_url_with_empty_auth_token() {
    let result = LibSqlConnection::establish("libsql://fake-host.example.com?authToken=");
    assert!(result.is_err());
    // Should get "authToken query parameter is empty" error
}

/// Test: Establish with libsql:// URL without any auth token (and no env var)
#[test]
fn test_remote_url_no_auth_token() {
    // Ensure LIBSQL_AUTH_TOKEN is not set
    std::env::remove_var("LIBSQL_AUTH_TOKEN");
    let result = LibSqlConnection::establish("libsql://fake-host.example.com");
    assert!(result.is_err());
}

/// Test: Establish with libsql:// URL using LIBSQL_AUTH_TOKEN env var
#[test]
fn test_remote_url_auth_from_env() {
    // Set the env var
    std::env::set_var("LIBSQL_AUTH_TOKEN", "env-token-value");
    let result = LibSqlConnection::establish("libsql://fake-host.example.com");
    // Clean up env var before asserting
    std::env::remove_var("LIBSQL_AUTH_TOKEN");
    // libsql lazy-connects so establish may succeed; query will fail
    if let Ok(mut conn) = result {
        let query_result = diesel::sql_query("SELECT 1").execute(&mut conn);
        assert!(query_result.is_err());
    }
}

/// Test: Establish with https:// URL (also goes through remote path)
#[test]
fn test_remote_url_https() {
    let result = LibSqlConnection::establish("https://fake-host.example.com?authToken=test-token");
    if let Ok(mut conn) = result {
        let query_result = diesel::sql_query("SELECT 1").execute(&mut conn);
        assert!(query_result.is_err());
    }
}

/// Test: Establish with http:// URL
#[test]
fn test_remote_url_http() {
    let result = LibSqlConnection::establish("http://fake-host.example.com?authToken=test-token");
    if let Ok(mut conn) = result {
        let query_result = diesel::sql_query("SELECT 1").execute(&mut conn);
        assert!(query_result.is_err());
    }
}

/// Test: Establish with &authToken= with empty token
#[test]
fn test_remote_url_ampersand_empty_auth_token() {
    let result = LibSqlConnection::establish("libsql://fake-host.example.com?foo=bar&authToken=");
    assert!(result.is_err());
}

/// Test: Establish with &authToken= with trailing &
#[test]
fn test_remote_url_ampersand_auth_token_trailing() {
    let result = LibSqlConnection::establish(
        "libsql://fake-host.example.com?foo=bar&authToken=my-token&baz=qux",
    );
    if let Ok(mut conn) = result {
        let query_result = diesel::sql_query("SELECT 1").execute(&mut conn);
        assert!(query_result.is_err());
    }
}

// ============================================================
// Coverage tests: connection.rs — ReplicaBuilder construction
// ============================================================

/// Test: ReplicaBuilder can be constructed and configured
/// Covers ReplicaBuilder::new, sync_interval, read_your_writes
#[test]
fn test_replica_builder_construction() {
    let builder = LibSqlConnection::replica_builder(
        "/tmp/test_replica.db",
        "libsql://fake.example.com",
        "fake-token",
    )
    .sync_interval(std::time::Duration::from_secs(60))
    .read_your_writes(false);

    // Establish will fail (remote not available) but builder construction is covered
    let result = builder.establish();
    assert!(result.is_err());
}

/// Test: ReplicaBuilder with read_your_writes true (default)
#[test]
fn test_replica_builder_defaults() {
    let builder = LibSqlConnection::replica_builder(
        "/tmp/test_replica2.db",
        "libsql://fake.example.com",
        "fake-token",
    );
    // Just establishing exercises the default read_your_writes(true) path
    let result = builder.establish();
    assert!(result.is_err());
}

// ============================================================
// Coverage tests: connection.rs — TokioRuntime inside existing runtime
// ============================================================

/// Test: Establish a connection from inside a tokio runtime (block_in_place path)
/// Covers connection.rs TokioRuntime when Handle::try_current().is_ok()
#[test]
fn test_establish_inside_tokio_runtime() {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
    rt.block_on(async {
        // We're inside a tokio runtime now. Establishing a sync connection
        // will hit the block_in_place path in TokioRuntime.
        tokio::task::block_in_place(|| {
            let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");
            diesel::sql_query("CREATE TABLE rt_test (id INTEGER PRIMARY KEY)")
                .execute(&mut conn)
                .expect("Failed to create table");
        });
    });
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
