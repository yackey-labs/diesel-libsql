//! OpenTelemetry instrumentation test for diesel-libsql.
//!
//! Run with: `cargo test --features otel`

#![cfg(feature = "otel")]

use diesel::prelude::*;
use diesel_libsql::{LibSqlConnection, OtelInstrumentation};

/// Test that OtelInstrumentation can be set on a connection and
/// does not panic when queries are executed.
#[test]
fn test_otel_instrumentation_no_panic() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");
    conn.set_instrumentation(OtelInstrumentation::new());

    // Create a table (triggers StartQuery/FinishQuery events)
    diesel::sql_query("CREATE TABLE otel_test (id INTEGER PRIMARY KEY, val TEXT)")
        .execute(&mut conn)
        .expect("Failed to create table");

    // Insert (triggers query events)
    diesel::sql_query("INSERT INTO otel_test (id, val) VALUES (1, 'hello')")
        .execute(&mut conn)
        .expect("Failed to insert");

    // Another insert
    diesel::sql_query("INSERT INTO otel_test (id, val) VALUES (99, 'check')")
        .execute(&mut conn)
        .expect("Failed to insert second row");

    // Transaction (triggers Begin/Commit events)
    conn.transaction::<_, diesel::result::Error, _>(|conn| {
        diesel::sql_query("INSERT INTO otel_test (id, val) VALUES (2, 'world')").execute(conn)?;
        Ok(())
    })
    .expect("Transaction failed");
}

/// Test: OTel transaction commit spans (Begin + Commit events)
/// Covers instrumentation.rs lines 143-155 (BeginTransaction, CommitTransaction)
#[test]
fn test_otel_transaction_commit() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");
    conn.set_instrumentation(OtelInstrumentation::new());

    diesel::sql_query("CREATE TABLE otel_txn (id INTEGER PRIMARY KEY, val TEXT)")
        .execute(&mut conn)
        .expect("Failed to create table");

    // Successful transaction — triggers Begin + Commit events
    conn.transaction::<_, diesel::result::Error, _>(|conn| {
        diesel::sql_query("INSERT INTO otel_txn (id, val) VALUES (1, 'committed')")
            .execute(conn)?;
        Ok(())
    })
    .expect("Transaction failed");

    // Verify data committed
    diesel::table! {
        otel_txn (id) {
            id -> Integer,
            val -> Text,
        }
    }

    let results: Vec<(i32, String)> = otel_txn::table
        .select((otel_txn::id, otel_txn::val))
        .load(&mut conn)
        .expect("Failed to load");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, "committed");
}

/// Test: OTel transaction rollback spans (Begin + Rollback events)
/// Covers instrumentation.rs lines 156-160 (RollbackTransaction)
#[test]
fn test_otel_transaction_rollback() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");
    conn.set_instrumentation(OtelInstrumentation::new());

    diesel::sql_query("CREATE TABLE otel_rollback (id INTEGER PRIMARY KEY, val TEXT)")
        .execute(&mut conn)
        .expect("Failed to create table");

    // Failed transaction — triggers Begin + Rollback events
    let result: Result<(), diesel::result::Error> = conn
        .transaction::<_, diesel::result::Error, _>(|conn| {
            diesel::sql_query("INSERT INTO otel_rollback (id, val) VALUES (1, 'will_rollback')")
                .execute(conn)?;
            Err(diesel::result::Error::RollbackTransaction)
        });
    assert!(result.is_err());
}

/// Test: OTel error path in FinishQuery
/// Covers instrumentation.rs lines 108-113 (error in FinishQuery)
#[test]
fn test_otel_query_error() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");
    conn.set_instrumentation(OtelInstrumentation::new());

    // Execute invalid SQL — triggers StartQuery then FinishQuery with error
    let result = diesel::sql_query("SELECT * FROM nonexistent_table_otel").execute(&mut conn);
    assert!(result.is_err());
}

/// Test: OTel with query text disabled
/// Covers instrumentation.rs with_query_text(false) path
#[test]
fn test_otel_no_query_text() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");
    conn.set_instrumentation(OtelInstrumentation::new().with_query_text(false));

    diesel::sql_query("CREATE TABLE otel_notext (id INTEGER PRIMARY KEY)")
        .execute(&mut conn)
        .expect("Failed to create table");

    diesel::sql_query("INSERT INTO otel_notext (id) VALUES (1)")
        .execute(&mut conn)
        .expect("Failed to insert");
}

/// Test: OTel establish connection events
/// Covers instrumentation.rs lines 119-129 (StartEstablishConnection),
/// lines 131-141 (FinishEstablishConnection success)
#[test]
fn test_otel_establish_connection_events() {
    use diesel::connection::SimpleConnection;

    // Set the default instrumentation to OTel before establishing
    let _ = diesel::connection::set_default_instrumentation(|| {
        Some(Box::new(OtelInstrumentation::new()))
    });

    // Establish triggers StartEstablishConnection + FinishEstablishConnection
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    // Verify the connection works
    conn.batch_execute("SELECT 1")
        .expect("Connection should work");

    // Reset to no instrumentation
    let _ = diesel::connection::set_default_instrumentation(|| None);
}

/// Test: OTel default instrumentation (covers Default impl)
#[test]
fn test_otel_default_impl() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    // Use Default::default() instead of new()
    conn.set_instrumentation(OtelInstrumentation::default());

    diesel::sql_query("CREATE TABLE otel_default_test (id INTEGER PRIMARY KEY)")
        .execute(&mut conn)
        .expect("Failed to create table");
}

/// Test: OTel establish with authToken URL (covers authToken redaction path)
#[test]
fn test_otel_auth_token_redaction() {
    use diesel::connection::SimpleConnection;

    // Set OTel as default instrumentation so StartEstablishConnection fires
    let _ = diesel::connection::set_default_instrumentation(|| {
        Some(Box::new(OtelInstrumentation::new()))
    });

    // Establish with a URL containing authToken — triggers redaction path
    let result = LibSqlConnection::establish("libsql://fake.example.com?authToken=secret123");
    // This will succeed (libsql lazy-connects) or fail. Either way, the OTel
    // StartEstablishConnection event fires and hits the authToken= branch.
    if let Ok(mut conn) = result {
        let _ = conn.batch_execute("SELECT 1");
    }

    let _ = diesel::connection::set_default_instrumentation(|| None);
}

/// Test: OTel establish connection failure (covers FinishEstablishConnection error path)
#[test]
fn test_otel_establish_error() {
    // Set OTel as default instrumentation
    let _ = diesel::connection::set_default_instrumentation(|| {
        Some(Box::new(OtelInstrumentation::new()))
    });

    // Establish with a URL that will fail to parse (no auth token, no env var)
    std::env::remove_var("LIBSQL_AUTH_TOKEN");
    let result = LibSqlConnection::establish("libsql://fake.example.com");
    assert!(result.is_err());
    // This triggers FinishEstablishConnection with error

    let _ = diesel::connection::set_default_instrumentation(|| None);
}
