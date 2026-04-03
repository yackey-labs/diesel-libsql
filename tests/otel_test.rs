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
