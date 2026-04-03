//! Remote Turso usage example for diesel-libsql.
//!
//! Requires a Turso database. Set these environment variables before running:
//!
//!   export LIBSQL_URL="libsql://your-db-name-your-org.turso.io"
//!   export LIBSQL_AUTH_TOKEN="your-auth-token"
//!
//! Run with: `cargo run --example remote_usage`

use diesel::prelude::*;
use diesel_libsql::LibSqlConnection;

diesel::table! {
    demo (id) {
        id -> Integer,
        value -> Text,
    }
}

fn main() {
    let url = std::env::var("LIBSQL_URL").unwrap_or_else(|_| {
        eprintln!("Error: LIBSQL_URL environment variable not set.");
        eprintln!();
        eprintln!("To run this example you need a Turso database:");
        eprintln!("  export LIBSQL_URL=\"libsql://your-db-name-your-org.turso.io\"");
        eprintln!("  export LIBSQL_AUTH_TOKEN=\"your-auth-token\"");
        std::process::exit(1);
    });

    // Auth token can be in the URL (?authToken=...) or in LIBSQL_AUTH_TOKEN env var.
    // LibSqlConnection::establish handles both.
    let mut conn = LibSqlConnection::establish(&url).expect("Failed to connect to Turso");

    // Create table (idempotent)
    diesel::sql_query(
        "CREATE TABLE IF NOT EXISTS demo (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    // Insert a row
    diesel::sql_query("INSERT INTO demo (value) VALUES ('hello from diesel-libsql')")
        .execute(&mut conn)
        .expect("Failed to insert");

    // Query
    let results: Vec<(i32, String)> = demo::table
        .select((demo::id, demo::value))
        .load(&mut conn)
        .expect("Failed to query");

    println!("Rows in demo table:");
    for (id, value) in &results {
        println!("  id={}, value={}", id, value);
    }

    // Clean up
    diesel::sql_query("DROP TABLE demo")
        .execute(&mut conn)
        .expect("Failed to drop table");

    println!("\nDone. Table dropped.");
}
