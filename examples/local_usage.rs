//! Local in-memory usage example for diesel-libsql.
//!
//! Run with: `cargo run --example local_usage`

use diesel::prelude::*;
use diesel_libsql::LibSqlConnection;

diesel::table! {
    users (id) {
        id -> Integer,
        name -> Text,
    }
}

fn main() {
    // Connect to an in-memory database
    let mut conn = LibSqlConnection::establish(":memory:").expect("Failed to connect");

    // Create a table
    diesel::sql_query(
        "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
    )
    .execute(&mut conn)
    .expect("Failed to create table");

    // Insert rows
    diesel::sql_query("INSERT INTO users (name) VALUES ('alice')")
        .execute(&mut conn)
        .expect("Failed to insert alice");

    diesel::sql_query("INSERT INTO users (name) VALUES ('bob')")
        .execute(&mut conn)
        .expect("Failed to insert bob");

    // Query all users
    let results: Vec<(i32, String)> = users::table
        .select((users::id, users::name))
        .load(&mut conn)
        .expect("Failed to query users");

    println!("All users:");
    for (id, name) in &results {
        println!("  id={}, name={}", id, name);
    }

    // Update a user
    diesel::update(users::table.filter(users::name.eq("alice")))
        .set(users::name.eq("alice_updated"))
        .execute(&mut conn)
        .expect("Failed to update");

    // Delete a user
    diesel::delete(users::table.filter(users::name.eq("bob")))
        .execute(&mut conn)
        .expect("Failed to delete");

    // Verify final state
    let results: Vec<(i32, String)> = users::table
        .select((users::id, users::name))
        .load(&mut conn)
        .expect("Failed to query users");

    println!("\nAfter update and delete:");
    for (id, name) in &results {
        println!("  id={}, name={}", id, name);
    }

    // Transaction example
    let tx_result: Result<(), diesel::result::Error> = conn
        .transaction::<_, diesel::result::Error, _>(|conn| {
            diesel::sql_query("INSERT INTO users (name) VALUES ('charlie')")
                .execute(conn)
                .expect("Failed to insert in tx");
            println!("\nInside transaction: inserted charlie");
            Ok(())
        });
    tx_result.expect("Transaction failed");

    let results: Vec<(i32, String)> = users::table
        .select((users::id, users::name))
        .load(&mut conn)
        .expect("Failed to query users");

    println!("\nFinal state:");
    for (id, name) in &results {
        println!("  id={}, name={}", id, name);
    }
}
