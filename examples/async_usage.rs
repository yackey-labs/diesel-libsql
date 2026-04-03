//! Async in-memory usage example for diesel-libsql.
//!
//! Run with: `cargo run --example async_usage --features async`

#[cfg(feature = "async")]
mod inner {
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

    pub async fn run() {
        // Connect to an in-memory database
        let mut conn = AsyncLibSqlConnection::establish(":memory:")
            .await
            .expect("Failed to connect");

        // Create a table
        diesel::sql_query(
            "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
        )
        .execute(&mut conn)
        .await
        .expect("Failed to create table");

        // Insert rows
        diesel::sql_query("INSERT INTO users (name) VALUES ('alice')")
            .execute(&mut conn)
            .await
            .expect("Failed to insert alice");

        diesel::sql_query("INSERT INTO users (name) VALUES ('bob')")
            .execute(&mut conn)
            .await
            .expect("Failed to insert bob");

        // Query all users
        let results: Vec<(i32, String)> = users::table
            .select((users::id, users::name))
            .load(&mut conn)
            .await
            .expect("Failed to query users");

        println!("All users:");
        for (id, name) in &results {
            println!("  id={}, name={}", id, name);
        }

        // Update a user
        diesel::update(users::table.filter(users::name.eq("alice")))
            .set(users::name.eq("alice_updated"))
            .execute(&mut conn)
            .await
            .expect("Failed to update");

        // Delete a user
        diesel::delete(users::table.filter(users::name.eq("bob")))
            .execute(&mut conn)
            .await
            .expect("Failed to delete");

        // Transaction example
        use diesel_async::scoped_futures::ScopedFutureExt;
        conn.transaction::<_, diesel::result::Error, _>(|conn| {
            async move {
                diesel::sql_query("INSERT INTO users (name) VALUES ('charlie')")
                    .execute(conn)
                    .await?;
                println!("\nInside transaction: inserted charlie");
                Ok(())
            }
            .scope_boxed()
        })
        .await
        .expect("Transaction failed");

        // Verify final state
        let results: Vec<(i32, String)> = users::table
            .select((users::id, users::name))
            .load(&mut conn)
            .await
            .expect("Failed to query users");

        println!("\nFinal state:");
        for (id, name) in &results {
            println!("  id={}, name={}", id, name);
        }
    }
}

#[cfg(feature = "async")]
#[tokio::main]
async fn main() {
    inner::run().await;
}

#[cfg(not(feature = "async"))]
fn main() {
    eprintln!("This example requires the `async` feature. Run with:");
    eprintln!("  cargo run --example async_usage --features async");
    std::process::exit(1);
}
