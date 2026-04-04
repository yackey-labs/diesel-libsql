//! A [Diesel](https://diesel.rs) backend for [libSQL](https://turso.tech/libsql)
//! (Turso's SQLite-compatible database).
//!
//! This crate lets you use Diesel's typed query builder, migrations, and
//! connection traits against local SQLite files, remote Turso databases, and
//! embedded replicas -- all through a single [`LibSqlConnection`] type.
//!
//! # Why not `diesel::sqlite`?
//!
//! - **ALTER COLUMN**: libSQL extends SQLite with `ALTER TABLE ... ALTER COLUMN`,
//!   exposed here via [`LibSqlConnection::alter_column`].
//! - **Remote Turso**: connect to `libsql://` URLs with auth tokens.
//! - **Embedded replicas**: local reads with remote sync via
//!   [`LibSqlConnection::establish_replica`].
//!
//! # Quick start
//!
//! ```rust,no_run
//! use diesel::prelude::*;
//! use diesel_libsql::LibSqlConnection;
//!
//! let mut conn = LibSqlConnection::establish(":memory:")
//!     .expect("Failed to connect");
//!
//! diesel::sql_query("CREATE TABLE demo (id INTEGER PRIMARY KEY, val TEXT)")
//!     .execute(&mut conn)
//!     .unwrap();
//! ```
//!
//! # Feature flags
//!
//! | Flag         | Description                                        |
//! |--------------|----------------------------------------------------|
//! | `r2d2`       | Sync connection pooling via `r2d2`                 |
//! | `async`      | Native async connection via `diesel-async`         |
//! | `deadpool`   | Async connection pooling via `deadpool` (implies `async`) |
//! | `bb8`        | Async connection pooling via `bb8` (implies `async`) |
//! | `otel`       | `OtelInstrumentation` for OpenTelemetry spans      |
//! | `encryption` | AES-256 encryption at rest via `establish_encrypted` (requires `cmake`) |

mod backend;
mod bind_collector;
mod connection;
mod from_sql;
mod query_builder;
mod row;
mod to_sql;
mod value;

/// Sync connection pooling via `r2d2`. Requires the `r2d2` feature.
#[cfg(feature = "r2d2")]
pub mod r2d2;

#[cfg(feature = "async")]
mod async_conn;

/// Async connection pooling via `deadpool`. Requires the `deadpool` feature.
#[cfg(feature = "deadpool")]
pub mod deadpool;

/// Async connection pooling via `bb8`. Requires the `bb8` feature.
#[cfg(feature = "bb8")]
pub mod bb8;

#[cfg(feature = "otel")]
mod instrumentation;

pub use backend::LibSql;
pub use bind_collector::{LibSqlBindCollector, LibSqlBindValue};
pub use connection::{LibSqlConnection, ReplicaBuilder};
pub use value::LibSqlValue;

#[cfg(feature = "async")]
pub use async_conn::{AsyncLibSqlConnection, AsyncLibSqlConnectionExt};

#[cfg(feature = "otel")]
pub use instrumentation::OtelInstrumentation;
