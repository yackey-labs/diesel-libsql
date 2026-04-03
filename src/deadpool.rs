//! Async connection pooling via [`deadpool`].
//!
//! Enable the `deadpool` feature to use [`AsyncLibSqlConnection`](crate::AsyncLibSqlConnection)
//! with a deadpool managed pool.
//!
//! # Example
//!
//! ```rust,no_run
//! use diesel_libsql::deadpool::{Pool, Manager};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let manager = Manager::new(":memory:");
//! let pool = Pool::builder(manager).max_size(4).build()?;
//!
//! let mut conn = pool.get().await?;
//! # Ok(())
//! # }
//! ```

/// Connection manager for the deadpool pool.
pub type Manager =
    diesel_async::pooled_connection::AsyncDieselConnectionManager<crate::AsyncLibSqlConnection>;

/// Deadpool pool for [`AsyncLibSqlConnection`](crate::AsyncLibSqlConnection).
pub type Pool = diesel_async::pooled_connection::deadpool::Pool<crate::AsyncLibSqlConnection>;

/// A connection checked out from the pool.
pub type Object = diesel_async::pooled_connection::deadpool::Object<crate::AsyncLibSqlConnection>;

/// Error building the pool.
pub type BuildError = diesel_async::pooled_connection::deadpool::BuildError;

/// Error getting a connection from the pool.
pub type PoolError = diesel_async::pooled_connection::deadpool::PoolError;
