//! Async connection pooling via [`bb8`].
//!
//! Enable the `bb8` feature to use [`AsyncLibSqlConnection`](crate::AsyncLibSqlConnection)
//! with a bb8 managed pool.
//!
//! # Example
//!
//! ```rust,no_run
//! use diesel_libsql::bb8::{Pool, Manager};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let manager = Manager::new(":memory:");
//! let pool = Pool::builder().max_size(4).build(manager).await?;
//!
//! let mut conn = pool.get().await?;
//! # Ok(())
//! # }
//! ```

/// Connection manager for the bb8 pool.
pub type Manager =
    diesel_async::pooled_connection::AsyncDieselConnectionManager<crate::AsyncLibSqlConnection>;

/// bb8 pool for [`AsyncLibSqlConnection`](crate::AsyncLibSqlConnection).
pub type Pool = diesel_async::pooled_connection::bb8::Pool<crate::AsyncLibSqlConnection>;

/// A connection checked out from the pool.
pub type PooledConnection<'a> =
    diesel_async::pooled_connection::bb8::PooledConnection<'a, crate::AsyncLibSqlConnection>;

/// Error from pool operations.
pub type RunError = diesel_async::pooled_connection::bb8::RunError;
