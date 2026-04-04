//! Connection pool support via `r2d2`.
//!
//! Enable the `r2d2` feature to use `LibSqlConnectionManager` with an
//! `r2d2::Pool` for connection pooling.
//!
//! # Example
//!
//! ```rust,no_run
//! use diesel_libsql::r2d2::LibSqlConnectionManager;
//!
//! let manager = LibSqlConnectionManager::new("file::memory:?cache=shared");
//! let pool = r2d2::Pool::builder()
//!     .max_size(4)
//!     .build(manager)
//!     .expect("Failed to create pool");
//!
//! let mut conn = pool.get().expect("Failed to get connection");
//! ```

use crate::connection::LibSqlConnection;
use diesel::connection::SimpleConnection;
use diesel::Connection;

/// An [`r2d2::ManageConnection`] implementation for [`LibSqlConnection`].
///
/// Create one with [`LibSqlConnectionManager::new`] and pass it to
/// [`r2d2::Pool::builder`].
pub struct LibSqlConnectionManager {
    database_url: String,
}

impl LibSqlConnectionManager {
    /// Create a new connection manager for the given database URL.
    ///
    /// The URL follows the same format as [`LibSqlConnection::establish`]:
    /// - `:memory:` or `file::memory:?cache=shared` for in-memory databases
    /// - A file path for local SQLite files
    /// - `libsql://host?authToken=TOKEN` for remote Turso databases
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
        }
    }
}

impl r2d2::ManageConnection for LibSqlConnectionManager {
    type Connection = LibSqlConnection;
    type Error = diesel::ConnectionError;

    fn connect(&self) -> Result<LibSqlConnection, Self::Error> {
        LibSqlConnection::establish(&self.database_url)
    }

    fn is_valid(&self, conn: &mut LibSqlConnection) -> Result<(), Self::Error> {
        conn.batch_execute("SELECT 1")
            .map_err(|e| diesel::ConnectionError::BadConnection(e.to_string()))
    }

    fn has_broken(&self, _conn: &mut LibSqlConnection) -> bool {
        false
    }
}
