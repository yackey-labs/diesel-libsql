//! Native async connection for diesel-libsql.
//!
//! Provides [`AsyncLibSqlConnection`] — a native [`diesel_async::AsyncConnection`]
//! implementation that talks directly to libsql's async API without any
//! `spawn_blocking` bridge.

use diesel::connection::{
    DynInstrumentation, Instrumentation, InstrumentationEvent, StrQueryHelper,
};
use diesel::query_builder::{AsQuery, QueryFragment, QueryId};
use diesel::result::*;
use diesel::ConnectionResult;
use diesel::QueryResult;
use futures_util::stream;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;

use crate::backend::LibSql;
use crate::connection::{build_query, parse_remote_url, LibSqlConnection};
use crate::row::LibSqlRow;

use diesel_async::AnsiTransactionManager;

/// A native async connection to a libSQL database.
///
/// Unlike the previous `SyncConnectionWrapper`-based approach, this implementation
/// calls libsql's async API directly — no `spawn_blocking`, no sync bridge.
///
/// # Quick start
///
/// ```rust,no_run
/// use diesel_async::AsyncConnection;
/// use diesel_async::RunQueryDsl;
/// use diesel_libsql::AsyncLibSqlConnection;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut conn = AsyncLibSqlConnection::establish(":memory:").await?;
///
/// diesel::sql_query("CREATE TABLE demo (id INTEGER PRIMARY KEY, val TEXT)")
///     .execute(&mut conn)
///     .await?;
/// # Ok(())
/// # }
/// ```
#[allow(missing_debug_implementations)]
pub struct AsyncLibSqlConnection {
    database: libsql::Database,
    connection: libsql::Connection,
    transaction_state: AnsiTransactionManager,
    metadata_lookup: (),
    instrumentation: DynInstrumentation,
    /// Whether this connection is backed by an embedded replica.
    is_replica: bool,
}

// Safety: AsyncLibSqlConnection is only used from a single task at a time
// (enforced by &mut self on all trait methods). The libsql connection is not
// shared across threads.
#[allow(unsafe_code)]
unsafe impl Send for AsyncLibSqlConnection {}

impl AsyncLibSqlConnection {
    /// Create an `AsyncLibSqlConnection` from pre-built libsql parts.
    ///
    /// Used internally by [`ReplicaBuilder::establish_async`].
    pub(crate) fn from_parts(database: libsql::Database, connection: libsql::Connection) -> Self {
        Self {
            database,
            connection,
            transaction_state: AnsiTransactionManager::default(),
            metadata_lookup: (),
            instrumentation: DynInstrumentation::none(),
            is_replica: true,
        }
    }
}

/// Extension methods for [`AsyncLibSqlConnection`].
///
/// These expose libSQL-specific functionality (replicas, ALTER COLUMN, sync)
/// in an async context.
pub trait AsyncLibSqlConnectionExt {
    /// Establish an embedded replica connection asynchronously.
    ///
    /// The replica keeps a local SQLite file at `local_path` that syncs
    /// from `remote_url` using the provided `auth_token`. Reads are local;
    /// writes go to the remote primary.
    fn establish_replica(
        local_path: &str,
        remote_url: &str,
        auth_token: &str,
    ) -> impl std::future::Future<Output = ConnectionResult<AsyncLibSqlConnection>> + Send;

    /// Sync the embedded replica with the remote primary.
    ///
    /// No-op if this connection is not a replica.
    fn sync(&mut self) -> impl std::future::Future<Output = QueryResult<()>> + Send;

    /// Execute a libSQL-specific `ALTER TABLE ... ALTER COLUMN ... TO ...` statement.
    fn alter_column(
        &mut self,
        table: &str,
        column: &str,
        new_definition: &str,
    ) -> impl std::future::Future<Output = QueryResult<()>> + Send;

    /// Run a transaction with `BEGIN IMMEDIATE` asynchronously.
    ///
    /// Acquires a reserved lock immediately, preventing other writers.
    fn immediate_transaction<T, E, F>(
        &mut self,
        f: F,
    ) -> impl std::future::Future<Output = Result<T, E>> + Send
    where
        F: for<'a> FnOnce(
                &'a mut AsyncLibSqlConnection,
            ) -> futures_util::future::BoxFuture<'a, Result<T, E>>
            + Send,
        T: Send,
        E: From<diesel::result::Error> + Send;

    /// Run a transaction with `BEGIN EXCLUSIVE` asynchronously.
    ///
    /// Acquires an exclusive lock immediately, preventing all other connections
    /// from reading or writing.
    fn exclusive_transaction<T, E, F>(
        &mut self,
        f: F,
    ) -> impl std::future::Future<Output = Result<T, E>> + Send
    where
        F: for<'a> FnOnce(
                &'a mut AsyncLibSqlConnection,
            ) -> futures_util::future::BoxFuture<'a, Result<T, E>>
            + Send,
        T: Send,
        E: From<diesel::result::Error> + Send;

    /// Returns the row ID of the last successful `INSERT`.
    ///
    /// Returns `0` if no `INSERT` has been performed on this connection.
    fn last_insert_rowid(&self) -> i64;
}

impl AsyncLibSqlConnectionExt for AsyncLibSqlConnection {
    async fn establish_replica(
        local_path: &str,
        remote_url: &str,
        auth_token: &str,
    ) -> ConnectionResult<AsyncLibSqlConnection> {
        let database = libsql::Builder::new_remote_replica(
            local_path,
            remote_url.to_string(),
            auth_token.to_string(),
        )
        .build()
        .await
        .map_err(|e| diesel::ConnectionError::BadConnection(e.to_string()))?;

        let connection = database
            .connect()
            .map_err(|e| diesel::ConnectionError::BadConnection(e.to_string()))?;

        Ok(AsyncLibSqlConnection {
            database,
            connection,
            transaction_state: AnsiTransactionManager::default(),
            metadata_lookup: (),
            instrumentation: DynInstrumentation::none(),
            is_replica: true,
        })
    }

    async fn sync(&mut self) -> QueryResult<()> {
        if !self.is_replica {
            return Ok(());
        }
        self.database.sync().await.map_err(|e| {
            Error::DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string()))
        })?;
        Ok(())
    }

    async fn alter_column(
        &mut self,
        table: &str,
        column: &str,
        new_definition: &str,
    ) -> QueryResult<()> {
        let sql = format!(
            "ALTER TABLE {} ALTER COLUMN {} TO {}",
            table, column, new_definition
        );
        <Self as diesel_async::SimpleAsyncConnection>::batch_execute(self, &sql).await
    }

    async fn immediate_transaction<T, E, F>(&mut self, f: F) -> Result<T, E>
    where
        F: for<'a> FnOnce(
                &'a mut AsyncLibSqlConnection,
            ) -> futures_util::future::BoxFuture<'a, Result<T, E>>
            + Send,
        T: Send,
        E: From<diesel::result::Error> + Send,
    {
        <Self as diesel_async::SimpleAsyncConnection>::batch_execute(self, "BEGIN IMMEDIATE")
            .await?;
        match f(self).await {
            Ok(value) => {
                <Self as diesel_async::SimpleAsyncConnection>::batch_execute(self, "COMMIT")
                    .await?;
                Ok(value)
            }
            Err(e) => {
                let _ =
                    <Self as diesel_async::SimpleAsyncConnection>::batch_execute(self, "ROLLBACK")
                        .await;
                Err(e)
            }
        }
    }

    async fn exclusive_transaction<T, E, F>(&mut self, f: F) -> Result<T, E>
    where
        F: for<'a> FnOnce(
                &'a mut AsyncLibSqlConnection,
            ) -> futures_util::future::BoxFuture<'a, Result<T, E>>
            + Send,
        T: Send,
        E: From<diesel::result::Error> + Send,
    {
        <Self as diesel_async::SimpleAsyncConnection>::batch_execute(self, "BEGIN EXCLUSIVE")
            .await?;
        match f(self).await {
            Ok(value) => {
                <Self as diesel_async::SimpleAsyncConnection>::batch_execute(self, "COMMIT")
                    .await?;
                Ok(value)
            }
            Err(e) => {
                let _ =
                    <Self as diesel_async::SimpleAsyncConnection>::batch_execute(self, "ROLLBACK")
                        .await;
                Err(e)
            }
        }
    }

    fn last_insert_rowid(&self) -> i64 {
        self.connection.last_insert_rowid()
    }
}

impl diesel_async::SimpleAsyncConnection for AsyncLibSqlConnection {
    async fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        self.instrumentation
            .on_connection_event(InstrumentationEvent::start_query(&StrQueryHelper::new(
                query,
            )));

        let result = self
            .connection
            .execute_batch(query)
            .await
            .map(|_| ())
            .map_err(|e| Error::DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())));

        self.instrumentation
            .on_connection_event(InstrumentationEvent::finish_query(
                &StrQueryHelper::new(query),
                result.as_ref().err(),
            ));

        result
    }
}

impl diesel_async::AsyncConnectionCore for AsyncLibSqlConnection {
    type ExecuteFuture<'conn, 'query> = futures_util::future::BoxFuture<'conn, QueryResult<usize>>;
    type LoadFuture<'conn, 'query> =
        futures_util::future::BoxFuture<'conn, QueryResult<Self::Stream<'conn, 'query>>>;
    type Stream<'conn, 'query> = BoxStream<'static, QueryResult<LibSqlRow>>;
    type Row<'conn, 'query> = LibSqlRow;
    type Backend = LibSql;

    fn load<'conn, 'query, T>(&'conn mut self, source: T) -> Self::LoadFuture<'conn, 'query>
    where
        T: AsQuery + 'query,
        T::Query: QueryFragment<LibSql> + QueryId + 'query,
    {
        let query = source.as_query();
        let (sql, params) = match build_query(&query, &mut self.metadata_lookup) {
            Ok(v) => v,
            Err(e) => return Box::pin(std::future::ready(Err(e))),
        };

        Box::pin(async move {
            self.instrumentation
                .on_connection_event(InstrumentationEvent::start_query(&StrQueryHelper::new(
                    &sql,
                )));

            let result = async {
                let stmt = self.connection.prepare(&sql).await.map_err(|e| {
                    Error::DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string()))
                })?;

                let rows_result = stmt.query(params).await.map_err(|e| {
                    Error::DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string()))
                })?;

                LibSqlConnection::collect_rows(rows_result).await
            }
            .await;

            self.instrumentation
                .on_connection_event(InstrumentationEvent::finish_query(
                    &StrQueryHelper::new(&sql),
                    result.as_ref().err(),
                ));

            let rows = result?;
            let s: BoxStream<'static, QueryResult<LibSqlRow>> =
                stream::iter(rows.into_iter().map(Ok)).boxed();
            Ok(s)
        })
    }

    fn execute_returning_count<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> Self::ExecuteFuture<'conn, 'query>
    where
        T: QueryFragment<LibSql> + QueryId + 'query,
    {
        let (sql, params) = match build_query(&source, &mut self.metadata_lookup) {
            Ok(v) => v,
            Err(e) => return Box::pin(std::future::ready(Err(e))),
        };

        Box::pin(async move {
            self.instrumentation
                .on_connection_event(InstrumentationEvent::start_query(&StrQueryHelper::new(
                    &sql,
                )));

            let result = self
                .connection
                .execute(&sql, params)
                .await
                .map(|affected| affected as usize)
                .map_err(|e| {
                    Error::DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string()))
                });

            self.instrumentation
                .on_connection_event(InstrumentationEvent::finish_query(
                    &StrQueryHelper::new(&sql),
                    result.as_ref().err(),
                ));

            result
        })
    }
}

impl diesel_async::AsyncConnection for AsyncLibSqlConnection {
    type TransactionManager = AnsiTransactionManager;

    async fn establish(database_url: &str) -> ConnectionResult<Self> {
        let mut instrumentation = diesel::connection::get_default_instrumentation();
        instrumentation.on_connection_event(InstrumentationEvent::start_establish_connection(
            database_url,
        ));

        let is_remote = database_url.starts_with("libsql://")
            || database_url.starts_with("https://")
            || database_url.starts_with("http://");

        let result = async {
            let database = if is_remote {
                let (url, auth_token) = parse_remote_url(database_url)?;
                libsql::Builder::new_remote(url, auth_token)
                    .build()
                    .await
                    .map_err(|e| diesel::ConnectionError::BadConnection(e.to_string()))?
            } else {
                libsql::Builder::new_local(database_url)
                    .build()
                    .await
                    .map_err(|e| diesel::ConnectionError::BadConnection(e.to_string()))?
            };

            let connection = database
                .connect()
                .map_err(|e| diesel::ConnectionError::BadConnection(e.to_string()))?;

            Ok(AsyncLibSqlConnection {
                database,
                connection,
                transaction_state: AnsiTransactionManager::default(),
                metadata_lookup: (),
                instrumentation: DynInstrumentation::none(),
                is_replica: false,
            })
        }
        .await;

        instrumentation.on_connection_event(InstrumentationEvent::finish_establish_connection(
            database_url,
            result.as_ref().err(),
        ));

        let mut conn = result?;
        conn.instrumentation = instrumentation.into();
        Ok(conn)
    }

    fn transaction_state(
        &mut self,
    ) -> &mut <Self::TransactionManager as diesel_async::TransactionManager<Self>>::TransactionStateData
    {
        &mut self.transaction_state
    }

    fn instrumentation(&mut self) -> &mut dyn Instrumentation {
        &mut *self.instrumentation
    }

    fn set_instrumentation(&mut self, instrumentation: impl Instrumentation) {
        self.instrumentation = instrumentation.into();
    }

    fn set_prepared_statement_cache_size(&mut self, _size: diesel::connection::CacheSize) {
        // No-op: we don't use a prepared statement cache currently
    }
}

impl diesel_async::pooled_connection::PoolableConnection for AsyncLibSqlConnection {}
