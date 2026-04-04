//! LibSql connection implementation.

use std::sync::Arc;

use diesel::connection::*;
use diesel::expression::QueryMetadata;
use diesel::query_builder::*;
use diesel::result::*;
use diesel::sql_types::TypeMetadata;
use diesel::QueryResult;

use crate::backend::LibSql;
use crate::bind_collector::LibSqlBindCollector;
use crate::row::LibSqlRow;
use crate::value::LibSqlValue;

/// Wrapper around a tokio runtime handle that works whether or not
/// we're already inside a tokio runtime.
struct TokioRuntime {
    runtime: Option<tokio::runtime::Runtime>,
}

impl TokioRuntime {
    fn new() -> Self {
        let runtime = if tokio::runtime::Handle::try_current().is_ok() {
            None
        } else {
            Some(
                tokio::runtime::Runtime::new()
                    .expect("Failed to create tokio runtime for LibSqlConnection"),
            )
        };
        TokioRuntime { runtime }
    }

    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        match &self.runtime {
            Some(rt) => rt.block_on(future),
            None => {
                tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(future))
            }
        }
    }
}

/// A Diesel connection backed by libsql.
///
/// Supports local SQLite databases (`:memory:` and file-based) as well as
/// remote Turso databases and embedded replicas.
#[allow(missing_debug_implementations)]
pub struct LibSqlConnection {
    database: libsql::Database,
    connection: libsql::Connection,
    runtime: TokioRuntime,
    transaction_state: AnsiTransactionManager,
    metadata_lookup: (),
    instrumentation: DynInstrumentation,
    /// Whether this connection is backed by an embedded replica.
    is_replica: bool,
}

// Safety: LibSqlConnection is only used from a single thread at a time.
// The libsql connection is not shared across threads.
#[allow(unsafe_code)]
unsafe impl Send for LibSqlConnection {}

impl LibSqlConnection {
    fn establish_inner(database_url: &str) -> ConnectionResult<Self> {
        let runtime = TokioRuntime::new();

        let is_remote = database_url.starts_with("libsql://")
            || database_url.starts_with("https://")
            || database_url.starts_with("http://");

        let database = if is_remote {
            // Parse auth token from ?authToken=TOKEN query param or LIBSQL_AUTH_TOKEN env var
            let (url, auth_token) = parse_remote_url(database_url)?;
            runtime
                .block_on(libsql::Builder::new_remote(url, auth_token).build())
                .map_err(|e| ConnectionError::BadConnection(e.to_string()))?
        } else {
            runtime
                .block_on(libsql::Builder::new_local(database_url).build())
                .map_err(|e| ConnectionError::BadConnection(e.to_string()))?
        };

        let connection = database
            .connect()
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;

        Ok(LibSqlConnection {
            database,
            connection,
            runtime,
            transaction_state: AnsiTransactionManager::default(),
            metadata_lookup: (),
            instrumentation: DynInstrumentation::none(),
            is_replica: false,
        })
    }

    /// Establish an embedded replica connection.
    ///
    /// The replica maintains a local SQLite file at `local_path` that syncs
    /// from `remote_url` using the provided `auth_token`. Reads are served
    /// locally; writes are delegated to the remote primary.
    ///
    /// Call [`sync`](Self::sync) to pull the latest state from the remote.
    pub fn establish_replica(
        local_path: &str,
        remote_url: &str,
        auth_token: &str,
    ) -> ConnectionResult<Self> {
        let runtime = TokioRuntime::new();

        let database = runtime
            .block_on(
                libsql::Builder::new_remote_replica(
                    local_path,
                    remote_url.to_string(),
                    auth_token.to_string(),
                )
                .build(),
            )
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;

        let connection = database
            .connect()
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;

        Ok(LibSqlConnection {
            database,
            connection,
            runtime,
            transaction_state: AnsiTransactionManager::default(),
            metadata_lookup: (),
            instrumentation: DynInstrumentation::none(),
            is_replica: true,
        })
    }

    /// Sync the embedded replica with the remote primary.
    ///
    /// Returns `Ok(())` on success. If this connection is not a replica
    /// (i.e., it is a local or pure-remote connection), this is a no-op.
    pub fn sync(&mut self) -> QueryResult<()> {
        if !self.is_replica {
            return Ok(());
        }
        self.runtime.block_on(self.database.sync()).map_err(|e| {
            Error::DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string()))
        })?;
        Ok(())
    }

    /// Execute a libSQL-specific `ALTER TABLE ... ALTER COLUMN ... TO ...` statement.
    ///
    /// The `new_definition` should include the column name, type, and any constraints.
    /// For example:
    /// ```ignore
    /// conn.alter_column("users", "name", "name TEXT NOT NULL DEFAULT 'unknown'")?;
    /// ```
    /// This generates: `ALTER TABLE users ALTER COLUMN name TO name TEXT NOT NULL DEFAULT 'unknown'`
    pub fn alter_column(
        &mut self,
        table: &str,
        column: &str,
        new_definition: &str,
    ) -> QueryResult<()> {
        let sql = format!(
            "ALTER TABLE {} ALTER COLUMN {} TO {}",
            table, column, new_definition
        );
        self.batch_execute(&sql)
    }

    /// Run a transaction with `BEGIN IMMEDIATE`.
    ///
    /// Acquires a reserved lock immediately, preventing other writers.
    /// Useful when you know you will write and want to avoid `SQLITE_BUSY`.
    pub fn immediate_transaction<T, E, F>(&mut self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut Self) -> Result<T, E>,
        E: From<diesel::result::Error>,
    {
        self.batch_execute("BEGIN IMMEDIATE")?;
        match f(self) {
            Ok(value) => {
                self.batch_execute("COMMIT")?;
                Ok(value)
            }
            Err(e) => {
                let _ = self.batch_execute("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Run a transaction with `BEGIN EXCLUSIVE`.
    ///
    /// Acquires an exclusive lock immediately, preventing all other connections
    /// from reading or writing.
    pub fn exclusive_transaction<T, E, F>(&mut self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut Self) -> Result<T, E>,
        E: From<diesel::result::Error>,
    {
        self.batch_execute("BEGIN EXCLUSIVE")?;
        match f(self) {
            Ok(value) => {
                self.batch_execute("COMMIT")?;
                Ok(value)
            }
            Err(e) => {
                let _ = self.batch_execute("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Returns the row ID of the last successful `INSERT`.
    ///
    /// Returns `0` if no `INSERT` has been performed on this connection.
    pub fn last_insert_rowid(&self) -> i64 {
        self.connection.last_insert_rowid()
    }

    /// Create a [`ReplicaBuilder`] for configuring an embedded replica connection.
    pub fn replica_builder(
        local_path: impl Into<String>,
        remote_url: impl Into<String>,
        auth_token: impl Into<String>,
    ) -> ReplicaBuilder {
        ReplicaBuilder::new(local_path, remote_url, auth_token)
    }

    /// Establish a local connection with encryption at rest.
    ///
    /// Uses AES-256-CBC encryption. The key must be exactly 32 bytes.
    #[cfg(feature = "encryption")]
    pub fn establish_encrypted(
        database_url: &str,
        encryption_key: Vec<u8>,
    ) -> ConnectionResult<Self> {
        let runtime = TokioRuntime::new();
        let config =
            libsql::EncryptionConfig::new(libsql::Cipher::Aes256Cbc, encryption_key.into());
        let database = runtime
            .block_on(
                libsql::Builder::new_local(database_url)
                    .encryption_config(config)
                    .build(),
            )
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;

        let connection = database
            .connect()
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;

        Ok(LibSqlConnection {
            database,
            connection,
            runtime,
            transaction_state: AnsiTransactionManager::default(),
            metadata_lookup: (),
            instrumentation: DynInstrumentation::none(),
            is_replica: false,
        })
    }

    fn run_query(&mut self, sql: &str, params: Vec<libsql::Value>) -> QueryResult<Vec<LibSqlRow>> {
        self.runtime.block_on(async {
            let stmt = self.connection.prepare(sql).await.map_err(|e| {
                Error::DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string()))
            })?;

            let rows_result = stmt.query(params).await.map_err(|e| {
                Error::DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string()))
            })?;

            Self::collect_rows(rows_result).await
        })
    }

    pub(crate) async fn collect_rows(mut rows: libsql::Rows) -> QueryResult<Vec<LibSqlRow>> {
        let column_count = rows.column_count();
        let column_names: Arc<[Option<String>]> = (0..column_count)
            .map(|i| rows.column_name(i).map(|s| s.to_string()))
            .collect::<Vec<_>>()
            .into();

        let mut result = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| {
            Error::DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string()))
        })? {
            let mut values = Vec::with_capacity(column_count as usize);
            for i in 0..column_count {
                let value = row.get_value(i).map_err(|e| {
                    Error::DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string()))
                })?;
                values.push(Some(libsql_value_to_owned(value)));
            }
            result.push(LibSqlRow {
                values,
                column_names: column_names.clone(),
            });
        }
        Ok(result)
    }

    fn execute_sql(&mut self, sql: &str, params: Vec<libsql::Value>) -> QueryResult<usize> {
        self.runtime.block_on(async {
            match self.connection.execute(sql, params.clone()).await {
                Ok(affected) => Ok(affected as usize),
                Err(libsql::Error::ExecuteReturnedRows) => {
                    // libsql's execute() rejects SELECT statements. Fall back to
                    // query() and return the row count. This happens when diesel's
                    // migration harness runs SELECT via execute_returning_count().
                    let mut rows = self
                        .connection
                        .query(sql, params)
                        .await
                        .map_err(|e| {
                            Error::DatabaseError(
                                DatabaseErrorKind::Unknown,
                                Box::new(e.to_string()),
                            )
                        })?;
                    let mut count = 0usize;
                    while rows.next().await.map_err(|e| {
                        Error::DatabaseError(
                            DatabaseErrorKind::Unknown,
                            Box::new(e.to_string()),
                        )
                    })?.is_some() {
                        count += 1;
                    }
                    Ok(count)
                }
                Err(e) => Err(Error::DatabaseError(
                    DatabaseErrorKind::Unknown,
                    Box::new(e.to_string()),
                )),
            }
        })
    }
}

/// Extract SQL string and owned params from a query source.
pub(crate) fn build_query<T>(
    source: &T,
    metadata_lookup: &mut (),
) -> QueryResult<(String, Vec<libsql::Value>)>
where
    T: QueryFragment<LibSql>,
{
    let mut qb = <LibSql as diesel::backend::Backend>::QueryBuilder::default();
    source.to_sql(&mut qb, &LibSql)?;
    let sql = qb.finish();

    let mut bind_collector = LibSqlBindCollector::default();
    source.collect_binds(&mut bind_collector, metadata_lookup, &LibSql)?;

    let params: Vec<libsql::Value> = bind_collector
        .binds
        .iter()
        .map(|(bind, _ty)| bind.to_libsql_value())
        .collect();

    Ok((sql, params))
}

impl SimpleConnection for LibSqlConnection {
    fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        self.instrumentation
            .on_connection_event(InstrumentationEvent::start_query(&StrQueryHelper::new(
                query,
            )));

        let result = self.runtime.block_on(async {
            self.connection.execute_batch(query).await.map_err(|e| {
                Error::DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string()))
            })
        });

        let result = result.map(|_| ());

        self.instrumentation
            .on_connection_event(InstrumentationEvent::finish_query(
                &StrQueryHelper::new(query),
                result.as_ref().err(),
            ));

        result
    }
}

impl ConnectionSealed for LibSqlConnection {}

impl Connection for LibSqlConnection {
    type Backend = LibSql;
    type TransactionManager = AnsiTransactionManager;

    fn establish(database_url: &str) -> ConnectionResult<Self> {
        let mut instrumentation = diesel::connection::get_default_instrumentation();
        instrumentation.on_connection_event(InstrumentationEvent::start_establish_connection(
            database_url,
        ));

        let establish_result = Self::establish_inner(database_url);
        instrumentation.on_connection_event(InstrumentationEvent::finish_establish_connection(
            database_url,
            establish_result.as_ref().err(),
        ));

        let mut conn = establish_result?;
        conn.instrumentation = instrumentation.into();
        Ok(conn)
    }

    fn execute_returning_count<T>(&mut self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Self::Backend> + QueryId,
    {
        let (sql, params) = build_query(source, &mut self.metadata_lookup)?;

        self.instrumentation
            .on_connection_event(InstrumentationEvent::start_query(&StrQueryHelper::new(
                &sql,
            )));

        let result = self.execute_sql(&sql, params);

        self.instrumentation
            .on_connection_event(InstrumentationEvent::finish_query(
                &StrQueryHelper::new(&sql),
                result.as_ref().err(),
            ));

        result
    }

    fn transaction_state(&mut self) -> &mut AnsiTransactionManager
    where
        Self: Sized,
    {
        &mut self.transaction_state
    }

    fn instrumentation(&mut self) -> &mut dyn Instrumentation {
        &mut *self.instrumentation
    }

    fn set_instrumentation(&mut self, instrumentation: impl Instrumentation) {
        self.instrumentation = instrumentation.into();
    }

    fn set_prepared_statement_cache_size(&mut self, _size: CacheSize) {
        // No-op: we don't use a prepared statement cache currently
    }
}

/// Iterator over rows returned from a query.
pub struct LibSqlCursor {
    rows: std::vec::IntoIter<LibSqlRow>,
}

impl Iterator for LibSqlCursor {
    type Item = QueryResult<LibSqlRow>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next().map(Ok)
    }
}

impl LoadConnection<DefaultLoadingMode> for LibSqlConnection {
    type Cursor<'conn, 'query> = LibSqlCursor;
    type Row<'conn, 'query> = LibSqlRow;

    fn load<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> QueryResult<Self::Cursor<'conn, 'query>>
    where
        T: Query + QueryFragment<Self::Backend> + QueryId + 'query,
        Self::Backend: QueryMetadata<T::SqlType>,
    {
        let (sql, params) = build_query(&source, &mut self.metadata_lookup)?;

        self.instrumentation
            .on_connection_event(InstrumentationEvent::start_query(&StrQueryHelper::new(
                &sql,
            )));

        let result = self.run_query(&sql, params);

        self.instrumentation
            .on_connection_event(InstrumentationEvent::finish_query(
                &StrQueryHelper::new(&sql),
                result.as_ref().err(),
            ));

        let rows = result?;
        Ok(LibSqlCursor {
            rows: rows.into_iter(),
        })
    }
}

impl diesel::migration::MigrationConnection for LibSqlConnection {
    fn setup(&mut self) -> QueryResult<usize> {
        use diesel::RunQueryDsl;
        diesel::sql_query(diesel::migration::CREATE_MIGRATIONS_TABLE).execute(self)
    }
}

impl WithMetadataLookup for LibSqlConnection {
    fn metadata_lookup(&mut self) -> &mut <LibSql as TypeMetadata>::MetadataLookup {
        &mut self.metadata_lookup
    }
}

impl MultiConnectionHelper for LibSqlConnection {
    fn to_any<'a>(
        lookup: &mut <Self::Backend as TypeMetadata>::MetadataLookup,
    ) -> &mut (dyn std::any::Any + 'a) {
        lookup
    }

    fn from_any(
        lookup: &mut dyn std::any::Any,
    ) -> Option<&mut <Self::Backend as TypeMetadata>::MetadataLookup> {
        lookup.downcast_mut()
    }
}

/// Parse a remote URL into (url, auth_token).
///
/// The auth token is extracted from a `?authToken=TOKEN` query parameter if present,
/// otherwise from the `LIBSQL_AUTH_TOKEN` environment variable.
pub(crate) fn parse_remote_url(database_url: &str) -> ConnectionResult<(String, String)> {
    // Check for ?authToken= query parameter
    if let Some(idx) = database_url.find("?authToken=") {
        let url = database_url[..idx].to_string();
        let token_start = idx + "?authToken=".len();
        // Token ends at next & or end of string
        let token = if let Some(amp) = database_url[token_start..].find('&') {
            &database_url[token_start..token_start + amp]
        } else {
            &database_url[token_start..]
        };
        if token.is_empty() {
            return Err(ConnectionError::BadConnection(
                "authToken query parameter is empty".to_string(),
            ));
        }
        return Ok((url, token.to_string()));
    }

    // Also check for &authToken= in case it's not the first param
    if let Some(idx) = database_url.find("&authToken=") {
        let url = database_url[..database_url.find('?').unwrap_or(idx)].to_string();
        let token_start = idx + "&authToken=".len();
        let token = if let Some(amp) = database_url[token_start..].find('&') {
            &database_url[token_start..token_start + amp]
        } else {
            &database_url[token_start..]
        };
        if token.is_empty() {
            return Err(ConnectionError::BadConnection(
                "authToken query parameter is empty".to_string(),
            ));
        }
        return Ok((url, token.to_string()));
    }

    // Fall back to env var
    match std::env::var("LIBSQL_AUTH_TOKEN") {
        Ok(token) if !token.is_empty() => Ok((database_url.to_string(), token)),
        _ => Err(ConnectionError::BadConnection(
            "No auth token provided: use ?authToken=TOKEN in the URL or set LIBSQL_AUTH_TOKEN"
                .to_string(),
        )),
    }
}

/// Builder for embedded replica connections with advanced configuration.
///
/// Created via [`LibSqlConnection::replica_builder`]. Allows setting
/// `sync_interval` and `read_your_writes` before establishing the connection.
pub struct ReplicaBuilder {
    local_path: String,
    remote_url: String,
    auth_token: String,
    sync_interval: Option<std::time::Duration>,
    read_your_writes: bool,
}

impl ReplicaBuilder {
    /// Create a new replica builder.
    pub fn new(
        local_path: impl Into<String>,
        remote_url: impl Into<String>,
        auth_token: impl Into<String>,
    ) -> Self {
        Self {
            local_path: local_path.into(),
            remote_url: remote_url.into(),
            auth_token: auth_token.into(),
            sync_interval: None,
            read_your_writes: true,
        }
    }

    /// Set automatic sync interval. The replica will periodically pull
    /// from the remote primary at this interval.
    pub fn sync_interval(mut self, interval: std::time::Duration) -> Self {
        self.sync_interval = Some(interval);
        self
    }

    /// Enable or disable read-your-writes consistency (default: true).
    ///
    /// When enabled, after a successful write the local replica immediately
    /// reflects the change without waiting for `sync()`.
    pub fn read_your_writes(mut self, enabled: bool) -> Self {
        self.read_your_writes = enabled;
        self
    }

    /// Build and establish the replica connection.
    pub fn establish(self) -> ConnectionResult<LibSqlConnection> {
        let runtime = TokioRuntime::new();
        let mut builder =
            libsql::Builder::new_remote_replica(self.local_path, self.remote_url, self.auth_token)
                .read_your_writes(self.read_your_writes);

        if let Some(interval) = self.sync_interval {
            builder = builder.sync_interval(interval);
        }

        let database = runtime
            .block_on(builder.build())
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;

        let connection = database
            .connect()
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;

        Ok(LibSqlConnection {
            database,
            connection,
            runtime,
            transaction_state: AnsiTransactionManager::default(),
            metadata_lookup: (),
            instrumentation: DynInstrumentation::none(),
            is_replica: true,
        })
    }

    /// Build and establish the replica connection asynchronously.
    #[cfg(feature = "async")]
    pub async fn establish_async(
        self,
    ) -> ConnectionResult<crate::async_conn::AsyncLibSqlConnection> {
        let mut builder =
            libsql::Builder::new_remote_replica(self.local_path, self.remote_url, self.auth_token)
                .read_your_writes(self.read_your_writes);

        if let Some(interval) = self.sync_interval {
            builder = builder.sync_interval(interval);
        }

        let database = builder
            .build()
            .await
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;

        let connection = database
            .connect()
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;

        Ok(crate::async_conn::AsyncLibSqlConnection::from_parts(
            database, connection,
        ))
    }
}

/// Convert a `libsql::Value` to our owned `LibSqlValue`.
pub(crate) fn libsql_value_to_owned(value: libsql::Value) -> LibSqlValue {
    match value {
        libsql::Value::Null => LibSqlValue::Null,
        libsql::Value::Integer(i) => LibSqlValue::Integer(i),
        libsql::Value::Real(f) => LibSqlValue::Real(f),
        libsql::Value::Text(s) => LibSqlValue::Text(s),
        libsql::Value::Blob(b) => LibSqlValue::Blob(b),
    }
}
