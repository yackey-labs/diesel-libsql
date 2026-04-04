//! OpenTelemetry instrumentation using the `opentelemetry` crate directly.
//!
//! When the `otel` feature is enabled, [`OtelInstrumentation`] can be set on
//! any `LibSqlConnection` (or `AsyncLibSqlConnection`) to emit OTel spans
//! for every database interaction, following
//! [OTel database semantic conventions v1.34](https://opentelemetry.io/docs/specs/semconv/database/).

use std::borrow::Cow;

use diesel::connection::{Instrumentation, InstrumentationEvent};
use opentelemetry::{
    global,
    trace::{Span, SpanKind, Status, Tracer},
    KeyValue,
};

/// A [`diesel::connection::Instrumentation`] implementation that creates
/// OpenTelemetry spans for database operations.
///
/// Span names and attributes follow the
/// [OTel database semantic conventions](https://opentelemetry.io/docs/specs/semconv/database/).
///
/// # Span kind
///
/// All database spans use `SpanKind::Client` per the OTel database span spec.
///
/// # Query text safety
///
/// By default, `db.query.text` is included in spans. This is safe because
/// the query text contains only parameterized SQL with `?` placeholders —
/// bind parameter values are never included. Call [`with_query_text(false)`](Self::with_query_text)
/// to disable if you don't want table/column names visible in your traces.
///
/// # Example
///
/// ```rust,no_run
/// use diesel::Connection;
/// use diesel_libsql::{LibSqlConnection, OtelInstrumentation};
///
/// let mut conn = LibSqlConnection::establish(":memory:")
///     .expect("Failed to connect");
///
/// // Query text included by default (parameterized SQL only, no bind values)
/// conn.set_instrumentation(OtelInstrumentation::new());
///
/// // Disable query text if you don't want table/column names in traces
/// conn.set_instrumentation(OtelInstrumentation::new().with_query_text(false));
/// ```
pub struct OtelInstrumentation {
    current_span: Option<opentelemetry::global::BoxedSpan>,
    /// Whether to include `db.query.text` in spans. Default: true.
    include_query_text: bool,
    /// Cached server address from the last establish connection event.
    server_address: Option<String>,
    /// Cached db.namespace from the last establish connection event.
    db_namespace: Option<String>,
}

impl OtelInstrumentation {
    /// Create a new `OtelInstrumentation` instance.
    ///
    /// Query text is included by default. The text contains only parameterized
    /// SQL with `?` placeholders — bind parameter values are never included.
    /// Call [`with_query_text(false)`](Self::with_query_text) to disable if
    /// you don't want table/column names in your traces.
    pub fn new() -> Self {
        Self {
            current_span: None,
            include_query_text: true,
            server_address: None,
            db_namespace: None,
        }
    }

    /// Enable or disable `db.query.text` in spans.
    ///
    /// When enabled, the parameterized SQL is included in every query span
    /// (e.g. `SELECT * FROM users WHERE name = ?`). Bind parameter **values**
    /// are never included — only `?` placeholders appear. This may still
    /// expose table/column names or query structure, so only enable in
    /// environments where your trace backend is access-controlled.
    pub fn with_query_text(mut self, enabled: bool) -> Self {
        self.include_query_text = enabled;
        self
    }
}

impl Default for OtelInstrumentation {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract the first table name from a SQL query for `db.collection.name`.
/// Returns `None` for queries where the table can't be trivially extracted.
fn extract_table_name(sql: &str) -> Option<&str> {
    let upper = sql.to_uppercase();
    let trimmed = upper.trim();

    // SELECT ... FROM <table>
    // DELETE FROM <table>
    if let Some(pos) = trimmed.find("FROM ") {
        let after_from = &sql[pos + 5..];
        return after_from.split_whitespace().next().map(|t| t.trim_matches('`'));
    }

    // INSERT INTO <table>
    if let Some(pos) = trimmed.find("INTO ") {
        let after_into = &sql[pos + 5..];
        return after_into.split_whitespace().next().map(|t| t.trim_matches('`'));
    }

    // UPDATE <table>
    if trimmed.starts_with("UPDATE ") {
        let after_update = &sql[7..];
        return after_update.split_whitespace().next().map(|t| t.trim_matches('`'));
    }

    None
}

/// Build a `db.query.summary` from the operation and table name.
/// Format: `{OP} {table}` (e.g. "SELECT items", "INSERT items").
fn build_query_summary(op_name: &str, table: Option<&str>) -> String {
    match table {
        Some(t) => format!("{} {}", op_name, t),
        None => op_name.to_string(),
    }
}

/// Redact auth tokens from a connection URL.
fn redact_url(url: &str) -> String {
    if let Some(idx) = url.find("authToken=") {
        format!("{}authToken=REDACTED", &url[..idx])
    } else {
        url.to_string()
    }
}

/// Extract db.namespace from a connection URL.
/// For file paths, uses the filename. For remote URLs, uses the host.
fn extract_namespace(url: &str) -> Option<String> {
    if url == ":memory:" {
        return Some(":memory:".to_string());
    }
    if url.starts_with("libsql://") || url.starts_with("http://") || url.starts_with("https://") {
        // Remote: use host as namespace
        url.split("://")
            .nth(1)
            .and_then(|rest| rest.split(['?', '/', ':']).next())
            .map(|s| s.to_string())
    } else {
        // Local file: use filename
        url.rsplit('/').next().map(|s| s.to_string())
    }
}

impl Instrumentation for OtelInstrumentation {
    fn on_connection_event(&mut self, event: InstrumentationEvent<'_>) {
        let tracer = global::tracer("diesel-libsql");

        match event {
            InstrumentationEvent::StartQuery { query, .. } => {
                let query_text = format!("{}", query);
                let op_name = query_text
                    .split_whitespace()
                    .next()
                    .unwrap_or("SQL")
                    .to_uppercase();

                let table = extract_table_name(&query_text);
                let summary = build_query_summary(&op_name, table);

                // Span name: "{op} {table}" per semconv, or just "{op}" if no table
                let span_name = if let Some(t) = table {
                    format!("{} {}", op_name, t)
                } else {
                    format!("{} libsql", op_name)
                };

                let mut attrs = vec![
                    KeyValue::new("db.system.name", "sqlite"),
                    KeyValue::new("db.operation.name", op_name),
                    KeyValue::new("db.query.summary", summary),
                ];

                if let Some(t) = table {
                    attrs.push(KeyValue::new("db.collection.name", t.to_string()));
                }

                if self.include_query_text {
                    attrs.push(KeyValue::new("db.query.text", query_text));
                }

                if let Some(ref addr) = self.server_address {
                    attrs.push(KeyValue::new("server.address", addr.clone()));
                }

                if let Some(ref ns) = self.db_namespace {
                    attrs.push(KeyValue::new("db.namespace", ns.clone()));
                }

                let span = tracer
                    .span_builder(span_name)
                    .with_kind(SpanKind::Client)
                    .with_attributes(attrs)
                    .start(&tracer);

                self.current_span = Some(span);
            }
            InstrumentationEvent::FinishQuery { error, .. } => {
                if let Some(ref mut span) = self.current_span.take() {
                    if let Some(err) = error {
                        span.set_status(Status::Error {
                            description: Cow::Owned(err.to_string()),
                        });
                        span.set_attribute(KeyValue::new("error.type", err.to_string()));
                    } else {
                        span.set_status(Status::Ok);
                    }
                    span.end();
                }
            }
            InstrumentationEvent::StartEstablishConnection { url, .. } => {
                let safe_url = redact_url(url);
                let namespace = extract_namespace(url);

                // Cache for subsequent query spans
                self.server_address = Some(safe_url.clone());
                self.db_namespace = namespace.clone();

                let mut attrs = vec![
                    KeyValue::new("db.system.name", "sqlite"),
                    KeyValue::new("server.address", safe_url),
                ];

                if let Some(ns) = namespace {
                    attrs.push(KeyValue::new("db.namespace", ns));
                }

                let span = tracer
                    .span_builder("db.connect")
                    .with_kind(SpanKind::Client)
                    .with_attributes(attrs)
                    .start(&tracer);

                self.current_span = Some(span);
            }
            InstrumentationEvent::FinishEstablishConnection { error, .. } => {
                if let Some(ref mut span) = self.current_span.take() {
                    if let Some(err) = error {
                        span.set_status(Status::Error {
                            description: Cow::Owned(err.to_string()),
                        });
                    } else {
                        span.set_status(Status::Ok);
                    }
                    span.end();
                }
            }
            InstrumentationEvent::BeginTransaction { depth, .. } => {
                let mut attrs = vec![
                    KeyValue::new("db.system.name", "sqlite"),
                    KeyValue::new("db.operation.name", "BEGIN"),
                    KeyValue::new("db.transaction.depth", depth.get() as i64),
                ];

                if let Some(ref addr) = self.server_address {
                    attrs.push(KeyValue::new("server.address", addr.clone()));
                }

                if let Some(ref ns) = self.db_namespace {
                    attrs.push(KeyValue::new("db.namespace", ns.clone()));
                }

                let span = tracer
                    .span_builder("db.transaction")
                    .with_kind(SpanKind::Client)
                    .with_attributes(attrs)
                    .start(&tracer);

                self.current_span = Some(span);
            }
            InstrumentationEvent::CommitTransaction { .. } => {
                if let Some(ref mut span) = self.current_span.take() {
                    span.set_attribute(KeyValue::new("db.operation.name", "COMMIT"));
                    span.end();
                }
            }
            InstrumentationEvent::RollbackTransaction { .. } => {
                if let Some(ref mut span) = self.current_span.take() {
                    span.set_attribute(KeyValue::new("db.operation.name", "ROLLBACK"));
                    span.end();
                }
            }
            _ => {} // non_exhaustive
        }
    }
}
