//! OpenTelemetry instrumentation using the `opentelemetry` crate directly.
//!
//! When the `otel` feature is enabled, [`OtelInstrumentation`] can be set on
//! any `LibSqlConnection` (or `AsyncLibSqlConnection`) to emit OTel spans
//! for every database interaction, following semantic conventions.

use std::borrow::Cow;

use diesel::connection::{Instrumentation, InstrumentationEvent};
use opentelemetry::{
    global,
    trace::{Span, Status, Tracer},
    KeyValue,
};

/// A [`diesel::connection::Instrumentation`] implementation that creates
/// OpenTelemetry spans for database operations.
///
/// Span names and attributes follow the
/// [OTel database semantic conventions](https://opentelemetry.io/docs/specs/semconv/database/).
///
/// # Example
///
/// ```rust,no_run
/// use diesel::Connection;
/// use diesel_libsql::{LibSqlConnection, OtelInstrumentation};
///
/// let mut conn = LibSqlConnection::establish(":memory:")
///     .expect("Failed to connect");
/// conn.set_instrumentation(OtelInstrumentation::new());
/// ```
pub struct OtelInstrumentation {
    current_span: Option<opentelemetry::global::BoxedSpan>,
}

impl OtelInstrumentation {
    /// Create a new `OtelInstrumentation` instance.
    pub fn new() -> Self {
        Self { current_span: None }
    }
}

impl Default for OtelInstrumentation {
    fn default() -> Self {
        Self::new()
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

                let mut span = tracer.start(format!("{} libsql", op_name));
                span.set_attribute(KeyValue::new("db.system", "sqlite"));
                span.set_attribute(KeyValue::new("db.query.text", query_text));
                span.set_attribute(KeyValue::new("db.operation.name", op_name));

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
                let mut span = tracer.start("db.connect");
                span.set_attribute(KeyValue::new("db.system", "sqlite"));
                span.set_attribute(KeyValue::new("server.address", url.to_string()));
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
                let mut span = tracer.start("db.transaction");
                span.set_attribute(KeyValue::new("db.system", "sqlite"));
                span.set_attribute(KeyValue::new("db.operation.name", "BEGIN"));
                span.set_attribute(KeyValue::new("db.transaction.depth", depth.get() as i64));
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
