# diesel-libsql

Diesel ORM backend for libSQL (Turso's SQLite-compatible database). Supports local, remote Turso, and embedded replica connections.

## Build Commands

- `cargo build` -- build the crate
- `cargo build --features r2d2` -- build with connection pooling
- `cargo build --features async` -- build with async support
- `cargo build --features tracing` -- build with tracing instrumentation
- `cargo test` -- run all local tests (8 tests, no external services)
- `cargo test --features r2d2` -- run all tests including the pool test (9 tests)
- `cargo test --features async` -- run async tests (3 tests)
- `cargo test --features tracing` -- run tracing tests (1 test)
- `cargo test --features "async,tracing,r2d2"` -- run all tests (13 tests)
- `cargo clippy --all-features -- -D warnings` -- lint with all features
- `cargo fmt --check` -- format check
- `cargo doc --no-deps` -- build rustdoc
- `cargo run --example local_usage` -- run local example
- `cargo run --example async_usage --features async` -- run async example
- `cargo run --example remote_usage` -- run remote example (requires LIBSQL_URL + LIBSQL_AUTH_TOKEN)

## Architecture

- `LibSql` backend type (separate from `diesel::sqlite::Sqlite`)
- Reuses `SqliteType` for type metadata, custom `LibSqlQueryBuilder` for SQL generation
- `LibSqlConnection` bridges libsql's async API to Diesel's sync `Connection` trait via tokio
- `alter_column()` exposes libSQL-specific `ALTER TABLE ALTER COLUMN`
- `establish_replica()` + `sync()` for embedded replicas
- `r2d2` feature gate adds `LibSqlConnectionManager` for connection pooling

## Key Conventions

- All public types have rustdoc
- Tests use `#[test]` (sync) -- the connection handles async bridging internally
- Remote tests gated with `#[ignore]`
- Feature flags: `r2d2` for connection pooling, `async` for diesel-async, `tracing` for OTel instrumentation
- Do not modify existing src files beyond adding doc comments and feature gates
