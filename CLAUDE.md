# diesel-libsql

Diesel ORM backend for libSQL (Turso's SQLite-compatible database). Supports local, remote Turso, and embedded replica connections.

## Build Commands

- `cargo build` -- build the crate
- `cargo build --features "r2d2,async,deadpool,bb8,otel"` -- build with all features
- `cargo test` -- run local sync tests
- `cargo test --features "r2d2,async,otel"` -- run all local tests (69 tests)
- `cargo test --features "deadpool,bb8"` -- run async pool tests
- `cargo test --test server --features async` -- run server tests (requires `turso dev --port 8081`)
- `cargo tarpaulin --features "r2d2,async,otel" --exclude-files "tests/*" --skip-clean --test local --test async_local --test otel_test` -- coverage (75%)
- `cargo clippy --all-features -- -D warnings` -- lint with all features
- `cargo fmt --check` -- format check
- `cargo doc --no-deps` -- build rustdoc
- `cargo run --example local_usage` -- run local example
- `cargo run --example async_usage --features async` -- run async example
- `cargo run --example remote_usage` -- run remote example (requires LIBSQL_URL + LIBSQL_AUTH_TOKEN)

## Architecture

- `LibSql` backend type (separate from `diesel::sqlite::Sqlite`)
- Reuses `SqliteType` for type metadata, custom `LibSqlQueryBuilder` for SQL generation
- `LibSqlConnection` (sync) bridges libsql's async API via tokio runtime
- `AsyncLibSqlConnection` (async) uses libsql's native async API directly -- no spawn_blocking
- `alter_column()` exposes libSQL-specific `ALTER TABLE ALTER COLUMN`
- `establish_replica()` / `ReplicaBuilder` for embedded replicas with sync_interval and read_your_writes
- `OtelInstrumentation` emits OpenTelemetry spans with database semantic conventions
- Connection pooling: `r2d2` (sync), `deadpool` and `bb8` (async)

## Conventions

- **Conventional commits** -- always use conventional commit format (`feat:`, `fix:`, `chore:`, `docs:`, `test:`, `refactor:`, `security:`)
- All public types have rustdoc
- Sync tests use `#[test]`, async tests use `#[tokio::test]`
- Remote/server tests gated with `#[ignore]` or require `turso dev` running
- Feature flags: `r2d2`, `async`, `deadpool`, `bb8`, `otel`, `encryption`
