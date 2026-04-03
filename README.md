# diesel-libsql

> **Community project** -- not affiliated with or maintained by the [Diesel](https://diesel.rs) or [Turso/libSQL](https://turso.tech/libsql) teams.

A Diesel ORM backend for libSQL -- Turso's SQLite-compatible database.

Use Diesel's typed query builder, migrations, and connection management against local SQLite files, remote Turso databases, and embedded replicas. Supports both sync and native async, with OpenTelemetry instrumentation and connection pooling built in.

## Why diesel-libsql?

Diesel's built-in SQLite backend uses the C SQLite API directly. That works for local files, but libSQL extends SQLite in ways the C API can't reach:

| | diesel-sqlite | diesel-libsql |
|---|---|---|
| Local file / `:memory:` | Yes | Yes |
| Remote Turso (HTTP) | No | Yes |
| Embedded replicas | No | Yes |
| `ALTER TABLE ALTER COLUMN` | No | Yes |
| Native async | No | Yes |
| Encryption at rest | No | Yes |
| OpenTelemetry spans | Manual | Built-in |

## Installation

```toml
[dependencies]
diesel-libsql = "0.1"
diesel = { version = "2.3", features = ["sqlite"] }
```

Pick the features you need:

```toml
# Async connection (native, not spawn_blocking)
diesel-libsql = { version = "0.1", features = ["async"] }

# Async + deadpool connection pool
diesel-libsql = { version = "0.1", features = ["deadpool"] }

# Async + bb8 connection pool
diesel-libsql = { version = "0.1", features = ["bb8"] }

# Sync connection pool
diesel-libsql = { version = "0.1", features = ["r2d2"] }

# OpenTelemetry instrumentation
diesel-libsql = { version = "0.1", features = ["otel"] }

# Encryption at rest (requires cmake)
diesel-libsql = { version = "0.1", features = ["encryption"] }
```

## Quick start

### Local

```rust
use diesel::prelude::*;
use diesel_libsql::LibSqlConnection;

let mut conn = LibSqlConnection::establish(":memory:")?;

diesel::sql_query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    .execute(&mut conn)?;
```

### Remote Turso

```rust
use diesel::prelude::*;
use diesel_libsql::LibSqlConnection;

// Token in URL
let mut conn = LibSqlConnection::establish(
    "libsql://my-db-my-org.turso.io?authToken=YOUR_TOKEN"
)?;

// Or set LIBSQL_AUTH_TOKEN env var and omit from URL
let mut conn = LibSqlConnection::establish("libsql://my-db-my-org.turso.io")?;
```

### Async

```rust
use diesel_async::{AsyncConnection, RunQueryDsl};
use diesel_libsql::AsyncLibSqlConnection;

let mut conn = AsyncLibSqlConnection::establish(":memory:").await?;

diesel::sql_query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    .execute(&mut conn)
    .await?;
```

The async connection talks directly to libsql's native async API -- no `spawn_blocking` wrapper.

## Connection URLs

| Format | Mode |
|---|---|
| `:memory:` | In-memory database |
| `/path/to/db.sqlite` | Local file |
| `libsql://host?authToken=TOKEN` | Remote Turso |
| `http://127.0.0.1:8081` | Local Turso dev server (`turso dev`) |

For remote URLs, the auth token can be in the URL (`?authToken=...`) or in the `LIBSQL_AUTH_TOKEN` environment variable.

## Embedded replicas

Read locally, write to a remote primary. Microsecond reads with eventual consistency.

```rust
use diesel_libsql::LibSqlConnection;

// Simple
let mut conn = LibSqlConnection::establish_replica(
    "./local-replica.db",
    "libsql://my-db-my-org.turso.io",
    "your-auth-token",
)?;

// With configuration
use diesel_libsql::ReplicaBuilder;
use std::time::Duration;

let mut conn = ReplicaBuilder::new(
    "./local-replica.db",
    "libsql://my-db-my-org.turso.io",
    "your-auth-token",
)
.sync_interval(Duration::from_secs(300))  // auto-sync every 5 minutes
.read_your_writes(true)                    // see your own writes immediately
.establish()?;

// Manual sync
conn.sync()?;
```

## ALTER TABLE ALTER COLUMN

libSQL lets you change column types and constraints after table creation -- something standard SQLite can't do.

```rust
conn.alter_column("users", "name", "name TEXT NOT NULL DEFAULT 'unknown'")?;
```

This generates `ALTER TABLE users ALTER COLUMN name TO name TEXT NOT NULL DEFAULT 'unknown'`.

Note: changes only apply to new inserts and updates. Existing rows are not retroactively modified.

## Transaction modes

Standard `transaction()` uses `BEGIN DEFERRED`. For write-heavy workloads, use explicit locking:

```rust
// Acquire a reserved lock immediately (prevents SQLITE_BUSY on write)
conn.immediate_transaction(|conn| {
    diesel::insert_into(users::table)
        .values(name.eq("alice"))
        .execute(conn)?;
    Ok(())
})?;

// Acquire an exclusive lock (blocks all other connections)
conn.exclusive_transaction(|conn| {
    // bulk operations here
    Ok(())
})?;
```

## Connection pooling

### Sync (r2d2)

```rust
use diesel_libsql::r2d2::LibSqlConnectionManager;

let manager = LibSqlConnectionManager::new("/path/to/db.sqlite");
let pool = r2d2::Pool::builder().max_size(4).build(manager)?;
let mut conn = pool.get()?;
```

### Async (deadpool)

```rust
use diesel_libsql::deadpool::{Manager, Pool};

let pool = Pool::builder(Manager::new("/path/to/db.sqlite"))
    .max_size(8)
    .build()?;
let mut conn = pool.get().await?;
```

### Async (bb8)

```rust
use diesel_libsql::bb8::{Manager, Pool};

let pool = Pool::builder()
    .max_size(8)
    .build(Manager::new("/path/to/db.sqlite"))
    .await?;
let mut conn = pool.get().await?;
```

Pooling is most valuable for **remote Turso** connections (reuses HTTP sessions, avoids repeated TLS handshakes) and **embedded replicas** (concurrent read access). For local-only file databases, a single connection is often sufficient.

## Migrations

Diesel migrations work out of the box. For local development, `diesel_cli` works directly since libSQL database files are SQLite-compatible:

```bash
diesel migration generate create_users
diesel migration run --database-url ./my.db
diesel migration revert --database-url ./my.db
```

For remote Turso or when using libSQL-specific SQL (like `ALTER COLUMN`), use programmatic migrations:

```rust
use diesel_migrations::{embed_migrations, MigrationHarness};

const MIGRATIONS: diesel_migrations::EmbeddedMigrations = embed_migrations!();

let mut conn = LibSqlConnection::establish("libsql://my-db.turso.io?authToken=...")?;
conn.run_pending_migrations(MIGRATIONS)?;
```

This is also the recommended pattern for production deployments -- migrations are compiled into your binary.

## Encryption at rest

Requires the `encryption` feature (and `cmake` at build time):

```rust
let mut conn = LibSqlConnection::establish_encrypted(
    "./encrypted.db",
    b"your-32-byte-encryption-key-here!".to_vec(),
)?;
```

Uses AES-256-CBC with per-page encryption and HMAC-SHA512 authentication.

## OpenTelemetry

Attach `OtelInstrumentation` to emit spans for every query, connection, and transaction:

```rust
use diesel_libsql::{LibSqlConnection, OtelInstrumentation};

let mut conn = LibSqlConnection::establish(":memory:")?;
conn.set_instrumentation(OtelInstrumentation::new());
```

Spans follow [OTel database semantic conventions](https://opentelemetry.io/docs/specs/semconv/database/database-spans/):

- `db.system = "sqlite"`
- `db.query.text` -- the SQL query
- `db.operation.name` -- `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `BEGIN`, `COMMIT`, `ROLLBACK`
- `server.address` -- connection URL
- `error.type` -- on failure

Works with both sync `LibSqlConnection` and async `AsyncLibSqlConnection`.

## Feature flags

| Flag | Description | Dependencies |
|---|---|---|
| `r2d2` | Sync connection pooling | `r2d2` |
| `async` | Native async connection | `diesel-async`, `futures-util` |
| `deadpool` | Async pool via deadpool (implies `async`) | `deadpool` |
| `bb8` | Async pool via bb8 (implies `async`) | `bb8` |
| `otel` | OpenTelemetry span instrumentation | `opentelemetry` |
| `encryption` | AES-256 encryption at rest | `libsql/encryption` (needs `cmake`) |

## How it works

diesel-libsql defines a new `LibSql` backend type for Diesel. It reuses Diesel's `SqliteType` for type metadata and generates identical SQL (backtick quoting, `?` bind params), but has its own value types (`LibSqlValue`, `LibSqlBindCollector`) that work with libsql's Rust API instead of the C SQLite API.

The async connection implements `diesel_async::AsyncConnection` natively -- queries go directly through libsql's async methods with no sync bridge or `spawn_blocking`.

## Status

This is a community-maintained crate. It is not an official project of [Diesel](https://diesel.rs) or [Turso](https://turso.tech). Bug reports and contributions are welcome via [GitHub issues](https://github.com/yackey-labs/diesel-libsql/issues).

## Known issues

Two low-severity vulnerabilities exist in transitive dependencies of the `libsql` crate (not in diesel-libsql itself). Both require upstream fixes in libsql:

- `rustls-webpki` < 0.103.10 — CRL matching logic bug. Blocked on libsql updating its `rustls` dependency.
- `libsql-sqlite3-parser` <= 0.13.0 — crash on invalid UTF-8. No patched version available yet.

These affect remote/replica connections only (local file mode does not use rustls).

## License

MIT — see [LICENSE](LICENSE).
