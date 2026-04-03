//! The LibSql backend definition.

use diesel::backend::*;
use diesel::sql_types::TypeMetadata;
use diesel::sqlite::SqliteType;

use crate::bind_collector::LibSqlBindCollector;
use crate::value::LibSqlValue;

/// The LibSql backend type for Diesel.
///
/// This is a separate backend from `diesel::sqlite::Sqlite` because libsql
/// uses its own Rust API rather than raw C `sqlite3_stmt*` pointers.
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, Default)]
pub struct LibSql;

/// Query builder for the LibSql backend.
///
/// Uses the same SQL syntax as SQLite (backtick quoting, `?` params).
#[derive(Default)]
pub struct LibSqlQueryBuilder {
    sql: String,
}

impl diesel::query_builder::QueryBuilder<LibSql> for LibSqlQueryBuilder {
    fn push_sql(&mut self, sql: &str) {
        self.sql.push_str(sql);
    }

    fn push_identifier(&mut self, identifier: &str) -> diesel::QueryResult<()> {
        self.sql.push('`');
        self.sql.push_str(&identifier.replace('`', "``"));
        self.sql.push('`');
        Ok(())
    }

    fn push_bind_param(&mut self) {
        self.sql.push('?');
    }

    fn finish(self) -> String {
        self.sql
    }
}

impl Backend for LibSql {
    type QueryBuilder = LibSqlQueryBuilder;
    type RawValue<'a> = LibSqlValue;
    type BindCollector<'a> = LibSqlBindCollector<'a>;
}

impl TypeMetadata for LibSql {
    type TypeMetadata = SqliteType;
    type MetadataLookup = ();
}

/// On conflict clause type for LibSql (mirrors SQLite behavior).
#[derive(Debug, Copy, Clone)]
pub struct LibSqlOnConflictClause;

impl sql_dialect::on_conflict_clause::SupportsOnConflictClause for LibSqlOnConflictClause {}
impl sql_dialect::on_conflict_clause::SupportsOnConflictClauseWhere for LibSqlOnConflictClause {}
impl sql_dialect::on_conflict_clause::PgLikeOnConflictClause for LibSqlOnConflictClause {}

/// Batch insert support type for LibSql.
#[derive(Debug, Copy, Clone)]
pub struct LibSqlBatchInsert;

/// Returning clause type for LibSql.
#[derive(Debug, Copy, Clone)]
pub struct LibSqlReturningClause;

impl sql_dialect::returning_clause::SupportsReturningClause for LibSqlReturningClause {}

impl SqlDialect for LibSql {
    type ReturningClause = LibSqlReturningClause;
    type OnConflictClause = LibSqlOnConflictClause;
    type InsertWithDefaultKeyword =
        sql_dialect::default_keyword_for_insert::DoesNotSupportDefaultKeyword;
    type BatchInsertSupport = LibSqlBatchInsert;
    type ConcatClause = sql_dialect::concat_clause::ConcatWithPipesClause;
    type DefaultValueClauseForInsert = sql_dialect::default_value_clause::AnsiDefaultValueClause;
    type EmptyFromClauseSyntax = sql_dialect::from_clause_syntax::AnsiSqlFromClauseSyntax;
    type SelectStatementSyntax = sql_dialect::select_statement_syntax::AnsiSqlSelectStatement;
    type ExistsSyntax = sql_dialect::exists_syntax::AnsiSqlExistsSyntax;
    type ArrayComparison = sql_dialect::array_comparison::AnsiSqlArrayComparison;
    type AliasSyntax = sql_dialect::alias_syntax::AsAliasSyntax;
    type WindowFrameClauseGroupSupport =
        sql_dialect::window_frame_clause_group_support::IsoGroupWindowFrameUnit;
    type WindowFrameExclusionSupport =
        sql_dialect::window_frame_exclusion_support::FrameExclusionSupport;
    type AggregateFunctionExpressions =
        sql_dialect::aggregate_function_expressions::PostgresLikeAggregateFunctionExpressions;
    type BuiltInWindowFunctionRequireOrder =
        sql_dialect::built_in_window_function_require_order::NoOrderRequired;
}

impl DieselReserveSpecialization for LibSql {}
impl TrustedBackend for LibSql {}
