//! SQLite-compatible INSERT semantics for the LibSql backend.
//!
//! libSQL speaks SQLite's dialect, which has no `DEFAULT` keyword inside `VALUES`. Diesel's own
//! SQLite backend therefore omits defaultable (`None`) columns from single-row inserts and runs
//! batch inserts row by row when rows omit different columns. This module gives the `LibSql`
//! backend the same behavior:
//!
//! * [`DefaultableColumnInsertValue::Default`] renders nothing, so the column falls back to its
//!   SQL default instead of producing `near "DEFAULT": syntax error`.
//! * A batch insert whose rows all bind the same columns runs as one multi-row
//!   `INSERT ... VALUES (..), (..)` statement; otherwise each row is inserted separately inside a
//!   single transaction, so the batch stays atomic either way.
//!
//! Batch inserts combined with `RETURNING`/`get_results` or `on_conflict` are not supported yet.

use diesel::associations::HasTable;
use diesel::backend::sql_dialect::default_keyword_for_insert::DoesNotSupportDefaultKeyword;
use diesel::connection::Connection;
use diesel::expression::{AppearsOnTable, Expression};
use diesel::insertable::{ColumnInsertValue, DefaultableColumnInsertValue, InsertValues};
use diesel::query_builder::{
    AstPass, BatchInsert, InsertStatement, QueryBuilder, QueryFragment, QueryId, ValuesClause,
};
use diesel::query_dsl::methods::ExecuteDsl;
use diesel::query_source::{Column, QuerySource, Table};
use diesel::QueryResult;

use crate::backend::{LibSql, LibSqlQueryBuilder};
use crate::connection::LibSqlConnection;

// The FROM-clause type every insert target renders; `NoFromClause` marks bare values.
type NoFromClause = diesel::internal::table_macro::NoFromClause;

impl<Col, Expr> InsertValues<LibSql, Col::Table>
    for DefaultableColumnInsertValue<ColumnInsertValue<Col, Expr>>
where
    Col: Column,
    Expr: Expression<SqlType = Col::SqlType> + AppearsOnTable<NoFromClause>,
    Self: QueryFragment<LibSql>,
{
    fn column_names(&self, mut out: AstPass<'_, '_, LibSql>) -> QueryResult<()> {
        if let Self::Expression(..) = *self {
            out.push_identifier(Col::NAME)?;
        }
        Ok(())
    }
}

impl<Col, Expr> QueryFragment<LibSql, DoesNotSupportDefaultKeyword>
    for DefaultableColumnInsertValue<ColumnInsertValue<Col, Expr>>
where
    Expr: QueryFragment<LibSql>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, LibSql>) -> QueryResult<()> {
        if let Self::Expression(ref inner) = *self {
            inner.walk_ast(out.reborrow())?;
        }
        Ok(())
    }
}

/// Renders the column list a row would bind, e.g. `` `title`, `label` ``.
struct ColumnNames<'a, V>(&'a V);

impl<V, Tab> QueryFragment<LibSql> for ColumnNames<'_, ValuesClause<V, Tab>>
where
    Tab: Table,
    V: InsertValues<LibSql, Tab>,
{
    fn walk_ast<'b>(&'b self, out: AstPass<'_, 'b, LibSql>) -> QueryResult<()> {
        self.0.values.column_names(out)
    }
}

fn column_signature<V, Tab>(row: &ValuesClause<V, Tab>) -> QueryResult<String>
where
    Tab: Table,
    V: InsertValues<LibSql, Tab>,
{
    let mut builder = LibSqlQueryBuilder::default();
    ColumnNames(row).to_sql(&mut builder, &LibSql)?;
    Ok(builder.finish())
}

/// `<op> INTO <table> (<cols>) VALUES (<row 1>), (<row 2>), ...` for rows sharing one column list.
struct LibSqlMultiRowInsert<'a, T: QuerySource, V, Op> {
    into_clause: T::FromClause,
    operator: &'a Op,
    rows: &'a [ValuesClause<V, T>],
}

impl<T, V, Op> QueryId for LibSqlMultiRowInsert<'_, T, V, Op>
where
    T: QuerySource,
{
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<T, V, Op> QueryFragment<LibSql> for LibSqlMultiRowInsert<'_, T, V, Op>
where
    T: Table,
    T::FromClause: QueryFragment<LibSql>,
    V: InsertValues<LibSql, T>,
    Op: QueryFragment<LibSql>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, LibSql>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();
        self.operator.walk_ast(out.reborrow())?;
        out.push_sql(" INTO ");
        self.into_clause.walk_ast(out.reborrow())?;
        out.push_sql(" ");
        let mut rows = self.rows.iter();
        if let Some(first) = rows.next() {
            first.walk_ast(out.reborrow())?;
        }
        for row in rows {
            out.push_sql(", (");
            row.values.walk_ast(out.reborrow())?;
            out.push_sql(")");
        }
        Ok(())
    }
}

/// `<op> INTO <table> (<cols>) VALUES (<row>)` (or `DEFAULT VALUES`) for a single row.
struct LibSqlSingleInsert<'a, T: QuerySource, V, Op> {
    into_clause: T::FromClause,
    operator: &'a Op,
    row: &'a ValuesClause<V, T>,
}

impl<T, V, Op> QueryId for LibSqlSingleInsert<'_, T, V, Op>
where
    T: QuerySource,
{
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<T, V, Op> QueryFragment<LibSql> for LibSqlSingleInsert<'_, T, V, Op>
where
    T: Table,
    T::FromClause: QueryFragment<LibSql>,
    V: InsertValues<LibSql, T>,
    Op: QueryFragment<LibSql>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, LibSql>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();
        self.operator.walk_ast(out.reborrow())?;
        out.push_sql(" INTO ");
        self.into_clause.walk_ast(out.reborrow())?;
        out.push_sql(" ");
        self.row.walk_ast(out.reborrow())
    }
}

impl<V, T, QId, Op, const STATIC_QUERY_ID: bool> ExecuteDsl<LibSqlConnection, LibSql>
    for InsertStatement<T, BatchInsert<Vec<ValuesClause<V, T>>, T, QId, STATIC_QUERY_ID>, Op>
where
    T: Table + HasTable<Table = T> + Copy + QueryId + 'static,
    T::FromClause: QueryFragment<LibSql>,
    V: InsertValues<LibSql, T>,
    Op: Copy + QueryId + QueryFragment<LibSql>,
{
    fn execute(query: Self, conn: &mut LibSqlConnection) -> QueryResult<usize> {
        let rows = &query.records.values;
        if rows.is_empty() {
            return Ok(0);
        }

        let first = column_signature(&rows[0])?;
        let mut uniform = !first.is_empty();
        for row in &rows[1..] {
            if !uniform {
                break;
            }
            uniform = column_signature(row)? == first;
        }

        if uniform || rows.len() == 1 {
            let statement = LibSqlMultiRowInsert {
                into_clause: query.target.from_clause(),
                operator: &query.operator,
                rows,
            };
            return conn.execute_returning_count(&statement);
        }

        conn.transaction(|conn| {
            let mut inserted = 0;
            for row in rows {
                let statement = LibSqlSingleInsert {
                    into_clause: query.target.from_clause(),
                    operator: &query.operator,
                    row,
                };
                inserted += conn.execute_returning_count(&statement)?;
            }
            Ok(inserted)
        })
    }
}
