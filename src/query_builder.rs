//! QueryFragment implementations and HasSqlType for the LibSql backend.
//!
//! SQLite-specific `QueryFragment<Sqlite>` impls do NOT apply to `LibSql`
//! because it's a different backend type. We must provide matching impls here.

use diesel::query_builder::{
    AstPass, BoxedLimitOffsetClause, IntoBoxedClause, LimitClause, LimitOffsetClause,
    NoLimitClause, NoOffsetClause, OffsetClause, QueryFragment, ReturningClause,
};
use diesel::sql_types::*;
use diesel::sqlite::SqliteType;
use diesel::QueryResult;

use crate::backend::{LibSql, LibSqlReturningClause};

// ============================================================
// HasSqlType implementations
// ============================================================

impl HasSqlType<SmallInt> for LibSql {
    fn metadata(_: &mut ()) -> SqliteType {
        SqliteType::SmallInt
    }
}

impl HasSqlType<Integer> for LibSql {
    fn metadata(_: &mut ()) -> SqliteType {
        SqliteType::Integer
    }
}

impl HasSqlType<BigInt> for LibSql {
    fn metadata(_: &mut ()) -> SqliteType {
        SqliteType::Long
    }
}

impl HasSqlType<Float> for LibSql {
    fn metadata(_: &mut ()) -> SqliteType {
        SqliteType::Float
    }
}

impl HasSqlType<Double> for LibSql {
    fn metadata(_: &mut ()) -> SqliteType {
        SqliteType::Double
    }
}

impl HasSqlType<Text> for LibSql {
    fn metadata(_: &mut ()) -> SqliteType {
        SqliteType::Text
    }
}

impl HasSqlType<Binary> for LibSql {
    fn metadata(_: &mut ()) -> SqliteType {
        SqliteType::Binary
    }
}

impl HasSqlType<Bool> for LibSql {
    fn metadata(_: &mut ()) -> SqliteType {
        SqliteType::Integer
    }
}

impl HasSqlType<Date> for LibSql {
    fn metadata(_: &mut ()) -> SqliteType {
        SqliteType::Text
    }
}

impl HasSqlType<Time> for LibSql {
    fn metadata(_: &mut ()) -> SqliteType {
        SqliteType::Text
    }
}

impl HasSqlType<Timestamp> for LibSql {
    fn metadata(_: &mut ()) -> SqliteType {
        SqliteType::Text
    }
}

impl HasSqlType<Numeric> for LibSql {
    fn metadata(_: &mut ()) -> SqliteType {
        SqliteType::Double
    }
}

impl HasSqlType<TinyInt> for LibSql {
    fn metadata(_: &mut ()) -> SqliteType {
        SqliteType::SmallInt
    }
}

// ============================================================
// LimitOffset QueryFragment impls (mirrors sqlite)
// ============================================================

impl QueryFragment<LibSql> for LimitOffsetClause<NoLimitClause, NoOffsetClause> {
    fn walk_ast<'b>(&'b self, _out: AstPass<'_, 'b, LibSql>) -> QueryResult<()> {
        Ok(())
    }
}

impl<L> QueryFragment<LibSql> for LimitOffsetClause<LimitClause<L>, NoOffsetClause>
where
    LimitClause<L>: QueryFragment<LibSql>,
{
    fn walk_ast<'b>(&'b self, out: AstPass<'_, 'b, LibSql>) -> QueryResult<()> {
        self.limit_clause.walk_ast(out)?;
        Ok(())
    }
}

impl<O> QueryFragment<LibSql> for LimitOffsetClause<NoLimitClause, OffsetClause<O>>
where
    OffsetClause<O>: QueryFragment<LibSql>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, LibSql>) -> QueryResult<()> {
        // SQLite requires a LIMIT clause before any OFFSET clause
        // Using LIMIT -1 is the same as no limit
        out.push_sql(" LIMIT -1 ");
        self.offset_clause.walk_ast(out)?;
        Ok(())
    }
}

impl<L, O> QueryFragment<LibSql> for LimitOffsetClause<LimitClause<L>, OffsetClause<O>>
where
    LimitClause<L>: QueryFragment<LibSql>,
    OffsetClause<O>: QueryFragment<LibSql>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, LibSql>) -> QueryResult<()> {
        self.limit_clause.walk_ast(out.reborrow())?;
        self.offset_clause.walk_ast(out.reborrow())?;
        Ok(())
    }
}

impl QueryFragment<LibSql> for BoxedLimitOffsetClause<'_, LibSql> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, LibSql>) -> QueryResult<()> {
        match (self.limit.as_ref(), self.offset.as_ref()) {
            (Some(limit), Some(offset)) => {
                QueryFragment::<LibSql>::walk_ast(limit.as_ref(), out.reborrow())?;
                QueryFragment::<LibSql>::walk_ast(offset.as_ref(), out.reborrow())?;
            }
            (Some(limit), None) => {
                QueryFragment::<LibSql>::walk_ast(limit.as_ref(), out.reborrow())?;
            }
            (None, Some(offset)) => {
                out.push_sql(" LIMIT -1 ");
                QueryFragment::<LibSql>::walk_ast(offset.as_ref(), out.reborrow())?;
            }
            (None, None) => {}
        }
        Ok(())
    }
}

// ============================================================
// IntoBoxedClause impls for LimitOffset
// ============================================================

impl<'a> IntoBoxedClause<'a, LibSql> for LimitOffsetClause<NoLimitClause, NoOffsetClause> {
    type BoxedClause = BoxedLimitOffsetClause<'a, LibSql>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause {
            limit: None,
            offset: None,
        }
    }
}

impl<'a, L> IntoBoxedClause<'a, LibSql> for LimitOffsetClause<LimitClause<L>, NoOffsetClause>
where
    L: QueryFragment<LibSql> + Send + 'a,
{
    type BoxedClause = BoxedLimitOffsetClause<'a, LibSql>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause {
            limit: Some(Box::new(self.limit_clause)),
            offset: None,
        }
    }
}

impl<'a, O> IntoBoxedClause<'a, LibSql> for LimitOffsetClause<NoLimitClause, OffsetClause<O>>
where
    O: QueryFragment<LibSql> + Send + 'a,
{
    type BoxedClause = BoxedLimitOffsetClause<'a, LibSql>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause {
            limit: None,
            offset: Some(Box::new(self.offset_clause)),
        }
    }
}

impl<'a, L, O> IntoBoxedClause<'a, LibSql> for LimitOffsetClause<LimitClause<L>, OffsetClause<O>>
where
    L: QueryFragment<LibSql> + Send + 'a,
    O: QueryFragment<LibSql> + Send + 'a,
{
    type BoxedClause = BoxedLimitOffsetClause<'a, LibSql>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause {
            limit: Some(Box::new(self.limit_clause)),
            offset: Some(Box::new(self.offset_clause)),
        }
    }
}

// ============================================================
// Returning clause QueryFragment impl
// ============================================================

impl<Expr> QueryFragment<LibSql, LibSqlReturningClause> for ReturningClause<Expr>
where
    Expr: QueryFragment<LibSql>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, LibSql>) -> QueryResult<()> {
        out.skip_from(true);
        out.push_sql(" RETURNING ");
        self.0.walk_ast(out.reborrow())?;
        Ok(())
    }
}
