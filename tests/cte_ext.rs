//! Recursive CTEs through `diesel-cte-ext` (feature `cte-ext`).
#![cfg(feature = "cte-ext")]

use diesel::dsl::sql;
use diesel::prelude::*;
use diesel::sql_types::Integer;
use diesel_cte_ext::builders::with_recursive;
use diesel_cte_ext::RecursiveParts;
use diesel_libsql::{LibSql, LibSqlConnection};

#[test]
fn with_recursive_counts_on_libsql() {
    let mut conn = LibSqlConnection::establish(":memory:").expect("in-memory libSQL database");
    let query = with_recursive::<LibSql, (), _, _, _, _>(
        "n",
        &["x"],
        RecursiveParts::new(
            sql::<Integer>("SELECT 1"),
            sql::<Integer>("SELECT x + 1 FROM n WHERE x < 5"),
            sql::<Integer>("SELECT x FROM n ORDER BY x"),
        ),
    );
    let values: Vec<i32> = query.load(&mut conn).expect("recursive CTE");
    assert_eq!(values, vec![1, 2, 3, 4, 5]);
}
