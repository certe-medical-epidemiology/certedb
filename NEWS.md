# certedb 1.15.1

* Fixed `get_diver_data()` failing with *"Could not parse cli `{}` expression: `d '2026-01-01'`"* since dbplyr 2.6.0. dbplyr 2.6.0 renders subqueries as `FROM (...) AS "q01"` instead of `FROM (...) "q01"`, after which `unify_select_clauses()` produced invalid SQL (`... ) AS AND (...`). The resulting database error was then masked, because dbplyr passes the raw SQL with ODBC literals such as `{d '2026-01-01'}` to cli when a field query fails.
  * Subquery aliases are now removed including the `AS` keyword, for both old and new dbplyr versions.
  * `get_diver_data()` now supplies the column names when creating the lazy tables that contain ODBC date/timestamp literals, so dbplyr no longer runs a separate field query on them. This also saves a round trip to the server. Database errors on such queries now surface during collecting, with the actual error message.
  * The *"Applying filter"* message no longer contains a dangling `AS`, in `get_diver_data()`, `get_mysql_data()` and `get_duckdb_data()`.
* Error handlers in all `get_*_data()` functions, `db_connect()`, `db_close()` and the query log no longer fail if the message of the original error cannot be formatted.
