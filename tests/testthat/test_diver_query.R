# ===================================================================== #
#  An R package by Certe:                                               #
#  https://github.com/certe-medical-epidemiology                        #
#                                                                       #
#  Licensed as GPL-v2.0.                                                #
#                                                                       #
#  Developed at non-profit organisation Certe Medical Diagnostics &     #
#  Advice, department of Medical Epidemiology.                          #
#                                                                       #
#  This R package is free software; you can freely use and distribute   #
#  it for both personal and commercial purposes under the terms of the  #
#  GNU General Public License version 2.0 (GNU GPL-2), as published by  #
#  the Free Software Foundation.                                        #
#                                                                       #
#  We created this package for both routine data analysis and academic  #
#  research and it was publicly released in the hope that it will be    #
#  useful, but it comes WITHOUT ANY WARRANTY OR LIABILITY.              #
# ===================================================================== #

# Regression tests for get_diver_data() with dbplyr >= 2.6.0, which renders
# subqueries as `SELECT *\nFROM (...) AS "q01"`. The base queries contain ODBC
# date/timestamp literals such as {d '2026-01-01'}, which cli cannot parse if
# they end up in a cli-formatted error message.

diver_base_queries <- c(
  range = "SELECT * FROM data WHERE OntvangstDatum BETWEEN {d '2026-01-01'} AND {d '2026-01-31'}",
  single = "SELECT * FROM data WHERE OntvangstDatum = {d '2026-01-01'}",
  timestamp = "SELECT * FROM data WHERE OntvangstDatum BETWEEN {ts '2026-01-01 00:00:00'} AND {ts '2026-01-01 23:59:59'}")

mock_diver_tbl <- function(base) {
  # lazy table on a simulated ODBC connection, built like `tbl(conn, sql(base))`
  lf <- dbplyr::lazy_frame(IsolaatTestCode = "x", OntvangstDatum = Sys.Date(),
                           con = dbplyr::simulate_odbc())
  lf$lazy_query <- dbplyr:::lazy_query_remote(dbplyr::sql(base), vars = colnames(lf))
  lf
}

test_that("unify_select_clauses() handles old and new dbplyr subquery aliases", {
  expected <- "SELECT * FROM data WHERE (OntvangstDatum = {d '2026-01-01'}) AND (\"Zorglijn\" = '2e lijn')"
  # dbplyr < 2.6.0
  old <- "SELECT \"q01\".*\nFROM (SELECT * FROM data WHERE OntvangstDatum = {d '2026-01-01'}) \"q01\"\nWHERE (\"Zorglijn\" = '2e lijn')"
  # dbplyr >= 2.6.0
  new <- "SELECT *\nFROM (SELECT * FROM data WHERE OntvangstDatum = {d '2026-01-01'}) AS \"q01\"\nWHERE (\"Zorglijn\" = '2e lijn')"
  expect_identical(unify_select_clauses(old, "data"), expected)
  expect_identical(unify_select_clauses(new, "data"), expected)
  # untouched when there is no subquery
  expect_identical(unify_select_clauses("SELECT * FROM data", "data"), "SELECT * FROM data")
  expect_identical(strip_subquery_alias("SELECT \"q01\".*\nFROM (SELECT 1) AS \"q01\""), "SELECT *\nFROM (SELECT 1)")
})

test_that("get_diver_data() query steps produce valid SQL with ODBC literals", {
  skip_if_not(exists("lazy_query_remote", envir = asNamespace("dbplyr"), inherits = FALSE))
  for (nm in names(diver_base_queries)) {
    base <- diver_base_queries[[nm]]
    where <- quote(IsolaatTestCode %like% "^RC_VITEK")
    # same steps as in get_diver_data()
    out <- mock_diver_tbl(base) |> dplyr::filter(!!where)
    out$lazy_query$where <- where_convert_like(out$lazy_query$where)
    new_qry <- unify_select_clauses(dbplyr::remote_query(out), "data")
    expect_false(grepl("q[0-9]+", new_qry), info = nm)
    expect_false(grepl("\\bAS\\b", new_qry), info = nm)
    expect_identical(new_qry,
                     paste0(sub("WHERE ", "WHERE (", base, fixed = TRUE),
                            ") AND (EVAL('regexp(value(\"IsolaatTestCode\"), \"^RC_VITEK\", true)'))"),
                     info = nm)
  }
})

test_that("database errors on SQL with ODBC literals are not masked by cli", {
  skip_if_not_installed("RSQLite")
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))
  DBI::dbWriteTable(con, "data", data.frame(OntvangstDatum = 1))
  # SQLite rejects the ODBC literal, just like a server would reject invalid SQL
  bad <- "SELECT * FROM data WHERE (OntvangstDatum = {d '2026-01-01'}) AS AND (x = 1)"
  # with `vars`, as in get_diver_data(), no field query is run when creating the table
  out <- dplyr::tbl(con, dbplyr::sql(bad), vars = "OntvangstDatum")
  e <- tryCatch(dplyr::collect(out), error = function(e) e)
  expect_s3_class(e, "error")
  msg <- safe_cnd_message(e)
  expect_false(grepl("Could not parse cli", msg, fixed = TRUE))
  expect_true(grepl("syntax error|unrecognized token", msg))
})

test_that("safe_cnd_message() never fails", {
  e <- simpleError("plain {d '2026-01-01'} message")
  expect_identical(safe_cnd_message(e), "plain {d '2026-01-01'} message")
  # a condition whose formatting fails
  bad <- structure(class = c("certedb_bad_condition", "error", "condition"),
                   list(message = "raw {d '2026-01-01'}", call = NULL))
  local({
    conditionMessage.certedb_bad_condition <- function(c) stop("formatting failed")
    registerS3method("conditionMessage", "certedb_bad_condition", conditionMessage.certedb_bad_condition)
    expect_identical(safe_cnd_message(bad), "raw {d '2026-01-01'}")
  })
})
