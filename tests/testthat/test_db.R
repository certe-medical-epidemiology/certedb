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

test_that("utils work", {
  expect_true(is_empty(""))
  expect_true(is_empty(NA))
  expect_true(is_empty(FALSE))
  expect_true(is_empty(NULL))
  expect_true(is_empty(""))
  
  expect_message(db_message("test", print = TRUE))
  expect_message(db_warning("test", print = TRUE))
  
  expect_message(msg_init("test", print = TRUE))
  expect_message(msg_ok(print = TRUE))
  expect_message(msg_init("test", print = TRUE))
  expect_message(msg_ok(dimensions = c(NA, 6), print = TRUE))
  expect_message(msg_init("test", print = TRUE))
  expect_message(msg_error(print = TRUE))
  
  out <- as_certedb_tibble(iris,
                           source = "Source",
                           qry = "SELECT * FROM test",
                           datetime = Sys.time(),
                           user = "User",
                           type = "Test DB")
  expect_output(print(out))
  expect_output(print(certedb_query(out)))
})

test_that("db works", {
  # create a local duckdb database
  db <- db_connect(duckdb::duckdb(), "my_duck.db")
  db |> db_write_table("my_iris_table", values = iris, overwrite = TRUE)
  expect_true("my_iris_table" %in% db_list_tables(db))
  expect_true(db_has_table(db, "my_iris_table"))
  db |> db_close()
  
  df <- get_duckdb_data(NULL, Species == "setosa", duckdb_path = "my_duck.db", duckdb_table = "my_iris_table")
  expect_output(print(df))
  expect_output(print(certedb_query(df)))
  
  db <- db_connect(duckdb::duckdb(), "my_duck.db")
  db |> db_drop_table("my_iris_table")
  expect_false("my_iris_table" %in% db_list_tables(db))
  db |> db_close()
  
  # remove the database
  unlink("my_duck.db")
  
})

test_that("utils work", {
  expect_s3_class(db_connect(dbplyr::simulate_sqlite()), "DBIConnection")
  expect_s3_class(db_connect(dbplyr::simulate_sqlite(), host = "hostname"), "DBIConnection")
  expect_s3_class(db_connect(dbplyr::simulate_sqlite(), cbase = "cbasename"), "DBIConnection")
  expect_s3_class(db_connect(dbplyr::simulate_sqlite(), cbase = "cbasename", project = "projectname"), "DBIConnection")
  expect_error(db_close(db_connect(dbplyr::simulate_sqlite())))
})
