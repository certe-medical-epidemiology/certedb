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
})

test_that("utils work", {
  expect_s3_class(db_connect(dbplyr::simulate_sqlite()), "DBIConnection")
  expect_s3_class(db_connect(dbplyr::simulate_sqlite(), host = "hostname"), "DBIConnection")
  expect_s3_class(db_connect(dbplyr::simulate_sqlite(), cbase = "cbasename"), "DBIConnection")
  expect_s3_class(db_connect(dbplyr::simulate_sqlite(), cbase = "cbasename", project = "projectname"), "DBIConnection")
  expect_error(db_close(db_connect(dbplyr::simulate_sqlite())))
})