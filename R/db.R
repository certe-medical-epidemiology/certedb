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

#' Connect to a Certe Database
#'
#' Connect to an internal (yet remote) Certe database server using [DBI::dbConnect()], or any other database.
#' @param driver database driver to use, such as [odbc::odbc()] and [duckdb::duckdb()]
#' @param ... database credentials
#' @param print a logical to indicate whether info about the connection should be printed
#' @rdname db_connect
#' @importFrom certestyle font_red font_green font_blue
#' @importFrom duckdb duckdb
#' @importFrom DBI dbConnect
#' @export
#' @examples
#' # create a local duckdb database
#' db <- db_connect(duckdb::duckdb(), "~/my_duck.db")
#' db
#' 
#' db |> db_write_table("my_iris_table", values = iris)
#' db |> db_list_tables()
#' db |> db_has_table("my_iris_table")
#' 
#' if (require(dplyr, warn.conflicts = FALSE)) {
#'   db |> 
#'     tbl("my_iris_table") |> 
#'     filter(Species == "setosa", Sepal.Width > 3) |> 
#'     collect() |> 
#'     as_tibble()
#' }
#' 
#' db |> db_drop_table("my_iris_table")
#' db |> db_list_tables()
#' 
#' db |> db_close()
#' 
#' # remove the database
#' unlink("~/my_duck.db")
db_connect <- function(driver,
                       ...,
                       print = TRUE) {
  dots <- list(...)
  # remove empty values
  dots <- dots[!vapply(FUN.VALUE = logical(1), dots, is_empty)]
  
  datasource <- ""
  if ("host" %in% names(dots)) {
    datasource <- paste0(" to host ", font_blue(dots$host))
  }
  if ("cbase" %in% names(dots)) {
    if ("project" %in% names(dots)) {
      datasource <- paste0(" to cBase ", font_blue(paste0("\"", dots$cbase, "\" [", dots$project, "]")))
    } else {
      datasource <- paste0(" to cBase ", font_blue(paste0("\"", dots$cbase, "\"")))
    }
    if ("dsn" %in% names(dots)) {
      datasource <- paste0(datasource, font_black(paste0(" (", dots$dsn, ")")))
    }
  }
  
  msg_init("Opening connection", datasource, "...", print = print)
  tryCatch({
    if (inherits(driver, "DBIConnection")) {
      con <- driver
    } else {
      con <- do.call(dbConnect, args = c(list(driver), dots))
    }},
    error = function(e) {
      msg_error(time = FALSE, print = print)
      stop(e$message, call. = FALSE)
    })
  msg_ok(time = FALSE, print = print)
  invisible(con)
}

#' @rdname db_connect
#' @param conn connection to close, such as the output of [db_connect()]
#' @param ... arguments passed on to [DBI::dbDisconnect()]
#' @importFrom certestyle font_red font_green
#' @importFrom DBI dbDisconnect
#' @export
db_close <- function(conn, ..., print = TRUE) {
  db_message("Closing connection...", new_line = FALSE, print = print)
  dots <- list(...)
  if (inherits(conn, "duckdb_connection") && !"shutdown" %in% names(dots)) {
    # for duckdb, `shutdown = TRUE` closes the connection in the right way
    dots <- c(dots, list(shutdown = TRUE))
  }
  tryCatch({
    do.call(dbDisconnect, c(conn, dots))
    db_message(font_green("OK"), type = NULL, print = print)
  }, error = function(e) {
    db_message(font_red("ERROR\n"), type = NULL, print = print)
    stop(e$message, call. = FALSE)
  },
  warning = function(e) {
    db_message(font_red("WARNING\n"), type = NULL, print = print)
    warning(e$message, call. = FALSE)
  })
  invisible(TRUE)
}

# methods for duckdb ----

#' @method db_list_tables duckdb_connection
#' @importFrom DBI dbListTables
#' @noRd
#' @export
db_list_tables.duckdb_connection <- function(con) {
  dbListTables(con)
}

#' @method copy_to duckdb_connection
#' @importFrom dplyr copy_to
#' @importFrom dbplyr src_dbi
#' @noRd
#' @export
copy_to.duckdb_connection <- function(dest, df, name = deparse(substitute(df)), overwrite = FALSE, ...) {
  copy_to(src_dbi(dest, auto_disconnect = FALSE), df = df, 
          name = name, overwrite = overwrite, ...)
}

#' @method db_write_table duckdb_connection
#' @importFrom DBI dbWriteTable
#' @noRd
#' @export
db_write_table.duckdb_connection <- function(con, table, types, values, temporary = FALSE, append = FALSE, ...) {
  dbWriteTable(conn = con, name = table, value = values, temporary = temporary, append = append, ...)
}

#' @method db_drop_table duckdb_connection
#' @importFrom DBI dbRemoveTable
#' @noRd
#' @export
db_drop_table.duckdb_connection <- function(con, table, force = FALSE, ...) {
  dbRemoveTable(conn = con, name = table, ...)
}

#' @method db_has_table duckdb_connection
#' @noRd
#' @export
db_has_table.duckdb_connection <- function(con, table) {
  table %in% db_list_tables(con)
}
