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

#' @rdname get_diver_data
#' @inheritParams get_diver_data
#' @param duckdb_path path to the local DuckDB database
#' @param duckdb_table name of the DuckDB database to retrieve data from
#' @export
#' @examples
#' 
#' 
#' # USING DUCKDB DATABASE ------------------------------------------------
#' 
#' # create a local duckdb database and write a table
#' db <- db_connect(duckdb::duckdb(), "~/my_duck.db")
#' db |> db_write_table("iris", values = iris)
#' db |> db_close()
#' 
#' df <- get_duckdb_data(date_range = NULL,
#'                       where = Species == "setosa" & Sepal.Width > 3,
#'                       # not needed in production environment:
#'                       duckdb_path = "~/my_duck.db",
#'                       duckdb_table = "iris",
#'                       review_qry = FALSE,
#'                       info = TRUE)
#' df
#' certedb_query(df)
#' 
#' # remove the database
#' unlink("~/my_duck.db")
get_duckdb_data <- function(date_range = this_year(),
                            where = NULL,
                            preset = NULL,
                            review_qry = interactive(),
                            duckdb_path = read_secret("db.duckdb_path"),
                            duckdb_table = read_secret("db.duckdb_table"),
                            auto_transform = TRUE,
                            info = interactive()) {
  
  if (is_empty(duckdb_path)) {
    stop("Duckdb path is empty")
  }
  if (is_empty(duckdb_table)) {
    stop("Duckdb table is empty")
  }
  if (is_empty(preset)) {
    preset <- NULL
  }
  conn <- db_connect(driver = duckdb::duckdb(),
                     dbdir = duckdb_path,
                     read_only = TRUE,
                     print = info)
  on.exit(suppressWarnings(suppressMessages(try(db_close(conn, print = info), silent = TRUE))))
  user <- paste0("CERTE\\", Sys.info()["user"])
  
  msg_init("Retrieving initial cBase...", print = info)
  if (!is.null(date_range)) {
    if (length(date_range) == 1) {
      date_range <- rep(date_range, 2)
    } else {
      # always earliest first, newest second
      date_range <- c(min(date_range, na.rm = TRUE), max(date_range, na.rm = TRUE))
    }
    if (all(date_range %in% 2000:2050, na.rm = TRUE)) {
      date_range[1] <- paste0(date_range[1], "-01-01")
      date_range[2] <- paste0(date_range[2], "-12-31")
    }
    date_range <- tryCatch(as.Date(date_range),
                           error = function(e) as.Date(date_range, origin = "1970-01-01"))
    if (length(unique(date_range)) > 1) {
      out <- conn |>
        # from https://www.dimins.com/online-help/workbench_help/Content/ODBC/di-odbc-sql-reference.html
        tbl(sql(paste0("SELECT * FROM ", duckdb_table, " WHERE ontvangstdatum BETWEEN '",
                       format2(date_range[1], "yyyy-mm-dd"), "' AND '",
                       format2(date_range[2], "yyyy-mm-dd"), "'")))
    } else {
      out <- conn |>
        tbl(sql(paste0("SELECT * FROM ", duckdb_table, " WHERE ontvangstdatum = '", format2(date_range[1], "yyyy-mm-dd"), "'")))
    }
  } else {
    out <- conn |> tbl(duckdb_table)
  }
  # apply filters
  if (!is.null(substitute(where))) {
    out <- out |> filter({{ where }})
  }
  if (!is.null(preset) && !all(is.na(preset$filter))) {
    # apply filter from preset
    preset_filter <- str2lang(preset$filter)
    out <- out |> filter(preset_filter)
  }
  msg_ok(time = TRUE, dimensions = dim(out), print = info)
  qry <- remote_query(out)
  
  if (isTRUE(review_qry) && interactive() && is.null(pandoc_to())) {
    choice <- utils::menu(title = paste0("\nCollect data from this query? (0 for Cancel)\n\n", qry,
                                         ifelse(!is.null(preset) & !all(is.na(preset$filter)),
                                                font_grey(paste0("\n(last part from preset \"", preset$name, "\")")),
                                                "")),
                          choices = c("Yes", "No", "Print column names"),
                          graphics = FALSE)
    if (choice == 3) {
      df <- collect(out, n = 1)
      cols <- vapply(FUN.VALUE = character(1), df, type_sum)
      print(paste0(names(cols), " <", cols, ">"), quote = FALSE)
      choice <- utils::menu(title = paste0("\nCollect data from this query? (0 for Cancel)\n\n", qry),
                            choices = c("Yes", "No"),
                            graphics = FALSE)
    }
    if (choice != 1) {
      db_close(conn)
      return(invisible())
    }
  } else {
    wh <- strsplit(unclass(remote_query(out)), "[ \n]WHERE[ \n]")[[1]]
    wh <- paste0(trimws(gsub("\"q[0-9]+\"", "", wh[2:length(wh)])), collapse = " AND ")
    msg("Applying filter: ", wh)
  }
  
  msg_init("Collecting data...", print = info)
  tryCatch({
    out <- collect(out)
  },
  error = function(e) {
    msg_error()
    stop(e$message, call. = FALSE)
  })
  msg_ok(time = TRUE, dimensions = dim(out), print = info)
  
  db_close(conn, print = info)
  
  if (!is.null(preset) && !all(is.na(preset$select))) {
    msg_init(paste0("Selecting columns from preset ", font_blue(paste0('"', preset$name, '"')), font_black("...")))
    out <- out |> select(preset$select)
    msg_ok(dimensions = dim(out))
  }
  
  if (isTRUE(auto_transform)) {
    msg_init("Transforming data set...")
    # transform data, and update column names
    out <- auto_transform(out, snake_case = TRUE)
    msg_ok()
  }
  
  as_certedb_tibble(out,
                    type = "DuckDB",
                    source = paste0("\"", duckdb_path, "\""),
                    qry = qry,
                    datetime = Sys.time(),
                    user = user)
}
