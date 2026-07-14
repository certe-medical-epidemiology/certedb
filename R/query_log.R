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

#' Read and Write the Query Log
#'
#' The [read_query_log()] function reads the query log from a SQLite database. The [write_query_log()] function writes a query log entry.
#' @param log_file path to the SQLite query log file. Defaults to the path set in `read_secret("db.query_log")`.
#' @param user user name that ran the query
#' @param source_type type of source, e.g. `"Diver"` or `"MySQL"`
#' @param source_dsn the DSN used to connect
#' @param source_project the project used to connect
#' @param source_cbase the cBase used to connect (if applicable)
#' @param query the SQL query that was run
#' @param rows number of rows returned
#' @param columns number of columns returned
#' @param duration_secs duration of the query in seconds
#' @param print a [logical] to indicate whether info should be printed
#' @importFrom DBI dbConnect dbDisconnect dbExecute dbAppendTable dbGetQuery
#' @rdname query_log
#' @export
read_query_log <- function(log_file = read_secret("db.query_log")) {
  if (is.null(log_file) || identical(log_file, "")) {
    stop("No log file path configured. Set 'db.query_log' or provide a path.", call. = FALSE)
  }
  if (!file.exists(log_file)) {
    message("Log file does not exist yet: ", log_file)
    return(invisible(data.frame()))
  }
  conn <- dbConnect(RSQLite::SQLite(), log_file)
  on.exit(dbDisconnect(conn))
  if (!"query_log" %in% DBI::dbListTables(conn)) {
    message("No query log entries found.")
    return(invisible(data.frame()))
  }
  out <- dbGetQuery(conn, "SELECT * FROM query_log ORDER BY datetime DESC")
  out$datetime <- as.POSIXct(out$datetime, format = "%Y-%m-%d %H:%M:%S", tz = "")
  out$interactive <- as.logical(out$interactive)
  dplyr::as_tibble(out)
}

#' @rdname query_log
write_query_log <- function(log_file,
                            user,
                            source_type,
                            source_dsn,
                            source_project,
                            source_cbase = NA_character_,
                            query,
                            rows,
                            columns,
                            duration_secs = NA_real_,
                            print = TRUE) {
  if (is.null(suppressMessages(suppressWarnings(log_file))) || identical(suppressMessages(suppressWarnings(log_file)), "")) {
    return(FALSE)
  }
  tryCatch(
    {
      conn <- dbConnect(RSQLite::SQLite(), log_file)
      on.exit(dbDisconnect(conn))
      dbExecute(conn, "PRAGMA journal_mode=WAL")
      dbExecute(conn, "PRAGMA busy_timeout=5000")
      dbExecute(conn, paste0(
        "CREATE TABLE IF NOT EXISTS query_log (",
        "  datetime TEXT NOT NULL,",
        "  user TEXT,",
        "  interactive INTEGER,",
        "  source_type TEXT,",
        "  source_dsn TEXT,",
        "  source_project TEXT,",
        "  source_cbase TEXT,",
        "  query TEXT,",
        "  rows INTEGER,",
        "  columns INTEGER,",
        "  duration_secs REAL",
        ")"
      ))
      df <- data.frame(
        datetime = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        user = as.character(user),
        interactive = as.integer(interactive()),
        source_type = as.character(source_type),
        source_dsn = as.character(source_dsn),
        source_project = as.character(source_project),
        source_cbase = as.character(source_cbase),
        query = as.character(query),
        rows = as.integer(rows),
        columns = as.integer(columns),
        duration_secs = as.double(duration_secs),
        stringsAsFactors = FALSE
      )
      dbAppendTable(conn, "query_log", df)
      TRUE
    },
    error = function(e) {
      msg("Could not write to log file: ", conditionMessage(e), print = print)
      FALSE
    }
  )
}
