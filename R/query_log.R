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
#' The [read_query_log()] function reads the query log from a SQLite database. The [write_query_log()] function writes a query log entry. Join queries from presets are stored in a separate table and linked to their parent query; use [read_query_log()] to retrieve them together.
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
#' @param joins_only if `TRUE`, return only the joins table instead of the main query log
#' @importFrom DBI dbConnect dbDisconnect dbExecute dbAppendTable dbGetQuery
#' @rdname query_log
#' @export
read_query_log <- function(log_file = read_secret("db.query_log"),
                           joins_only = FALSE) {
  if (is.null(log_file) || identical(log_file, "")) {
    stop("No log file path configured. Set 'db.query_log' or provide a path.", call. = FALSE)
  }
  if (!file.exists(log_file)) {
    message("Log file does not exist yet: ", log_file)
    return(invisible(data.frame()))
  }
  conn <- dbConnect(RSQLite::SQLite(), log_file)
  on.exit(dbDisconnect(conn))
  tables <- DBI::dbListTables(conn)
  if (!"query_log" %in% tables) {
    message("No query log entries found.")
    return(invisible(data.frame()))
  }
  if (isTRUE(joins_only)) {
    if (!"query_log_joins" %in% tables) {
      message("No join log entries found.")
      return(invisible(data.frame()))
    }
    out <- dbGetQuery(conn, "SELECT * FROM query_log_joins ORDER BY query_log_id DESC")
    return(dplyr::as_tibble(out))
  }
  has_joins <- "query_log_joins" %in% tables
  out <- dbGetQuery(conn, "SELECT * FROM query_log ORDER BY datetime DESC")
  out$datetime <- as.POSIXct(out$datetime, format = "%Y-%m-%d %H:%M:%S", tz = "")
  out$interactive <- as.logical(out$interactive)
  if (has_joins) {
    joins <- dbGetQuery(conn, "SELECT * FROM query_log_joins ORDER BY query_log_id, id")
    n_joins <- stats::aggregate(id ~ query_log_id, data = joins, FUN = length)
    colnames(n_joins) <- c("id", "n_joins")
    out <- merge(out, n_joins, by = "id", all.x = TRUE)
    out$n_joins[is.na(out$n_joins)] <- 0L
    out <- out[order(out$datetime, decreasing = TRUE), ]
  } else {
    out$n_joins <- 0L
  }
  dplyr::as_tibble(out)
}

sanitise_query <- function(query) {
  query <- gsub(" +", " ", gsub("\n", " ", query, fixed = TRUE))
  query
}

log_db_connect <- function(log_file) {
  conn <- dbConnect(RSQLite::SQLite(), log_file)
  dbExecute(conn, "PRAGMA journal_mode=WAL")
  dbExecute(conn, "PRAGMA busy_timeout=5000")
  dbExecute(conn, paste0(
    "CREATE TABLE IF NOT EXISTS query_log (",
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,",
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
  dbExecute(conn, paste0(
    "CREATE TABLE IF NOT EXISTS query_log_joins (",
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,",
    "  query_log_id INTEGER NOT NULL REFERENCES query_log(id),",
    "  cbase TEXT,",
    "  join_type TEXT,",
    "  by_columns TEXT,",
    "  filter_values_n INTEGER,",
    "  rows INTEGER,",
    "  columns INTEGER,",
    "  duration_secs REAL",
    ")"
  ))
  conn
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
    return(invisible(FALSE))
  }
  tryCatch(
    {
      conn <- log_db_connect(log_file)
      on.exit(dbDisconnect(conn))
      query <- sanitise_query(as.character(query))
      df <- data.frame(
        datetime = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        user = as.character(user),
        interactive = as.integer(interactive()),
        source_type = as.character(source_type),
        source_dsn = as.character(source_dsn),
        source_project = as.character(source_project),
        source_cbase = as.character(source_cbase),
        query = query,
        rows = as.integer(rows),
        columns = as.integer(columns),
        duration_secs = as.double(duration_secs),
        stringsAsFactors = FALSE
      )
      dbAppendTable(conn, "query_log", df)
      log_id <- dbGetQuery(conn, "SELECT last_insert_rowid() AS id")$id
      pkg_env$last_log_id <- log_id
      TRUE
    },
    error = function(e) {
      msg("Could not write to log file: ", conditionMessage(e), print = print)
      FALSE
    }
  )
}

write_query_log_join <- function(log_file,
                                 query_log_id,
                                 cbase,
                                 join_type,
                                 by_columns,
                                 filter_values_n,
                                 rows,
                                 columns,
                                 duration_secs = NA_real_,
                                 print = TRUE) {
  if (is.null(suppressMessages(suppressWarnings(log_file))) || identical(suppressMessages(suppressWarnings(log_file)), "")) {
    return(invisible(FALSE))
  }
  tryCatch(
    {
      conn <- log_db_connect(log_file)
      on.exit(dbDisconnect(conn))
      df <- data.frame(
        query_log_id = as.integer(query_log_id),
        cbase = as.character(cbase),
        join_type = as.character(join_type),
        by_columns = paste(as.character(by_columns), collapse = ", "),
        filter_values_n = as.integer(filter_values_n),
        rows = as.integer(rows),
        columns = as.integer(columns),
        duration_secs = as.double(duration_secs),
        stringsAsFactors = FALSE
      )
      dbAppendTable(conn, "query_log_joins", df)
      TRUE
    },
    error = function(e) {
      msg("Could not write join to log file: ", conditionMessage(e), print = print)
      FALSE
    }
  )
}
