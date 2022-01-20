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

#' @importFrom dbplyr sql
#' @importFrom dplyr tbl collect
query <- function(...,
                  select_set = "mmb2",
                  add_cols = c(),
                  connection = db_connect(),
                  collect = TRUE,
                  limit = Inf) {
  
  where <- build_where(...)
  if (where == "") {
    stop("No date filter set")
  }
  
  conn <- connection # this will initiate it if it's a call to a function
  
  preset_path <- paste0(read_secret("path.refmap"), "/preset_", gsub("preset_", "", select_set), ".sql")
  if (file.exists(preset_path)) {
    select <- c(readLines(preset_path), add_cols)
  } else {
    select <- add_cols
  }
  from <- build_from(select)
  select <- build_select(select)
  
  qry <- sql(paste(select, from, where, collapse = " "))
  
  msg_init("Running query...")
  tryCatch(out <- head(tbl(conn, qry), n = limit),
           error = function(e) {
             msg_error()
             db_close(conn)
             stop(e$message, call. = FALSE)
           })
  msg_ok()
  
  if (isTRUE(collect)) {
    msg_init("Collecting data...")
    tryCatch({
      out <- collect(out)
    },
    error = function(e) {
      msg_error()
      db_close(conn)
      stop(e$message, call. = FALSE)
    })
    msg_ok()
    db_close(conn)
  }
  
  out
}

build_select <- function(...) {
  paste("SELECT ", paste(c(...), collapse = ", "))
}

#' @importFrom dbplyr translate_sql_ simulate_dbi
#' @importFrom rlang exprs
build_where <- function(...) {
  expr <- exprs(...)
  where_statements <- translate_sql_(expr, con = simulate_dbi())
  where_statements <- gsub("^`db`[.]", "", where_statements)
  # backticks don't seem to work in WHERE statements
  where_statements <- gsub("`", "", where_statements)
  if (length(where_statements) > 0) {
    paste(" WHERE", paste("(", where_statements[which(nchar(where_statements) > 3)], ")", collapse = " AND "))
  } else {
    ""
  }
}

build_from <- function(...) {
  cols_tables <- gsub("[.].*$", "", c(...))
  from_statements <- readLines(read_secret("db.from_statements_path"))
  from_which <- unique(c(1, 2, which(from_statements %like% paste0(" ", cols_tables, " ", collapse = "|"))))
  paste(from_statements[from_which], collapse = " ")
}

#' Connect to a Certe Database
#'
#' Connect to an internal (yet remote) Certe database server using [DBI::dbConnect()].
#' @param dbname database name
#' @param credentials database credentials as a [list] with named items, such as `credentials = list(host = "0.0.0.0", port = 3306)`
#' @param driver database driver to use
#' @param info a logical to indicate whether info about the connection should be printed
#' @details If no credentials are set, they will be retrieved using [`read_secret()`][certetoolbox::read_secret()].
#' @rdname db_connect
#' @importFrom certestyle font_red font_green font_blue
#' @export
db_connect <- function(dbname = "certemmb",
                       credentials = list(),
                       driver = get_driver(dbname = dbname),
                       info = TRUE) {
  
  if (length(credentials) == 0) {
    # retrieve secrets
    secrets <- read_secret(paste0("db.", dbname))
    credentials <- stats::setNames(lapply(secrets, function(l) l[[1]]),
                                   vapply(FUN.VALUE = character(1), secrets, function(l) names(l)))
  }
  
  if (is.null(credentials$host) || credentials$host == "") {
    host <- "localhost"
  }
  if (is.null(credentials$port) || credentials$port == "") {
    port <- 3306
  } else {
    port <- as.integer(credentials$port)
  }
  
  msg_init("Connecting to database '", font_blue(dbname), "'...")
  tryCatch(
    con <- do.call(DBI::dbConnect,
            args = c(list(driver, dbname),
                     credentials)),
    error = function(e) {
      msg_error(time = FALSE)
      stop(e$message, call. = FALSE)
    })
  msg_ok(time = FALSE)
  con
}

#' @rdname db_connect
#' @param conn connection to close, such as the output of [db_connect()]
#' @param ... arguments passed on to [DBI::dbDisconnect()]
#' @export
db_close <- function(conn, ...) {
  db_message("Closing connection...", new_line = FALSE)
  tryCatch(
    DBI::dbDisconnect(conn, ...),
    error = function(e) {
      db_message(font_red("ERROR\n"), type = NULL)
      stop(e$message, call. = FALSE)
    })
  db_message(font_green("OK"), type = NULL)
  invisible(TRUE)
}

#' @rdname db_connect
#' @format NULL
#' @details `db` is a [list] with all the database tables and columns.
#' @export
"db"
