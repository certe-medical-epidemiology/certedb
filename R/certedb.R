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

#' @importFrom dplyr `%>%` filter
#' @importFrom dbplyr tbl_lazy
build_ <- function(select) {
  
}

#' Connect to Certe Database
#'
#' Connect to a remote database using [DBI::dbConnect()].
#' @param dbname database name
#' @param host,port,username,password credentials
#' @param driver database driver to use
#' @param info Default is \code{FALSE}. Display of connecting info.
#' @rdname db_connect
#' @importFrom certestyle font_red font_green
#' @export
db_connect <- function(dbname = "certemmb",
                       host = NULL,
                       port = NULL,
                       username = NULL,
                       password = NULL,
                       driver = get_driver(dbname = dbname),
                       info = TRUE) {
  
  if (is.null(host)) {
    host <- suppressWarnings(read_secret(paste0("db.", dbname, ".host")))
  }
  if (is.null(port)) {
    port <- suppressWarnings(read_secret(paste0("db.", dbname, ".port")))
  }
  if (is.null(username)) {
    username <- suppressWarnings(read_secret(paste0("db.", dbname, ".username")))
  }
  if (is.null(password)) {
    password <- suppressWarnings(read_secret(paste0("db.", dbname, ".password")))
  }
  
  if (host == "") {
    host <- "localhost"
  }
  if (port == "") {
    port <- 3306
  } else if (port %unlike% "^[0-9]+$") {
    stop("`port` must be a numeric value.")
    port <- as.integer(port)
  }
  
  db_message("Connecting to database '", dbname, "'...", new_line = FALSE)
  tryCatch(
    con <- DBI::dbConnect(driver,
                          dbname = dbname,
                          host = host,
                          port = port,
                          username = username,
                          password = password),
    error = function(e) {
      db_message(font_red("ERROR"), type = NULL)
      stop(e$message, call. = FALSE)
    })
  db_message(font_green("OK"), type = NULL)
  con
}

#' @rdname db_connect
#' @param conn connection to close, such as the output of [db_connect()]
#' @param ... arguments passed on to [DBI::dbDisconnect()]
#' @export
db_close <- function(conn, ...) {
  DBI::dbDisconnect(conn = conn, ...)
}

#' @rdname db_connect
#' @format NULL
#' @details `db` is a [list] with all the database tables and columns.
#' @export
"db"
