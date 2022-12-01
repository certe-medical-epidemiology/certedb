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
#' Connect to an internal (yet remote) Certe database server using [DBI::dbConnect()].
#' @param driver database driver to use, such as [odbc::odbc()]
#' @param ... database credentials
#' @param info a logical to indicate whether info about the connection should be printed
#' @rdname db_connect
#' @importFrom certestyle font_red font_green font_blue
#' @importFrom DBI dbConnect
#' @export
db_connect <- function(driver,
                       ...,
                       info = TRUE) {
  dots <- list(...)
  # remove empty values
  dots <- dots[!vapply(FUN.VALUE = logical(1), dots, is_empty)]
  
  datasource <- ""
  if ("host" %in% names(dots)) {
    datasource <- paste0(" to host ", font_blue(dots$host))
  }
  if ("cbase" %in% names(dots)) {
    if ("project" %in% names(dots)) {
      datasource <- paste0(" to cbase ", font_blue(paste0("\"", dots$cbase, "\" [", dots$project, "]")))
    } else {
      datasource <- paste0(" to cbase ", font_blue(paste0("\"", dots$cbase, "\"")))
    }
    if ("dsn" %in% names(dots)) {
      datasource <- paste0(datasource, font_black(paste0(" (", dots$dsn, ")")))
    }
  }
  
  msg_init("Opening connection", datasource, "...")
  tryCatch({
    if (inherits(driver, "DBIConnection")) {
      con <- driver
    } else {
      con <- do.call(dbConnect, args = c(list(driver), dots))
    }},
    error = function(e) {
      msg_error(time = FALSE)
      stop(e$message, call. = FALSE)
    })
  msg_ok(time = FALSE)
  invisible(con)
}

#' @rdname db_connect
#' @param conn connection to close, such as the output of [db_connect()]
#' @param ... arguments passed on to [DBI::dbDisconnect()]
#' @importFrom certestyle font_red font_green
#' @importFrom DBI dbDisconnect
#' @export
db_close <- function(conn, ...) {
  db_message("Closing connection...", new_line = FALSE)
  tryCatch({
    dbDisconnect(conn, ...)
    db_message(font_green("OK"), type = NULL)
  }, error = function(e) {
    db_message(font_red("ERROR\n"), type = NULL)
    stop(e$message, call. = FALSE)
  },
  warning = function(e) {
    db_message(font_red("WARNING\n"), type = NULL)
    warning(e$message, call. = FALSE)
  })
  invisible(TRUE)
}
