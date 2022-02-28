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

#' Download Data from Diver Server
#' 
#' @param date_range date range, can be length 1 or 2 to filter on the column `Ontvangstdatum`), or `NULL` to not filter on any date column. Defaults to current year.
#' @param where arguments to filter data on, will be passed on to [`filter()`][dplyr::filter()]
#' @param diver_project name of the Diver project
#' @param diver_cbase name of the Diver cbase, can be left blank to select using a popup window
#' @param review_qry a [logical] to indicate whether the query must be reviewed first, defaults to `TRUE` in interactive mode and `FALSE` otherwise
#' @importFrom dbplyr sql remote_query
#' @importFrom dplyr tbl `%>%` filter 
#' @importFrom certestyle format2
get_diver_data <- function(date_range = c(paste0(format(Sys.Date(), "%Y"), "-01-01"),
                                          paste0(format(Sys.Date(), "%Y"), "-12-31")),
                           where = NULL,
                           diver_project = read_secret("db.diver_project"),
                           diver_cbase = read_secret("db.diver_cbase"),
                           review_qry = interactive()) {
  
  conn <- db_connect(driver = odbc::odbc(),
                     dsn = read_secret("db.diver_dsn"),
                     server = read_secret("db.diver_server"),
                     project = diver_project,
                     cbase = diver_cbase)
  
  if (!is.null(date_range)) {
    if (length(date_range) == 1) {
      date_range <- rep(date_range, 2)
    } else if (length(date_range) != 2) {
      date_range <- c(min(date_range, na.rm = TRUE), max(date_range, na.rm = TRUE))
    }
    date_range <- as.Date(date_range)
    out <- conn %>%
      tbl(sql(paste0("select * from data where ",
                     "Ontvangstdatum BETWEEN ",
                     "EVAL('date(\"", format2(date_range[1], "yyyy/mm/dd"), "\")') AND ",
                     "EVAL('date(\"", format2(date_range[2], "yyyy/mm/dd"), "\")')")))
  } else {
    out <- conn %>% tbl("data")
  }
  
  # apply filters
  out <- out %>% filter({{ where }})
  
  if (isTRUE(review_qry)) {
    choice <- utils::menu(title = paste0("Run this query? (0 for Cancel)\n\n", remote_query(out)),
                          choices = c("Yes", "No"),
                          graphics = FALSE)
    if (choice != 1) {
      db_close(conn)
      return(invisible())
    }
  }
  
  msg_init("Collecting data...")
  tryCatch({
    out <- collect(out)
  },
  error = function(e) {
    msg_error()
    stop(e$message, call. = FALSE)
  })
  msg_ok(time = TRUE, dimensions = dim(out))
  
  db_close(conn)
  
  out
}
