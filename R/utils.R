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

#' @importFrom RMariaDB MariaDB
#' @importFrom odbc odbc
get_driver <- function(dbname) {
  if (dbname == "certemmb") {
    RMariaDB::MariaDB()
  } else if (dbname == "diver") {
    odbc::odbc()
  } else {
    stop("no valid database name: '", dbname, "'", call. = FALSE) 
  }
}

#' @importFrom certestyle font_black font_blue font_red
db_message <- function(...,
                       print = interactive() | Sys.getenv("IN_PKGDOWN") != "",
                       type = "info",
                       new_line = TRUE) {
  # at default, only prints in interactive mode and for the website generation
  if (isTRUE(print)) {
    msg <- paste0(font_black(c(...), collapse = NULL), collapse = "")
    # get info icon
    if (isTRUE(base::l10n_info()$`UTF-8`) && interactive()) {
      # \u2139 is a symbol officially named 'information source'
      icon <- "\u2139"
    } else {
      icon <- "i"
    }
    if (is.null(type)) {
      icon <- ""
    } else if (type == "info") {
      icon <- font_blue(icon)
    } else if (type == "warning") {
      icon <- font_red(icon)
    }
    message(trimws(paste(icon, font_black(msg))), appendLF = new_line)
  }
}

db_warning <- function(..., print = interactive() | Sys.getenv("IN_PKGDOWN") != "") {
  plot2_message(..., print = print, type = "warning")
}
