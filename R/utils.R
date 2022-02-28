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

pkg_env <- new.env(hash = FALSE)

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
  db_message(..., print = print, type = "warning")
}

msg_init <- function(...) {
  db_message(..., type = "info", new_line = FALSE)
  pkg_env$time <- Sys.time()
}

#' @importFrom certestyle font_green
msg_ok <- function(time = TRUE, dimensions = NULL) {
  time_diff <- Sys.time() - pkg_env$time
  if (is.null(dimensions)) {
    size <- ""
  } else {
    size <- paste0(", ",
                   format(dimensions[1], big.mark = ","),
                   "x",
                   format(dimensions[2], big.mark = ","),
                   " observations")
  }
  db_message(font_green("OK"),
             ifelse(isTRUE(time),
                    paste0(" (", format(round(time_diff, digits = 1)), size, ")"),
                    ""),
             type = NULL)
  pkg_env$time <- NULL
}

#' @importFrom certestyle font_red
msg_error <- function(time = TRUE) {
  time_diff <- Sys.time() - pkg_env$time
  db_message(font_red("ERROR"),
             ifelse(isTRUE(time),
                    paste0(" (", format(round(time_diff, digits = 1)), ")"),
                    ""),
             type = NULL)
  pkg_env$time <- NULL
}

is_empty <- function(x) {
  is.null(x) || isFALSE(x) || identical(x, "") || all(is.na(as.character(x)))
}
