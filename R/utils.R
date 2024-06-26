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

#' @importFrom dplyr db_list_tables
#' @export
dplyr::db_list_tables

#' @importFrom dplyr db_drop_table
#' @export
dplyr::db_drop_table

#' @importFrom dplyr db_write_table
#' @export
dplyr::db_write_table

#' @importFrom dplyr db_has_table
#' @export
dplyr::db_has_table

globalVariables(c("Antibioticumcode",
                  "MIC_gescreend",
                  "Ontvangstdatum",
                  "Ordernummer",
                  "R_AB_Group",
                  "RIS_gerapporteerd",
                  ".",
                  "aut_1e",
                  "aut_def",
                  "aut_usr_1e",
                  "aut_usr_def",
                  "bacteriecode",
                  "bacteriecode_oud",
                  "noord_zuid",
                  "ontvangstdatum",
                  "ontvangstdatumtijd",
                  "ordernr",
                  "postcode",
                  "type",
                  "val1_1e",
                  "val1_def",
                  "val1_usr_1e",
                  "val1_usr_def",
                  "val2_1e",
                  "val2_def",
                  "val2_usr_1e",
                  "val2_usr_def",
                  "value"))

#' @importFrom cli symbol
format_dimensions <- function(dims) {
  dims <- c(dims[1], dims[2])
  dims <- formatC(dims, 
                  big.mark = ifelse(identical(getOption("OutDec"), ","), ".", ","),
                  format = "d",
                  preserve.width = "individual")
  dims <- trimws(paste0(" ", dims, " ", collapse = symbol$times))
}

#' @importFrom rlang cnd_message
#' @importFrom certestyle font_stripstyle
format_error <- function(e, replace = character(0), by = character(0)) {
  if (inherits(e, "rlang_error")) {
    txt <- cnd_message(e)
    txt <- font_stripstyle(txt)
    txt <- gsub(".*Caused by error[:](\n!)?", "", txt)
  } else {
    txt <- c(e$message, e$parent$message, e$parent$parent$message, e$parent$parent$parent$message, e$call)
  }
  txt <- txt[txt %unlike% "^Problem while"]
  if (length(txt) == 0) {
    # return original error
    stop(e, call. = FALSE)
  }
  for (i in seq_len(length(replace))) {
    txt <- gsub(replace[i], by[i], txt)
  }
  if (all(txt == "")) {
    txt <- "Unknown error"
  }
  txt <- trimws(txt)
  paste0(txt, collapse = "\n")
}

#' @importFrom certestyle font_black font_blue font_red font_green font_grey
db_message <- function(...,
                       print = interactive() | Sys.getenv("IN_PKGDOWN") != "",
                       type = "info",
                       new_line = TRUE,
                       prefix_time = FALSE) {
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
    } else if (type == "ok") {
      icon <- font_green(icon)
    } else if (type == "warning") {
      icon <- font_red(icon)
    }
    message(trimws(paste0(icon, " ",
                         ifelse(prefix_time == TRUE, font_grey(paste0("[", format(Sys.time()), "] ")), ""),
                         font_black(msg))),
            appendLF = new_line)
  }
}

db_warning <- function(..., print = interactive() | Sys.getenv("IN_PKGDOWN") != "") {
  db_message(..., print = print, type = "warning")
}

msg_init <- function(..., print = interactive(), prefix_time = FALSE) {
  db_message(..., type = "info", new_line = FALSE, print = print, prefix_time = prefix_time)
  pkg_env$time <- Sys.time()
}

msg <- function(..., print = interactive()) {
  db_message(..., type = "ok", new_line = TRUE, print = print)
}

#' @importFrom certestyle font_green
msg_ok <- function(time = TRUE, dimensions = NULL, print = interactive(), ...) {
  time_diff <- difftime(Sys.time(), pkg_env$time)
  if (is.null(dimensions)) {
    size <- ""
  } else {
    format_dim <- function(dimensions) {
      if (is.na(dimensions[1])) {
        paste0(format(dimensions[2], big.mark = ","), " columns")
      } else {
        paste0(format(dimensions[1], big.mark = ","), " \u00D7 ", format(dimensions[2], big.mark = ","), " observations")
      }
    }
    size <- paste0("; ", format_dim(dimensions))
  }
  if (length(time_diff) == 0) {
    time_diff <- "0 secs"
  } else {
    time_diff <- format(round(time_diff, digits = 1))
  }
  db_message(font_green(" OK"),
             ifelse(isTRUE(time),
                    paste0(" (", time_diff, size, ")"),
                    ""),
             ...,
             type = NULL,
             print = print)
  pkg_env$time <- NULL
}

#' @importFrom certestyle font_red
msg_error <- function(time = TRUE, print = interactive(), ...) {
  time_diff <- difftime(Sys.time(), pkg_env$time)
  db_message(font_red(" ERROR"),
             ifelse(isTRUE(time),
                    paste0(" (", format(round(time_diff, digits = 1)), ")"),
                    ""),
             ...,
             type = NULL,
             print = print)
  pkg_env$time <- NULL
}

is_empty <- function(x) {
  is.null(x) || isFALSE(x) || identical(x, "") || all(is.na(as.character(x)))
}
