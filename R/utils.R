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

globalVariables(c(".",
                  "AanvragerCode",
                  "Antibioticumcode",
                  "aut_1e",
                  "aut_def",
                  "aut_usr_1e",
                  "aut_usr_def",
                  "bacteriecode",
                  "bacteriecode_oud",
                  "IsAangevraagd",
                  "join_data",
                  "MateriaalKorteNaam",
                  "McraStatus",
                  "MIC_gescreend",
                  "noord_zuid",
                  "Ontvangstdatum",
                  "ontvangstdatum",
                  "ontvangstdatumtijd",
                  "ordernr",
                  "Ordernummer",
                  "postcode",
                  "R_AB_Group",
                  "ResultaatStatus",
                  "ResultaatTekst",
                  "RIS_gerapporteerd",
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
                    paste0(" (", time_diff, size, ") "),
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
                    paste0(" (", format(round(time_diff, digits = 1)), ") "),
                    ""),
             ...,
             type = NULL,
             print = print)
  pkg_env$time <- NULL
}

is_empty <- function(x) {
  is.null(x) || isFALSE(x) || identical(x, "") || all(is.na(as.character(x)))
}


#' @importFrom certestyle font_blue font_black
where_convert_objects <- function(where, info) {
  where_split <- strsplit(paste0(trimws(where), collapse = " "), " ", fixed = TRUE)[[1]]
  converted <- list()
  
  for (i in seq_len(length(where_split))) {
    old <- where_split[i]
    if (old %unlike% "^[A-Za-z0-9.]") {
      next
    }
    evaluated <- tryCatch(eval(parse(text = old)), error = function(e) NULL)
    if (!is.null(evaluated)) {
      new <- paste0(trimws(deparse(evaluated)), collapse = " ")
      if (!identical(new, old)) {
        converted <- c(converted,
                       stats::setNames(list(new), old))
        where_split[i] <- new
      }
    }
  }
  
  if (length(converted) > 0) {
    msg_txt <- character(0)
    for (i in seq_len(length(converted))) {
      msg_txt <- c(msg_txt,
                   paste0(font_black("\n  - Replaced "), font_blue(names(converted)[i]), font_black(" with "), font_blue(converted[[i]])))
    }
    converted <- paste0(msg_txt, collapse = "")
  } else {
    converted <- ""
  }
  msg_ok(time = FALSE, dimensions = NULL, print = info, converted)
  where_split <- paste0(trimws(where_split), collapse = " ")
  str2lang(where_split)
}

where_convert_di_gl <- function(where) {
  for (i in seq_len(length(where))) {
    where_txt <- paste0(trimws(deparse(where[[i]])), collapse = " ")
    has_di_gl <- FALSE
    while (where_txt %like% "(di|gl)[$]") {
      # use while and sub(), not gsub(), to go over each mention of 'di$'
      has_di_gl <- TRUE
      where_txt <- sub("((certedb::)?(di|gl)[$][A-Za-z0-9`.-]+)",
                       eval(str2lang(sub(".*((certedb::)?(di|gl)[$][A-Za-z0-9`.-]+).*", "\\1", where_txt, perl = TRUE))),
                       where_txt,
                       perl = TRUE)
      if (where_txt %like% "^[a-zA-Z0-9]+[-]") {
        stop("Hyphen (-) in a column name cannot be used to query a cBase", call. = FALSE)
      }
    }
    if (has_di_gl == TRUE) {
      where[[i]] <- str2lang(where_txt)
    }
  }
  where
}

#' @importFrom rlang is_quosure quo_get_expr quo_set_expr
where_convert_like <- function(full_where) {
  # this transforms
  # where = Materiaalnaam %like% "bloed"
  # to
  # WHERE (EVAL('regexp(value("Materiaalnaam"),"bloed", true)')) 
  
  for (where_part in seq_len(length(full_where))) {
    query_object <- full_where[[where_part]]
    if (is_quosure(query_object)) {
      query_object <- quo_get_expr(query_object)
    }
    
    split_AND <- unlist(strsplit(paste0(trimws(deparse(query_object)),
                                        collapse = " "),
                                 " & ",
                                 fixed = TRUE))
    merged_AND <- character(length(split_AND))
    for (i in seq_len(length(split_AND))) {
      split_OR <- unlist(strsplit(split_AND[i],
                                  " | ",
                                  fixed = TRUE))
      
      for (j in seq_len(length(split_OR))) {
        qry <- split_OR[j]
        if (qry %unlike% "%(like|like_case|unlike|unlike_case)%") {
          next
        }
        qry_language <- str2lang(qry)
        qry_language_text <- as.character(qry_language)
        if (qry_language_text[1] == "%like%") {
          qry <- paste0("EVAL('regexp(value(\"",
                        qry_language_text[2], "\"), \"",
                        qry_language_text[3], "\", true)')")
          
        } else if (qry_language_text[1] == "%unlike%") {
          qry <- paste0("!EVAL('regexp(value(\"",
                        qry_language_text[2], "\"), \"",
                        qry_language_text[3], "\", true)')")
          
        } else if (qry_language_text[1] == "%like_case%") {
          qry <- paste0("EVAL('regexp(value(\"",
                        qry_language_text[2], "\"), \"",
                        qry_language_text[3], "\", false)')")
          
        } else if (qry_language_text[1] == "%unlike_case%") {
          qry <- paste0("!EVAL('regexp(value(\"",
                        qry_language_text[2], "\"), \"",
                        qry_language_text[3], "\", false)')")
        }
        split_OR[j] <- qry
      }
      merged_AND[i] <- paste0(split_OR, collapse = " | ")
    }
    
    merged <- str2lang(paste0(merged_AND, collapse = " & "))
    if (is_quosure(full_where[[where_part]])) {
      full_where[[where_part]] <- rlang::quo_set_expr(full_where[[where_part]], merged)
    } else {
      full_where[[where_part]] <- merged
    }
    
  }
  full_where
}

unify_select_clauses <- function(query, diver_tablename = "data") {
  # this transforms:
  #
  # SELECT "q01".*
  # FROM (SELECT * FROM data WHERE OntvangstDatum = {d '2024-01-01'}) "q01"
  # WHERE ("MateriaalGroep" IN ('Bloed', 'Bloedkweken')
  #   AND "BepalingNaam" IN ('Banale kweek', 'Kweek')
  #   AND "Zorglijn" = '2e lijn')
  #
  # to:
  #
  # SELECT * FROM data WHERE (OntvangstDatum = {d '2024-01-01'})
  #   AND ("MateriaalGroep" IN ('Bloed', 'Bloedkweken')
  #   AND "BepalingNaam" IN ('Banale kweek', 'Kweek')
  #   AND "Zorglijn" = '2e lijn')
  if (query %like% '"q01"') {
    qry <- gsub('"q[0-9]+"', "", strsplit(query, "(\n| )WHERE(\n| )")[[1]])
    qry <- qry[-1]
    query <- paste0("SELECT * FROM ", diver_tablename, " WHERE (", paste0(trimws(qry), collapse = " AND "))
  }
  query
}

limit_txt_length <- function(txt, max) {
  txt_out <- substr(txt, 1, max)
  txt_out[nchar(txt) > max] <- paste0(txt_out[nchar(txt) > max], "...[OUTPUT TRUNCATED]")
  txt_out
}
