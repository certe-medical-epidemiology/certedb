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

#' Available Presets for `get_diver_data()`
#' 
#' Work with presets for [get_diver_data()]. This automates selecting, filtering, and joining cBases.
#' @importFrom dplyr tibble
#' @details The function [presets()] returns a data.frame with available presets, as defined in the secrets YAML file under `db.presets`.
#' @section Required YAML Format:
#' 
#' This YAML information should be put into the YAML file that is read using [`read_secret()`][certetoolbox::read_secret()]. Afterwards, the name of the preset can be used as [`get_diver_data(preset = "...")`][get_diver_data()].
#' 
#' The most basic YAML form:
#' 
#' ```yaml
#' name_new_preset:
#'   cbase: "location/to/name.cbase"
#'   date_col: "ColumnNameDate"
#' ```
#' 
#' The most extensive YAML form:
#' 
#' ```yaml
#' name_new_preset:
#'   cbase: "location/to/name.cbase"
#'   date_col: "ColumnNameDate"
#'   filter: ColumnName1 %in% c("Filter1", "Filter2") & ColumnName2 %in% c("Filter3", "Filter4")
#'   select: ColumnName1, ColumnName2, ColumnNameReceiptDate, new_name = OldName
#'   join:
#'     cbase: "location/to/another.cbase"
#'     by: ColumnName1, ColumnName2
#'     type: "left"
#'     select: ColumnName1, ColumnName2, ColumnName3, everything(), !starts_with("abc")
#'     wide_names_from: ColumnName3
#'   join2:
#'     cbase: "location/to/yet_another.cbase"
#'     by: ColumnName1, ColumnName2
#'     type: "left"
#'     select: ColumnName1, ColumnName2, ColumnName3, everything(), !where(is.numeric)
#'     wide_names_from: ColumnName3
#'     wide_name_trans: gsub("_", "..", .x)
#' ```
#' 
#' For all presets, `cbase` and `date_col` are required.
#' 
#' Input for `select` will be passed on to [`select()`][dplyr::select()], meaning that column names can be used, but also `tidyselect` functions such as [`everything()`][tidyselect::language]. Input for `filter` will be passed on to [`filter()`][dplyr::filter()].
#' 
#' ## Joins
#' 
#' For joins, you must set at least `cbase`, `by`, and `type` ("left", "right", "inner", etc., [see here][dplyr::left_join()]). 
#' 
#' An unlimited number of joins can be used, but all so-called 'keys' must be unique and start with `join`, e.g. `joinA` / `joinB` or `join` / `join2`.
#' 
#' If `wide_names_from` is set, the dataset is first transformed to long format using the columns specified in `by`, and then reshaped to wide format with values in `wide_names_from`.
#' 
#' Use `wide_name_trans` to transform the values in `wide_names_from` before the reshaping to wide format is applied. Use `.x` for the column values. As this will be applied before the data is transformed to a wide format, it allows to refine the values in `wide_names_from` using e.g., [`case_when()`][dplyr::case_when()].
#' @rdname presets
#' @export
presets <- function() {
  p <- read_secret("db.presets")
  out <- tibble(preset = names(p),
                cbase = NA_character_,
                filter = NA_character_,
                select = NA_character_)
  for (i in seq_len(length(p))) {
    out[i, "cbase"] <- p[[i]]$cbase
    
    if (is.null(p[[i]]$filter)) {
      out[i, "filter"] <- "(all)"
    } else {
      out[i, "filter"] <- p[[i]]$filter
    }
    
    if (is.null(p[[i]]$select)) {
      out[i, "select"] <- "(all)"
    } else {
      cols <- strsplit(p[[i]]$select, ", ?")[[1]]
      out[i, "select"] <- paste0(length(cols), ": ", paste(cols, collapse = ", "))
    }
    
    joins <- which(names(p[[i]]) %like% "^join")
    out[i, "joins"] <- length(joins)
  }
  structure(out, class = c("presets", class(out)))
}

#' @export
#' @importFrom dplyr mutate across everything
#' @noRd
print.presets <- function(x, ...) {
  out <- mutate(x,
         across(everything(),
                function(y) {
                  y[!is.na(y) & nchar(y) > 35] <- paste0(substr(y[!is.na(y) & nchar(y) > 35], 1, 35), "...")
                  y
                }))
  class(out) <- class(out)[!class(out) == "presets"]
  print(out, ...)
}

#' @rdname presets
#' @param preset name of the preset
#' @details The function [get_preset()] will return all the details of a preset as a [list].
#' @export
get_preset <- function(preset) {
  if (is_empty(preset)) {
    stop("A preset must be set.", call. = FALSE)
  }
  
  p <- read_secret("db.presets")
  if (!tolower(preset) %in% tolower(names(p))) {
    stop("Preset not found: \"", preset, "\". Available are: ", paste0('"', names(p), '"', collapse = ", "), ".", call. = FALSE)
  }
  
  p <- p[tolower(names(p)) == tolower(preset)][[1]]
  p <- p[!vapply(FUN.VALUE = logical(1), p, is.null)]
  
  p <- c(list(name = preset),
         p)
  
  joins <- which(names(p) %like% "^join")
  for (j in joins) {
    p[[j]] <- p[[j]][!vapply(FUN.VALUE = logical(1), p[[j]], is.null)]
    if (!all(c("type", "by", "cbase") %in% names(p[[j]]))) {
      stop("'join' elements in presets must at least contain 'type', 'by', and 'cbase'", call. = FALSE)
    }
    if (!paste0(p[[j]]$type, "_join") %in% ls(envir = asNamespace("dplyr"))) {
      stop("preset value join$type must indicate the type of join, e.g. \"left\", \"right\", \"full\", or \"inner\".")
    }
    if (!is.null(p[[j]]$by)) {
      p[[j]]$by <- strsplit(p[[j]]$by, ", ?")[[1]]
    }
  }
  
  # fix the selects - if we use a list, then this works: `... |> select(!!!preset$select)`
  if (!is.null(p$select)) {
    p$select <- strsplit(p$select, ", ?")[[1]]
    lst <- p$select |> trimws() |> as.list() |> stats::setNames(p$select)
    lst[which(p$select %like% "=")] <- trimws(gsub("^.*=", "", names(lst[which(p$select %like% "=")])))
    names(lst)[which(p$select %like% "=")] <- trimws(gsub("=.*$", "", names(lst[which(p$select %like% "=")])))
    p$select <- lst
  }
  for (j in joins) {
    if (!is.null(p[[j]]$select)) {
      p[[j]]$select <- strsplit(p[[j]]$select, ", ?")[[1]]
      lst <- p[[j]]$select |> trimws() |> as.list() |> stats::setNames(p[[j]]$select)
      lst[which(p[[j]]$select %like% "=")] <- trimws(gsub("^.*=", "", names(lst[which(p[[j]]$select %like% "=")])))
      names(lst)[which(p[[j]]$select %like% "=")] <- trimws(gsub("=.*$", "", names(lst[which(p[[j]]$select %like% "=")])))
      p[[j]]$select <- lst
    }
  }
  p
}
