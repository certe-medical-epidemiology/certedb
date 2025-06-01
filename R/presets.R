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
#'     filter: ColumnName1 == "abc"
#'     select: ColumnName1, col_name_2 = ColumnName2, ColumnName3, everything(), !starts_with("abc")
#'     wide_names_from: ColumnName3
#'   join2:
#'     cbase: "location/to/yet_another.cbase"
#'     by: ColumnName1, ColumnName2
#'     type: "left"
#'     select: ColumnName1, ColumnName2, ColumnName3
#'     wide_names_from: ColumnName3
#'     wide_name_trans: gsub("_", "..", .x)
#'   post-processing:
#'     - "file 1.R"
#'     - "file 2.R"
#' ```
#' 
#' For all presets, `cbase` and `date_col` are required.
#' 
#' ## Order of running
#' 
#' The YAML keys run in this order:
#' 
#' 1. Download cBase and filter (applied in `WHERE` statement)
#' 2. Select
#' 3. Join(s)
#' 4. Post-processing
#' 
#' After this, the arguments in [get_diver_data()] will run:
#' 
#' 5. Post-WHERE if `post_where` is set, using [`filter()`][dplyr::filter()]
#' 6. Distinct if `distinct = TRUE`, using [`distinct()`][dplyr::distinct()]
#' 7. Auto-transform if `autotransform = TRUE`, using [`auto_transform()`][certetoolbox::auto_transform()]
#' 
#' ## cBase (`cbase`)
#' 
#' This cBase must be a filepath and must exist on the Diver server. For joins, this can also be another type of file, see *Joins*
#' 
#' ## Select (`select`)
#' 
#' Input for `select` will be passed on to [`select()`][dplyr::select()], meaning that column names can be used, but also `tidyselect` functions such as [`everything()`][tidyselect::language].
#' 
#' ## Filters (`filter`)
#' 
#' Input for `filter` will be passed on to [`filter()`][dplyr::filter()].
#' 
#' ## Joins (`join`)
#' 
#' For joins, you must set at least `cbase`, `by`, and `type` ("left", "right", "inner", etc., [see here][dplyr::left_join()]).
#' 
#' Other files than a cBase in the field `cbase` will be imported using [certetoolbox::import()], such as an Excel or CSV file. This can be any file on any local or remote location (even live internet files). For example:
#' 
#' ```yaml
#' join:
#'   cbase: "location/to/excel_file.xlsx"
#'   by: ColumnName1
#'   type: "left"
#'   
#'   
#' join:
#'   cbase: "https://github.com/certe-medical-epidemiology/certegis/blob/main/data/geo_gemeenten.rda"
#'   by: ColumnName1
#'   type: "left"
#' ```
#' 
#' An unlimited number of joins can be used, but all so-called 'keys' **must be unique** and start with `join`, e.g. `joinA` / `joinB` or `join` / `join2` / `join3`.
#' 
#' If `wide_names_from` is set, the dataset is first transformed to long format using the columns specified in `by`, and then reshaped to wide format with values in `wide_names_from`.
#' 
#' Use `wide_name_trans` to transform the values in `wide_names_from` before the reshaping to wide format is applied. Use `.x` for the column values. As this will be applied before the data is transformed to a wide format, it allows to refine the values in `wide_names_from` using e.g., [`case_when()`][dplyr::case_when()].
#' 
#' ## Post-processing (`post-processing`)
#' 
#' Any data transformation can be done after the data have been downloaded and processed according to all previous steps. Use `x` to indicate the data set.
#' 
#' ```yaml
#' name_new_preset:
#'   cbase: "location/to/name.cbase"
#'   date_col: "ColumnNameDate"
#'   post-processing: |
#'     x |>
#'       mutate(Column1 = case_when(Column2 = "A" ~ 1,
#'                                  Column3 = "B" ~ 2,
#'                                  TRUE ~ 3))
#' ```
#' 
#' As shown, in YAML the `|` character can be used to start a multi-line statement.
#' 
#' The `post-processing` field can also be one or more R file paths, which will be read using [readLines()]. Each of these chunks is valid:
#' 
#' ```yaml
#' name_new_preset:
#'   cbase: "location/to/name.cbase"
#'   date_col: "ColumnNameDate"
#'   post-processing: "location/to/file.R"
#'   
#' name_new_preset:
#'   cbase: "location/to/name.cbase"
#'   date_col: "ColumnNameDate"
#'   post-processing:
#'     - "location/to/file 1.R"
#'     - "location/to/file 2.R"
#'   
#' name_new_preset:
#'   cbase: "location/to/name.cbase"
#'   date_col: "ColumnNameDate"
#'   post-processing1: "location/to/file 1.R"
#'   post-processing2: "location/to/file 2.R"
#' ```
#' 
#' Note that all post-processing steps will run directly after the querying the data and thus **before auto-transformation** if `autotransform = TRUE` in [get_diver_data()].
#' @rdname presets
#' @export
presets <- function() {
  p <- read_secret("db.presets")
  out <- tibble(preset = names(p),
                cbase = NA_character_,
                filter = NA_character_,
                select = NA_character_,
                joins = NA_character_,
                postprocessing = NA_character_)
  for (i in seq_len(length(p))) {
    if (is.null(p[[i]]$cbase)) {
      out[i, "cbase"] <- "None"
    } else {
      out[i, "cbase"] <- p[[i]]$cbase
    }
    
    if (is.null(p[[i]]$filter)) {
      out[i, "filter"] <- "None"
    } else {
      out[i, "filter"] <- p[[i]]$filter
    }
    
    if (is.null(p[[i]]$select)) {
      out[i, "select"] <- "None"
    } else {
      cols <- strsplit(p[[i]]$select, ", ?")[[1]]
      out[i, "select"] <- paste0(length(cols), ": ", paste(cols, collapse = ", "))
    }
    
    joins <- which(names(p[[i]]) %like% "^join")
    if (length(joins) == 0) {
      out[i, "joins"] <- "None"
    } else {
      out[i, "joins"] <- paste0(length(joins), " join", ifelse(length(joins) > 1, "s", ""))
    }
    
    if (is.null(p[[i]]$`post-processing`)) {
      out[i, "postprocessing"] <- "None"
    } else {
      post <- strsplit(p[[i]]$`post-processing`, "\n")[[1]]
      out[i, "postprocessing"] <- paste(length(post), "lines")
    }
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
