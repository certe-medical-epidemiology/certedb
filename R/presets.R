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
#  We created this package for both routine data analysis and academic  #cbase = "Source"
#  research and it was publicly released in the hope that it will be    #
#  useful, but it comes WITHOUT ANY WARRANTY OR LIABILITY.              #
# ===================================================================== #

#' Available Presets for Diver Server
#' 
#' This returns a data.frame with available presets, as defined in the secrets YAML file under `db.presets`.
#' @importFrom dplyr tibble
#' @export
presets <- function() {
  p <- read_secret("db.presets")
  out <- tibble(preset = names(p),
                cbase = NA_character_,
                filter = NA_character_,
                select = NA_character_,
                join = NA_character_)
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
    
    if (is.null(p[[i]]$join)) {
      out[i, "join"] <- "(none)"
    } else {
      out[i, "join"] <- p[[i]]$join$cbase
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
  

  if (!is.null(p$join)) {
    p$join <- p$join[!vapply(FUN.VALUE = logical(1), p$join, is.null)]
    if (!all(c("type", "by", "cbase") %in% names(p$join))) {
      stop("'join' elements in presets must at least contain 'type', 'by', and 'cbase'", call. = FALSE)
    }
    if (!paste0(p$join$type, "_join") %in% ls(envir = asNamespace("dplyr"))) {
      stop("preset value join$type must indicate the type of join, e.g. \"left\", \"right\", \"full\", or \"inner\".")
    }
  }
  if (!is.null(p$join$by)) {
    p$join$by <- strsplit(p$join$by, ", ?")[[1]]
  }
  
  # fix the selects - if we use a list, then this works: `... |> select(!!!preset$select)`
  if (!is.null(p$select)) {
    p$select <- strsplit(p$select, ", ?")[[1]]
    lst <- p$select |> trimws() |> as.list() |> stats::setNames(p$select)
    lst[which(p$select %like% "=")] <- trimws(gsub("^.*=", "", names(lst[which(p$select %like% "=")])))
    names(lst)[which(p$select %like% "=")] <- trimws(gsub("=.*$", "", names(lst[which(p$select %like% "=")])))
    p$select <- lst
  }
  if (!is.null(p$join$select)) {
    p$join$select <- strsplit(p$join$select, ", ?")[[1]]
    lst <- p$join$select |> trimws() |> as.list() |> stats::setNames(p$join$select)
    lst[which(p$join$select %like% "=")] <- trimws(gsub("^.*=", "", names(lst[which(p$join$select %like% "=")])))
    names(lst)[which(p$join$select %like% "=")] <- trimws(gsub("=.*$", "", names(lst[which(p$join$select %like% "=")])))
    p$join$select <- lst
  }
  p
}
