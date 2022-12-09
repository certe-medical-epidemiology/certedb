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
                select = NA_character_)
  for (i in seq_len(length(p))) {
    out[i, "cbase"] <- p[[i]]$cbase
    out[i, "filter"] <- p[[i]]$filter
    if (!is.null(p[[i]]$filter)) {
      out[i, "filter"] <- "(none)"
    }
    if (!is.null(p[[i]]$select)) {
      cols <- strsplit(p[[i]]$select, ", ?")[[1]]
      if (length(cols) > 5) {
        out[i, "select"] <- paste0(length(cols), ": ", paste(cols[1:5], collapse = ", "), ", ...")
      } else {
        out[i, "select"] <- paste0(length(cols), ": ", paste(cols, collapse = ", "))
      }
    } else {
      out[i, "select"] <- "(all)"
    }
  }
  out
}

get_preset <- function(preset) {
  if (is_empty(preset)) {
    stop("A preset must be set.", call. = FALSE)
  }
  p <- read_secret("db.presets")
  if (!tolower(preset) %in% tolower(names(p))) {
    stop("Preset not found: \"", preset, "\". Available are: ", paste0('"', names(p), '"', collapse = ", "), ".", call. = FALSE)
  }
  p <- p[which(tolower(names(p)) == tolower(preset))]
  p <- c(p[[1]], list(name = names(p)))
  if (!all(is.na(p$select))) {
    p$select <- strsplit(p$select, ", ?")[[1]]
  }
  p
}
