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

#' Voorgedefinieerde variabelen selecteren
#'
#' Dit downloadt bestanden uit de map \code{Sys.getenv("R_REFMAP")} die de structuur \code{preset_*.sql} hebben. Gebruik een of meerdere van deze (zelf te maken) bestanden als parameter \code{select_preset} in de functies \code{\link{certedb_getmmb}} en \code{\link{certedb_getmmb_tat}}. Zie Details.
#' @param name Tekst(vector) om te zoeken in de map \code{Sys.getenv("R_REFMAP")}.
#' @param vector Tekst(vector) met kolomnamen die opgeslagen moet worden als nieuwe preset. Deze kunnen geselecteerd worden met \code{\link{db}}.
#' @param fullname Standaard is \code{FALSE}. Bestandsnamen weergeven met prefix \code{prefix_} en suffix \code{.sql}.
#' @param recursive Ook onderliggende mappen doorzoeken.
#' @details Gebruik: \cr
#'   - \code{preset.list} voor het weergeven van alle presets in de map \code{Sys.getenv("R_REFMAP")}; \cr
#'   - \code{preset.add} voor het toevoegen van een nieuwe preset; \cr
#'   - \code{preset.read} voor het uitlezen van een preset; \cr
#'   - \code{preset.thisfolder.list} voor het weergeven van alle presets in de huidige map(pen); \cr
#'   - \code{preset.thisfolder} voor het weergeven van de eerst gevonden preset in de huidge map; \cr
#'   - \code{preset.exists} voor toetsen of een preset bestaat.
#' @aliases preset
#' @name preset
#' @rdname preset
#' @export
preset.list <- function(fullname = FALSE, recursive = FALSE) {
  lst <- list.files(path = Sys.getenv("R_REFMAP"),
                    pattern = "^(preset_).*(.sql)$",
                    ignore.case = FALSE,
                    recursive = recursive)
  if (fullname == TRUE) {
    lst
  } else {
    gsub("^(preset_)(.*)(.sql)$", "\\2", lst)
  }
}

#' @rdname preset
#' @export
preset.thisfolder.list <- function(fullname = FALSE, recursive = TRUE) {
  lst <- list.files(path = getwd(),
                    pattern = "^(preset_).*(.sql)$",
                    ignore.case = FALSE,
                    recursive = recursive)
  if (fullname == TRUE) {
    lst
  } else {
    gsub("^(preset_)(.*)(.sql)$", "\\2", lst)
  }
}

#' @rdname preset
#' @export
preset.exists <- function(name, returnlocation = FALSE) {
  found <- all(paste0('preset_', name, '.sql') %in% preset.list(TRUE))
  if (returnlocation == TRUE) {
    .R_REFMAP(paste0('preset_', name, '.sql'))
  } else {
    found
  }
}

#' @rdname preset
#' @export
preset.read <- function(name) {
  lst <- list(0)
  for (i in 1:length(name)) {
    if (gsub("\\+", "/", name[i]) %unlike% "/") {
      # controleer Sys.getenv("R_REFMAP")
      if (!preset.exists(name[i])) {
        stop('Preset `', name[i], '` not found, file does not exist: ',
             preset.exists(name[i], returnlocation = TRUE), '.',
             call. = FALSE)
      }
      lst[[i]] <- read.table(file = .R_REFMAP(paste0('preset_', name[i], '.sql')),
                             sep = "\n",
                             header = FALSE,
                             stringsAsFactors = FALSE)[,1]
    } else {
      # dan is het een volledige bestandsnaam
      if (file.exists(name[i])) {
        lst[[i]] <- read.table(file = name[i],
                               sep = "\n",
                               header = FALSE,
                               stringsAsFactors = FALSE)[,1]
      } else if (file.exists(paste0(getwd(), '/', name[i]))) {
        lst[[i]] <- read.table(file = paste0(getwd(), '/', name[i]),
                               sep = "\n",
                               header = FALSE,
                               stringsAsFactors = FALSE)[,1]
      } else {
        stop('File `', name[i], '` not found.', call. = FALSE)
      }
    }
  }
  lst %>% unlist() %>% unique() %>% gsub(",$", "", .)
}

#' @rdname preset
#' @export
preset.add <- function(name, vector) {
  if (length(name) > 1) {
    stop('`name` must be of length 1.', call. = FALSE)
  }
  
  if (preset.exists(name)) {
    if (!readline("This preset already exists. Would you like to overwrite it? [Y/n] ") %in% c("", "Y", "y", "J", "j")) {
      return(invisible())
    }
  }
  
  vector <- select_translate_asterisk(vector)
  vector <- unique(vector)
  write(x = concat(vector, sep = "\n"),
        file = .R_REFMAP(paste0('preset_', name, '.sql')),
        append = FALSE,
        ncolumns = 1)
  if (preset.exists(name)) {
    cat("Preset `", name, "` succesfully saved.\n", sep = "")
  }
}

#' @rdname preset
#' @export
preset.thisfolder <- function(recursive = TRUE) {
  f <- list.files(path = getwd(),
                  pattern = "^(preset_).*(.sql)$",
                  ignore.case = TRUE,
                  recursive = recursive)
  if (length(f) == 0) {
    stop("This folder does not contain a preset. File names of presets must start with 'preset' and end with '.sql'.", call. = FALSE)
  } else if (length(f) > 1) {
    cat(paste0("Selecting first preset found: ", f[1], ".\n"))
  } else {
    cat(paste0("Selecting preset: ", f, ".\n"))
  }
  paste0(getwd(), "/", f[1])
}
