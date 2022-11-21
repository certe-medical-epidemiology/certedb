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
#' @param date_range date range, can be length 1 or 2 (or more to use the min/max) to filter on the column `Ontvangstdatum`. Defaults to current year. Use `NULL` to set no date filter. Can also be years, or functions such as [`last_month()`][certetoolbox::last_month()].
#' @param where arguments to filter data on, will be passed on to [`filter()`][dplyr::filter()]. Use [c()] to combine multiple search strings, see Examples. **Do not use `&&` or `||` but only `&` or `|` in filtering.**
#' @param diver_project name of the Diver project
#' @param diver_cbase name of the Diver cbase, can be left blank to select using a popup window (using `""`, `NA`, `NULL` or `FALSE`)
#' @param review_qry a [logical] to indicate whether the query must be reviewed first, defaults to `TRUE` in interactive mode and `FALSE` otherwise
#' @param antibiogram_type antibiotic transformation mode. Leave blank to strip antibiotic results from the data, `"rsi"` to keep RSI values, `"mic"` to keep MIC values or `"disk"` to keep disk diffusion values. Values will be cleaned with [`as.rsi()`][AMR::as.rsi()], [`as.mic()`][AMR::as.mic()] or [`as.disk()`][AMR::as.disk()].
#' @param distinct logical to apply [distinct()] to the resulting data set
#' @importFrom dbplyr sql remote_query
#' @importFrom dplyr tbl filter collect matches mutate across select distinct first type_sum arrange desc
#' @importFrom certestyle format2
#' @importFrom tidyr pivot_wider
#' @importFrom AMR as.rsi as.mic as.disk
#' @export
#' @examples 
#' \dontrun{
#' 
#' get_diver_data(date_range = 2022, where = Bepalingcode == "PXNCOV")
#' get_diver_data(2022, Bepalingcode == "PXNCOV")
#' 
#' get_diver_data(last_month(),
#'                Bepalingcode == "PXNCOV" & Zorglijn == "2e lijn")
#'                
#' get_diver_data(c(2020:2022),
#'                where = c(Bepalingcode == "PXNCOV", Zorglijn == "2e lijn"))
#' 
#' 
#' # USING R LANGUAGE -----------------------------------------------------
#' 
#' # Use di to see variables after typing '$':
#' get_diver_data(where = di$Materiaalcode %like% "bloed")
#' 
#' 
#' # USING DIVER INTEGRATOR LANGUAGE --------------------------------------
#' 
#' # Use Diver Integrator functions such as regexp() within EVAL():              
#' get_diver_data(where = EVAL('regexp(value("Materiaalcode"),"^B")'))
#' 
#' # With ignore case:
#' get_diver_data(where = EVAL('regexp(value("Materiaalcode"),"^b", true)'))
#' 
#' }
get_diver_data <- function(date_range = c(paste0(format(Sys.Date(), "%Y"), "-01-01"),
                                          paste0(format(Sys.Date(), "%Y"), "-12-31")),
                           where = NULL,
                           diver_project = read_secret("db.diver_project"),
                           diver_cbase = read_secret("db.diver_cbase"),
                           review_qry = interactive(),
                           select = "mmb",
                           antibiogram_type = "rsi",
                           distinct = TRUE) {

  conn <- db_connect(driver = odbc::odbc(),
                     dsn = read_secret("db.diver_dsn"),
                     server = read_secret("db.diver_server"),
                     project = diver_project,
                     cbase = ifelse(is_empty(diver_cbase), "", diver_cbase))
  
  msg_init("Running query...")
  if (!is.null(date_range)) {
    if (length(date_range) == 1) {
      date_range <- rep(date_range, 2)
    } else if (length(date_range) != 2) {
      date_range <- c(min(date_range, na.rm = TRUE), max(date_range, na.rm = TRUE))
    }
    if (all(date_range %in% c(2000:2050), na.rm = TRUE)) {
      date_range[1] <- paste0(date_range[1], "-01-01")
      date_range[2] <- paste0(date_range[2], "-12-31")
    }
    date_range <- as.Date(date_range)
    out <- conn |>
      tbl(sql(paste0("select * from data where ",
                     "Ontvangstdatum BETWEEN ",
                     "EVAL('date(\"", format2(date_range[1], "yyyy/mm/dd"), "\")') AND ",
                     "EVAL('date(\"", format2(date_range[2], "yyyy/mm/dd"), "\")')")))
  } else {
    out <- conn |> tbl("data")
  }
  # apply filters
  where <- substitute(where)
  for (i in seq_len(length(where))) {
    where_txt <- deparse(where[[i]])
    if (where_txt %like% "di[$]") {
      where[[i]] <- str2lang(gsub("di$", "", where_txt, fixed = TRUE))
    }
  }
  out <- out |> filter(where) |> R_to_DI()
  msg_ok(time = TRUE, dimensions = dim(out))
  
  if (isTRUE(review_qry)) {
    choice <- utils::menu(title = paste0("\nCollect data from this query? (0 for Cancel)\n\n", remote_query(out)),
                          choices = c("Yes", "No", "Print column names"),
                          graphics = FALSE)
    if (choice == 3) {
      df <- collect(out, n = 1)
      cols <- vapply(FUN.VALUE = character(1), df, type_sum)
      print(paste0(names(cols), " <", cols, ">"), quote = FALSE)
      choice <- utils::menu(title = paste0("\nCollect data from this query? (0 for Cancel)\n\n", remote_query(out)),
                            choices = c("Yes", "No"),
                            graphics = FALSE)
    }
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
  
  if (isTRUE(distinct)) {
    out_distinct <- distinct(out)
    if (nrow(out_distinct) < nrow(out)) {
      msg("Removed ", nrow(out) - nrow(out_distinct), " duplicate rows")
      out <- out_distinct
    }
  }
  
  if (diver_cbase %like% "MMBGL_Datamgnt_BepalingTotaalLevel") {
    out <- out |>
      mutate(ABMC = Antibioticumcode, Ab_RSI = RIS_gerapporteerd, Ab_MIC = MIC_gescreend) |> 
      select(-c(Antibioticumcode:R_AB_Group))
  }
  
  if (!any(colnames(out) %like% "^(Ab_|ABMC$)")) {
    # do nothing
  } else if (isTRUE(is_empty(antibiogram_type))) {
    msg_init("Removing AB columns...")
    out <- out |>
      select(!matches("^(Ab_|ABMC$)")) |> 
      distinct()
    msg_ok(dimensions = dim(out))
  } else if (isTRUE(antibiogram_type == "rsi")) {
    msg_init("Transforming RSIs...")
    ab_vars <- unique(out$ABMC)
    ab_vars <- ab_vars[!is.na(ab_vars)]
    out <- out |>
      pivot_wider(names_from = "ABMC",
                  values_from = "Ab_RSI",
                  id_cols = !matches("^Ab_"),
                  values_fill = list(Ab_RSI = NA),
                  values_fn = first) |> 
      mutate(across(ab_vars, function(x) {
        x[x %in% c("-", "NULL", "N", "NA")] <- NA
        as.rsi(x)
      }))
    if ("NA" %in% colnames(out)) {
      out <- out |> select(-"NA")
    }
    msg_ok(dimensions = dim(out))
  } else if (isTRUE(antibiogram_type == "mic")) {
    msg_init("Transforming MICs...")
    ab_vars <- unique(out$ABMC)
    ab_vars <- ab_vars[!is.na(ab_vars)]
    out <- out |>
      pivot_wider(names_from = "ABMC",
                  values_from = "Ab_MIC",
                  id_cols = !matches("^Ab_"),
                  values_fill = list(Ab_RSI = NA),
                  values_fn = first) |> 
      mutate(across(ab_vars, function(x) {
        x[x %in% c("F", "Neg", "Pos")] <- NA
        as.mic(x)
      }))
    if ("NA" %in% colnames(out)) {
      out <- out |> select(-"NA")
    }
    msg_ok(dimensions = dim(out))
  } else if (isTRUE(antibiogram_type == "disk")) {
    msg_init("Transforming disk diameters...")
    ab_vars <- unique(out$ABMC)
    ab_vars <- ab_vars[!is.na(ab_vars)]
    out <- out |>
      pivot_wider(names_from = "ABMC",
                  values_from = "Ab_Diameter",
                  id_cols = !matches("^Ab_"),
                  values_fill = list(Ab_RSI = NA),
                  values_fn = first) |> 
      mutate(across(ab_vars, as.disk))
    if ("NA" %in% colnames(out)) {
      out <- out |> select(-"NA")
    }
    msg_ok(dimensions = dim(out))
  }
  
  if (isTRUE(distinct)) {
    out_distinct <- distinct(out)
    if (nrow(out_distinct) < nrow(out)) {
      msg("Removed ", out_distinct - out, " duplicate rows")
      out <- out_distinct
    }
  }
  
  if ("Ordernummer" %in% colnames(out)) {
    out <- out |> arrange(desc(Ordernummer))
  } else {
    out <- out |> arrange(desc(Ontvangstdatum))
  }
  
  out
}

R_to_DI <- function(out) {
  # this transforms
  # where = Materiaalnaam %like% "bloed"
  # to
  # WHERE (EVAL('regexp(value("Materiaalnaam"),"bloed", true)')) 
  
  wheres <- out$lazy_query$where
  
  for (i in seq_len(length(wheres))) {
    elements <- as.character(wheres[[i]][[2]])
    if (length(elements) == 0) {
      next
    }
    if (elements[1] == "%like%") {
      wheres[[i]][[2]][[1]] <- as.symbol("EVAL")
      wheres[[i]][[2]][[2]] <- paste0("regexp(value(\"", elements[2], "\"), \"", elements[3], "\", true)")
      wheres[[i]][[2]] <- wheres[[i]][[2]][1:2]
    } else if (elements[1] == "%like_case%") {
      wheres[[i]][[2]][[1]] <- as.symbol("EVAL")
      wheres[[i]][[2]][[2]] <- paste0("regexp(value(\"", elements[2], "\"), \"", elements[3], "\", false)")
      wheres[[i]][[2]] <- wheres[[i]][[2]][1:2]
    } else {
      for (j in seq_len(length(elements))) {
        elements[j] <- gsub("(.*) %like% (.*)", "EVAL('regexp(value(\"\\1\"), \"\\2\", true)')", elements[j])
      }
      wheres[[i]][[2]] <- paste0(elements[2], elements[1], elements[3])
    }
  }
  out$lazy_query$where <- wheres
out
}