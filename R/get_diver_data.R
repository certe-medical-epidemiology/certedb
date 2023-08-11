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
#' This function can be used to download Spectre data from DiveLine on a Diver server (from [Dimensional Insight](https://www.dimins.com)). This function will set up an ODBC connection (using [db_connect()]), which requires their quite limited [DI-ODBC driver](https://www.dimins.com/online-help/workbench_help/Content/ODBC/di-odbc.html).
#' @param date_range date range, can be length 1 or 2 (or more to use the min/max) to filter on the column `Ontvangstdatum`. Defaults to [this_year()]. Use `NULL` to set no date filter. Can also be years, or functions such as [`last_month()`][certetoolbox::last_month()].
#' @param where arguments to filter data on, will be passed on to [`filter()`][dplyr::filter()]. **Do not use `&&` or `||` but only `&` or `|` in filtering.**
#' @param diver_cbase,diver_project,diver_dsn,diver_testserver properties to set in [db_connect()]. The `diver_cbase` argument will be based on `preset`, but can also be set to blank `NULL` to manually select a cBase in a popup window.
#' @param review_qry a [logical] to indicate whether the query must be reviewed first, defaults to `TRUE` in interactive mode and `FALSE` otherwise. This will always be `FALSE` in Quarto / R Markdown, since the output of [knitr::pandoc_to()] must be `NULL`.
#' @param antibiogram_type antibiotic transformation mode. Leave blank to strip antibiotic results from the data, `"sir"` to keep SIR values, `"mic"` to keep MIC values or `"disk"` to keep disk diffusion values. Values will be cleaned with [`as.sir()`][AMR::as.sir()], [`as.mic()`][AMR::as.mic()] or [`as.disk()`][AMR::as.disk()].
#' @param preset a preset to choose from [presets()]. Will be ignored if `diver_cbase` is set, even if it is set to `NULL`.
#' @param distinct [logical] to apply [distinct()] to the resulting data set
#' @param auto_transform [logical] to apply [auto_transform()] to the resulting data set
#' @param info a logical to indicate whether info about the connection should be printed
#' @param query a [data.frame] to view the query of, or a [character] string to run as query in [certedb_getmmb()] (which will ignore all other arguments, except for `where`, `auto_transform` and `info`).
#' @details These functions return a 'certedb tibble' from Diver or MOLIS, which prints information in the tibble header about the used cBase and current user.
#' 
#' Use [certedb_query()] to retrieve the original query that was used to download the data.
#' @importFrom dbplyr sql remote_query
#' @importFrom dplyr tbl filter collect matches mutate across select distinct first type_sum arrange desc
#' @importFrom certestyle format2 font_blue font_black font_grey
#' @importFrom certetoolbox auto_transform this_year
#' @importFrom tidyr pivot_wider
#' @importFrom AMR as.sir as.mic as.disk
#' @importFrom knitr pandoc_to
#' @rdname get_diver_data
#' @export
#' @examples 
#' \dontrun{
#' 
#' # these two work identical:
#' get_diver_data(date_range = 2022, where = Bepalingcode == "PXNCOV")
#' get_diver_data(2022, Bepalingcode == "PXNCOV")
#' 
#' # for the `where`, use `&`, `|`, or `c()`:
#' get_diver_data(last_month(),
#'                Bepalingcode == "PXNCOV" & Zorglijn == "2e lijn")
#' get_diver_data(c(2020:2022),
#'                where = c(Bepalingcode == "PXNCOV", Zorglijn == "2e lijn"))
#' 
#' 
#' # USING DIVER INTEGRATOR LANGUAGE --------------------------------------
#' 
#' # Use Diver Integrator functions such as regexp() within EVAL():              
#' get_diver_data(where = EVAL('regexp(value("Materiaalcode"),"^B")'))
#' 
#' # With ignore case (defaults to false):
#' get_diver_data(where = EVAL('regexp(value("Materiaalcode"),"^b", true)'))
#' 
#' }
get_diver_data <- function(date_range = this_year(),
                           where = NULL,
                           review_qry = interactive(),
                           antibiogram_type = "sir",
                           distinct = TRUE,
                           auto_transform = TRUE,
                           preset = read_secret("db.preset_default"),
                           diver_cbase = NULL,
                           diver_project = read_secret("db.diver_project"),
                           diver_dsn = if (diver_testserver == FALSE) read_secret("db.diver_dsn") else  read_secret("db.diver_dsn_test"),
                           diver_testserver = FALSE,
                           info = interactive()) {
  
  if (is_empty(preset)) {
    preset <- NULL
  } else if (missing(diver_cbase)) {
    # get preset
    preset <- get_preset(preset)
    diver_cbase <- preset$cbase
  } else {
    preset <- NULL
  }
  if (is_empty(diver_cbase)) {
    diver_cbase <- ""
  }
  conn <- db_connect(driver = odbc::odbc(),
                     dsn = diver_dsn,
                     project = diver_project,
                     cbase = diver_cbase,
                     print = info)
  user <- conn@info$username
  
  if (diver_cbase == "") {
    diver_cbase <- "(manually selected)"
  }
  
  msg_init("Retrieving initial cBase...", print = info)
  if (!is.null(date_range)) {
    if (length(date_range) == 1) {
      date_range <- rep(date_range, 2)
    } else {
      # always earliest first, newest second
      date_range <- c(min(date_range, na.rm = TRUE), max(date_range, na.rm = TRUE))
    }
    if (all(date_range %in% 2000:2050, na.rm = TRUE)) {
      date_range[1] <- paste0(date_range[1], "-01-01")
      date_range[2] <- paste0(date_range[2], "-12-31")
    }
    date_range <- tryCatch(as.Date(date_range),
                           error = function(e) as.Date(date_range, origin = "1970-01-01"))
    if (length(unique(date_range)) > 1) {
      out <- conn |>
        # from https://www.dimins.com/online-help/workbench_help/Content/ODBC/di-odbc-sql-reference.html
        tbl(sql(paste0("SELECT * FROM data WHERE Ontvangstdatum BETWEEN ",
                       "{d '", format2(date_range[1], "yyyy-mm-dd"), "'} AND ",
                       "{d '", format2(date_range[2], "yyyy-mm-dd"), "'}")))
    } else {
      out <- conn |>
        tbl(sql(paste0("SELECT * FROM data WHERE Ontvangstdatum = {d '", format2(date_range[1], "yyyy-mm-dd"), "'}")))
    }
  } else {
    out <- conn |> tbl("data")
  }
  # apply filters
  # where <- substitute(where)
  # for (i in seq_len(length(where))) {
  #   where_txt <- deparse(where[[i]])
  #   if (where_txt %like% "di[$]") {
  #     where[[i]] <- str2lang(gsub("di$", "", where_txt, fixed = TRUE))
  #   }
  # }
  if (!is.null(substitute(where))) {
    out <- out |> filter({{ where }}) #|> R_to_DI()
  }
  if (!is.null(preset) && !all(is.na(preset$filter))) {
    # apply filter from preset
    preset_filter <- str2lang(preset$filter)
    out <- out |> filter(preset_filter)
  }
  msg_ok(time = TRUE, dimensions = dim(out), print = info)
  qry <- remote_query(out)
  
  if (isTRUE(review_qry) && interactive() && is.null(pandoc_to())) {
    choice <- utils::menu(title = paste0("\nCollect data from this query? (0 for Cancel)\n\n", qry,
                                         ifelse(!is.null(preset) & !all(is.na(preset$filter)),
                                                font_grey(paste0("\n(last part from preset \"", preset$name, "\")")),
                                                "")),
                          choices = c("Yes", "No", "Print column names"),
                          graphics = FALSE)
    if (choice == 3) {
      df <- collect(out, n = 1)
      cols <- vapply(FUN.VALUE = character(1), df, type_sum)
      print(paste0(names(cols), " <", cols, ">"), quote = FALSE)
      choice <- utils::menu(title = paste0("\nCollect data from this query? (0 for Cancel)\n\n", qry),
                            choices = c("Yes", "No"),
                            graphics = FALSE)
    }
    if (choice != 1) {
      db_close(conn)
      return(invisible())
    }
  } else {
    wh <- strsplit(unclass(remote_query(out)), "[ \n]WHERE[ \n]")[[1]]
    wh <- paste0(trimws(gsub("\"q[0-9]+\"", "", wh[2:length(wh)])), collapse = " AND ")
    msg("Applying filter: ", wh)
  }
  
  msg_init("Collecting data...", print = info)
  tryCatch({
    out <- collect(out)
  },
  error = function(e) {
    msg_error()
    stop(e$message, call. = FALSE)
  })
  msg_ok(time = TRUE, dimensions = dim(out), print = info)
  
  db_close(conn, print = info)
  
  for (i in seq_len(ncol(out))) {
    # 2023-02-13 fix for Diver, logicals/booleans seem corrupt
    if (is.logical(out[, i, drop = TRUE])) {
      out[, i] <- as.logical(as.character(out[, i, drop = TRUE]))
    }
  }
  
  if (isTRUE(distinct)) {
    out_distinct <- distinct(out)
    if (nrow(out_distinct) < nrow(out)) {
      msg("Removed ", nrow(out) - nrow(out_distinct), " duplicate rows")
      out <- out_distinct
    }
  }
  
  if (diver_cbase %like% "MMBGL_Datamgnt_BepalingTotaalLevel") {
    out <- out |>
      mutate(ABMC = Antibioticumcode, Ab_SIR = RIS_gerapporteerd, Ab_MIC = MIC_gescreend) |> 
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
  } else if (isTRUE(antibiogram_type == "sir")) {
    msg_init("Transforming SIRs...")
    ab_vars <- unique(out$ABMC)
    ab_vars <- ab_vars[!is.na(ab_vars)]
    SIR_col <- ifelse("Ab_RSI" %in% colnames(out), "Ab_RSI", "Ab_SIR")
    out <- out |>
      pivot_wider(names_from = "ABMC",
                  values_from = SIR_col,
                  id_cols = !matches("^Ab_"),
                  values_fn = first) |> 
      mutate(across(ab_vars, function(x) {
        x[x %in% c("-", "NULL", "N", "NA")] <- NA
        as.sir(x)
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
  
  if (!is.null(preset) && !all(is.na(preset$select))) {
    msg_init(paste0("Selecting columns from preset ", font_blue(paste0('"', preset$name, '"')), font_black("...")))
    out <- out |> select(preset$select)
    msg_ok(dimensions = dim(out))
  }
  
  if (isTRUE(auto_transform)) {
    msg_init("Transforming data set...")
    if ("Ordernummer" %in% colnames(out)) {
      out$Ordernummer[out$Ordernummer %like% "[0-9]{2}[.][0-9]{4}[.][0-9]{4}"] <- gsub(".", "", out$Ordernummer[out$Ordernummer %like% "[0-9]{2}[.][0-9]{4}[.][0-9]{4}"], fixed = TRUE)
      out <- out |> arrange(desc(Ordernummer))
    } else {
      out <- out |> arrange(desc(Ontvangstdatum))
    }
    # transform data, and update column names
    out <- auto_transform(out, snake_case = TRUE)
    msg_ok()
  }
  
  as_certedb_tibble(out,
                    cbase = diver_cbase,
                    qry = qry,
                    datetime = Sys.time(),
                    user = user,
                    type = "Diver")
}

#' @rdname get_diver_data
#' @importFrom certestyle format2
#' @importFrom pillar style_subtle
#' @export
certedb_query <- function(query,
                          where = NULL,
                          auto_transform = TRUE,
                          info = interactive()) {
  
  if (is.data.frame(query)) {
    diver_data <- query
    query <- attributes(diver_data)$qry
    if (is.null(query)) {
      message("No query found.")
    } else {
      if (!is.null(attributes(diver_data)$cbase)) {
        msg <- paste0("# This query was run on '", attributes(diver_data)$cbase, "'")
        if (!is.null(attributes(diver_data)$datetime)) {
          msg <- paste0(msg, " on ", format2(attributes(diver_data)$datetime, "yyyy-mm-dd HH:MM"))
        }
        if (!is.null(attributes(diver_data)$user)) {
          msg <- paste0(msg, " by ", attributes(diver_data)$user)
        }
        cat(style_subtle(msg), "\n")
      }
      cat(query)
    }
    return(invisible(query))
  }
  
  # OLD part for get_mmb, remove later when certedb_getmmb() is obsolete
  where <- deparse(substitute(where))
  if (!identical(where, "NULL")) {
    where <- where_R2SQL(where, info = info)
    query <- paste(query, "WHERE", where)
  }
  
  certedb_getmmb(query = query,
                 auto_transform = auto_transform,
                 info = info,
                 review_where = FALSE,
                 only_show_query = FALSE,
                 first_isolates = FALSE,
                 eucast_rules = FALSE,
                 mic = FALSE,
                 rsi = FALSE,
                 tat_hours = FALSE)
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
