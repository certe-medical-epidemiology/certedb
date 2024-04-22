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

#' Download Data from a Local or Remote Database
#' 
#' Thes functions can be used to download local or remote database data, e.g. Spectre data from DiveLine on a Diver server (from [Dimensional Insight](https://www.dimins.com)). The [get_diver_data()] function sets up an ODBC connection (using [db_connect()]), which requires their quite limited [DI-ODBC driver](https://www.dimins.com/online-help/workbench_help/Content/ODBC/di-odbc.html).
#' @param date_range date range, can be length 1 or 2 (or more to use the min/max) to filter on the column `Ontvangstdatum`. Defaults to [this_year()]. Use `NULL` to set no date filter. Can also be years, or functions such as [`last_month()`][certetoolbox::last_month()].
#' @param date_column column name to filter `date_range` on, will be determined automatically if using `NULL`.
#' @param where arguments to filter data on, will be passed on to [`filter()`][dplyr::filter()]. **Do not use `&&` or `||` but only `&` or `|` in filtering.**
#' @param diver_cbase,diver_project,diver_dsn,diver_testserver properties to set in [db_connect()]. The `diver_cbase` argument will be based on `preset`, but can also be set to blank `NULL` to manually select a cBase in a popup window.
#' @param diver_tablename name of the database table to download data from. This is hard-coded by DI and should normally never be changed.
#' @param review_qry a [logical] to indicate whether the query must be reviewed first, defaults to `TRUE` in interactive mode and `FALSE` otherwise. This will always be `FALSE` in Quarto / R Markdown, since the output of [knitr::pandoc_to()] must be `NULL`.
#' @param antibiogram_type antibiotic transformation mode. Leave blank to strip antibiotic results from the data, `"sir"` to keep SIR values, `"mic"` to keep MIC values or `"disk"` to keep disk diffusion values. Values will be cleaned with [`as.sir()`][AMR::as.sir()], [`as.mic()`][AMR::as.mic()] or [`as.disk()`][AMR::as.disk()].
#' @param preset a preset to choose from [presets()]. Will be ignored if `diver_cbase` is set, even if it is set to `NULL`.
#' @param distinct [logical] to apply [distinct()] to the resulting data set
#' @param auto_transform [logical] to apply [auto_transform()] to the resulting data set
#' @param info a logical to indicate whether info about the connection should be printed
#' @param query a [data.frame] to view the query of, or a [character] string to run as query in [certedb_getmmb()] (which will ignore all other arguments, except for `where`, `auto_transform` and `info`).
#' @param limit maximum number of rows to return.
#' @param as_background_job run data collection as a background job
#' @details These functions return a 'certedb tibble' from Diver or MOLIS, which prints information in the tibble header about the used source and current user.
#' 
#' Use [certedb_query()] to retrieve the original query that was used to download the data.
#' @importFrom dbplyr sql remote_query
#' @importFrom dplyr tbl filter collect matches mutate across select distinct first type_sum arrange desc
#' @importFrom certestyle format2 font_black font_blue font_bold font_green font_grey font_italic
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
#' get_diver_data(date_range = 2024, where = BepalingCode == "PXNCOV")
#' get_diver_data(2024, BepalingCode == "PXNCOV")
#' 
#' # use di$ to pull a list with column names while you type
#' get_diver_data(2024, di$BepalingCode == "PXNCOV")
#' 
#' # for the `where`, use `&` or `|`:
#' get_diver_data(last_month(),
#'                di$BepalingCode == "PXNCOV" & di$Zorglijn == "2e lijn")
#' get_diver_data(c(2020:2024),
#'                where = di$BepalingCode == "PXNCOV" | di$Zorglijn == "2e lijn")
#' 
#' 
#' # use %like%, %unlike%, %like_case% or %unlike_case% for regular expressions
#' get_diver_data(2024, where = di$MateriaalNaam %like% "Bloed")
#' get_diver_data(2024, where = di$MateriaalNaam %unlike% "Bloed")
#' get_diver_data(2024, where = di$MateriaalNaam %like_case% "bloed")
#' get_diver_data(2024, where = di$MateriaalNaam %unlike_case% "Bloed")
#' 
#' get_diver_data(2024,
#'                where = di$BepalingNaam %like% "Noro" &
#'                          di$PatientLeeftijd >= 75)
#'                          
#' # R objects will be converted
#' materialen <- c("A", "B", "C")
#' get_diver_data(2024, where = di$MateriaalNaam %in% materialen)
#' leeftijden <- 65:85
#' get_diver_data(2024, where = di$PatientLeeftijd %in% leeftijden)
#' 
#' 
#' # USING DIVER INTEGRATOR LANGUAGE --------------------------------------
#' 
#' # Use Diver Integrator functions within EVAL():              
#' get_diver_data(2024, where = EVAL('regexp(value("MateriaalCode"),"^B")'))
#' 
#' get_diver_data(
#'   2024,
#'   where = EVAL('rolling(12, value("OntvangstDatum"), date("2024/11/27"))')
#' )
#' 
#' # See the website for an overview of allowed functions:
#' # https://www.dimins.com/online-help/workbench_help/Content/ODBC/di-odbc-sql-reference.html
#' }
get_diver_data <- function(date_range = this_year(),
                           where = NULL,
                           date_column = read_secret("db.diver_date_column"),
                           review_qry = interactive(),
                           antibiogram_type = "sir",
                           distinct = TRUE,
                           auto_transform = TRUE,
                           preset = read_secret("db.preset_default"),
                           diver_cbase = NULL,
                           diver_project = read_secret("db.diver_project"),
                           diver_dsn = if (diver_testserver == FALSE) read_secret("db.diver_dsn") else  read_secret("db.diver_dsn_test"),
                           diver_testserver = FALSE,
                           diver_tablename = "data",
                           info = interactive(),
                           limit = Inf,
                           as_background_job = FALSE,
                           ...) {
  
  if (is_empty(preset)) {
    preset <- NULL
  }
  if (missing(diver_cbase)) {
    # get preset
    preset <- get_preset(preset)
    diver_cbase <- preset$cbase
  } else {
    if (!is.null(preset)) {
      msg("Ignoring `preset = \"", preset, "\"` since `diver_cbase` is set")
    }
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
  on.exit(db_close(conn, print = info))
  user <- conn@info$username
  
  if (diver_cbase == "") {
    diver_cbase <- "(manually selected)"
  }
  
  if (!is.null(date_range)) {
    
    msg_init("Checking date columns...", print = info)
    case_row1 <- conn |> tbl(diver_tablename) |> utils::head(1) |> collect()
    cbase_columns <- case_row1 |> colnames()
    date_cols <- cbase_columns[which(vapply(FUN.VALUE = logical(1), case_row1, inherits, c("Date", "POSIXct")))]
    
    if (!is.null(date_column) && !identical(date_column, "") && !date_column %in% cbase_columns) {
      msg_error(time = FALSE, print = info,
                paste0("\n  Column \"", date_column, "\" does not exist in this cBase, change argument `date_column` to one of: ",
                       paste0('\n  - "', date_cols, '"', collapse = "")))
      return(invisible())
    } else if (is.null(date_column) || identical(date_column, "")) {
      if (length(date_cols) == 0) {
        msg_error(time = FALSE, print = info, "No date column found in this cBase")
        return(invisible())
      }
      date_column <- date_cols[1]
      msg_ok(time = FALSE, dimensions = NULL, print = info, paste0("; Picked column ", font_blue(paste0('"', date_column, '"'))))
    } else {
      msg_ok(time = FALSE, dimensions = NULL, print = info) 
    }
    
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
    msg_init("Retrieving initial cBase...", print = info)
    if (length(unique(date_range)) > 1) {
      out <- conn |>
        # from https://www.dimins.com/online-help/workbench_help/Content/ODBC/di-odbc-sql-reference.html
        tbl(sql(paste0("SELECT * FROM ", diver_tablename, " WHERE ", date_column, " BETWEEN ",
                       "{d '", format2(date_range[1], "yyyy-mm-dd"), "'} AND ",
                       "{d '", format2(date_range[2], "yyyy-mm-dd"), "'}")))
    } else {
      out <- conn |>
        tbl(sql(paste0("SELECT * FROM ", diver_tablename, " WHERE ", date_column, " = {d '", format2(date_range[1], "yyyy-mm-dd"), "'}")))
    }
  } else {
    msg_init("Retrieving initial cBase...", print = info)
    out <- conn |> tbl(diver_tablename)
  }
  out_bak <- out
  msg_ok(dimensions = dim(out), print = info)
  
  limit <- max(as.double(limit))
  if (!is.infinite(limit)) {
    if (!is.na(limit)) {
      limit <- as.integer(limit)
      out <- out |> head(n = limit)
    }
  }
  
  msg_init("Validating WHERE statement...", print = info)
  # fill in columns from the 'di' object
  where <- where_convert_di(substitute(where))
  # convert objects, this will return msg "OK"
  where <- where_convert_objects(deparse(substitute(where)), info = info)
  
  if (!is.null(substitute(where))) {
    out <- out |> filter({{ where }})
    out$lazy_query$where <- where_convert_like(out$lazy_query$where)
  }
  if (!is.null(preset) && !all(is.na(preset$filter))) {
    # apply filter from preset
    msg_init(paste0("Validating WHERE statement from preset ", font_blue(paste0('"', preset$name, '"')), font_black("...")),
             print = info)
    preset_filter <- str2lang(preset$filter)
    out <- out |> filter(preset_filter)
    msg_ok(print = info, dimensions = dim(out))
  }
  
  qry <- remote_query(out)
  qry_print <- gsub(paste0("(", paste0("\"?", colnames(out), "\"?", collapse = "|"), ")"), font_italic(font_green("\\1")),
                    gsub("( AND | OR |NOT| IN |EVAL| BETWEEN )", font_blue("\\1"),
                         gsub("(SELECT|FROM|WHERE|LIMIT)", font_bold(font_blue("\\1")),
                              gsub("}\n  AND ", "} AND ", 
                                   gsub(" AND ", "\n  AND ", qry)))))
  
  if (isTRUE(review_qry) && interactive() && is.null(pandoc_to())) {
    choice <- utils::menu(title = paste0("\nCollect data from this query? (0 for Cancel)\n\n", qry_print,
                                         ifelse(!is.null(preset) & !all(is.na(preset$filter)),
                                                font_grey(paste0("\n(last part from preset \"", preset$name, "\")")),
                                                "")),
                          choices = c("Yes", "No", "Print column names"),
                          graphics = FALSE)
    if (choice == 3) {
      df <- collect(out_bak, n = 1)
      cols <- vapply(FUN.VALUE = character(1), df, type_sum)
      # print column names with their class in <..> brackets
      print(paste0(names(cols), " <", cols, ">"), quote = FALSE)
      choice <- utils::menu(title = paste0("\nCollect data from this query? (0 for Cancel)\n\n", qry_print),
                            choices = c("Yes", "No"),
                            graphics = FALSE)
    }
    if (choice != 1) {
      return(invisible())
    }
  } else if (!interactive()) {
    wh <- strsplit(unclass(remote_query(out)), "[ \n]WHERE[ \n]")[[1]]
    wh <- paste0(trimws(gsub("\"q[0-9]+\"", "", wh[2:length(wh)])), collapse = " AND ")
    wh <- gsub(" AND ", "\n  AND ", wh, fixed = TRUE)
    wh <- gsub("[ \n]LIMIT .*", "", wh)
    msg("Applying filter: ", wh)
  }
  
  
  if (isTRUE(as_background_job)) {
    temp_dir <- tempdir()
    job <- rstudioapi::jobRunScript(path = system.file("background_db.R", package = "certedb"),
                                    name = paste0("get_diver_data()"),
                                    workingDir = temp_dir,
                                    importEnv = FALSE,
                                    exportEnv = "R_GlobalEnv")
    rstudioapi::jobSetState(job, "idle")
    rstudioapi::jobSetStatus(job, "Waiting to start...")
    rstudioapi::jobAddOutput(job, "Waiting to start...\n\n")
    env <- list(job = job,
                call = sys.call(),
                distinct = distinct,
                auto_transform = auto_transform,
                diver_cbase = diver_cbase,
                diver_dsn = diver_dsn,
                diver_project = diver_project,
                qry = qry,
                user = user)
    saveRDS(env, file.path(temp_dir, "env.rds"))
    rstudioapi::sendToConsole(code = 'msg("Running job in background.")', execute = TRUE, echo = FALSE, focus = TRUE)
    return(invisible())
  }
  
  msg_init("Collecting data...", print = info, prefix_time = TRUE)
  tryCatch({
    out <- collect(out)
  },
  error = function(e) {
    msg_error(time = FALSE, print = info)
    stop(format_error(e), call. = FALSE)
  })
  msg_ok(dimensions = dim(out), print = info)
  
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
    msg_init("Removing AB columns...", print = info, prefix_time = TRUE)
    out <- out |>
      select(!matches("^(Ab_|ABMC$)")) |> 
      distinct()
    msg_ok(time = FALSE, print = info, dimensions = dim(out))
  } else if (isTRUE(antibiogram_type == "sir")) {
    msg_init("Transforming SIRs...", print = info, prefix_time = TRUE)
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
    msg_ok(print = info, dimensions = dim(out))
  } else if (isTRUE(antibiogram_type == "mic")) {
    msg_init("Transforming MICs...", print = info, prefix_time = TRUE)
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
    msg_ok(print = info, dimensions = dim(out))
  } else if (isTRUE(antibiogram_type == "disk")) {
    msg_init("Transforming disk diameters...", print = info, prefix_time = TRUE)
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
    msg_ok(print = info, dimensions = dim(out))
  }
  
  if (isTRUE(distinct)) {
    out_distinct <- distinct(out)
    if (nrow(out_distinct) < nrow(out)) {
      msg("Removed ", out_distinct - out, " duplicate rows")
      out <- out_distinct
    }
  }
  
  if (!is.null(preset) && !all(is.na(preset$select))) {
    msg_init(paste0("Selecting columns from preset ", font_blue(paste0('"', preset$name, '"')), font_black("...")),
             print = info,
             prefix_time = TRUE)
    out <- out |> select(preset$select)
    msg_ok(print = info, dimensions = dim(out))
  }
  
  if (isTRUE(auto_transform)) {
    msg_init("Transforming data set...", print = info, prefix_time = TRUE)
    if ("Ordernummer" %in% colnames(out)) {
      out$Ordernummer[out$Ordernummer %like% "[0-9]{2}[.][0-9]{4}[.][0-9]{4}"] <- gsub(".", "", out$Ordernummer[out$Ordernummer %like% "[0-9]{2}[.][0-9]{4}[.][0-9]{4}"], fixed = TRUE)
      out <- out |> arrange(desc(Ordernummer))
    } else {
      out <- out |> arrange(desc(Ontvangstdatum))
    }
    # transform data, and update column names
    out <- auto_transform(out, snake_case = TRUE)
    msg_ok(print = info)
  }
  
  as_certedb_tibble(out,
                    source = diver_cbase,
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
      if (!is.null(attributes(diver_data)$source)) {
        msg <- paste0("# This query was run on ", attributes(diver_data)$source)
        if (!is.null(attributes(diver_data)$datetime)) {
          msg <- paste0(msg, " on ", format2(attributes(diver_data)$datetime, "yyyy-mm-dd HH:MM"))
        }
        if (!is.null(attributes(diver_data)$user)) {
          msg <- paste0(msg, " by ", attributes(diver_data)$user)
        }
        if (!is.null(attributes(diver_data)$dims)) {
          msg <- paste0(msg, ", yielding a ", 
                        ifelse(is.null(attributes(diver_data)$type), " ", paste(attributes(diver_data)$type, "tibble ")),
                        "of ", format_dimensions(attributes(diver_data)$dims))
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
                   paste0("\n  - Replaced ", font_blue(names(converted)[i]), font_black(" with "), font_blue(converted[[i]])))
    }
    converted <- paste0(msg_txt, collapse = "")
  } else {
    converted <- ""
  }
  msg_ok(time = FALSE, dimensions = NULL, print = info, converted)
  where_split <- paste0(trimws(where_split), collapse = " ")
  str2lang(where_split)
}

where_convert_di <- function(where) {
  for (i in seq_len(length(where))) {
    where_txt <- paste0(trimws(deparse(where[[i]])), collapse = " ")
    if (where_txt %like% "di[$]") {
      where[[i]] <- str2lang(gsub("di$", "", where_txt, fixed = TRUE))
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
