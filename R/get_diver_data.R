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
#' @param date_range date range, can be length 1 or 2 (or more to use the min/max) to filter on the column specified in the YAML file, see [presets]. Defaults to [this_year()]. Use `NULL` to set no date filter. Can also be years, or functions such as [`last_month()`][certetoolbox::last_month()]. Date-time ojects will be converted to dates, so using times as input is useless. It is supported to filter on a date-time column though.
#' @param where,post_where arguments to filter data on, will be passed on to [`filter()`][dplyr::filter()]. **Do not use `&&` or `||` but only `&` or `|` in filtering.** The `post_where` will be run after all downloading and post-processing, but before the auto-transform.
#' @param diver_cbase,diver_project,diver_dsn,diver_testserver properties to set in [db_connect()]. The `diver_cbase` argument will be based on `preset`, but can also be set to blank `NULL` to manually select a cBase in a popup window.
#' @param diver_tablename name of the database table to download data from. This is hard-coded by DI and should normally never be changed.
#' @param review_qry a [logical] to indicate whether the query must be reviewed first, defaults to `TRUE` in interactive mode and `FALSE` otherwise. This will always be `FALSE` in Quarto / R Markdown, since the output of [knitr::pandoc_to()] must be `NULL`.
#' @param antibiogram_type antibiotic transformation mode. Leave blank to strip antibiotic results from the data, `"sir"` to keep SIR values, `"mic"` to keep MIC values or `"disk"` to keep disk diffusion values. Values will be cleaned with [`as.sir()`][AMR::as.sir()], [`as.mic()`][AMR::as.mic()] or [`as.disk()`][AMR::as.disk()].
#' @param preset a preset to choose from [presets()]. Will be ignored if `diver_cbase` is set, even if it is set to `NULL`. Be sure to read [the documentation][presets()] on how to use presets, and to see in which order the YAML keys will be run.
#' @param add_cols extra column to include in the selection. Can be named to set a new column name. Use [gl] or [di] to quickly pick from a list.
#' @param date_column column name of data set to query. Normally this should be set in a preset, but this argument can be used to override that.
#' @param distinct [logical] to apply [distinct()] to the resulting data set
#' @param auto_transform [logical] to apply [auto_transform()] to the resulting data set
#' @param snake_case [logical] to convert column names to [snake case](https://en.wikipedia.org/wiki/Snake_case), **only** when `auto_transform = TRUE`
#' @param info a logical to indicate whether info about the connection should be printed
#' @param log_file a file path to which the query will be logged, after collection and before any transformation. Use `NULL` or `""` to prevent logging.
#' @param query a [data.frame] to view the query of, or a [character] string to run as query in [certedb_getmmb()] (which will ignore all other arguments, except for `where`, `auto_transform` and `info`).
#' @param limit maximum number of rows to return.
#' @param only_real_patients [logical] to include only real patients, i.e., remove test and Q&A samples
#' @param only_conducted_tests [logical] to include only tests that were not stopped
#' @param only_validated [logical] to include only validated tests
#' @param only_requested [logical] to include only requested tests
#' @param in_background run data collection in the background using [callr::r_bg()]. Use `...$get_result()` to retrieve results, or `...$is_active()` to check whether the background process still runs.
#' @details These functions return a 'certedb tibble' from Diver or the `certemmb` MySQL database, which prints information in the tibble header about the used source and current user.
#' 
#' Use [certedb_query()] to retrieve the original query that was used to download the data.
#' @importFrom dbplyr sql remote_query
#' @importFrom dplyr tbl filter collect matches mutate across select distinct first type_sum arrange desc all_of pull
#' @importFrom rlang parse_expr quo sym
#' @importFrom certestyle format2 font_black font_blue font_bold font_green font_grey font_italic
#' @importFrom certetoolbox auto_transform this_year import
#' @importFrom tidyr pivot_longer unite pivot_wider
#' @importFrom AMR as.sir as.mic as.disk
#' @importFrom knitr pandoc_to
#' @rdname get_diver_data
#' @export
#' @examples 
#' \dontrun{
#' 
#' # peek-preview of a cBase:
#' get_diver_data(diver_cbase = "models/MedEpi/GLIMS10_Resistenties.cbase",
#'                date_range = NULL, limit = 10, review_qry = FALSE, auto_transform = FALSE)
#' 
#' # these two work identical:
#' get_diver_data(date_range = 2024, where = BepalingCode == "PXNCOV")
#' get_diver_data(2024, BepalingCode == "PXNCOV")
#' 
#' # use gl$ to pull a list with column names while you type
#' get_diver_data(2024, gl$BepalingCode == "PXNCOV")
#' 
#' # gl$ can also be used for adding columns, which can also be named
#' get_diver_data(2024, gl$BepalingCode == "PXNCOV", add_cols = gl$AfnameJaar)
#' get_diver_data(2024, gl$BepalingCode == "PXNCOV", add_cols = c(jaar = gl$AfnameJaar))
#' 
#' # for the `where`, use `&` or `|`:
#' get_diver_data(last_month(),
#'                gl$BepalingCode == "PXNCOV" & gl$Zorglijn == "2e lijn")
#' get_diver_data(c(2020:2024),
#'                where = gl$BepalingCode == "PXNCOV" | gl$Zorglijn == "2e lijn")
#' 
#' 
#' # use %like%, %unlike%, %like_case% or %unlike_case% for regular expressions
#' get_diver_data(2024, where = gl$MateriaalNaam %like% "Bloed")
#' get_diver_data(2024, where = gl$MateriaalNaam %unlike% "Bloed")
#' get_diver_data(2024, where = gl$MateriaalNaam %like_case% "bloed")
#' get_diver_data(2024, where = gl$MateriaalNaam %unlike_case% "Bloed")
#' 
#' get_diver_data(2024,
#'                where = gl$BepalingNaam %like% "Noro" &
#'                          gl$PatientLeeftijd >= 75)
#'                          
#' # R objects will be converted
#' materialen <- c("A", "B", "C")
#' get_diver_data(2024, where = gl$MateriaalNaam %in% materialen)
#' leeftijden <- 65:85
#' get_diver_data(2024, where = gl$PatientLeeftijd %in% leeftijden)
#' 
#' 
#' # USING DIVER INTEGRATOR LANGUAGE --------------------------------------
#' 
#' # See the website for an overview of allowed functions:
#' # https://www.dimins.com/online-help/workbench_help/Content/ODBC/di-odbc-sql-reference.html
#' 
#' # Use Diver Integrator functions within EVAL():              
#' get_diver_data(2024, where = EVAL('regexp(value("MateriaalCode"),"^B")'))
#' 
#' get_diver_data(
#'   2024,
#'   where = EVAL('rolling(12, value("OntvangstDatum"), date("2024/11/27"))')
#' )
#' 
#' }
get_diver_data <- function(date_range = this_year(),
                           where = NULL,
                           post_where = NULL,
                           review_qry = interactive(),
                           antibiogram_type = "sir",
                           distinct = TRUE,
                           auto_transform = TRUE,
                           snake_case = TRUE,
                           preset = read_secret("db.preset_default"),
                           add_cols = NULL,
                           date_column = NULL,
                           diver_cbase = NULL,
                           diver_project = read_secret("db.diver_project"),
                           diver_dsn = if (diver_testserver == FALSE) read_secret("db.diver_dsn") else read_secret("db.diver_dsn_test"),
                           diver_testserver = FALSE,
                           diver_tablename = "data",
                           info = interactive(),
                           log_file = read_secret("db.query_log"),
                           limit = Inf,
                           only_real_patients = TRUE,
                           only_conducted_tests = TRUE,
                           only_validated = FALSE,
                           only_requested = FALSE,
                           in_background = FALSE,
                           ...) {
  
  if (!is.null(list(...)$dates) && missing(date_range)) {
    date_range <- list(...)$dates
  }
  
  if (is_empty(preset)) {
    preset <- NULL
  }
  if (is_empty(diver_cbase)) {
    diver_cbase <- NULL
  }
  if (is.null(diver_cbase)) {
    # get preset
    preset <- get_preset(preset)
    diver_cbase <- preset$cbase
    if (is.null(date_column)) {
      date_column <- preset$date_col
    }
  } else {
    if (!is.null(preset)) {
      msg("Ignoring `preset = \"", preset, "\"` since `diver_cbase` is set")
    }
    preset <- NULL
  }
  if (is_empty(date_column) && !is.null(date_range)) {
    stop("if 'date_range' is set, 'date_column' must be set, or 'date_col' must be given in the preset", call. = FALSE)
  }
  
  if (isTRUE(in_background)) {
    message("NOTE: use ...$get_result() to retrieve results when they are available.\n\n",
            "If the call was:\n",
            "  data_123 <- get_diver_data(..., in_background = TRUE)\n",
            "Then after a while retrieve results using:\n",
            "  data_123 <- data_123$get_result()")
    out <- callr::r_bg(certedb::get_diver_data,
                       args = list(date_range = date_range,
                                   where = where,
                                   review_qry = FALSE,
                                   antibiogram_type = antibiogram_type,
                                   distinct = distinct,
                                   auto_transform = auto_transform,
                                   date_column = date_column,
                                   preset = preset,
                                   diver_cbase = diver_cbase,
                                   diver_project = diver_project,
                                   diver_dsn = diver_dsn,
                                   diver_testserver = diver_testserver,
                                   diver_tablename = diver_tablename,
                                   info = info,
                                   log_file = log_file,
                                   limit = limit,
                                   only_real_patients = only_real_patients,
                                   only_conducted_tests = only_conducted_tests,
                                   only_validated = only_validated,
                                   only_requested = only_requested,
                                   in_background = FALSE,
                                   ...),
                       package = TRUE)
    return(out)
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
    
    date_type <- conn |>
      tbl(sql(paste("SELECT", date_column, "FROM", diver_tablename))) |>
      utils::head(1) |>
      collect() |>
      pull(date_column)
    if (inherits(date_type, "POSIXct")) {
      # from https://www.dimins.com/online-help/workbench_help/Content/ODBC/di-odbc-sql-reference.html
      # You can use the driver-agnostic ODBC formatting to make a timestamp if you are working with two different types of ODBC platforms. The format for that is `{ts 'YYYY/MM/DD hh:mm:ss'}`.
      time_function <- "ts"
      date_format <- c("yyyy-mm-dd 00:00:00", "yyyy-mm-dd 23:59:59")
    } else {
      # You can use the driver-agnostic ODBC formatting to make a date if you are working with two different types of ODBC platforms. The format for that is `{d 'YYYY-MM-DD'}`.
      time_function <- "d"
      date_format <- c("yyyy-mm-dd", "yyyy-mm-dd")
    } 
    
    msg_init("Retrieving initial cBase...", print = info)
    if (length(unique(date_range)) > 1) {
      out <- conn |>
        tbl(sql(paste0("SELECT * FROM ", diver_tablename, " WHERE ",
                       date_column, " BETWEEN {", time_function, " '", format2(date_range[1], date_format[1]), "'} ",
                       "AND {", time_function, " '", format2(date_range[2], date_format[2]), "'}")))
    } else if (time_function == "ts") {
      # filter on single date, but it's a timestamp, so filter between 00:00:00-23:59:59
      out <- conn |>
        tbl(sql(paste0("SELECT * FROM ", diver_tablename, " WHERE ",
                       date_column, " BETWEEN {", time_function, " '", format2(date_range[1], date_format[1]), "'} ",
                       "AND {", time_function, " '", format2(date_range[2], date_format[2]), "'}")))
    } else {
      out <- conn |>
        tbl(sql(paste0("SELECT * FROM ", diver_tablename, " WHERE ",
                       date_column, " = {", time_function, " '", format2(date_range[1], date_format[1]), "'}")))
    }
  } else {
    msg_init("Retrieving initial cBase...", print = info)
    out <- conn |> tbl(diver_tablename)
  }
  out_bak <- out
  msg_ok(dimensions = dim(out), print = info)
  
  # limit ----
  limit <- max(as.double(limit))
  if (!is.infinite(limit)) {
    if (!is.na(limit)) {
      limit <- as.integer(limit)
      out <- out |> utils::head(n = limit)
    }
  }
  
  # set query ----
  if (!is.null(tryCatch(where, error = function(e) 0))) {
    msg_init("Validating WHERE statement...", print = info)
    # fill in columns from the 'di' or 'gl' object
    if (isTRUE(list(...)$where_as_character)) {
      where <- str2lang(where)
    }
    where <- where_convert_di_gl(substitute(where))
    # convert objects, this will return msg "OK"
    where <- where_convert_objects(deparse(substitute(where)), info = info)
    
    if (!is.null(substitute(where))) {
      out <- out |> filter({{ where }})
      out$lazy_query$where <- where_convert_like(out$lazy_query$where)
    }
  }
  if (!is.null(preset) && !all(is.na(preset$filter))) {
    ## filter ----
    msg_init(paste0("Validating WHERE statement from preset ", font_blue(paste0('"', preset$name, '"')), font_black("...")),
             print = info)
    
    tryCatch({
      preset_filter <- str2lang(preset$filter)
      out <- out |> filter(preset_filter)
      msg_ok(print = info, dimensions = dim(out))
    }, error = function(e) {
      msg_error(time = TRUE, print = info, font_red("==> Skipping this step\n"), format_error(e))
    })
  }
  
  # unify query ----
  new_qry <- unify_select_clauses(remote_query(out), diver_tablename)
  out <- conn |> tbl(sql(new_qry))
  
  qry <- remote_query(out)
  qry_print <- gsub(paste0("(", paste0("\"?", colnames(out), "\"?", collapse = "|"), ")"), font_italic(font_green("\\1")),
                    gsub("( AND | OR |NOT| IN |EVAL| BETWEEN )", font_blue("\\1"),
                         gsub("(SELECT|FROM|WHERE|LIMIT)", font_bold(font_blue("\\1")),
                              gsub("}\n  (AND|OR) ", "} \\1 ", 
                                   gsub(" (AND|OR) ", "\n  \\1 ", qry)))))
  
  if (isTRUE(review_qry) && interactive() && is.null(pandoc_to())) {
    choice <- utils::menu(title = paste0("\nCollect data from this query? (0 for Cancel)\n\n", qry_print,
                                         ifelse(!is.null(preset) & !all(is.na(preset$filter)),
                                                font_grey(paste0("\n-- (a part of) the WHERE comes from preset \"", preset$name, "\"")),
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
  
  # collect ----
  msg_init("Collecting data...", print = info, prefix_time = TRUE)
  tryCatch({
    out <- collect(out)
  },
  error = function(e) {
    msg_error(time = FALSE, print = info)
    stop(format_error(e), call. = FALSE)
  })
  log <- FALSE
  if (!is.null(suppressMessages(suppressWarnings(log_file))) && suppressMessages(suppressWarnings(log_file)) != "") {
    # log queries
    df <- data.frame(datetime = format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"), 
                     user = user,
                     interactive = interactive(),
                     source_dsn = diver_dsn,
                     source_project = diver_project,
                     source_cbase = diver_cbase,
                     query = limit_txt_length(gsub(" +", " ", gsub("\n", " ", new_qry, fixed = TRUE)), 5000),
                     rows = NROW(out),
                     columns = NCOL(out))
    utils::write.table(x = df,
                       file = log_file,
                       append = TRUE,
                       row.names = FALSE,
                       col.names = !file.exists(log_file),
                       sep = ",")
    log <- TRUE
  }
  msg_ok(time = TRUE, dimensions = dim(out), print = info,
         ifelse(log, paste0(font_black("- logged to "), font_blue(paste0('"', log_file, '"'))), font_red("- not logged")))
  
  for (i in seq_len(ncol(out))) {
    # 2023-02-13 fix for Diver, logicals/booleans seem corrupt
    if (is.logical(out[, i, drop = TRUE])) {
      out[, i] <- as.logical(as.character(out[, i, drop = TRUE]))
    }
  }
  
  # select ----
  if (!is.null(preset) && !all(is.na(preset$select))) {
    msg_init(paste0("Selecting columns from preset ", font_blue(paste0('"', preset$name, '"')),
                    font_black(ifelse(!is.null(add_cols),
                                      paste0(" and ", length(add_cols), " extra column", ifelse(length(add_cols) == 1, "", "s"), "..."),
                                      "..."))),
             print = info,
             prefix_time = TRUE)
    
    convert_to_tidyselect <- function(x) {
      # get tidyselect functions
      tidyselect_env <- asNamespace("tidyselect")
      tidyselect_functions <- ls(envir = tidyselect_env, all.names = FALSE)
      tidyselect_functions <- tidyselect_functions[vapply(FUN.VALUE = logical(1),
                                                          tidyselect_functions,
                                                          function(x) tryCatch(is.function(getExportedValue(x, ns = tidyselect_env)),
                                                                               error = function(e) FALSE))]
      is_negated <- grepl("^!", x)
      x_clean <- sub("^!", "", x)
      function_name <- sub("\\(.*\\)$", "", x_clean)
      if (function_name %in% tidyselect_functions) {
        parsed_expr <- parse_expr(x_clean)
        if (is_negated) {
          return(quo(-!!parsed_expr))
        } else {
          return(quo(!!parsed_expr))
        }
      } else {
        return(sym(x))
      }
    }
    
    tryCatch({
      preset$select <- lapply(preset$select, convert_to_tidyselect)
      lst_select <- preset$select
      lst_select.bak <- lst_select
      # first remove names, since otherwise everything() will lead to all column being names "everything_[0-9]"
      # but since this makes `..., col_a = ColumnA` not work anymore, we run it twice
      names(lst_select) <- NULL
      new_cols <- out |> select(!!!lst_select) |> colnames()
      lst_select_new <- as.list(new_cols)
      names(lst_select_new) <- new_cols
      # now replace the names for the ones that we set in the preset
      for (i in seq_len(length(lst_select.bak))) {
        if (is.name(lst_select.bak[[i]]) && as.character(lst_select.bak[[i]]) %in% names(lst_select_new)) {
          names(lst_select_new)[which(as.character(lst_select.bak[[i]]) == names(lst_select_new))] <- names(lst_select.bak)[i]
        }
      }
      # add extra columns if requested, but only if not in select already
      if (!is.null(add_cols)) {
        add_cols <- add_cols[!add_cols %in% names(lst_select_new)]
        nms <- names(add_cols)
        if (is.null(nms)) {
          nms <- add_cols
        } else {
          nms[nms == ""] <- add_cols[nms == ""]
        }
        lst_add_cols <- stats::setNames(as.list(add_cols), nms)
        lst_select_new <- c(lst_select_new, lst_add_cols)
      }
      
      # use unquote-splice `!!!` since input is list
      out <- out |> select(!!!lst_select_new)
      msg_ok(print = info, dimensions = dim(out))
    }, error = function(e) {
      msg_error(time = TRUE, print = info, font_red("==> Skipping this step\n"), format_error(e))
    })
    
  }
  
  # join ----
  joins <- which(names(preset) %like% "^join")
  for (j in joins) {
    
    tryCatch({
      
      join_object <- preset[[j]]
      
      src <- join_object$cbase
      src_txt <- paste0("cBase ", font_blue(paste0("\"", basename(src), "\"")))
      out_join <- NULL
      if (src %unlike% "[.]cbase$") {
        src_txt <- paste0("file ", font_blue(paste0("\"", src, "\"")))
        out_join <- suppressWarnings(suppressMessages(import(src)))
      }
      
      msg_init("Joining data from ", src_txt, "...", print = info, prefix_time = TRUE)
      
      join_fn <- getExportedValue(paste0(join_object$type, "_join"), ns = asNamespace("dplyr"))
      join_cols <- join_object$by
      
      if (is.null(out_join)) {
        join_where <- vapply(FUN.VALUE = character(1),
                             join_cols,
                             function(x) paste0(ifelse(mode(out[[x]]) == "numeric", "", "'"),
                                                unique(out[[x]]),
                                                ifelse(mode(out[[x]]) == "numeric", "", "'"),
                                                collapse = ", "))
        join_where <- paste0(join_cols, " %in% c(", join_where, ")", collapse = " & ")
        if (!is.null(join_object$filter)) {
          join_where <- paste0(join_object$filter, " & (", join_where, ")")
        }
        
        # query other cBase
        out_join <- get_diver_data(date_range = NULL, # no date range, so base solely on previous dataset
                                   where = join_where,
                                   where_as_character = TRUE, # this leads to transformation with str2lang()
                                   diver_cbase = join_object$cbase,
                                   preset = NULL,
                                   diver_project = diver_project,
                                   diver_dsn = diver_dsn,
                                   diver_tablename = diver_tablename,
                                   distinct = distinct,
                                   limit = Inf, # no limit, so base solely on previous dataset
                                   auto_transform = FALSE, # if `auto_transform` was set to TRUE, this will be applied later
                                   review_qry = FALSE,
                                   in_background = FALSE, # if `in_background` was set to TRUE, this already runs in the background
                                   info = FALSE)
      }
      
      
      if (!is.null(join_object$select)) {
        out_join <- out_join |> select(!!!join_object$select)
      }
      if (!is.null(join_object$wide_names_from)) {
        if (!is.null(join_object$wide_name_trans)) {
          out_join <- out_join |>
            mutate(across(join_object$wide_names_from,
                          function(.x) !!str2lang(as.character(join_object$wide_name_trans))))
        }
        out_join <- out_join |>
          pivot_longer(-all_of(c(join_cols, join_object$wide_names_from)),
                       names_to = "type",
                       values_to = "value") |>
          unite(col = "col", join_object$wide_names_from, type) |>
          distinct(across(c(join_cols, "col")), .keep_all = TRUE) |>
          pivot_wider(names_from = col, values_from = value)
      }
      
      if (inherits(out_join, "sf")) {
        # otherwise join will fail
        loadNamespace("sf")
      }
      
      # the actual join
      out <- out |> join_fn(out_join, by = join_cols, suffix = c("", "2"))
      msg_ok(dimensions = dim(out), print = info)
      
    }, error = function(e) {
      msg_error(time = TRUE, print = info, font_red("==> Skipping this step\n"), format_error(e))
    })
    
  }
  
  # post-processing ----
  post_processing <- which(names(preset) %like% "^post-processing")
  if (length(post_processing) > 0) {
    msg_init("Post-processing data...", print = info, prefix_time = TRUE)
    tryCatch({
      
      for (p in post_processing) {
        postpr <- preset[[p]]
        # since post-processing must contain `x` to indicate the data set, set it here in the environment
        x <- out
        if (length(postpr) == 1 && is.character(postpr) && file.exists(postpr)) {
          # support R files
          postpr <- paste0(readLines(postpr), collapse = "\n")
        }
        out <- eval(parse(text = paste0("{\n", postpr, "\n}")))
      }
      msg_ok(dimensions = dim(out), print = info)
      
    }, error = function(e) {
      msg_error(time = TRUE, print = info, font_red("==> Skipping this step\n"), format_error(e))
    })
  }
  
  # distinct ----
  if (isTRUE(distinct)) {
    out_new <- distinct(out)
    if (nrow(out_new) < nrow(out)) {
      msg_init("Removing ", nrow(out) - nrow(out_new), " rows since ", font_blue("`distinct = TRUE`"), "...", print = info, prefix_time = TRUE)
      out <- out_new
      msg_ok(dimensions = dim(out), print = info)
    }
  }
  
  # post WHERE ----
  if (!is.null(tryCatch(post_where, error = function(e) 0))) {
    msg_init("Validating post-WHERE statement...", print = info, prefix_time = TRUE)
    # fill in columns from the 'di' or 'gl' object
    post_where <- where_convert_di_gl(substitute(post_where))
    # convert objects, this will return msg "OK"
    post_where <- where_convert_objects(deparse(substitute(post_where)), info = info)
    out_new <- out |> filter(!!post_where)
    if (nrow(out_new) < nrow(out)) {
      msg_init("Removing ", nrow(out) - nrow(out_new), " rows from ", font_blue("post_where"), "...", print = info, prefix_time = TRUE)
      out <- out_new
      msg_ok(dimensions = dim(out), print = info)
    }
  }
  
  # only real patients ----
  if (isTRUE(only_real_patients)) {
    out_new <- out
    if ("MateriaalKorteNaam" %in% colnames(out_new)) {
      out_new <- out_new |> filter(is.na(MateriaalKorteNaam) | MateriaalKorteNaam != "NPM")
    }
    if ("AanvragerCode" %in% colnames(out_new)) {
      out_new <- out_new |> filter(AanvragerCode %unlike% "^(NPM[-]|CERTE_QC)")
    }
    if (nrow(out_new) < nrow(out)) {
      msg_init("Removing ", nrow(out) - nrow(out_new), " rows since ", font_blue("`only_real_patients = TRUE`"), "...", print = info, prefix_time = TRUE)
      out <- out_new
      msg_ok(dimensions = dim(out), print = info)
    }
  }
  
  # only conducted tests ----
  if (isTRUE(only_conducted_tests) && "ResultaatTekst" %in% colnames(out)) {
    out_new <- out |> filter(is.na(ResultaatTekst) | ResultaatTekst %unlike% "Niet verricht|kweek niet ingezet|materiaal ongeschikt")
    if (nrow(out_new) < nrow(out)) {
      msg_init("Removing ", nrow(out) - nrow(out_new), " rows since ", font_blue("`only_conducted_tests = TRUE`"), "...", print = info, prefix_time = TRUE)
      out <- out_new
      msg_ok(dimensions = dim(out), print = info)
    }
  }
  
  # only validated ----
  if (isTRUE(only_validated)) {
    if ("ResultaatStatus" %in% colnames(out) && "McraStatus" %in% colnames(out) ) {
      out_new <- out |> filter(tolower(trimws(ResultaatStatus)) == "validated" | tolower(trimws(McraStatus)) %in% c("ReadValidated", "Closed"))
    } else if ("ResultaatStatus" %in% colnames(out)) {
      out_new <- out |> filter(tolower(trimws(ResultaatStatus)) == "validated")
    } else if ("McraStatus" %in% colnames(out)) {
      out_new <- out |> filter(tolower(trimws(McraStatus)) %in% c("ReadValidated", "Closed"))
    }
    if (nrow(out_new) < nrow(out)) {
      msg_init("Removing ", nrow(out) - nrow(out_new), " rows since ", font_blue("`only_validated = TRUE`"), "...", print = info, prefix_time = TRUE)
      out <- out_new
      msg_ok(dimensions = dim(out), print = info)
    }
  }
  
  # only requested ----
  if (isTRUE(only_requested) && "IsAangevraagd" %in% colnames(out)) {
    out_new <- out |> filter(IsAangevraagd == 1)
    if (nrow(out_new) < nrow(out)) {
      msg_init("Removing ", nrow(out) - nrow(out_new), " rows since ", font_blue("`only_requested = TRUE`"), "...", print = info, prefix_time = TRUE)
      out <- out_new
      msg_ok(dimensions = dim(out), print = info)
    }
  }
  
  # auto-transform ----
  if (isTRUE(auto_transform)) {
    msg_init("Transforming data set...", print = info, prefix_time = TRUE)
    if ("Ordernummer" %in% colnames(out)) {
      out$Ordernummer[out$Ordernummer %like% "[0-9]{2}[.][0-9]{4}[.][0-9]{4}"] <- gsub(".", "", out$Ordernummer[out$Ordernummer %like% "[0-9]{2}[.][0-9]{4}[.][0-9]{4}"], fixed = TRUE)
      out <- out |> arrange(desc(Ordernummer))
    }
    
    tryCatch({
      # transform data, and update column names
      out <- auto_transform(out, snake_case = snake_case)
      msg_ok(print = info)
    }, error = function(e) {
      msg_error(time = TRUE, print = info, font_red("==> Skipping this step\n"), format_error(e))
    })
    
  }
  
  # return ----
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
  
  if (is.null(query)) {
    return(invisible())
  }
  
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
