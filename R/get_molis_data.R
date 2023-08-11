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

#' @rdname get_diver_data
#' @param dates date range, can be length 1 or 2 (or more to use the min/max) to filter on the column `Ontvangstdatum`. Defaults to [this_year()]. Use `NULL` to set no date filter. Can also be years, or functions such as [`last_month()`][certetoolbox::last_month()].
#' @param review_where a [logical] to indicate whether the query must be reviewed first, defaults to `TRUE` in interactive mode and `FALSE` otherwise. This will always be `FALSE` in Quarto / R Markdown, since the output of [knitr::pandoc_to()] must be `NULL`.
#' @param select_preset settings for old `certedb_getmmb()` function
#' @param select settings for old `certedb_getmmb()` function
#' @param add_cols settings for old `certedb_getmmb()` function
#' @param info settings for old `certedb_getmmb()` function
#' @param first_isolates settings for old `certedb_getmmb()` function
#' @param eucast_rules settings for old `certedb_getmmb()` function
#' @inheritParams AMR::as.mo
#' @param mic settings for old `certedb_getmmb()` function
#' @param disk settings for old `certedb_getmmb()` function
#' @param zipcodes settings for old `certedb_getmmb()` function
#' @param ziplength settings for old `certedb_getmmb()` function
#' @param tat_hours settings for old `certedb_getmmb()` function
#' @param only_real_patients settings for old `certedb_getmmb()` function
#' @param only_conducted_tests settings for old `certedb_getmmb()` function
#' @param only_validated settings for old `certedb_getmmb()` function
#' @param only_show_query settings for old `certedb_getmmb()` function
#' @param auto_transform [logical] to apply [auto_transform()] to the resulting data set
#' @param ... not used anymore, previously settings for old `certetools::certedb_getmmb()` function
#' @details Using `certedb_query("your qry here")` is identical to using `certedb_getmmb(query = "your qry here")`.
#' @importFrom dplyr mutate arrange desc `%>%` rename
#' @importFrom dbplyr sql remote_query
#' @importFrom certestyle format2
#' @importFrom certetoolbox auto_transform ref_dir
#' @importFrom AMR as.mo eucast_rules first_isolate
#' @importFrom knitr pandoc_to
#' @export
certedb_getmmb <- function(dates = NULL,
                           where = NULL,
                           select_preset = "mmb",
                           preset = "mmb",
                           select = NULL,
                           add_cols = NULL,
                           info = interactive(),
                           first_isolates = FALSE,
                           eucast_rules = "all",
                           keep_synonyms = getOption("AMR_keep_synonyms", FALSE),
                           mic = FALSE,
                           disk = FALSE,
                           zipcodes = FALSE,
                           ziplength = 4,
                           tat_hours = FALSE,
                           only_real_patients = TRUE,
                           only_conducted_tests = TRUE,
                           only_validated = FALSE,
                           only_show_query = FALSE,
                           review_where = interactive(),
                           auto_transform = TRUE,
                           query = NULL,
                           ...) {
  
  # Connect to database
  conn <- db_connect(RMariaDB::MariaDB(),
                     dbname = unlist(read_secret("db.certemmb"))["dbname"],
                     host = unlist(read_secret("db.certemmb"))["host"],
                     port = as.integer(unlist(read_secret("db.certemmb"))["port"]),
                     username = unlist(read_secret("db.certemmb"))["username"],
                     password = unlist(read_secret("db.certemmb"))["password"],
                     print = info)
  on.exit(suppressWarnings(suppressMessages(try(db_close(conn, print = info), silent = TRUE))))
  user <- paste0("CERTE\\", Sys.info()["user"])
  
  if (is.null(query)) {
    
    eucast_rules_setting <- eucast_rules
    if (is.function(eucast_rules_setting)) {
      eucast_rules_setting <- "all"
    }
    
    dates_depsub <- deparse(substitute(dates))
    where_depsub <- deparse(substitute(where))
    if (all(gsub("-", "", where_depsub, fixed = TRUE) %like% "^[0-9]+$")) {
      #stop("Invalid `where`, it contains a date or year. ")
      warning("Using `where` as `dates[2]` (", where_depsub, ")", immediate. = TRUE, call. = FALSE)
      where <- NULL
      if (length(dates) == 1) {
        dates <- c(dates, where_depsub)
      } else if (length(dates) == 2) {
        dates[2] <- where_depsub
      }
    }
    
    # date range
    if (is.null(dates)) {
      # take current year
      dates <- c(format2(Sys.Date(), "yyyy"), "-01-01",
                 format2(Sys.Date(), "yyyy"), "-12-31")
    } else {
      # alleen jaar opgegeven
      if (dates[1] %like% "^[12][90][0-9][0-9]$") {
        dates[1] <- paste0(dates[1], "-01-01")
      }
      if (dates[2] %like% "^[12][90][0-9][0-9]$") {
        dates[2] <- paste0(dates[2], "-12-31")
      }
      
      dates <- as.character(dates)
      dates_int <- suppressWarnings(as.integer(dates))
      
      if (!is.na(dates_int[1])) {
        dates[1] <- as.character(as.Date(as.integer(dates[1]), origin = "1970-01-01"))
      }
      if (!is.na(dates_int[2])) {
        dates[2] <- as.character(as.Date(as.integer(dates[2]), origin = "1970-01-01"))
      }
      
      if (all(dates %unlike% "^[12][90][0-9][0-9]-[01][0-9]-[0123][0-9]$") && all(dates %in% c("", NA))) {
        # bestaat nog niet uit yyyy-mm-dd
        stop("Invalid value(s) for `dates`. Use format yyyy-mm-dd.")
      }
      if (length(dates) == 1 || (identical(dates[2], "") || identical(dates[2], NA_character_))) {
        # datum tot einde van jaar
        dates <- c(dates[1], paste0(format2(dates[1], "yyyy"), "-12-31"))
      } else if (length(dates) > 2) {
        dates <- c(min(as.Date(dates)), max(as.Date(dates)))
      }
      
      if (is.na(dates[1])) {
        warning("First `dates` element is NA.", immediate. = TRUE, call. = FALSE)
      }
      if (is.na(dates[2])) {
        warning("Second `dates` element is NA.", immediate. = TRUE, call. = FALSE)
      }
    }
    dates <- paste0("'", gsub('["\']', "", dates), "'")
    
    if (is.null(select)) {
      if (preset != "mmb") {
        select <- preset.read(preset)
      } else {
        select <- preset.read(select_preset)
      }
    }
    select <- select_translate_asterisk(select)
    
    if (!is.null(add_cols)) {
      add_cols <- unlist(add_cols)
      select <- c(select, select_translate_asterisk(add_cols))
    }
    
    needs_mic <- mic == TRUE || isTRUE(list(...)$MIC)
    needs_disk <- disk == TRUE || isTRUE(list(...)$DISK)
    if (needs_mic) {
      select <- c(select,
                  names(certedb::db)[which(names(certedb::db) %like% "^i_mic.*_mic$")])
    }
    if (needs_disk) {
      select <- c(select,
                  names(certedb::db)[which(names(certedb::db) %like% "^i_disk.*_disk$")])
    }
    if (zipcodes == TRUE) {
      select <- c(select, "p.postcode")
      msg("Note: Adding 'p.postcode'.", print = info)
    }
    
    select <- gsub(",$", "", select)
    select <- unique(select)
    select <- select |> paste0(collapse = ",\n  ")
    
    query <- paste0("SELECT\n  ",
                    select, "\n",
                    "FROM\n  ",
                    "{from}\n",
                    "WHERE\n  ",
                    "{datelimit}",
                    "{additional_where}")
    
    ignore_start_stop <- FALSE
    
    if (tat_hours == FALSE) {
      if (any(dates_depsub %like% "^where\\(")) {
        where <- where_R2SQL(dates_depsub, info = info)
      } else {
        where <- where_R2SQL(deparse(substitute(where)), info = info)
      }
      if (where == "enddate") {
        where <- ""
      }
    }
    
    # extra where
    if (where != "") {
      if (where %like% "[.]ontvangstdatum " || where %like% "[.]jaar ") {
        if (isTRUE(info)) {
          warning("`dates` will be ignored because date criteria has been set with `where`.",
                  call. = FALSE,
                  immediate. = TRUE)
        }
        ignore_start_stop <- TRUE
        query <- sub("{additional_where}", where, query, fixed = TRUE)
      } else {
        query <- sub("{additional_where}", paste0("\n  AND ", where), query, fixed = TRUE)
      }
    } else {
      query <- sub("{additional_where}", "", query, fixed = TRUE)
    }
    
    if (ignore_start_stop == FALSE) {
      query <- sub("{datelimit}", paste("o.ontvangstdatum BETWEEN", dates[1], "AND", dates[2]), query, fixed = TRUE)
    } else {
      query <- sub("{datelimit}", "", query, fixed = TRUE)
    }
    
    if (only_real_patients == TRUE) {
      if (query %like% "[.]is_echte_patient =") {
        stop("`is_echte_patient` cannot be set while `only_real_patients = TRUE`.")
      }
      query <- paste(query, "\n  AND u.is_echte_patient = TRUE")
    }
    if (only_conducted_tests == TRUE) {
      if (query %like% "[.]is_verricht =") {
        stop("`is_verricht` cannot be set while `only_conducted_tests = TRUE`.")
      }
      query <- paste(query, "\n  AND u.is_verricht = TRUE")
    }
    if (!missing(only_validated)) {
      if (query %like% "[.]uitslag_int =") {
        stop("`uitslag_int` cannot be set while `only_validated` is set.")
      }
      if (only_validated == TRUE) {
        query <- paste(query, "\n  AND uitslag_int <> '(in behandeling)'")
      }
    }
    
    # FROM clausule
    # primair van uitslagen halen, orders eraan
    from <- paste0("temporary_certemm_uitslagen AS u",
                   "\n  LEFT JOIN\n  ",
                   "temporary_certemm_orders AS o ON o.ordernr = u.ordernr")
    from <- from_addjoins(query, from)
    query <- sub("{from}", from, query, fixed = TRUE)
    
    if (only_show_query == TRUE) {
      db_close(conn, print = info)
      return(sql(paste0("\n", query, "\n")))
    }
    
  }
  
  msg_init("Retrieving initial data...", print = info)
  # remove last semicolon, otherwise returns error
  query <- gsub(";$", "", query)
  out <- conn |> tbl(sql(query))
  qry <- remote_query(out)
  msg_ok(time = TRUE, dimensions = dim(out), print = info)
  
  if (isTRUE(review_where) && interactive() && is.null(pandoc_to())) {
    choice <- utils::menu(title = paste0("\nCollect data with this WHERE? (0 for Cancel)\n\n",
                                         gsub("(.*)\nWHERE\n  (.*)", "\\2", qry_beautify(query))),
                          choices = c("Yes", "No", "Print query"),
                          graphics = FALSE)
    if (choice == 3) {
      cat(paste0("\nQuery:\n\n", qry_beautify(query), "\n\n"))
      choice <- utils::menu(title = "\nCollect data?",
                            choices = c("Yes", "No"),
                            graphics = FALSE)
    }
    if (choice %in% c(0, 2)) { # 'Enter an item from the menu, or 0 to exit'
      return(invisible())
    }
  }
  
  msg_init("Collecting data...", print = info)
  tryCatch({
    out <- collect(out)
  },
  error = function(e) {
    msg_error(print = info)
    stop(e$message, call. = FALSE)
  })
  msg_ok(time = TRUE, dimensions = dim(out), print = info)
  db_close(conn, print = info)
  
  if (tat_hours == TRUE && select %like% " dlt[.]") {
    msg_init("Calculating time differences in hours...", print = info)
    out <- out |>
      mutate(val1_usr_1e = val1_usr_1e |> as.character(),
             val2_usr_1e = val2_usr_1e |> as.character(),
             aut_usr_1e = aut_usr_1e |> as.character(),
             val1_usr_def = val1_usr_def |> as.character(),
             val2_usr_def = val2_usr_def |> as.character(),
             aut_usr_def = aut_usr_def |> as.character()) |>
      mutate(val1_1e.val2_1e = (difftime(val2_1e, val1_1e, units = "mins") / 60) |> as.double(),
             val2_1e.aut_1e = (difftime(aut_1e, val2_1e, units = "mins") / 60) |> as.double(),
             ontvangst.val2_1e = (difftime(val2_1e, ontvangstdatumtijd, units = "mins") / 60) |> as.double(),
             ontvangst.aut_1e = (difftime(aut_1e, ontvangstdatumtijd, units = "mins") / 60) |> as.double(),
             val1_def.val2_def = (difftime(val2_def, val1_def, units = "mins") / 60) |> as.double(),
             val2_def.aut_def = (difftime(aut_def, val2_def, units = "mins") / 60) |> as.double(),
             ontvangst.val2_def = (difftime(val2_def, ontvangstdatumtijd, units = "mins") / 60) |> as.double(),
             ontvangst.aut_def = (difftime(aut_def, ontvangstdatumtijd, units = "mins") / 60) |> as.double(),
             dgn_ontvangst.val1_1e = as.integer(as.Date(val1_1e) - ontvangstdatum),
             dgn_ontvangst.val1_def = as.integer(as.Date(val1_def) - ontvangstdatum),
             dgn_ontvangst.val2_1e = as.integer(as.Date(val2_1e) - ontvangstdatum),
             dgn_ontvangst.val2_def = as.integer(as.Date(val2_def) - ontvangstdatum),
             dgn_ontvangst.aut_1e = as.integer(as.Date(aut_1e) - ontvangstdatum),
             dgn_ontvangst.aut_def = as.integer(as.Date(aut_def) - ontvangstdatum),
             weekdag = weekdays(as.Date(ontvangstdatum), abbreviate = FALSE)
      )
    msg_ok(print = info)
    
    msg_init("Adding region/year/quarter column `reg_jr_q`...", print = info)
    out <- out |>
      mutate(reg_jr_q = paste0(noord_zuid |> substr(1, 1), "|", format2(ontvangstdatum, "yyyy-QQ")))
    msg_ok(print = info)
  }
  
  if ((isTRUE(auto_transform) || isTRUE(first_isolates) || !isFALSE(eucast_rules_setting)) && !is.null(select) && select %like% " i[.]" && nrow(out) > 0) {
    if (!"mo" %in% colnames(out) && "bacteriecode" %in% colnames(out)) {
      if (!all(is.na(out$bacteriecode))) {
        suppressWarnings(
          out <- out |>
            rename(bacteriecode_oud = bacteriecode) |>
            mutate(bacteriecode = as.mo(bacteriecode_oud, keep_synonyms = keep_synonyms))
        )
      }
    }
    if (!"bacteriecode" %in% colnames(out)) {
      msg("Note: No isolates available.", print = info)
    } else {
      if (!needs_mic && !needs_disk && eucast_rules_setting != FALSE) {
        msg_init(paste("Applying EUCAST", eucast_rules_setting, "rules..."), print = info)
        out <- suppressMessages(suppressWarnings(eucast_rules(out, col_mo = "bacteriecode", rules = eucast_rules_setting, info = FALSE)))
        msg_ok(print = info)
      }
      
      if (isTRUE(first_isolates)) {
        patid <- colnames(out)[colnames(out) %in% c("patid", "patidnb")][1]
        if (all(c("bacteriecode", "ontvangstdatum", patid) %in% colnames(out), na.rm = TRUE)) {
          msg_init("Applying first isolates...", print = info)
          out$eerste_isolaat <- first_isolate(
            x = out, col_date = "ontvangstdatum", col_patient_id = patid, col_mo = "bacteriecode",
            method = "episode-based", episode_days = 365, specimen_group = NULL, info = FALSE)
          out$eerste_isolaat_gewogen <- first_isolate(
            x = out, col_date = "ontvangstdatum", col_patient_id = patid, col_mo = "bacteriecode",
            method = "phenotype-based", episode_days = 365, specimen_group = NULL, info = FALSE)
          
          if ("mtrlgroep" %in% colnames(out)) {
            # also add specimen-specific isolates
            out$eerste_urineisolaat <- first_isolate(
              x = out, col_date = "ontvangstdatum", col_patient_id = patid, col_mo = "bacteriecode", col_specimen = "mtrlgroep",
              method = "episode-based", episode_days = 365, specimen_group = "Urine", info = FALSE)
            out$eerste_urineisolaat_gewogen <- first_isolate(
              x = out, col_date = "ontvangstdatum", col_patient_id = patid, col_mo = "bacteriecode", col_specimen = "mtrlgroep",
              method = "phenotype-based", episode_days = 365, specimen_group = "Urine", info = FALSE)
            
            out$eerste_bloedisolaat <- first_isolate(
              x = out, col_date = "ontvangstdatum", col_patient_id = patid, col_mo = "bacteriecode", col_specimen = "mtrlgroep",
              method = "episode-based", episode_days = 365, specimen_group = "Bloed", info = FALSE)
            out$eerste_bloedisolaat_gewogen <- first_isolate(
              x = out, col_date = "ontvangstdatum", col_patient_id = patid, col_mo = "bacteriecode", col_specimen = "mtrlgroep",
              method = "phenotype-based", episode_days = 365, specimen_group = "Bloed", info = FALSE)
            
            out$eerste_pusisolaat <- first_isolate(
              x = out, col_date = "ontvangstdatum", col_patient_id = patid, col_mo = "bacteriecode", col_specimen = "mtrlgroep",
              method = "episode-based", episode_days = 365, specimen_group = "Pus", info = FALSE)
            out$eerste_pusisolaat_gewogen <- first_isolate(
              x = out, col_date = "ontvangstdatum", col_patient_id = patid, col_mo = "bacteriecode", col_specimen = "mtrlgroep",
              method = "phenotype-based", episode_days = 365, specimen_group = "Pus", info = FALSE)
            
            out$eerste_respisolaat <- first_isolate(
              x = out, col_date = "ontvangstdatum", col_patient_id = patid, col_mo = "bacteriecode", col_specimen = "mtrlgroep",
              method = "episode-based", episode_days = 365, specimen_group = "Respiratoir", info = FALSE)
            out$eerste_respisolaat_gewogen <- first_isolate(
              x = out, col_date = "ontvangstdatum", col_patient_id = patid, col_mo = "bacteriecode", col_specimen = "mtrlgroep",
              method = "phenotype-based", episode_days = 365, specimen_group = "Respiratoir", info = FALSE)
            
            out$eerste_fecesisolaat <- first_isolate(
              x = out, col_date = "ontvangstdatum", col_patient_id = patid, col_mo = "bacteriecode", col_specimen = "mtrlgroep",
              method = "episode-based", episode_days = 365, specimen_group = "Feces", info = FALSE)
            out$eerste_fecesisolaat_gewogen <- first_isolate(
              x = out, col_date = "ontvangstdatum", col_patient_id = patid, col_mo = "bacteriecode", col_specimen = "mtrlgroep",
              method = "phenotype-based", episode_days = 365, specimen_group = "Feces", info = FALSE)
            
          }
          msg_ok(print = info)
        }
      }
    }
  }
  
  if (zipcodes == TRUE && ziplength[1L] < 6) {
    msg_init("Transforming zip codes...", print = info)
    out <- out |> mutate(postcode = postcode |> substr(1, ziplength[1L]))
    msg_ok(print = info)
  }
  
  if (isTRUE(auto_transform)) {
    msg_init("Transforming data set...", print = info)
    if ("ordernr" %in% colnames(out)) {
      out <- out |> arrange(desc(ordernr))
    } else if ("ontvangstdatum" %in% colnames(out)) {
      out <- out |> arrange(desc(ontvangstdatum))
    }
    out <- auto_transform(out)
    # also transform 0/1 values to FALSE/TRUE
    for (col in colnames(out)) {
      vals <- out[, col, drop = TRUE]
      if (is.integer(vals) && all(vals %in% c(0, 1, NA)) && !all(is.na(vals))) {
        out[, col] <- as.logical(vals)
      }
    }
    msg_ok(print = info)
  }
  
  as_certedb_tibble(out,
                  type = "MOLIS",
                  cbase = "certemmb",
                  qry = qry,
                  datetime = Sys.time(),
                  user = user)
}

#' @rdname get_diver_data
#' @export
certedb_getmmb_tat <- function(dates = NULL,
                               where = NULL,
                               add_cols = NULL,
                               info = interactive(),
                               only_real_patients = TRUE,
                               only_conducted_tests = TRUE,
                               only_validated = TRUE,
                               only_show_query = FALSE,
                               ...) {
  
  where_test <- deparse(substitute(where))
  if (any(where_test %like% "^where\\(")) {
    where <- where_R2SQL(where_test, info = info)
  } else {
    where <- where_R2SQL(deparse(substitute(where)), info = info)
  }
  # deparse-foutje als getmm_tat gebruikt wordt:
  if (where == "enddate") {
    where <- ""
  }
  
  certedb_getmmb(dates = dates,
                 where = where,
                 add_cols = add_cols,
                 info = info,
                 only_real_patients = only_real_patients,
                 only_conducted_tests = only_conducted_tests,
                 only_validated = only_validated,
                 only_show_query = only_show_query,
                 select_preset = "tat",
                 first_isolates = FALSE,
                 eucast_rules = FALSE,
                 mic = FALSE,
                 rsi = FALSE,
                 tat_hours = TRUE,
                 ...)
}

select_translate_asterisk <- function(select) {
  select.bak <- select
  select_list <- select |> as.list()
  for (i in seq_len(length(select))) {
    # alle tabel.* uitschrijven naar c(tabel.a, tabel.b, ...)
    if (select[i] %like% "[.][*]") {
      found <- gregexpr("[.][*]", select[i])
      select_tbl <- substr(select[i], 1, found |> as.integer() - 1)
      select_items <- certedb::db[names(certedb::db) %like% paste0("^", select_tbl, "[.][a-z]+")] |>
        unlist() |>
        unname()
      select_list[[i]] <- select_items
    }
  }
  select_list <- select_list |> unlist()
  # ondersteuning voor add_cols = c("test" = db$m.mtrlcode) -> "m.mtrlcode AS test"
  for (i in seq_len(length(select.bak))) {
    if (!is.null(names(select.bak)[i])) {
      if (names(select.bak)[i] != "") {
        select_list[i] <- paste(select_list[i], "AS", names(select.bak)[i])
      }
    }
  }
  select_list
}

from_addjoins <- function(query, from) {
  # dependencies:
  
  # alles:
  # u < o
  # u < d (datums)
  # u < l_uitvoer
  # u < o < l_instelling
  # u < o < l_ontvangst
  # u < t
  # u < m
  # u < o < a
  # u < o < aanvr
  # u < o < beh
  # u < o < p
  # u < i
  # u < i_mic
  # u < i_disk
  # u < b
  
  if (query %like% " dlt[.]") {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_doorlooptijden AS dlt ON dlt.ordernr_testcode_mtrlcode = u.ordernr_testcode_mtrlcode")
  }
  if (query %like% " r[.]") {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "certemm_res AS r ON r.ordernr = u.ordernr AND r.anamc = u.testcode AND r.mtrlcode = u.mtrlcode AND r.stamteller = u.stam")
  }
  if (query %like% " d[.]") {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_aanmaakdatums AS d ON d.ordernr_testcode_mtrlcode = u.ordernr_testcode_mtrlcode")
  }
  if (query %like% " l_instelling[.]") {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_locaties AS l_instelling ON l_instelling.instelling = o.instelling")
  }
  if (query %like% " l_ontvangst[.]") {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_locaties AS l_ontvangst ON l_ontvangst.instelling = o.recsitenb")
  }
  if (query %like% " l_uitvoer[.]") {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_locaties AS l_uitvoer ON l_uitvoer.instelling = u.uitvafd")
  }
  if (query %like% " t[.]") {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_testgroepen AS t ON t.testcode = u.testcode")
  }
  if (query %like% " m[.]") {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_materiaalgroepen AS m ON m.mtrlcode = u.mtrlcode")
  }
  if (query %like% " aanvr[.]" && query %like% " a[.]") {
    warning("Encountered table references `a.*` and `aanvr.*`. This will lead to an extra and unnecessary LEFT JOIN.",
            call. = FALSE,
            immediate. = TRUE)
  }
  if (query %like% " a[.]") {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_aanvragers_praktijken AS a ON a.aanvragercode = o.aanvrager")
  }
  if (query %like% " aanvr[.]") {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_aanvragers_praktijken AS aanvr ON aanvr.aanvragercode = o.aanvrager")
  }
  if (query %like% " beh[.]") {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_aanvragers_praktijken AS beh ON beh.aanvragercode = o.behandelaar")
  }
  if (query %like% " p[.]") {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "certemm_pat AS p ON p.patidnb = o.patidnb")
  }
  if (query %like% " cbs[.]") {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_cbs AS cbs ON cbs.postcode = SUBSTRING(p.postcode FROM 1 FOR 4)")
  }
  if (query %like% " pat[.]") {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_patienten AS pat ON pat.patidnb = o.patidnb")
  }
  if (query %like% " i[.]") {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_isolaten_rsi AS i ON i.ordernr_testcode_mtrlcode_stam = u.ordernr_testcode_mtrlcode_stam")
  }
  if (query %like% " i_mic[.]") {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_isolaten_mic AS i_mic ON i_mic.ordernr_testcode_mtrlcode_stam = u.ordernr_testcode_mtrlcode_stam")
  }
  if (query %like% " i_disk[.]") {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_isolaten_disk AS i_disk ON i_disk.ordernr_testcode_mtrlcode_stam = u.ordernr_testcode_mtrlcode_stam")
  }
  if (query %like% " b[.]") {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_bacterienlijst AS b ON b.bacteriecode = u.bacteriecode")
  }
  if (query %like% " g[.]") {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_grampreparaten AS g ON g.ordernr = u.ordernr")
  }
  from
}

where_R2SQL <- function(where = NULL, info = TRUE) {
  where <- where %>%
    paste0(" ", ., " ") %>%
    gsub('"', "'", ., fixed = TRUE) %>%
    paste0(collapse = " ") %>%
    gsub(" {2,255}", " ", .)
  
  if (where |> substr(1, 1) == "'") {
    where <- where |> substr(2, nchar(where))
  }
  if (where |> substr(nchar(where), nchar(where)) == "'") {
    where <- where |> substr(1, nchar(where) - 1)
  }
  
  if (where %like% "^ [where(].*[)] $") {
    where <- paste0(" ", substr(where, 8, nchar(where) - 2), " ")
  }
  
  if (where == " NULL ") {
    return("")
  }
  
  where <- where %>%
    # spaties voor en na toevoegen
    paste0(" ", ., " ") %>%
    # logische negatie verwerken:
    # !test LIKE 'a' -> test NOT LIKE 'a'
    # !(test LIKE 'a') -> (test NOT LIKE 'a')
    gsub(" [!]([(]*)([0-9a-zA-Z$_.]+) ", " \\1\\2 NOT ", .) %>%
    # !is.na(test) of !is.null(test) -> test IS NOT NULL
    gsub(" (!is.na|!is.null)[(]([0-9a-zA-Z$_.]+)[)] ", " \\2 IS NOT NULL ", .) %>%
    # is.na(test) of is.null(test) -> test IS NULL
    gsub(" (is.na|is.null)[(]([0-9a-zA-Z$_.]+)[)] ", " \\2 IS NULL ", .) %>%
    # komma's naar '&' als dit niet opgevolgd wordt door een getal of aanhalingsteken
    # gsub(', ?([a-zA-Z])', ' & \\1', .) %>% # 2018-10-16 zie mailwisseling - gaat fout bij waarden die , bevatten
    # operators vertalen
    gsub(" != ", " <> ", ., fixed = TRUE) %>%
    gsub(" ! = ", " <> ", ., fixed = TRUE) %>%
    gsub(" & ", " AND ", ., fixed = TRUE) %>%
    gsub(" && ", " AND ", ., fixed = TRUE) %>%
    gsub(" | ", " OR ", ., fixed = TRUE) %>%
    gsub(" || ", " OR ", ., fixed = TRUE) %>%
    gsub(" == ", " = ", ., fixed = TRUE) %>%
    gsub(" %in% ", " IN ", ., fixed = TRUE) %>%
    gsub(" c(", " (", ., fixed = TRUE) %>%
    gsub(" %like_case% ", " REGEXP BINARY ", ., fixed = TRUE) %>%
    gsub(" %like% ", " REGEXP ", ., fixed = TRUE) %>%
    gsub(" %unlike_case% ", " NOT REGEXP BINARY ", ., fixed = TRUE) %>%
    gsub(" %unlike% ", " NOT REGEXP ", ., fixed = TRUE) %>%
    gsub(" NOT = ", " != ", ., fixed = TRUE) # anders fout: !a == b -> a NOT = b
  
  # ondersteuning voor R-evaluaties:
  # o.jaar IN 2016:2018 -> o.jaar IN (2016, 2017, 2018)
  eval_num <- gregexpr(" [(]*[0-9]+[:][0-9]+[)]*", where)
  if (unlist(eval_num)[1] != -1) {
    starts <- eval_num |> unlist()
    lengths <- attributes(eval_num[[1]])$match.length
    where_backup <- where
    for (i in seq_len(length(starts))) {
      # getallen als 2015:2017 zoeken
      nums_old <- where_backup |>
        substr(starts[i] + 1,
               starts[i] + lengths[i])
      
      # uitrekenen en omzetten naar MySQL lijst
      nums_new <- eval(parse(text = nums_old)) |>
        paste0(collapse = ", ") %>%
        paste0(" (", . , ") ")
      
      # nieuwe where maken
      where <- sub(nums_old, nums_new, where, fixed = TRUE)
    }
  }
  
  # ondersteuning van evaluaties zoals mtcars$mpg en last_week()
  obj_refs <- gregexpr("([a-zA-Z0-9._]+[$]`?[a-zA-Z0-9._]+`?|[a-zA-Z0-9._]+[(][)])", where)
  if (unlist(obj_refs)[1] != -1) {
    starts <- obj_refs |> unlist()
    lengths <- attributes(obj_refs[[1]])$match.length
    where_backup <- where
    for (i in seq_len(length(starts))) {
      # objecten als mtcars$mpg zoeken
      obj <- where_backup |>
        substr(starts[i],
               starts[i] + lengths[i])
      
      # evalueren
      obj_evaluated <- eval(parse(text = obj))
      if ((length(obj_evaluated) != 1 &
           !is.numeric(obj_evaluated) &
           any(obj_evaluated %unlike% "[a-z][.][a-z]")) |
          inherits(obj_evaluated, c("Date", "POSIXt"))) {
        obj_evaluated <- paste0("'", obj_evaluated, "'")
      }
      if (length(obj_evaluated) > 1) {
        # naar MySQL-lijst omzetten
        obj_evaluated <- obj_evaluated |>
          paste0(collapse = ", ") %>%
          paste0(" (", . , ")")
      }
      msg(paste0("Replaced element `",
                 trimws(obj),
                 "` with value: ",
                 ifelse(nchar(obj_evaluated) > 25,
                        paste0(obj_evaluated |> substr(1, 25), "...)"),
                        obj_evaluated)),
          print = info)
      
      # spatie toevoegen
      obj_evaluated <- paste0(obj_evaluated, " ")
      
      # nieuwe where maken
      where <- sub(obj, obj_evaluated, where, fixed = TRUE)
    }
  }
  
  # ondersteuning voor variabelen uit Global Environment
  where_list <- where |> strsplit(" ") |> unlist()
  for (i in seq_len(length(where_list))) {
    if (where_list[i] %in% ls(envir = .GlobalEnv)) {
      newval <- eval(parse(text = where_list[i]))
      if (NCOL(newval) == 1) {
        if (!all(newval %like% "^[0-9.,]+$")) {
          newval <- paste0("'", newval, "'")
        }
        if (length(newval) > 1) {
          # naar MySQL-lijst omzetten
          newval <- newval |>
            paste0(collapse = ", ") %>%
            paste0("(", . , ")")
        }
        msg(paste0("Replaced object `",
                   where_list[i],
                   "` with value: ",
                   ifelse(nchar(newval) > 25,
                          paste0(newval |> substr(1, 25), "...)"),
                          newval)),
            print = info)
        where_list[i] <- newval
      } else {
        stop(paste0("variable `",
                    where_list[i],
                    "` can only be a single value or vector."))
      }
    }
  }
  where <- where_list |> paste0(collapse = " ") %>%
    # extra spaties verwijderen
    gsub(" {2,255}", " ", .) |>
    trimws()
  
  # proberen om expressie te evalueren als er een functie voorkomt (dus met haakje)
  where_list <- where |> strsplit("=") |> unlist()
  for (i in seq_len(length(where_list))) {
    if (where_list[i] %like% "[(]") {
      where_list[i] <- tryCatch(paste0("'",
                                       eval(parse(text = trimws(where_list[i]))),
                                       "'"),
                                error = function(e) where_list[i])
    }
  }
  where <- where_list |> paste0(collapse = " = ") %>%
    # extra spaties verwijderen
    gsub(" {2,255}", " ", .) |>
    trimws()
  
  if (where %like% "^['].+[']$" || where %like% '^["].+["]$') {
    where <- where |> substr(2, nchar(.) - 1)
  }
  
  # extra witregels maken zodat SQL-query de juiste indents krijgt
  if (where %like% " OR ") {
    where <- paste0("(", gsub(" OR ", "\n    OR ", where, fixed = TRUE), ")")
    if (where %like% " AND ") {
      where <- paste0(gsub(" AND ", "\n    AND ", where, fixed = TRUE))
    }
  } else if (where %like% " AND ") {
    where <- paste0(gsub(" AND ", "\n  AND ", where, fixed = TRUE))
  }
  where
}

diff_min_sec <- function(diff) {
  mins <- diff |> as.integer() |> as.double()
  secs <- ((diff |> as.double() - mins) * 60) |> round(0)
  if (mins != 0) {
    diff_text <- paste(mins, "minutes and ")
  } else {
    diff_text <- ""
  }
  diff_text <- paste0(diff_text, secs, " seconds.")
  diff_text
}

preset.list <- function(fullname = FALSE, recursive = FALSE) {
  lst <- list.files(path = read_secret("path.refmap"),
                    pattern = "^(preset_).*(.sql)$",
                    ignore.case = FALSE,
                    recursive = recursive)
  if (fullname == TRUE) {
    lst
  } else {
    gsub("^(preset_)(.*)(.sql)$", "\\2", lst)
  }
}

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

preset.exists <- function(name, returnlocation = FALSE) {
  found <- all(paste0('preset_', name, '.sql') %in% preset.list(TRUE))
  if (returnlocation == TRUE) {
    ref_dir(paste0('preset_', name, '.sql'))
  } else {
    found
  }
}

preset.read <- function(name) {
  lst <- list(0)
  for (i in seq_len(length(name))) {
    if (gsub("\\+", "/", name[i]) %unlike% "/") {
      # check read_secret("path.refmap")
      if (!preset.exists(name[i])) {
        stop('Preset `', name[i], '` not found, file does not exist: ',
             preset.exists(name[i], returnlocation = TRUE), '.',
             call. = FALSE)
      }
      lst[[i]] <- utils::read.table(file = ref_dir(paste0('preset_', name[i], '.sql')),
                                    sep = "\n",
                                    header = FALSE,
                                    stringsAsFactors = FALSE)[,1]
    } else {
      # dan is het een volledige bestandsnaam
      if (file.exists(name[i])) {
        lst[[i]] <- utils::read.table(file = name[i],
                                      sep = "\n",
                                      header = FALSE,
                                      stringsAsFactors = FALSE)[,1]
      } else if (file.exists(paste0(getwd(), '/', name[i]))) {
        lst[[i]] <- utils::read.table(file = paste0(getwd(), '/', name[i]),
                                      sep = "\n",
                                      header = FALSE,
                                      stringsAsFactors = FALSE)[,1]
      } else {
        stop('File `', name[i], '` not found.', call. = FALSE)
      }
    }
  }
  lst |> unlist() |> unique() %>% gsub(",$", "", .)
}

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

qry_beautify <- function(query) {
  query <- query %>%
    strsplit("\n") %>%
    unlist()
  
  for (i in seq_len(length(query))) {
    query[i] <- query[i] %>%
      strsplit("(--|#)") %>%
      unlist() %>%
      gsub(";", " ;", ., fixed = TRUE) %>%
      gsub(" {2,255}", " ", ., fixed = FALSE) %>%
      trimws()
  }
  query <- query[query != "" & !is.na(query)] %>% paste0(collapse = " ")
  if (query %>% substr(nchar(.), nchar(.)) == ";") {
    query <- query %>% substr(0, nchar(.) - 1)
  }
  
  query <- gsub("^([a-z]+) ",
                "\\U\\1 ",
                query,
                perl = TRUE,
                ignore.case = TRUE)
  
  query <- gsub(" (select|from|left|right|join|as|on|in|where|group by|order by|limit|trim|and|case|when|then|end|else|min|max|date|time|replace|asc|desc|between) ",
                " \\U\\1 ",
                query,
                perl = TRUE,
                ignore.case = TRUE)
  
  query <- query %>%
    gsub("^(CREATE DATABASE|DROP DATABASE|USE|CREATE TABLE|ALTER TABLE|DROP TABLE|DESCRIBE|SELECT|INSERT|UPDATE|DELETE|REPLACE|TRUNCATE|START TRANSACTION|COMMIT|ROLLBACK) ", "\\U\\1\n", ., ignore.case = TRUE, perl = TRUE) %>%
    gsub(" (SELECT|FROM|LEFT JOIN|RIGHT JOIN|JOIN|WHERE|LIMIT|GROUP BY|ORDER BY) ", "\n\\U\\1\n", ., ignore.case = TRUE, perl = TRUE) %>%
    gsub("\n", "\n  ", .) %>%
    gsub("  (SELECT|FROM|LEFT JOIN|RIGHT JOIN|JOIN|WHERE|LIMIT|GROUP BY|ORDER BY)\n", "\\U\\1\n", ., ignore.case = TRUE, perl = TRUE) %>%
    gsub("( BETWEEN '[0-9-]+' AND)", "\\1_betweendate", .) %>% # AND van BETWEEN datums niet aanpassen
    gsub(" AND ", "\n  AND ", .) %>% # alle AND in WHERE op nieuwe regel
    gsub("AND_betweendate", "AND", .) # de 'between AND' terugzetten
  
  if (query %>% strsplit("(SELECT|FROM)") %>% unlist() %>% length() == 3) {
    select_list <- query %>%
      gsub(".*SELECT(.*)FROM.*", "\\1", .) %>%
      gsub(", ", ",\n  ", .) %>%
      trimws() %>%
      paste(" ", .)
    query <- gsub("(.*SELECT).*(FROM.*)", paste0("\\1\n", select_list, "\n\\2"), query)
  }
  query
}
