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


#' SQL-query uitvoeren op MySQL-/MariaDB-database
#'
#' Een SQL-query uitvoeren op een MySQL-/MariaDB-database van bijv. Certe. De output krijgt een \code{qry}-attribuut, dat met \code{\link{qry}} opgehaald kan worden.
#' @param query (Bestand met) SQL-tekst die uitgevoerd moet worden. Tabelnamen in de query hoeven geen \code{"temporary_"} of \code{"certemm_"} te bevatten en zijn hoofdletterongevoelig. Deze query wordt bij het object opgeslagen als eigenschap \code{query} dat bekeken kan worden met de functie \code{\link{qry}}.
#' @param limit Standaard is 10.000.000. Het aantal rijen dat maximaal opgehaald moet worden.
#' @param con Standaard is leeg, waarmee de verbinding gemaakt wordt op basis van de omgevingsvariabelen van de huidige gebruiker: \code{"DB_HOST"}, \code{"DB_PORT"}, \code{"DB_USERNAME"} en \code{"DB_PASSWORD"}.
#' @param dbname Standaard is \code{"certemmb"}. Naam van de database die geselecteerd moet worden. Wordt genegeerd als \code{con} al een bestaande verbinding is.
#' @param info Standaard is \code{TRUE}. Printen van voortgang van run/fetch en het uiteindelijke aantal rijen en kolommen dat gedownload is.
#' @param check_mtrl_test_codes Standaard is \code{TRUE}. Controleren van materiaal- en testcodes in de tijdelijke tabellen van de database. Hiervoor moet ook \code{info = TRUE} zijn.
#' @param binary_as_logical Standaard is \code{TRUE}. Kolommen die alleen de waarden \code{0} en/of \code{1} bevatten, tranformeren naar logical m.b.v. \code{\link{tbl_binary2logical}}.
#' @param auto_append_prefix Standaard is \code{TRUE}. Wanneer tabellen die voorkomen in de query niet bestaan, wordt gezocht naar tabellen met dezelfde naam die beginnen met \code{"temporary_"} of \code{"certemm_"}. Wanneer zo'n tabel gevonden, wordt die gebruikt.
#' @param auto_transform Standaard is \code{TRUE}. Automatisch alle gedownloade kolommen transformeren met \code{\link{tbl_guess_columns}}.
#' @inheritParams tbl_guess_columns
#' @param ... Overige ongebruikte parameters
#' @export
#' @seealso \code{\link{certedb}} voor alleen het verbinden met de Certe-databaseserver en \code{\link{certedb_getmmb}} om ineens alle relevante MMB-gegevens te downloaden.
#' @examples
#' \dontrun{
#' locaties <- certedb_query("SELECT * FROM temporary_certemm_locaties")
#' locaties <- certedb_query("SELECT * FROM temporary_certemm_locaties", limit = 5)
#'
#' # Door `auto_append_prefix = TRUE` wordt:
#' locaties <- certedb_query("SELECT * FROM locaties")
#' # vertaald naar:
#' locaties <- certedb_query("SELECT * FROM temporary_certemm_locaties")
#' }
certedb_query <- function(query,
                          limit = 1e+07,
                          con = NULL,
                          dbname = 'certemmb',
                          info = TRUE,
                          check_mtrl_test_codes = TRUE,
                          binary_as_logical = TRUE,
                          auto_append_prefix = TRUE,
                          auto_transform = TRUE,
                          datenames = 'en',
                          dateformat = '%Y-%m-%d',
                          timeformat = '%H:%M',
                          decimal.mark = '.',
                          big.mark = '',
                          timezone = 'UTC',
                          na = c("", "NULL", "NA"),
                          ...) {
  
  if (!is.numeric(limit)) {
    stop('Not a valid limit.')
  }
  
  # voor als certedb_query(sql(...)) gebruikt wordt:
  query <- query %>% as.character()
  
  # als query bestand is, dan deze lezen
  if (file.exists(query)) {
    certedb_timestamp('Reading query from file... ', print = info, timestamp = FALSE, appendLF = FALSE)
    file_con <- file(query, encoding = "UTF-8")
    # bestand inlezen, wordt een vector met elk element = 1 regel
    query <- readLines(file_con, warn = FALSE, ok = TRUE)
    close(file_con)
    if (info == TRUE) {
      cat('OK.\n')
    }
  }
  query <- qry_beautify(query)
  
  if (!qry_isvalid(query)) {
    if (nchar(query) > 20) {
      subquery <- paste0(substr(query, 0, 20), '...')
    } else {
      subquery <- query
    }
    stop("Not a valid SQL-query: '", subquery, "'. If this is a file, please check its existance.",
         call. = FALSE)
  }
  
  if (!missing(limit)) {
    if (query %like% ' LIMIT ') {
      if (info == TRUE) {
        warning('LIMIT already set in query, ignoring `limit = ',
                format(limit, scientific = FALSE),
                '`',
                call. = FALSE,
                immediate. = TRUE)
      }
    } else {
      query <- paste(query, 'LIMIT', format(limit, scientific = FALSE))
    }
  }
  
  # verbinden met database
  if (!isS4(con)) {
    dbcon <- certedb(dbname = dbname, info = info)
    on.exit(certedb_close(dbcon))
  } else {
    dbcon <- con
  }
  
  if (auto_append_prefix == TRUE) {
    query_tbls <- strsplit(query, " ", fixed = TRUE) %>% unlist()
    # alle tabellen die voorkomen in deze query, alles wat na FROM of JOIN komt:
    query_tbls_index <- which(tolower(query_tbls) %in% tolower(c("FROM", "JOIN"))) + 1
    
    tbls_this_qry <- query_tbls[query_tbls_index]
    tbls_checked <- tbls_this_qry %>% certedb_check_tbls(con = dbcon, info = info)
    query_tbls[query_tbls_index] <- tbls_checked
    query <- query_tbls %>% concat(" ")
  }
  
  if (check_mtrl_test_codes == TRUE & info == TRUE) {
    certedb_checkmmb_mtrlcodes(con = dbcon, dbname = dbname)
    certedb_checkmmb_testcodes(con = dbcon, dbname = dbname)
  }
  
  starttime <- Sys.time()
  
  # Run + Fetch
  certedb_timestamp('Running query...', print = info, appendLF = FALSE)
  db_data <- dbcon %>% tbl(sql(query)) %>% collect()
  certedb_timestamp('OK', print = info, timestamp = FALSE, appendLF = TRUE)
  
  # Transform
  if (auto_transform == TRUE & nrow(db_data) > 0) {
    certedb_timestamp(paste0('Transforming data (', paste0(dim(db_data), collapse = "x"), ')...'), print = info, appendLF = FALSE)
    if (info == TRUE) {
      db_data <- db_data %>% tbl_guess_columns(datenames = datenames,
                                               dateformat = dateformat,
                                               timeformat = timeformat,
                                               decimal.mark = decimal.mark,
                                               big.mark = big.mark,
                                               timezone = timezone,
                                               na = na)
    } else {
      suppressMessages(
        db_data <- db_data %>% tbl_guess_columns(datenames = datenames,
                                                 dateformat = dateformat,
                                                 timeformat = timeformat,
                                                 decimal.mark = decimal.mark,
                                                 big.mark = big.mark,
                                                 timezone = timezone,
                                                 na = na)
      )
    }
    certedb_timestamp('OK', print = info, timestamp = FALSE, appendLF = TRUE)
  } else if (nrow(db_data) > 0 & "POSIXct" %in% unlist(lapply(db_data, class))) {
    # er komen wel tijden voor, terwijl dit mogelijk verkeerde tijdzones zijn!
    warning("Data may contain wrong timezones. Correct with as.UTC(x).")
  }
  if (binary_as_logical == TRUE) {
    db_data <- db_data %>% tbl_binary2logical()
  }
  
  difference <- difftime(Sys.time(), starttime, units = 'mins')
  certedb_timestamp('Done.', print = info)
  if (info == TRUE & nrow(db_data) == 0) {
    message("NOTE: NO OBSERVATIONS FOUND.")
  }
  if (info == TRUE) {
    cat('Downloaded',
        nrow(db_data) %>% format2(),
        'obs. of',
        ncol(db_data) %>% format2(),
        'variables, in',
        diff_min_sec(difference),
        '\n\n')
  }
  
  qry(db_data) <- query
  db_data
  
}

#' Verbinding maken met MySQL-/MariaDB-database
#'
#' Dit maakt verbinding met een MySQL-/MariaDB-database zoals de Certe-databaseserver.
#' @param host,port,username,password Standaard zijn omgevingsvariabelen van de huidige gebruiker: \code{"DB_HOST"} (bij ontbreken wordt dit \code{localhost}), \code{"DB_PORT"} (bij ontbreken wordt dit \code{3306}), \code{"DB_USERNAME"} en \code{"DB_PASSWORD"}. Inloggegevens voor de MySQL/MariaDB-server.
#' @param dbname Standaard is \code{"certemmb"}. Naam van de database die geselecteerd moet worden.
#' @param info Standaard is \code{FALSE}. Weergeven van informatie over het verbinden.
#' @export
#' @seealso \code{\link{certedb_query}} voor het direct gebruik van query's en \code{\link{certedb_close}} voor het sluiten van een verbinding.
#' @examples
#' \dontrun{
#'
#' # gegevens direct downloaden naar tibble:
#' locaties <- certe_db() %>%
#'   tbl("temporary_certemm_locaties") %>%
#'   collect()
#'
#' # alleen query weergeven en niet uitvoeren:
#' locaties <- certe_db() %>%
#'   tbl("temporary_certemm_locaties") %>%
#'   show_query()
#'
#' # rest werkt met dpylr, zoals filter() en select():
#' locaties <- certe_db() %>%
#'   tbl("temporary_certemm_locaties") %>%
#'   filter(zkhgroepcode > 0, cu_sd == "Noord") %>%
#'   select(instelling) %>%
#'   collect()
#'
#' # gebruik head() zoals LIMIT in MySQL:
#' locaties <- certe_db() %>%
#'   tbl("temporary_certemm_locaties") %>%
#'   head(5) %>%
#'   collect()
#' }
certedb <- function(host = Sys.getenv("DB_HOST"),
                    port = Sys.getenv("DB_PORT"),
                    username = Sys.getenv("DB_USERNAME"),
                    password = Sys.getenv("DB_PASSWORD"),
                    dbname = 'certemmb',
                    info = FALSE) {
  
  if (host == "") {
    host <- 'localhost'
  }
  if (port == "") {
    port <- 3306
  } else if (!is.double2(port)) {
    stop('`port` must be a numeric value.')
  }
  
  if (username == "" | password == "") {
    if (info == TRUE) {
      warning('`username` or `password` is empty.', call. = FALSE, immediate. = TRUE)
    }
  }
  
  if (info == TRUE) {
    cat(paste0('Connecting to database `', dbname, '`... '))
  }
  # set proxy met als wachtwoord de omgevingsvariabele "R_WW"
  set_certe_proxy()
  con <- RMariaDB::dbConnect(RMariaDB::MariaDB(),
                             dbname = dbname,
                             host = host,
                             port = port %>% as.integer(),
                             username = username,
                             password = password)
  certedb_timestamp('OK', print = info, timestamp = FALSE, appendLF = TRUE)
  con
}

certedb_close <- function(conn, ...) {
  RMariaDB::dbDisconnect(conn = conn, ...)
}

#' Tabellen ophalen uit MySQL-/MariaDB-database
#'
#' Alle tabellen uit een MySQL-/MariaDB-database zoals de Certe-database ophalen en in een \code{\link{list}} plaatsen, waarbij elke waarde in de lijst ook de naam is van het element in de lijst.
#' @param con Standaard is leeg, waarmee de verbinding gemaakt wordt op basis van de omgevingsvariabelen van de huidige gebruiker: \code{"DB_HOST"}, \code{"DB_PORT"}, \code{"DB_USERNAME"} en \code{"DB_PASSWORD"}.
#' @param dbname Standaard is \code{"certemmb"}. Naam van de database die geselecteerd moet worden. Wordt genegeerd als \code{con} al een bestaande verbinding is.
#' @export
#' @return \code{\link{list}} met alle gevonden SQL-tabellen.
#' @seealso \code{\link{certedb_query}} voor het direct gebruik van query's.
certedb_tbls <- function(con = NULL, dbname = 'certemmb') {
  
  if (!isS4(con)) {
    dbcon <- certedb(dbname = dbname)
    on.exit(certedb_close(dbcon))
  } else {
    dbcon <- con
  }
  
  tbls <- dbcon %>% RMariaDB::dbListTables()
  tbls <- tbls %>% as.list() %>% setNames(tbls)
  for (i in 1:length(tbls)) {
    tblcols <- dbcon %>% RMariaDB::dbListFields(tbls[[i]])
    tblcols_values <- paste0(tbls[[i]], '.', tblcols)
    tblcols_values <- c(tbls[[i]], tblcols_values)
    names(tblcols_values) <- c('.', tblcols)
    tblcols_values <- tblcols_values %>% as.list()
    tbls[[i]] <- tblcols_values
  }
  tbls
}

#' Controleren van nieuwe materiaal- en testcodes
#'
#' Controleert originele materiaal- en testcodes en vergelijkt ze met de nieuwe \code{temporary_*}-tabellen.
#' @param ... Parameters die doorgegeven worden aan \code{\link{certedb_query}}.
#' @export
#' @aliases certedb_checkmmb_mtrlcodes certedb_checkmmb_testcodes
#' @rdname certedb_checkmmb
certedb_checkmmb_mtrlcodes <- function(...) {
  cat("Checking database for new mtrlcodes... ")
  query <- c("SELECT m1.*",
             "FROM certemm_materialen AS m1",
             "LEFT JOIN temporary_certemm_materiaalgroepen AS m2 ON m1.mtrlcode = m2.mtrlcode",
             "WHERE m2.mtrlcode IS NULL") %>% concat(" ")
  suppressWarnings(
    result <- certedb_query(query, info = FALSE, ...)
  )
  if (nrow(result) > 0) {
    message("Warning\nSome mtrlcodes not found in temporary_certemm_materiaalgroepen:")
    print(result[, c('mtrlcode', 'kortenaam', 'langenaam')], header = FALSE, row.names = FALSE, n = Inf)
  } else {
    cat("No new codes found.\n")
  }
}

#' @export
#' @rdname certedb_checkmmb
certedb_checkmmb_testcodes <- function(info = TRUE, ...) {
  cat("Checking database for new testcodes... ")
  query <- c("SELECT t1.*",
             "FROM certemm_testen AS t1",
             "LEFT JOIN temporary_certemm_testgroepen AS t2 ON t1.anamc = t2.testcode",
             "WHERE t2.testcode IS NULL") %>% concat(" ")
  suppressWarnings(
    result <- certedb_query(query, info = FALSE, ...)
  )
  if (nrow(result) > 0) {
    message("Warning\nSome testcodes not found in temporary_certemm_testgroepen:")
    r <- result[, c('anamc', 'kortenaam', 'langenaam')]
    colnames(r) <- c('testcode', 'kortenaam', 'langenaam')
    print(r, header = FALSE, row.names = FALSE, n = Inf)
  } else {
    cat("No new codes found.\n")
  }
}


#' Tabellen valideren in MySQL-/MariaDB-database
#'
#' Hiermee worden tabellen opgezocht in een MySQL-/MariaDB-database. Niet-bestaande tabellen worden hoofdletterongevoelig vergeleken met bestaande tabellen. Wanneer een niet-bestaande tabel wel voorkomt met een prefix eindigend op een underscore (\code{"_"}, wordt de tabelnaam overschreven door de bestaande tabelnaam die alfabetisch als laatste voorkomt.
#' @param tbls Tabellen in de database die gecontroleerd moeten worden.
#' @param con Standaard is leeg, waarmee de verbinding gemaakt wordt op basis van de omgevingsvariabelen van de huidige gebruiker: \code{"DB_HOST"}, \code{"DB_PORT"}, \code{"DB_USERNAME"} en \code{"DB_PASSWORD"}.
#' @param dbname Standaard is \code{"certemmb"}. Naam van de database die geselecteerd moet worden. Wordt genegeerd als \code{con} al een bestaande verbinding is.
#' @param info Standaard is \code{TRUE}. Waarschuwing weergeven over niet-bestaande tabellen die vervangen worden door bestaande tabellen.
#' @export
certedb_check_tbls <- function(tbls, con = NULL, dbname = 'certemmb', info = TRUE) {
  
  if (!isS4(con)) {
    dbcon <- certedb(dbname = dbname, info = info)
    on.exit(certedb_close(dbcon))
  } else {
    dbcon <- con
  }
  
  existing_tbls <- certedb_tbls(dbcon) %>% unlist() %>% unname()
  not_existing <- tbls[!tbls %in% existing_tbls]
  not_existing <- not_existing[which(not_existing %like% '^[0-9A-Z$_]+$')]
  
  if (length(not_existing) != 0) {
    
    # proberen tabel te vinden die er dan wel op lijkt
    for (i in 1:length(not_existing)) {
      available_tbls <- existing_tbls[which(existing_tbls %like% paste0('_', not_existing[i], '$'))]
      # altijd laatste selecteren:
      available_tbls <- rev(available_tbls)
      if (length(available_tbls) == 0) {
        stop('Table not found in database: `', not_existing[i], '`.', call. = FALSE)
      } else {
        if (info == TRUE) {
          message(paste0('Note: Selecting table `',
                         available_tbls[1],
                         '` instead of non-existing table `',
                         not_existing[i],
                         '`'))
        }
        tbls <- gsub(not_existing[i], available_tbls[1], tbls, fixed = TRUE)
      }
    }
  }
  tbls
}
