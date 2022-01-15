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

build_ <- function(select) {
  
}

#' Run SQL-query on a MySQL-/MariaDB-database
#'
#' Run an SQL-query on a MySQL-/MariaDB-database from, for example, Certe. Output gets a \code{qry}-attribute, which can be retrieved with \code{\link{qry}}.
#' @param query (File containing) SQL-text to be run. Table names in the query don't need to contain \code{"temporary_"} or \code{"certemm_"} and are not case sensitive. This query will be saved with the object as attribute \code{query} which can be viewed with the \code{\link{qry}} function.
#' @param limit Default is 10.000.000. The maximum amount of rows to be retrieved.
#' @param con Default is empty, by which the connection is made based on the current user's environment variables: \code{"db.mmb.host"}, \code{"db.mmb.port"}, \code{"db.mmb.username"} en \code{"db.mmb.password"}.
#' @param dbname Default is \code{"certemmb"}. Name of the database to be selected. Will be ignored if \code{con} is already an existing connection.
#' @param info Default is \code{TRUE}. Printing of run/fetch progress en the resulting number of rows and columns downloaded.
#' @param check_mtrl_test_codes Default is \code{TRUE}. Check on material- and testcodes in the database's temporary tables. Also requires \code{info = TRUE}.
#' @param binary_as_logical Default is \code{TRUE}. Transform columns only containing \code{0} and/or \code{1} values to logical using \code{\link{tbl_binary2logical}}.
#' @param auto_append_prefix Default is \code{TRUE}. When the selected columns in query are non-existent, eponymous tables starting with \code{"temporary_"} or \code{"certemm_"}. When such a table is found, it will be used instead.
#' @param auto_transform Default is \code{TRUE}. Automatically transform all downloaded columns with \code{\link{tbl_guess_columns}}.
#' @inheritParams tbl_guess_columns
#' @param ... not used
#' @export
#' @seealso \code{\link{certedb}} for connecting to the Certe-databaseserver and \code{\link{certedb_getmmb}} for downloading all relevant MMB data.
#' @examples
#' \dontrun{
#' locaties <- certedb_query("SELECT * FROM temporary_certemm_locaties")
#' locaties <- certedb_query("SELECT * FROM temporary_certemm_locaties", limit = 5)
#'
#' # With `auto_append_prefix = TRUE`:
#' locaties <- certedb_query("SELECT * FROM locaties")
#' # will be transformed to:
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
  
  # if when certedb_query(sql(...)) is used:
  query <- query %>% as.character()
  
  # if query is file, then read file
  if (file.exists(query)) {
    certedb_timestamp('Reading query from file... ', print = info, timestamp = FALSE, appendLF = FALSE)
    file_con <- file(query, encoding = "UTF-8")
    # read file, becomes a vector with each element = 1 row
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
  
  # connect to database
  if (!isS4(con)) {
    dbcon <- certedb(dbname = dbname, info = info)
    on.exit(certedb_close(dbcon))
  } else {
    dbcon <- con
  }
  
  if (auto_append_prefix == TRUE) {
    query_tbls <- strsplit(query, " ", fixed = TRUE) %>% unlist()
    # all tables occurring in this query, everything coming after FROM or JOIN:
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

#' Connecting to the MySQL-/MariaDB-database
#'
#' Makes a connection to a MySQL-/MariaDB-database like the Certe-databaseserver.
#' @param host,port,username,password Default is the current user's environment variables: \code{"db.mmb.host"} (if missing will be \code{localhost}), \code{"db.mmb.port"} (if missing will be \code{3306}), \code{"db.mmb.username"} and \code{"db.mmb.password"}. Login credentials for the MySQL/MariaDB-server.
#' @param dbname Default is \code{"certemmb"}. Name of the database to be selected.
#' @param info Default is \code{FALSE}. Display of connecting info.
#' @export
#' @seealso \code{\link{certedb_query}} for the use of query's and \code{\link{certedb_close}} for closing a connection.
#' @examples
#' \dontrun{
#'
#' # download data directly to tibble:
#' locaties <- certedb() %>%
#'   tbl("temporary_certemm_locaties") %>%
#'   collect()
#'
#' # only show query without running it:
#' locaties <- certedb() %>%
#'   tbl("temporary_certemm_locaties") %>%
#'   show_query()
#'
#' # works with dpylr functions like filter() and select():
#' locaties <- certedb() %>%
#'   tbl("temporary_certemm_locaties") %>%
#'   filter(zkhgroepcode > 0, cu_sd == "Noord") %>%
#'   select(instelling) %>%
#'   collect()
#'
#' # use head() like LIMIT in MySQL:
#' locaties <- certedb() %>%
#'   tbl("temporary_certemm_locaties") %>%
#'   head(5) %>%
#'   collect()
#' }
certedb <- function(dbname = "certemmb",
                    host = NULL,
                    port = NULL,
                    username = NULL,
                    password = NULL,
                    driver = get_driver(dbname = dbname),
                    info = FALSE) {
  
  if (is.null(host)) {
    host <- read_secret(paste0("db.", dbname, ".host"))
  }
  if (is.null(port)) {
    port <- read_secret(paste0("db.", dbname, ".port"))
  }
  if (is.null(username)) {
    username <- read_secret(paste0("db.", dbname, ".username"))
  }
  if (is.null(password)) {
    password <- read_secret(paste0("db.", dbname, ".password"))
  }
  
  if (host == "") {
    host <- "localhost"
  }
  if (port == "") {
    port <- 3306
  } else if (port %unlike% "^[0-9]+$") {
    stop("`port` must be a numeric value.")
    port <- as.integer(port)
  }
  
  certedb_timestamp(paste0('Connecting to database `', dbname, '`... '),
                    print = info,
                    timestamp = FALSE,
                    appendLF = FALSE)
  con <- DBI::dbConnect(driver,
                        dbname = dbname,
                        host = host,
                        port = port,
                        username = username,
                        password = password)
  certedb_timestamp("OK", print = info, timestamp = FALSE, appendLF = TRUE)
  con
}

certedb_close <- function(conn, ...) {
  DBI::dbDisconnect(conn = conn, ...)
}

#' Retrieve tables from MySQL-/MariaDB-database
#'
#' Retrieve all tables from a MySQL-/MariaDB-database like the Certe-database ophalen and place them in a \code{\link{list}}, where each listed value is also the name of the element in the list.
#' @param con Default is empty, by which the connection is made based on the current user's environment variables: \code{"db.mmb.host"}, \code{"db.mmb.port"}, \code{"db.mmb.username"} en \code{"db.mmb.password"}.
#' @param dbname Default is \code{"certemmb"}. Name of the database to be selected. Will be ignored if \code{con} is already an existing connection.
#' @export
#' @return \code{\link{list}} containing all retrieved SQL-tables.
#' @seealso \code{\link{certedb_query}} for using query's.
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

#' Check for new material- and testcodes
#'
#' Check original material- and testcodes and compares them to the new \code{temporary_*}-tables.
#' @param ... Arguments to be used in \code{\link{certedb_query}}.
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


#' Verify tables in MySQL-/MariaDB-database
#'
#' Checks for tables in a MySQL-/MariaDB-database. Non-existing tables will be compared (case insensitive) to existing tables. When a non-existing table occurs with a prefix ending with an underscore (\code{"_"}), the table name will be overwritten with an existing table name listed last in alphabetical order.
#' @param tbls Tables in the database to be verified.
#' @param con Default is empty, by which the connection is made based on the current user's environment variables: \code{"db.mmb.host"}, \code{"db.mmb.port"}, \code{"db.mmb.username"} en \code{"db.mmb.password"}.
#' @param dbname Default is \code{"certemmb"}. Name of the database to be selected. Will be ignored if \code{con} is already an existing connection.
#' @param info Default is \code{TRUE}. Display a warning about non-existing tables to be replaced by existing tables.
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
    
    # attempt to find a resembling table 
    for (i in 1:length(not_existing)) {
      available_tbls <- existing_tbls[which(existing_tbls %like% paste0('_', not_existing[i], '$'))]
      # always select last:
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
