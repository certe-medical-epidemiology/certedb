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

.onLoad <- function(...) {
  # download the database structure and add it as an element `db` to the package environment,
  # so the filtering on database data can be typed faster
  # (the SQL query to get the structure in is the "data-raw" folder)
  db_tbls_path <- suppressWarnings(read_secret("db.db_tbls_path"))
  if (db_tbls_path != "" && file.exists(db_tbls_path)) {
    # read CSV file
    db_tbls <- utils::read.csv(db_tbls_path, check.names = FALSE)
    if (is.data.frame(db_tbls) &&
        all(c("table_name", "column_name", "data_type", "max_length") %in% colnames(db_tbls))) {
      db_tbls <- db_tbls[which(db_tbls$table_name %unlike% "(^test|__|brmoverkenner)"), ]
      # add the database structure to a list in the package environment
      db <- as.list(paste0(db_tbls$table_name, ".", db_tbls$column_name))
      names(db) <- paste0(db_tbls$table_name, ".", db_tbls$column_name)
      # this the global db:
      assign(x = "db",
             value = db,
             envir = asNamespace("certedb"))
      # this is a list for each database:
      unique_dbs <- unique(db_tbls$database_schema)
      for (i in seq_len(length(unique_dbs))) {
        db_tbls_i <- db_tbls[which(db_tbls$database_schema == unique_dbs[i]), ]
        db_i <- as.list(paste0(db_tbls$table_name, ".", db_tbls$column_name))
        names(db_i) <- paste0(db_tbls$table_name, ".", db_tbls$column_name)
        assign(x = paste0("db.", unique_dbs[i]),
               value = db_i,
               envir = asNamespace("certedb"))
      }
    }
  }
}

.onAttach <- function(...) {
  if ("db" %in% ls(envir = asNamespace("certedb"))) {
    packageStartupMessage("certedb: database structure imported")
  }
}
