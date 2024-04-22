# reserve time to get 'env.rds' exported
Sys.sleep(5)

# we are currently in the folder set with `workingDir` in rstudioapi::jobRunScript()
env <- readRDS("env.rds")

rstudioapi::jobAddOutput(env$job, "Environment found, now running.")
rstudioapi::jobSetState(env$job, "running")
rstudioapi::jobSetProgress(env$job, 25)
rstudioapi::jobSetStatus(env$job, "Collecting data...")

conn <- certedb::db_connect(driver = odbc::odbc(),
                            dsn = env$diver_dsn,
                            project = env$diver_project,
                            cbase = env$diver_cbase,
                            print = FALSE)

tryCatch({
  out <- DBI::dbGetQuery(conn, paste(as.character(env$qry), collapse = " "))
  # out <- dplyr::collect(env$out)
},
error = function(e) {
  certedb::db_close(conn, print = FALSE)
  stop(certedb:::format_error(e), call. = FALSE)
})

certedb::db_close(conn, print = FALSE)
rstudioapi::jobAddOutput(env$job, paste0("Retrieved ", nrow(out), " rows with ", ncol(out), " columns\n"))

for (i in seq_len(ncol(out))) {
  # 2023-02-13 fix for Diver, logicals/booleans seem corrupt
  if (is.logical(out[, i, drop = TRUE])) {
    out[, i] <- as.logical(as.character(out[, i, drop = TRUE]))
  }
}


if (isTRUE(env$distinct)) {
  out_distinct <- dplyr::distinct(out)
  if (nrow(out_distinct) < nrow(out)) {
    rstudioapi::jobAddOutput(job, paste0("Removed ", nrow(out) - nrow(out_distinct), " duplicate rows\n"))
    out <- out_distinct
  }
}


if (isTRUE(env$auto_transform)) {
  rstudioapi::jobSetProgress(env$job, 75)
  rstudioapi::jobSetStatus(env$job, "Transforming data...")
  if ("Ordernummer" %in% colnames(out)) {
    `%like%` <- certetoolbox::`%like%`
    out$Ordernummer[out$Ordernummer %like% "[0-9]{2}[.][0-9]{4}[.][0-9]{4}"] <- gsub(".", "", out$Ordernummer[out$Ordernummer %like% "[0-9]{2}[.][0-9]{4}[.][0-9]{4}"], fixed = TRUE)
    out <- out |> dplyr::arrange(dplyr::desc(Ordernummer))
  } else {
    out <- out |> dplyr::arrange(dplyr::desc(Ontvangstdatum))
  }
  # transform data, and update column names
  out <- certetoolbox::auto_transform(out, snake_case = TRUE)
}

rstudioapi::jobSetProgress(env$job, 100)
rstudioapi::jobSetState(env$job, "succeeded")

diver_data <- certedb:::as_certedb_tibble(out,
                                          source = env$diver_cbase,
                                          qry = env$qry,
                                          datetime = Sys.time(),
                                          user = env$user,
                                          type = "Diver")

# assign("data_123", value = out, envir = base::globalenv(), inherits = FALSE)
rm(list = ls()[ls() != "diver_data"])
unlink("env.rds")
rstudioapi::showDialog(title = "Background Job Finished",
                       message = paste0("Diver dataset (", format(nrow(diver_data), big.mark = " "),
                                        " x ", format(ncol(diver_data), big.mark = " "),
                                        ") has been downloaded and saved as 'diver_data' to the global environment."))
