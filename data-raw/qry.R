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

#' Query van een object
#'
#' Hiermee kan snel de eigenschap (\emph{attribute}) \code{qry} van een object weergegeven worden of aan een object toegewezen worden. Deze eigenschap wordt altijd toegewezen aan de output van \code{\link{certedb_query}} en vóór toewijzing gevalideerd.
#' @param x Object.
#' @param value Waarde om toe te kennen.
#' @param query Bestaande query als tekst.
#' @details De functie \code{qry_beautify} wordt intern gebruikt om een query netjes op te maken door statements (zoals SELECT en FROM) als hoofdletters weer te geven, commentaren te verwijderen en enters toe te voegen. \cr
#'   De functie \code{qry_isvalid} toetst of een query begint met een van de statements op \url{https://mariadb.com/kb/en/library/basic-sql-statements/}.
#' @rdname qry
#' @export
#' @encoding UTF-8
#' @examples
#' \dontrun{
#' data <- certedb_query("SELECT * FROM certemm_ord LIMIT 1")
#' qry(data)
#'
#' qry(data) <- NULL
#' qry(data)
#' }
qry <- function(x) {
  q <- attributes(x)$qry
  if (is.null(q)) {
    message('No query found.')
  } else {
    q
  }
}

#' @noRd
#' @export
print.qry <- function(x, ...) {
  cat(qry, "\n")
}

#' @rdname qry
#' @export
`qry<-` <- function(x, value) {
  if (is.null(value)) {
    attributes(x)$qry <- NULL
    return(x)
  }
  
  query <- qry_beautify(value)
  
  if (!qry_isvalid(query)) {
    stop("This is not a valid SQL query.", call. = FALSE)
  }
  
  attr(x, 'qry') <- sql(query)
  x
}

#' @rdname qry
#' @export
has_qry <- function(tbl) {
  !is.null(suppressMessages(qry(tbl)))
}

#' @rdname qry
#' @export
qry_beautify <- function(query) {
  query <- query %>%
    strsplit('\n') %>%
    unlist()
  
  for (i in 1:length(query)) {
    query[i] <- query[i] %>%
      # alles na "--" en "#" verwijderen
      strsplit.select(1, "(--|#)") %>%
      # tabelnamen worden ongeldig als ze ';' bevatten, spaties ervoor
      gsub(';', ' ;', ., fixed = TRUE) %>%
      # meerdere spaties door enkele vervangen
      gsub(' {2,255}', ' ', ., fixed = FALSE) %>%
      # tabs en spates voor en na elke regel verwijderen
      trimws('both')
  }
  query <- query[query != "" & !is.na(query)] %>% concat(' ')
  if (query %>% substr(nchar(.), nchar(.)) == ';') {
    query <- query %>% substr(0, nchar(.) - 1)
  }
  
  # query met hoofdletter statements
  # eerste woord:
  query <- gsub("^([a-z]+) ",
                "\\U\\1 ",
                query,
                perl = TRUE,
                ignore.case = TRUE)
  # alle andere statements:
  query <- gsub(" (select|from|left|right|join|as|on|in|where|group by|order by|limit|trim|and|case|when|then|end|else|min|max|date|time|replace|asc|desc|between) ",
                " \\U\\1 ",
                query,
                perl = TRUE,
                ignore.case = TRUE)
  # opmaken met enters
  query <- query %>%
    gsub("^(CREATE DATABASE|DROP DATABASE|USE|CREATE TABLE|ALTER TABLE|DROP TABLE|DESCRIBE|SELECT|INSERT|UPDATE|DELETE|REPLACE|TRUNCATE|START TRANSACTION|COMMIT|ROLLBACK) ", "\\U\\1\n", ., ignore.case = TRUE, perl = TRUE) %>%
    gsub(" (SELECT|FROM|LEFT JOIN|RIGHT JOIN|JOIN|WHERE|LIMIT|GROUP BY|ORDER BY) ", "\n\\U\\1\n", ., ignore.case = TRUE, perl = TRUE) %>%
    # gsub(', ?', ',\n', .) %>%
    gsub("\n", "\n  ", .) %>%
    gsub("  (SELECT|FROM|LEFT JOIN|RIGHT JOIN|JOIN|WHERE|LIMIT|GROUP BY|ORDER BY)\n", "\\U\\1\n", ., ignore.case = TRUE, perl = TRUE) %>%
    gsub("( BETWEEN '[0-9-]+' AND)", "\\1_betweendate", .) %>% # AND van BETWEEN datums niet aanpassen
    gsub(" AND ", "\n  AND ", .) %>% # alle AND in WHERE op nieuwe regel
    gsub("AND_betweendate", "AND", .) # de 'between AND' terugzetten
  
  # deel tussen SELECT en FROM met enters
  if (query %>% strsplit("(SELECT|FROM)") %>% unlist() %>% length() == 3) {
    # bevat een SELECT en een FROM
    select_list <- query %>%
      gsub(".*SELECT(.*)FROM.*", "\\1", .) %>%
      gsub(", ", ",\n  ", .) %>%
      trimws() %>%
      paste(" ", .)
    query <- gsub("(.*SELECT).*(FROM.*)", paste0("\\1\n", select_list, "\n\\2"), query)
  }
  
  query
}

#' @rdname qry
#' @export
qry_isvalid <- function(query) {
  # query moet starten met een van deze statements
  # bron: https://mariadb.com/kb/en/library/basic-sql-statements/
  qry_beautify(query) %like% "^(CREATE DATABASE|DROP DATABASE|USE|CREATE TABLE|ALTER TABLE|DROP TABLE|DESCRIBE|SELECT|INSERT|UPDATE|DELETE|REPLACE|TRUNCATE|START TRANSACTION|COMMIT|ROLLBACK)(\n| )"
}
