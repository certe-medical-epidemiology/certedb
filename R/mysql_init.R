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

#' Initialise MySQL Connection for Connections Pane
#' 
#' This sets up a MySQL connection to be viewable in the Connections pane.
#' @export
#' @importFrom DBI dbConnect
mysql_init <- function() {
  # Connect to database
  con <- dbConnect(RMariaDB::MariaDB(),
                   dbname = unlist(read_secret("db.certemmb"))["dbname"],
                   host = unlist(read_secret("db.certemmb"))["host"],
                   port = as.integer(unlist(read_secret("db.certemmb"))["port"]),
                   username = unlist(read_secret("db.certemmb"))["username"],
                   password = unlist(read_secret("db.certemmb"))["password"])
  connections::connection_view(con,
                               host = unname(unlist(read_secret("db.certemmb"))["host"]),
                               name = unname(unlist(read_secret("db.certemmb"))["dbname"]))
  
}
