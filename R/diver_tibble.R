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

#' @importFrom dplyr as_tibble
as_diver_tibble <- function(tbl_df, cbase, qry, datetime, user, type = "Diver") {
  out <- as_tibble(tbl_df)
  structure(out,
            class = c("diver_tibble", class(out)),
            type = type,
            cbase = cbase,
            qry = qry,
            datetime = datetime,
            user = user)
}

#' @importFrom pillar tbl_sum dim_desc
#' @importFrom certestyle format2
#' @exportS3Method tbl_sum diver_tibble
tbl_sum.diver_tibble <- function(x, ...) {
  out <- dim_desc(x)
  names(out) <- paste("A", attributes(x)$type, "tibble")
  if (!is.null(attributes(x)$cbase)) {
    out <- c(out, "Retrieved from" = attributes(x)$cbase)
  }
  if (!is.null(attributes(x)$datetime)) {
    out <- c(out, "Retrieved on" = format2(attributes(x)$datetime, "yyyy-mm-dd HH:MM"))
  }
  if (!is.null(attributes(x)$user)) {
    out <- c(out, "Retrieved by" = attributes(x)$user)
  }
  out
}

#' @importFrom pillar tbl_format_footer style_subtle
#' @importFrom cli symbol
#' @exportS3Method tbl_format_footer diver_tibble
tbl_format_footer.diver_tibble <- function(x, setup, ...) {
  footer <- NextMethod()
  if (is.null(attributes(x)$qry)) {
    return(footer)
  } else {
    c(footer,
      style_subtle(paste0("# ", cli::symbol$info, " Use `diver_query()` to get the original query of this ", attributes(x)$type, " tibble")))
  }
}
