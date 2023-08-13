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
as_certedb_tibble <- function(tbl_df, source, qry, datetime, user, type) {
  out <- as_tibble(tbl_df)
  structure(out,
            class = c("certedb_tibble", class(out)),
            dims = dim(out),
            type = type,
            source = source,
            qry = qry,
            datetime = datetime,
            user = user)
}

#' @importFrom pillar tbl_sum dim_desc
#' @importFrom certestyle format2
#' @exportS3Method tbl_sum certedb_tibble
tbl_sum.certedb_tibble <- function(x, ...) {
  out <- dim_desc(x)
  names(out) <- paste("A", attributes(x)$type, "tibble")
  if (identical(attributes(x)$dims, dim(x))) {
    if (!is.null(attributes(x)$source)) {
      out <- c(out, "Retrieved from" = attributes(x)$source)
    }
    if (!is.null(attributes(x)$datetime)) {
      out <- c(out, "Retrieved on" = format2(attributes(x)$datetime, "yyyy-mm-dd HH:MM"))
    }
    if (!is.null(attributes(x)$user)) {
      out <- c(out, "Retrieved by" = attributes(x)$user)
    }
  }
  out
}

#' @importFrom pillar tbl_format_footer style_subtle
#' @importFrom cli symbol
#' @exportS3Method tbl_format_footer certedb_tibble
tbl_format_footer.certedb_tibble <- function(x, setup, ...) {
  footer <- NextMethod()
  if (is.null(attributes(x)$qry)) {
    return(footer)
  } else {
    old_dims <- attributes(x)$dims
    if (identical(old_dims, dim(x))) {
      c(footer,
        style_subtle(paste0("# ", cli::symbol$info, " Use `certedb_query()` to get the query of this ", attributes(x)$type, " tibble")))
    } else {
      c(footer,
        style_subtle(paste0("# ", cli::symbol$info, " Use `certedb_query()` to get the query of the original ", 
                            dim_desc(as.data.frame(matrix(0, nrow = old_dims[1], ncol = old_dims[2]))), " ",
                            attributes(x)$type, " tibble")))
    }
  }
}
