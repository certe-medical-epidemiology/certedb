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

#' Search cBase Interactively
#' 
#' Use this Shiny app to search a cBase. There is also an RStudio add-in.
#' @importFrom dplyr mutate_all
#' @importFrom certestyle colourpicker
#' @inheritParams get_diver_data
#' @export
cbase_explorer <- function(preset = read_secret("db.preset_default_shiny"),
                           diver_cbase = NULL,
                           diver_project = read_secret("db.diver_project"),
                           diver_dsn = if (diver_testserver == FALSE) read_secret("db.diver_dsn") else read_secret("db.diver_dsn_test"),
                           diver_testserver = FALSE,
                           diver_tablename = "data") {

  for (pkg in c("shiny", "DT", "tidyr")) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      stop("Package '", pkg, "' is required for cbase_explorer(). Please install it with `install.packages(\"", pkg, "\")`.", call. = FALSE)
    }
  }

  pkg_env$qry_shiny <- list(date_range = c(paste0(format(Sys.Date(), "%Y"), "-01-01"),
                                           paste0(format(Sys.Date(), "%Y"), "-12-31")),
                            where = NULL)
  
  ui <- shiny::fluidPage(
    shiny::tags$style(paste0(
      ".well { background-color: ", colourpicker("certeblauw6"), "; font-size: 12px; line-height: 1; margin-top: 12px; }",
      ".well label { color: ", colourpicker("certeblauw"), "; }",
      "h4 { color: ", colourpicker("certeblauw"), "; font-size: 14px; font-weight: bold; text-decoration: underline; text-align: center; }",
      "#zoek { background-color: ", colourpicker("certeblauw"), "; border-color: ", colourpicker("certeblauw"), "; color: white; }",
      "#zoek:hover { background-color: ", colourpicker("certeblauw0"), "; border-color: ", colourpicker("certeblauw"), "; }",
      ".btn { color: ", colourpicker("certeblauw"), "; border-color: ", colourpicker("certeblauw"), "; }",
      ".btn:hover { color: ", colourpicker("certeblauw"), "; background-color: ", colourpicker("certeblauw6"), "; border-color: ", colourpicker("certeblauw"), "; }",
      ".main { margin: 0px; padding: 0px; }",
      ".data-output { width: 100%; overflow: scroll; height: 750px;}",
      "table * { line-height: 14px; font-size: 12px;}",
      "td { padding-top: 1px !important; padding-bottom: 1px !important; padding-left: 0px !important; padding-right: 0px !important; }",
      collapse = " ")),
    shiny::sidebarLayout(
      shiny::sidebarPanel(width = 3,
                   shiny::selectInput("preset_select", "Preset", choices = presets()$preset, selected = preset),
                   shiny::h4("Ordergegevens"),
                   shiny::textInput("Ordernummer", "Ordernummer"),
                   shiny::dateRangeInput("datumbereik", label = shiny::textOutput("date_col"),
                                  start = as.Date(paste0(format(Sys.Date(), "%Y"), "-01-01")),
                                  end = as.Date(paste0(format(Sys.Date(), "%Y"), "-12-31")),
                                  language = "nl"),
                   shiny::textInput("BepalingCode", "BepalingCode"),
                   shiny::textInput("BepalingOmschrijving", "BepalingOmschrijving (reguliere expressie)"),
                   shiny::uiOutput("overig"),
                   shiny::h4("Patient"),
                   shiny::textInput("PatientBSN", "PatientBSN"),
                   shiny::textInput("PatientID", "PatientID"),
                   shiny::textInput("PatientNaam", "PatientNaam (reguliere expressie)"),
                   shiny::textInput("PatientGeboortedatum", "PatientGeboortedatum"),
                   shiny::actionButton("zoek", "Zoeken", width = "31%"),
                   shiny::actionButton("sluit", "Sluiten", width = "25%"),
                   shiny::actionButton("kopie", "Syntax kopieren", width = "41%"),
                   shiny::br(),
                   shiny::br(),
                   shiny::actionButton("opslaan", "Data opslaan als 'df_explorer'", width = "100%")
      ),
      shiny::mainPanel(width = 9, class = "main",
                shiny::div(class = "data-output",
                    DT::DTOutput("data_table")
                )
      )
    )
  )
  
  server <- function(input, output, session) {
    
    output$date_col <- shiny::renderText({
      paste0(get_preset(input$preset_select)$date_col, " (gebaseerd op preset)")
    })
    
    output$overig <- shiny::renderUI({
      df <- get_diver_data(date_range = NULL,
                           limit = 0,
                           auto_transform = FALSE,
                           info = FALSE,
                           review_qry = FALSE,
                           preset = input$preset_select,
                           diver_project = diver_project,
                           diver_dsn = diver_dsn,
                           diver_testserver = diver_testserver,
                           diver_tablename = diver_tablename)
      cols <- colnames(df)
      shiny::tagList(
        shiny::h4("Overig criterium"),
        shiny::selectInput("OverigVeld", "Kolom", choices = cols),
        shiny::textInput("OverigWaarde", "Waarde", value = ""),
        shiny::checkboxInput("OverigRegex", "Reguliere expressie", value = TRUE)
      )
    })
    
    data <- shiny::eventReactive(input$zoek, {
      where_clauses <- vapply(FUN.VALUE = character(1),
                              names(input),
                              function(x) {
                                if (x %unlike% "^(data_table|datumbereik|preset_select|zoek|sluit|kopie|opslaan|OverigWaarde|OverigRegex)" && !is.null(input[[x]]) && !all(input[[x]] == "", na.rm = TRUE)) {
                                  if (x == "OverigVeld") {
                                    val <- trimws(input$OverigWaarde)
                                    if (isTRUE(input$OverigRegex)) {
                                      paste0(input$OverigVeld, " %like% \"", val, "\"")
                                    } else {
                                      paste0(input$OverigVeld, " == \"", val, "\"")
                                    }
                                  } else if (x %in% c("PatientNaam", "BepalingOmschrijving")) {
                                    paste0(x, " %like% \"", input[[x]], "\"")
                                    # } else if (input[[x]] %like% "^[0-9]+$") {
                                    #   paste0(x, " == ", input[[x]])
                                  } else {
                                    val <- input[[x]]
                                    if (is.character(input[[x]])) {
                                      val <- trimws(input[[x]])
                                    }
                                    paste0(x, " == \"", val, "\"")
                                  }
                                } else {
                                  NA_character_
                                }
                              })
      where_clauses <- paste0(where_clauses[!is.na(where_clauses)], collapse = " & ")
      where_as_character <- TRUE
      if (where_clauses == "") {
        where_clauses <- NULL
        where_as_character <- FALSE
      }
      
      pkg_env$qry_shiny <- list(date_range = c(input$datumbereik[1], input$datumbereik[2]),
                                where = where_clauses)
      
      out <- get_diver_data(date_range = c(input$datumbereik[1], input$datumbereik[2]),
                            where = where_clauses,
                            where_as_character = where_as_character,
                            auto_transform = FALSE,
                            info = FALSE,
                            review_qry = FALSE,
                            preset = input$preset_select,
                            diver_project = diver_project,
                            diver_dsn = diver_dsn,
                            diver_testserver = diver_testserver,
                            diver_tablename = diver_tablename)
      
      pkg_env$cbase_explorer_last <- out
      
      out <- out |>
        mutate(order_unique = paste0(Ordernummer, BepalingCode))
      
      # scroll horizontally
      out <- out |>
        mutate_all(as.character) |>
        tidyr::pivot_longer(
          -order_unique,
          names_to = "variable",
          values_to = "value") |>
        tidyr::pivot_wider(
          names_from = order_unique,
          values_from = value)
      
      vars <- unique(c("Ordernummer", "Monsternummer", "BepalingCode", out$variable))
      vars <- vars[vars %in% out$variable]
      
      out$variable <- factor(out$variable, levels = vars, ordered = TRUE)
      out <- out |>
        arrange(variable) %>%
        stats::setNames(c("Ordernummer", colnames(.)[-1]))
      
      out
    })
    
    shiny::observeEvent(input$sluit, {
      shiny::stopApp()
    })
    
    shiny::observeEvent(input$kopie, {
      qry_text <- paste0("get_diver_data(date_range = c(\"", pkg_env$qry_shiny$date_range[1], "\", \"", pkg_env$qry_shiny$date_range[2], "\")",
                         ifelse(is.null(pkg_env$qry_shiny$where),
                                "",
                                paste0(",\n               where = ", pkg_env$qry_shiny$where)),
                         ",\n               preset = \"", input$preset_select, "\")")
      clipr::write_clip(qry_text)
      rstudioapi::showDialog(title = "Gekopieerd",
                             message = paste0("Tekst gekopieerd naar klembord:\n\n", qry_text))
    })
    
    shiny::observeEvent(input$opslaan, {
      envir <- globalenv()
      assign(x = "df_explorer", value = pkg_env$cbase_explorer_last, envir = envir)
      shiny::showNotification(session = session, "Opgeslagen naar global environment.")
    })
    
    output$data_table <- DT::renderDT({
      data()
    }, options = list(paging = FALSE, info = FALSE, scrollX = TRUE, pageLength = -1), rownames = FALSE)
    
  }
  
  viewer <- shiny::dialogViewer(dialogName = "cBase Explorer",
                         width = 1600,
                         height = 750)
  
  suppressMessages(
    shiny::runGadget(app = ui,
              server = server,
              viewer = viewer,
              stopOnCancel = FALSE))
  
}
