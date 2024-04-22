
shiny_get_data <- function() {
  if (!"shiny" %in% rownames(utils::installed.packages())) {
    stop("This function requires the 'shiny' package", call. = FALSE)
  }
  require("shiny")
  
  cbase_presets <- presets()
  
  ui <- fluidPage(
    
    sidebarLayout(
      sidebarPanel(
        width = 5,
        selectInput("preset",
                    "Preset",
                    choices = c("", cbase_presets$preset),
                    selected = ""), # read_secret("db.preset_default")),
        textOutput("cBase_txt"),
        uiOutput("selections"),
        checkboxInput("autotransform", "Auto-transform", value = FALSE),
      ),
      
      mainPanel(
        width = 7,
        h4("Voorbeeld:"),
        shinycssloaders::withSpinner(
          verbatimTextOutput("example")
        ),
      )
    )
  )
  
  server <- function(input, output) {
    
    pkg_env$first_1000 <- NULL
    observe({
      if (input$preset != "") {
        pkg_env$first_1000 <- get_diver_data(preset = input$preset,
                                             review_qry = FALSE,
                                             info = FALSE,
                                             limit = 1000,
                                             auto_transform = FALSE)
      }
    })
    
    
    output$cBase_txt <- renderText({
      paste0("Uit cBase: ", cbase_presets$cbase[which(cbase_presets$preset == input$preset)])
    })
    
    output$selections <- renderUI({
      try(trigger <- input$preset, silent = TRUE)
      #try(trigger <- input$filter1, silent = TRUE)
      #try(trigger <- input$filter1_values, silent = TRUE)
      print(colnames(pkg_env$first_1000))
      tagList(
        dateRangeInput("dates", "Date range:", start = min(this_year()), end = max(this_year())),
        selectInput("filter1", "Filter variable:", choices = unique(colnames(pkg_env$first_1000))),
        radioButtons("filter1_is_like", "", choices = c("is", "is not", "like", "not like")),
        uiOutput("filter1_values_ui")
      )
    })
    output$filter1_values_ui <- renderUI({
      selectInput("filter1_values", "", choices = unique(pkg_env$first_1000[[input$filter1]]),
                  selectize = TRUE, multiple = TRUE)
    })
    
    output$example <- renderPrint({
      try(trigger <- input$preset, silent = TRUE)
      try(filter1 <- input$filter1, silent = TRUE)
      try(filter1_values <- input$filter1_values, silent = TRUE)
      out <- NULL
      if (is.null(input$filter1_values)) {
        out <- pkg_env$first_100[1:10, , drop = FALSE]
      } else {
        if (input$filter1_is_like == "is") {
          print(input$dates)
          out <- get_diver_data(preset = input$preset,
                                where = BepalingNaam %in% input$filter1_values,
                                review_qry = FALSE,
                                info = FALSE,
                                limit = 10,
                                auto_transform = input$autotransform)
        }
      }
      as_tibble(out)
    })
    
    
  }
  
  shinyApp(ui = ui, server = server)
  
}
