library(shiny)
library(dplyr)
library(lubridate)
library(bupaR)
library(processmapR)
library(DiagrammeR)


t_event_log_app <- read.csv(here::here("Data", "t_event_log_201801.csv"), stringsAsFactors = FALSE) %>%
  mutate(TIMESTAMP = ymd_hms(TIMESTAMP))

ui <- fluidPage(
  titlePanel("New Business Acquisition Cycle"),
  sidebarLayout(
    sidebarPanel(
      sliderInput("traceFreqInput", "Trace Freq", min = 0, max = 1, value = 0.5),
      selectInput("prodLineInput", "Product Line", choices = c("Home", "TPML", "Life", "Casco")),
      selectInput("autoUwInput", "Automated Process", choices = c("I", "N"))
    ),
    mainPanel(grVizOutput("processMap", width = "100%", height = "800px"))
  )
)

server <- function(input, output) {
  filtered <- reactive({
    t_event_log_app %>%
      filter(
        PRODUCT_LINE == input$prodLineInput & AUTOUW == input$autoUwInput
      ) %>%
      eventlog(
        case_id = "CASE_ID",
        activity_id = "EVENT_NAME_HU",
        activity_instance_id = "ACTIVITY_INST_ID",
        lifecycle_id = "LIFECYCLE_ID",
        timestamp = "TIMESTAMP",
        resource_id = "PARTNER_NAME"
      ) %>%
      filter_endpoints(start_activities = "alairas", end_activities = "jutalek_kifizetes") %>%
      filter_trace_frequency(percentage = input$traceFreqInput, reverse = F)
  })


  output$processMap <- renderGrViz({
    filtered() %>%
      process_map(type_nodes = frequency("absolute"), type_edges = performance(median, "days"), rankdir = "TB")
  })
}

shinyApp(ui = ui, server = server)