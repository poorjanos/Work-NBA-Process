library(shiny)
library(dplyr)
library(lubridate)
library(bupaR)
library(processmapR)
library(DiagrammeR)


t_event_log_app <- read.csv(here::here("Data", "t_event_log_201801.csv"), stringsAsFactors = FALSE) %>%
  mutate(
    TIMESTAMP = ymd_hms(TIMESTAMP),
    PRODUCT_LINE = as.factor(PRODUCT_LINE),
    SALES_CHANNEL_CODE = as.factor(SALES_CHANNEL_CODE),
    MEDIUM_TYPE = as.factor(MEDIUM_TYPE),
    AUTOUW = as.factor(case_when(
      .$AUTOUW == "I" ~ "Automatikus",
      TRUE ~ "Manualis"
    ))
  )

ui <- fluidPage(
  titlePanel("Kotvenyesites: alairastol jutlekfizetesig"),
  sidebarLayout(
    sidebarPanel(
      sliderInput("traceFreqInput", "Szálgyakoriság (trace frequency)", min = 0, max = 1, value = 0.25),
      checkboxGroupInput("prodLineInput", "Termekkategoriak:",
        choices = levels(t_event_log_app$PRODUCT_LINE),
        selected = "Home"
      ),
      checkboxGroupInput("autoUwInput", "Automatikus/manualis folyamatag:",
        choices = levels(t_event_log_app$AUTOUW),
        selected = levels(t_event_log_app$AUTOUW)
      ),
      checkboxGroupInput("SalesChannelInput", "Ertékesitesi csatorna:",
        choices = levels(t_event_log_app$SALES_CHANNEL_CODE),
        selected = levels(t_event_log_app$SALES_CHANNEL_CODE),
        inline = TRUE
      ),
      checkboxGroupInput("mediumInput", "Kotesi mod:",
        choices = levels(t_event_log_app$MEDIUM_TYPE),
        selected = levels(t_event_log_app$MEDIUM_TYPE),
        inline = TRUE
      ),
      actionButton("runFilter", "Kerem a folyamarajzot!")
    ),
    mainPanel(grVizOutput("processMap", width = "100%", height = "800px"))
  )
)

server <- function(input, output) {

filtered <- eventReactive(input$runFilter, {
                  t_event_log_app %>%
                    filter(
                      PRODUCT_LINE %in% input$prodLineInput & 
                        AUTOUW %in% input$autoUwInput &
                        SALES_CHANNEL_CODE %in% input$SalesChannelInput &
                        MEDIUM_TYPE %in% input$mediumInput
                    ) %>%
                    eventlog(
                      case_id = "CASE_ID",
                      activity_id = "EVENT_NAME_HU",
                      activity_instance_id = "ACTIVITY_INST_ID",
                      lifecycle_id = "LIFECYCLE_ID",
                      timestamp = "TIMESTAMP",
                      resource_id = "PARTNER_NAME"
                    ) %>%
                    filter_trace_frequency(percentage = input$traceFreqInput, reverse = F) %>%
                    filter_endpoints(start_activities = "alairas", end_activities = "jutalek_kifizetes") %>% 
                    filter_activity("enyil_papir_erkezes", reverse = TRUE)
                })  
               
  output$processMap <- renderGrViz({
    filtered() %>%
      process_map(type_nodes = frequency("absolute"), type_edges = frequency("absolute"), rankdir = "TB")
  })
}

shinyApp(ui = ui, server = server)