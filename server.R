library(shiny)
library(dplyr)
library(lubridate)
library(bupaR)
library(processmapR)
library(DiagrammeR)

t_event_log_app <- read.csv(here::here("Data", "t_event_log.csv"), stringsAsFactors = FALSE) %>%
  mutate(
    TIMESTAMP = ymd_hms(TIMESTAMP),
    PRODUCT_LINE = as.factor(PRODUCT_LINE),
    SALES_CHANNEL = as.factor(SALES_CHANNEL),
    MEDIUM_TYPE = as.factor(MEDIUM_TYPE),
    AUTOUW = as.factor(case_when(
      .$AUTOUW == "I" ~ "Automatikus",
      TRUE ~ "Manuális"
    ))
  )

server <- function(input, output) {

filtered <- eventReactive(input$runFilter, {
                  t_event_log_app %>%
                    filter(
                      PRODUCT_LINE %in% input$prodLineInput & 
                        AUTOUW %in% input$autoUwInput &
                        SALES_CHANNEL %in% input$SalesChannelInput &
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
                    filter_endpoints(start_activities = "alairas",
                                     end_activities = "jutalek_kifizetes") %>% 
                    filter_activity("enyil_papir_erkezes", reverse = TRUE)
                })  
               
  output$freqMap <- renderGrViz({
    filtered() %>%
      process_map(type_nodes = frequency("absolute"),
                  type_edges = frequency("absolute"), rankdir = "TB")
    })
  
  output$perfMap <- renderGrViz({
    filtered() %>%
      process_map(performance(median, "days"), rankdir = "TB")
    })
}