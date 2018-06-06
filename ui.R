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