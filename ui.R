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

ui <- fluidPage(
  titlePanel("Kötvényesítés: interaktív folyamattérkép"),
  sidebarLayout(
    sidebarPanel(
      sliderInput("traceFreqInput", "Szálgyakoriság (trace frequency)",
                  min = 0, max = 1, value = 0.25),
      checkboxGroupInput("prodLineInput", "Termékkategóriák:",
        choices = levels(t_event_log_app$PRODUCT_LINE),
        selected = "Home"
      ),
      checkboxGroupInput("autoUwInput", "Automatikus/manualis folyamatág:",
        choices = levels(t_event_log_app$AUTOUW),
        selected = levels(t_event_log_app$AUTOUW)
      ),
      checkboxGroupInput("SalesChannelInput", "Értékesitesi csatorna:",
        choices = levels(t_event_log_app$SALES_CHANNEL),
        selected = levels(t_event_log_app$SALES_CHANNEL),
        inline = TRUE
      ),
      checkboxGroupInput("mediumInput", "Kötesi mód:",
        choices = levels(t_event_log_app$MEDIUM_TYPE),
        selected = levels(t_event_log_app$MEDIUM_TYPE),
        inline = TRUE
      ),
      actionButton("runFilter", "Kérem a folyamarajzot!")
    ),
    mainPanel(
      tabsetPanel(type = "tabs",
                  tabPanel("Gyakoriság", grVizOutput("freqMap", width = "100%", height = "800px")),
                  tabPanel("Teljesítmény", grVizOutput("perfMap", width = "100%", height = "800px")))
    )
  )
)