library(bupaR)
library(processmapR)
library(DiagrammeR)
library(tidyverse)
library(lubridate)
library(broom)

# Load dataset
t_event_log_app <- read.csv(here::here("Data", "t_event_log.csv"),
                            stringsAsFactors = FALSE) %>%
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


# Select cols to transform to eventlog with bupar::eventlog
t_event_log_clean <- t_event_log_app %>%
  select(CASE_ID, EVENT_NAME, TIMESTAMP, ACTIVITY_INST_ID, LIFECYCLE_ID, PARTNER_NAME, PRODUCT_LINE) 


# Simple nestting
# by_product <- t_event_log_clean %>% 
#   group_by(PRODUCT_LINE) %>% 
#   nest()


# Data manipulation funcs to use in purrr::map
trace_num <- function(df){
  number_of_traces(
    eventlog(
           df,
           case_id = "CASE_ID",
           activity_id = "EVENT_NAME",
           activity_instance_id = "ACTIVITY_INST_ID",
           lifecycle_id = "LIFECYCLE_ID",
           timestamp = "TIMESTAMP",
           resource_id = "PARTNER_NAME"
           ))
}


trace_cov <- function(df){
  trace_coverage(
    eventlog(
      df,
      case_id = "CASE_ID",
      activity_id = "EVENT_NAME",
      activity_instance_id = "ACTIVITY_INST_ID",
      lifecycle_id = "LIFECYCLE_ID",
      timestamp = "TIMESTAMP",
      resource_id = "PARTNER_NAME"
    ), level = "trace")
}


# Returns df 7x1 -> unnest will fail
# through_time <- function(df) {
#   throughput_time(
#     eventlog(
#       df,
#       case_id = "CASE_ID",
#       activity_id = "EVENT_NAME",
#       activity_instance_id = "ACTIVITY_INST_ID",
#       lifecycle_id = "LIFECYCLE_ID",
#       timestamp = "TIMESTAMP",
#       resource_id = "PARTNER_NAME"
#     ),
#     level = "log", units = "day"
#   )[c("mean", "median", "min", "max", "st_dev", "q1", "q3")]
# }


# Returns df 1x7 -> unnest will work
through_time <- function(df) {
  tidyr::spread(
    data = data.frame(
      metric = c("mean", "median", "min", "max", "st_dev", "q1", "q3"),
      values = throughput_time(
        eventlog(
          df,
          case_id = "CASE_ID",
          activity_id = "EVENT_NAME",
          activity_instance_id = "ACTIVITY_INST_ID",
          lifecycle_id = "LIFECYCLE_ID",
          timestamp = "TIMESTAMP",
          resource_id = "PARTNER_NAME"
        ),
        level = "log", units = "day"
      )[c("mean", "median", "min", "max", "st_dev", "q1", "q3")], row.names = NULL
    ),
    key = metric, value = values
  )
}


trace_len <- function(df) {
  tidyr::spread(
    data = data.frame(
      metric = c("mean", "median", "min", "max", "st_dev", "q1", "q3", "iqr"),
      values = trace_length(
        eventlog(
          df,
          case_id = "CASE_ID",
          activity_id = "EVENT_NAME",
          activity_instance_id = "ACTIVITY_INST_ID",
          lifecycle_id = "LIFECYCLE_ID",
          timestamp = "TIMESTAMP",
          resource_id = "PARTNER_NAME"
        ),
        level = "log", units = "day"
      )[c("mean", "median", "min", "max", "st_dev", "q1", "q3", "iqr")], row.names = NULL
    ),
    key = metric, value = values
  )
}


# Gen nested tables with aggregated stats in nested tables
by_product <- t_event_log_clean %>%
  group_by(PRODUCT_LINE) %>%
  nest() %>%
  mutate(
    trace_number = map(data, trace_num),
    through_time = map(data, through_time),
    trace_length = map(data, trace_len)
  )


# Retrieve aggregates
by_product %>% select(PRODUCT_LINE, trace_number) %>% unnest()
by_product %>% select(PRODUCT_LINE, through_time) %>% unnest()
by_product %>% select(PRODUCT_LINE, trace_length) %>% unnest()



