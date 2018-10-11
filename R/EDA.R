# Explore event log

# Get event log
t_event_log_app <- read.csv(here::here("Data", "t_event_log.csv"), stringsAsFactors = FALSE) %>%
  mutate(
    TIMESTAMP = ymd_hms(TIMESTAMP),
    PRODUCT_LINE = as.factor(PRODUCT_LINE),
    SALES_CHANNEL = as.factor(SALES_CHANNEL),
    MEDIUM_TYPE = as.factor(MEDIUM_TYPE),
    AUTOUW = as.factor(case_when(
      .$AUTOUW == "I" ~ "Automatikus",
      TRUE ~ "Manualis"
    ))
  )

# Filter event log
t_event_log_app_filtered <-  t_event_log_app %>%
  filter(
    PRODUCT_LINE == "Home"
  ) %>%
  eventlog(
    case_id = "CASE_ID",
    activity_id = "EVENT_NAME_HU",
    activity_instance_id = "ACTIVITY_INST_ID",
    lifecycle_id = "LIFECYCLE_ID",
    timestamp = "TIMESTAMP",
    resource_id = "PARTNER_NAME"
  ) %>%
  #filter_trace_frequency(percentage = input$traceFreqInput, reverse = F) %>%
  #filter_endpoints(start_activities = "alairas", end_activities =
  #                  c("orvosi_validalas lezar_meneszt", "adat_ellenorzes lezar_meneszt")) %>% 
  filter_activity("enyil_papir_erkezes", reverse = TRUE) %>% 
  filter_activity_presence("orvosi_elbiralas tovabbad", method = "all")

# Gen exploratory dashboards
t_event_log_app_filtered %>% activity_dashboard()

t_event_log_app_filtered %>%
  start_activities("activity") %>% 
  plot

t_event_log_app_filtered %>%
  end_activities("activity") %>% 
  plot


t_event_log_app_filtered %>% activity_presence() %>% # as of cases
  plot()

t_event_log_app_filtered %>%
  trace_coverage("trace") %>%
  plot()

t_throughput_case <- t_event_log_app_filtered %>% throughput_time(level = "case", units = "day")
ggplot(t_throughput_case, aes(x = throughput_time)) +
  geom_histogram(bins = 20) +
  theme_minimal()

# Gen process maps
 t_event_log_app_filtered %>%
   filter_trace_frequency(percentage = 0.5, reverse = F) %>%
   filter_activity_frequency(percentage = 1, reverse = F) %>%
      process_map(type_nodes = frequency("absolute"),
                  type_edges = frequency("absolute"), rankdir = "TB")