# Load required libs
library(config)
library(here)
library(dplyr)
library(ggplot2)
library(tidyr)
library(lubridate)
library(bupaR)
library(processmapR)
library(processmonitR)

#########################################################################################
# Data Extraction #######################################################################
#########################################################################################

# Set JAVA_HOME, set max. memory, and load rJava library
Sys.setenv(JAVA_HOME = "C:\\Program Files\\Java\\jre1.8.0_171")
options(java.parameters = "-Xmx2g")
library(rJava)

# Output Java version
.jinit()
print(.jcall("java/lang/System", "S", "getProperty", "java.version"))

# Load RJDBC library
library(RJDBC)

# Get credentials
kontakt <-
  config::get("kontakt", file = "C:\\Users\\PoorJ\\Projects\\config.yml")

# Create connection driver
jdbcDriver <-
  JDBC(driverClass = "oracle.jdbc.OracleDriver", classPath = "C:\\Users\\PoorJ\\Desktop\\ojdbc7.jar")

# Open connection: kontakt---------------------------------------------------------------
jdbcConnection <-
  dbConnect(
    jdbcDriver,
    url = kontakt$server,
    user = kontakt$uid,
    password = kontakt$pwd
  )

# Fetch data
query <- "select * from t_newbusiness_event_log"
t_newbusiness_event_log <- dbGetQuery(jdbcConnection, query)

# Close db connection: kontakt
dbDisconnect(jdbcConnection)


#########################################################################################
# Data Transformation ###################################################################
#########################################################################################
#########################################################################################

t_event_log <- t_newbusiness_event_log %>%
  mutate(ACTIVITY_INST_ID = as.numeric(row.names(.)),
         CONTRACT_PERIOD = paste0(substr(CONTRACT_PERIOD, 1, 4), "/", substr(CONTRACT_PERIOD, 6, 7))) %>%
  gather(
    LIFECYCLE_ID,
    TIMESTAMP,
    -CASE_ID,
    -EVENT_NAME,
    -EVENT_NAME_HU,
    -PRODUCT_CODE,
    -PRODUCT_LINE,
    -PARTNER_CODE,
    -PARTNER_NAME,
    -KTI_CODE,
    -KTI_NAME,
    -SALES_CHANNEL_CODE,
    -SALES_CHANNEL,
    -MEDIUM_TYPE,
    -AUTOUW,
    -CONTRACT_PERIOD,
    -ACTIVITY_INST_ID
  ) %>%
  mutate(TIMESTAMP = ymd_hms(TIMESTAMP)) %>%
  arrange(CASE_ID, TIMESTAMP) %>%
  mutate(LIFECYCLE_ID = case_when(
    .$LIFECYCLE_ID == "EVENT_BEGIN" ~ "START",
    TRUE ~ "END"
  ))

# Gen app input
write.csv(t_event_log %>% filter(CONTRACT_PERIOD == '2018/01'), here::here("Data", "t_event_log_201801.csv"))




# Transform df into bupaR eventlog
t_log_bupar <- t_event_log %>%
  filter(PRODUCT_LINE == "Home") %>% 
  eventlog(
    case_id = "CASE_ID",
    activity_id = "EVENT_NAME_HU",
    activity_instance_id = "ACTIVITY_INST_ID",
    lifecycle_id = "LIFECYCLE_ID",
    timestamp = "TIMESTAMP",
    resource_id = "PARTNER_NAME"
  )

# Filter for frequent events
event_log_filt <- t_log_bupar %>%
  filter_activity_frequency(percentage = 0.9, reverse = F) 

# Filter for frequent traces
event_log_filt <- t_log_bupar %>%
  filter_endpoints(start_activities = "alairas", end_activities = "jutalek_kifizetes") %>% 
  filter_trace_frequency(percentage = 0.25, reverse = F) 

# Plot frequency map
event_log_filt %>% 
  process_map()

# Plot performance map
event_log_filt %>% 
  process_map(performance(median, "days"))


# Plot custom
event_log_filt %>% 
process_map(type_nodes = frequency("absolute"), type_edges = frequency("absolute"), rankdir = "TB")


event_log_filt %>% 
  process_map(type_nodes = frequency("absolute"), type_edges = performance(median, "days"), rankdir = "TB")

# Build filters for
# AutoUW v Manual
# Prod line
# Medium type
# Trace frequency

activity_dashboard(t_log_bupar)