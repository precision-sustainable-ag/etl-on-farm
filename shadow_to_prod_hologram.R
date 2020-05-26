message(Sys.time(), "\n\n")
decolonize <- function(tm) {
  tm <- gsub("[-:]", "_", tm)
  gsub(" ", "__", tm)
}

suppressPackageStartupMessages(
  library(loggit)
)

set_logfile(
  glue::glue(
    "{getwd()}/log/shadow_to_prod_hologram_{decolonize(Sys.time())}.log"
  )
)

message("Execution start")

real_time_rq <- httr::GET("example.com")
real_time <- httr::parse_http_date(httr::headers(real_time_rq)$date)

off <- Sys.time() - real_time
loggit(
  "INFO",
  glue::glue("Offset of this server and real time is {format(off)}"),
  offset_s = off
)

source("secret.R")
source("initializers.R")
source("sql_constructors.R")

suppressPackageStartupMessages({
  library(DBI)
  library(dplyr)
  library(dbplyr)
  library(RSQLite)
})

message("Connecting to Shadow DB")
con_sh_s <- etl_connect_shadow("sensors")


rawuids_to_push <- tbl(con_sh_s, "from_raw") %>% 
  filter(pushed_to_prod == 0) %>% 
  collect() %>% 
  pull(rawuid) %>% 
  head(1000) %>% 
  unique()

loggit(
  "INFO",
  "Found sensor strings to push",
  rows = length(rawuids_to_push)
)

if (length(rawuids_to_push) == 0) {
  stop("Ending script execution.", call. = FALSE)
  set_logfile(logfile = NULL)
}

rows_aff <- etl_upsert_sensors(
  "water_gateway_data", 
  "water_gateway_data", 
  rawuids_to_push,
  unicity = "water_gateway_data_gateway_serial_no_timestamp_key"
  )

loggit(
  "INFO",
  "water_gateway_data",
  rows = rows_aff
)

rows_aff <- etl_upsert_sensors(
  "water_node_data", 
  "water_node_data", 
  rawuids_to_push,
  unicity = "water_node_data_node_serial_no_timestamp_key"
  )
loggit(
  "INFO",
  "water_node_data",
  rows = rows_aff
)

rows_aff <- etl_upsert_sensors(
  "water_sensor_data", 
  "water_sensor_data", 
  rawuids_to_push,
  unicity = "water_sensor_data_node_serial_no_timestamp_tdr_address_key"
  )
loggit(
  "INFO",
  "water_sensor_data",
  rows = rows_aff
)

rows_aff <- etl_upsert_sensors(
  "ambient_sensor_data", 
  "ambient_sensor_data", 
  rawuids_to_push,
  unicity = "ambient_sensor_data_node_serial_no_timestamp_key"
  )

loggit(
  "INFO",
  "ambient_sensor_data",
  rows = rows_aff
)


rows_aff <- etl_mark_pushed(con_sh_s, "from_raw", rawuids_to_push)
loggit(
  "INFO",
  "from_raw",
  rows = rows_aff
)

dbDisconnect(con_sh_s)


message("Execution end")

set_logfile(logfile = NULL)
