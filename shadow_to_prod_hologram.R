message(Sys.time(), "\n\n")

source("secret.R")
source("initializers.R")
source("sql_constructors.R")

suppressPackageStartupMessages({
  library(DBI)
  library(dplyr)
  library(dbplyr)
  library(RSQLite)
})

message("Connecting to Shadow DB\n")
con_sh_s <- etl_connect_shadow("sensors")


rawuids_to_push <- tbl(con_sh_s, "from_raw") %>% 
  filter(pushed_to_prod == 0) %>% 
  pull(rawuid) %>% 
  head(1000) %>% 
  unique()

message("Found ", length(rawuids_to_push), " rows to push\n")

if (length(rawuids_to_push) == 0) {
  stop("Ending script execution.")
}


message("Pushing to `water_gateway_data`, rows inserted:")
etl_upsert_sensors(
  "water_gateway_data", 
  "water_gateway_data", 
  rawuids_to_push,
  unicity = "water_gateway_data_gateway_serial_no_timestamp_key"
  )

message("Pushing to `water_node_data`, rows inserted:")
etl_upsert_sensors(
  "water_node_data", 
  "water_node_data", 
  rawuids_to_push,
  unicity = "water_node_data_node_serial_no_timestamp_key"
  )

message("Pushing to `water_sensor_data`, rows inserted:")
etl_upsert_sensors(
  "water_sensor_data", 
  "water_sensor_data", 
  rawuids_to_push,
  unicity = "water_sensor_data_node_serial_no_timestamp_tdr_address_key"
  )

message("Pushing to `ambient_sensor_data`, rows inserted:")
etl_upsert_sensors(
  "ambient_sensor_data", 
  "ambient_sensor_data", 
  rawuids_to_push,
  unicity = "ambient_sensor_data_node_serial_no_timestamp_key"
  )


message("\nMarking rows as pushed:")

etl_mark_pushed(con_sh_s, "from_raw", rawuids_to_push)

dbDisconnect(con_sh_s)


message(Sys.time(), "\n\n")
