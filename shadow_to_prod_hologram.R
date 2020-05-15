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
etl_push_sensors(
  "water_gateway_data", 
  "water_gateway_data", 
  rawuids_to_push,
  unicity = c("gateway_serial_no", "timestamp")
  )

message("Pushing to `water_node_data`, rows inserted:")
etl_push_sensors(
  "water_node_data", 
  "water_node_data", 
  rawuids_to_push,
  unicity = c("node_serial_no", "timestamp")
  )

message("Pushing to `water_sensor_data`, rows inserted:")
etl_push_sensors(
  "water_sensor_data", 
  "water_sensor_data", 
  rawuids_to_push,
  unicity = c("node_serial_no", "timestamp", "tdr_address")
  )

message("Pushing to `ambient_sensor_data`, rows inserted:")
etl_push_sensors(
  "ambient_sensor_data", 
  "ambient_sensor_data", 
  rawuids_to_push,
  unicity = c("node_serial_no", "timestamp")
  )


message("\nMarking rows as pushed:")

indices <- glue::glue_collapse(rawuids_to_push, sep = ", ")

dbExecute(
  con_sh_s,
  glue::glue(
  "
  UPDATE from_raw
  SET pushed_to_prod = 1
  WHERE rawuid IN ({indices})
  "
  )
)

dbDisconnect(con_sh_s)


message(Sys.time(), "\n\n")
