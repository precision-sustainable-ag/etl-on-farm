message(Sys.time(), "\n\n")

x <- na.omit(stringr::str_match(commandArgs(), "--file=(.+)")[,2])
if (length(x)) setwd(dirname(x))

decolonize <- function(s) {
  s <- stringr::str_replace_all(s, "[[:punct:]]", "_") 
  stringr::str_replace_all(s, "[[:space:]]", "__")
}

suppressPackageStartupMessages(
  library(loggit)
)

set_logfile(
  glue::glue(
    "{getwd()}/log/shadow_to_prod_stresscams_{decolonize(Sys.time())}.log"
  )
)
set_timestamp_format("%Y-%m-%dT%H:%M:%OS4%z")

message("Execution start")

real_time_rq <- httr::RETRY("GET", "example.com")
real_time <- httr::parse_http_date(httr::headers(real_time_rq)$date)

off <- Sys.time() - real_time
loggit(
  "INFO",
  glue::glue("Offset of this server and real time is {format(off)}"),
  offset_s = as.double(off, units = "secs")
)

source("secret.R")
source("utils/initializers.R")
source("utils/sql_constructors.R")

suppressPackageStartupMessages({
  library(DBI)
  library(dplyr)
  library(dbplyr)
  library(RSQLite)
})

message("Connecting to Shadow DB")
con_sh_st <- etl_connect_shadow("stresscams")


rawuids_to_push <- tbl(con_sh_st, "from_raw") %>% 
  filter(pushed_to_prod == 0) %>% 
  collect() %>% 
  pull(rawuid) %>% 
  head(500) %>% 
  unique()

loggit(
  "INFO",
  "Found stresscam records to push",
  rows = length(rawuids_to_push)
)

if (length(rawuids_to_push) == 0) {
  stop("Ending script execution.", call. = FALSE)
  set_logfile(logfile = NULL)
}

rows_aff <- etl_upsert_sensors(
  "stresscam_data", 
  "stresscam_data", 
  rawuids_to_push,
  unicity = "stresscam_device_id_timestamp_utc_key"
)

loggit(
  "INFO",
  "water_gateway_data",
  rows = rows_aff
)

# rows_aff <- etl_upsert_sensors(
#   "water_node_data", 
#   "water_node_data", 
#   rawuids_to_push,
#   unicity = "water_node_data_node_serial_no_timestamp_key"
# )
# loggit(
#   "INFO",
#   "water_node_data",
#   rows = rows_aff
# )
# 
# rows_aff <- etl_upsert_sensors(
#   "water_sensor_data", 
#   "water_sensor_data", 
#   rawuids_to_push,
#   unicity = "water_sensor_data_node_serial_no_timestamp_tdr_address_key"
# )
# loggit(
#   "INFO",
#   "water_sensor_data",
#   rows = rows_aff
# )
# 
# rows_aff <- etl_upsert_sensors(
#   "ambient_sensor_data", 
#   "ambient_sensor_data", 
#   rawuids_to_push,
#   unicity = "ambient_sensor_data_node_serial_no_timestamp_key"
# )
# 
# loggit(
#   "INFO",
#   "ambient_sensor_data",
#   rows = rows_aff
# )


rows_aff <- etl_mark_pushed(con_sh_st, "from_raw", rawuids_to_push)
loggit(
  "INFO",
  "from_raw",
  rows = rows_aff
)

dbDisconnect(con_sh_st)


message("Execution end")

set_logfile(logfile = NULL)
