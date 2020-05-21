message(Sys.time(), "\n\n")

real_time_rq <- httr::GET("example.com")
real_time <- httr::parse_http_date(httr::headers(real_time_rq)$date)

message(
  "Offset of this server and real time is:\n", 
  format(Sys.time() - real_time)
  )

source("secret.R")
source("initializers.R")
source("parse_sensor_strings.R")

suppressPackageStartupMessages({
  library(DBI)
  library(dplyr)
  library(dbplyr)
  library(RSQLite)
  })


message("Connecting to Raw DB")
con_raw <- etl_connect_raw()
message("Connecting to Shadow DB\n")
con_sh_s <- etl_connect_shadow("sensors")


last_gotten_sensor <- tbl(con_sh_s, "from_raw") %>% 
  summarise(uid = max(rawuid, na.rm = TRUE)) %>% 
  collect() %>% 
  pull(uid)

message("Last row in shadow is from Raw uid: ", last_gotten_sensor)


recent_sensor_rows <- tbl(etl_connect_raw(con_raw), "hologram") %>% 
  filter(uid > last_gotten_sensor, uid < last_gotten_sensor + 100) %>% 
  collect()

message("Collected ", nrow(recent_sensor_rows), " rows")


# extract out data and rawdb id into list elements
recent_sensor_rows_list <- 
  recent_sensor_rows %>% 
  select(uid, data) %>% 
  purrr::pmap(rawdb_hologram_to_lst)

# indices of working nodes vs gateways/awakes/sensorless nodes
nodes_idx <- recent_sensor_rows_list %>% 
  purrr::map_lgl(~stringr::str_count(.x$data, "~") > 10)

message("Found ", sum(nodes_idx), " nodes and ", sum(!nodes_idx), " others\n")

message(Sys.time(), " parsing gateways")

# gateways and awake messages
parsed_others <- recent_sensor_rows_list[!nodes_idx] %>% 
  purrr::map(etl_parse_gws) %>% 
  bind_rows() %>% 
  mutate_all(~na_if(., -999))

message(Sys.time(), " finished\n")


message(Sys.time(), " parsing nodes")

parsed_nodes <- recent_sensor_rows_list[nodes_idx] %>% 
  purrr::map(etl_parse_nds)

message(Sys.time(), " finished\n")

metas <- purrr::map(parsed_nodes, "water_node_data") %>% 
  bind_rows() %>% mutate_all(~na_if(., -999))
TDRs <- purrr::map(parsed_nodes, "water_sensor_data") %>% 
  bind_rows() %>% mutate_all(~na_if(., -999))
ambs <- purrr::map(parsed_nodes, "ambient_sensor_data") %>% 
  bind_rows() %>% mutate_all(~na_if(., -999))


# write rows to shadow DB
# gateways, node metadata, TDR, ambient, 
#   and record of pulled Raw rows
dbWriteTable(
  con_sh_s,
  "water_gateway_data",
  parsed_others,
  append = TRUE
)


dbWriteTable(
  con_sh_s,
  "water_node_data",
  metas,
  append = TRUE
)

dbWriteTable(
  con_sh_s,
  "water_sensor_data",
  TDRs,
  append = TRUE
)

dbWriteTable(
  con_sh_s,
  "ambient_sensor_data",
  ambs,
  append = TRUE
)

dbWriteTable(
  con_sh_s,
  "from_raw",
  recent_sensor_rows %>% 
    select(rawuid = uid),
  append = TRUE
)

message("Successfully written to Shadow DB")

# TODO
# pass over shadow gateway table to fill hologram_metadata
# currently no data in there, so not a top priority

dbDisconnect(con_raw)
dbDisconnect(con_sh_s)

message(Sys.time(), "\n\n")
