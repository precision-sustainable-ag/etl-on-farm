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
    "{getwd()}/log/raw_to_shadow_hologram_{decolonize(Sys.time())}.log"
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
source("parse_sensor_strings.R")

suppressPackageStartupMessages({
  library(DBI)
  library(dplyr)
  library(dbplyr)
  library(RSQLite)
  })


message("Connecting to Raw DB")
con_raw <- etl_connect_raw()
message("Connecting to Shadow DB")
con_sh_s <- etl_connect_shadow("sensors")


last_gotten_sensor <- tbl(con_sh_s, "from_raw") %>% 
  summarise(uid = max(rawuid, na.rm = TRUE)) %>% 
  collect() %>% 
  pull(uid)

message(
  glue::glue("Last row in shadow is from Raw uid: {last_gotten_sensor}")
  )


recent_sensor_rows <- tbl(etl_connect_raw(con_raw), "hologram") %>% 
  filter(uid > last_gotten_sensor, uid < last_gotten_sensor + 600) %>% 
  collect()

loggit(
  "INFO",
  "Collected sensor strings",
  rows = nrow(recent_sensor_rows)
)

# extract out data and rawdb id into list elements
recent_sensor_rows_list <- 
  recent_sensor_rows %>% 
  select(uid, data) %>% 
  purrr::pmap(rawdb_hologram_to_lst)

# indices of working nodes vs gateways/awakes/sensorless nodes
nodes_idx <- recent_sensor_rows_list %>% 
  purrr::map_lgl(~stringr::str_count(.x$data, "~") > 10)


loggit(
  "INFO",
  "Alleged gateway strings",
  rows = sum(!nodes_idx, na.rm = T)
)
loggit(
  "INFO",
  "Alleged node strings",
  rows = sum(nodes_idx, na.rm = T)
)


# gateways and awake messages
parsed_others <- recent_sensor_rows_list[!nodes_idx] %>% 
  purrr::map(etl_parse_gws) %>% 
  bind_rows() %>% 
  mutate_all(~na_if(., -999))
message("Parsed gateways")



parsed_nodes <- recent_sensor_rows_list[nodes_idx] %>% 
  purrr::map(etl_parse_nds)

message("Parsed nodes")


metas <- purrr::map(parsed_nodes, "water_node_data") %>% 
  bind_rows() %>% mutate_all(~na_if(., -999))
TDRs <- purrr::map(parsed_nodes, "water_sensor_data") %>% 
  bind_rows() %>% mutate_all(~na_if(., -999))
ambs <- purrr::map(parsed_nodes, "ambient_sensor_data") %>% 
  bind_rows() %>% mutate_all(~na_if(., -999))

message("Stored local dfs")

message("Writing to shadow DB")
# write rows to shadow DB
# gateways, node metadata, TDR, ambient, 
#   and record of pulled Raw rows

rows_aff <- dbWriteTable(
  con_sh_s,
  "water_gateway_data",
  parsed_others,
  append = TRUE
)

loggit(
  "INFO",
  "water_gateway_data",
  rows = nrow(parsed_others)
)

rows_aff <- dbWriteTable(
  con_sh_s,
  "water_node_data",
  metas,
  append = TRUE
)

loggit(
  "INFO",
  "water_node_data",
  rows = nrow(metas)
)

rows_aff <- dbWriteTable(
  con_sh_s,
  "water_sensor_data",
  TDRs,
  append = TRUE
)

loggit(
  "INFO",
  "water_sensor_data",
  rows = nrow(TDRs)
)

rows_aff <- dbWriteTable(
  con_sh_s,
  "ambient_sensor_data",
  ambs,
  append = TRUE
)

loggit(
  "INFO",
  "ambient_sensor_data",
  rows = nrow(ambs)
)

rows_aff <- dbWriteTable(
  con_sh_s,
  "from_raw",
  recent_sensor_rows %>% 
    select(rawuid = uid),
  append = TRUE
)

loggit(
  "INFO",
  "from_raw",
  rows = nrow(recent_sensor_rows)
)


# TODO
# pass over shadow gateway table to fill hologram_metadata
# currently no data in there, so not a top priority

dbDisconnect(con_raw)
dbDisconnect(con_sh_s)

message("Execution end")

set_logfile(logfile = NULL, confirm = F)
