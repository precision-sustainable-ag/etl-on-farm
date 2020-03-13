suppressPackageStartupMessages({
  library(purrr)
  library(dplyr)
  library(httr)
  library(lubridate)
  library(readr)
  library(stringr)
  library(googlesheets)
  library(tidyr)
})


# schedule at 3/7/11/15/19/23:15 (should be an hour after posts)

message("\n-------------\n")
message(Sys.time())

safely_read_csv <- function(file, ...) {
  if (file.size(file) > 5e6) {return(NULL)} else {
    read_csv(file, ...)
  }
}


# https://hologram.io/docs/reference/cloud/http/#/introduction

# Paths and keys ----
source("secret.R")

# Look up CROWN devices ----
devices <- RETRY(
  "GET",
  baseurl, path = "api/1/devices", 
  query = list(
    apikey = alondra, 
    tagname = "CROWN", 
    limit = 200, 
    withlocation = T
    )
  )

device_list <- content(devices)$data %>% set_names(., map(., "name"))
message("# Fetched info for devices: ", length(device_list), "\n")


# Get info on device (lat/lon) ----
#   Takes response from `devices`
device_info <- function(resp) {
  if (content(resp)$success != T) {return(NULL)}
  
  x <- content(resp)$data
  
  device_name <- map_chr(x, "name", .default = NA)
  device_id <- map_chr(x, "id", .default = NA)
  
  l_s <- map(x, "lastsession")
  lat <- map_dbl(l_s, "latitude", .default = NA)
  lon <- map_dbl(l_s, "longitude", .default = NA)
  ts_up <- map_chr(l_s, "session_end", .default = NA)
  
  tibble(
    device_name, device_id, lat, lon, ts_up
  )
}

# Store device session info ----
#   latest timestamp, lat/lon, device name, etc

device_sessions <- device_info(devices)
possibly(write_csv, otherwise = "Device session info write failed.")(
  device_sessions, file.path(holo_path, "devices.csv"),
  append = T
  )




# Times are in UTC

lookup_path <- file.path(holo_path, "latest_timestamp.csv")

if (file.exists(lookup_path)) {
  lookups_max_ts <- 
    read_csv(lookup_path, col_types = "cciT") %>% 
    group_by(device_id) %>% 
    arrange(desc(max_ts), .by_group = T) %>% 
    slice(1) %>% 
    ungroup()
} else {
  lookups_max_ts <- tibble(device_id = character(0), max_ts = numeric(0))
}

# Get data from device ----
#   Takes device response from `device_info`
#   Takes lookup table from preceding block, `lookups_max_ts`


getter_continuing <- function(device_obj, lk_tbl) {
  dev_tags <- map_chr(device_obj$tags, "name", .default = NA)
  
  team <- dev_tags[dev_tags != "CROWN"]
  
  device_id <- device_obj$id
  
  timestart <- lk_tbl$max_ts[lk_tbl$device_id == device_id]
  
  query <- list(
    apikey = alondra,
    deviceid = device_id, 
    limit = 2000
  )
  
  if (length(timestart) == 1 & is.numeric(timestart)) {
    query$timestart <- timestart
    message("Found last timestamp for ", device_id, " at ", as_datetime(timestart-1))
  } else {
    message("Found no timestamp for ", device_id, ", fetching all messages")
  }
  
  
  messages <- RETRY(
    "GET",
    baseurl, 
    path = "api/1/csr/rdm",
    query = query
  )
  
  if (status_code(messages) != 200) {
    # TODO store an error somewhere TODO
    return(NULL)
  }
  
  messages_0 <- content(messages)$data
  
  tries <- 0
  
  while (content(messages)$continues & tries < 5) {
    earliest <- content(messages)$data %>% 
      map_chr("logged") %>% 
      ymd_hms() %>% 
      as.integer() %>% 
      min()
    
    query$timeend <- earliest
    
    message("__Continue fetching device ", device_id, " before ", as_datetime(earliest))
    
    messages <- RETRY(
      "GET",
      baseurl, 
      path = "api/1/csr/rdm",
      query = query
    )
    tries <- tries + 1
    messages_0 <- c(messages_0, content(messages)$data)
  }
  
  messages_0
  
  messages_data <- messages_0 %>%
    map("data") %>%
    map(jsonlite::fromJSON)

  tibble(
    ts_up = map_chr(messages_data, "received", .default = NA),
    device_id = map_chr(messages_data, "device_id", .default = NA),
    device_name = map_chr(messages_data, "device_name", .default = NA),
    data_base64 = map_chr(messages_data, "data", .default = NA)
  ) %>%
    mutate(
      data = map(data_base64, possibly(jsonlite::base64_dec, otherwise = NA)),
      data = map_chr(data, possibly(rawToChar, otherwise = NA)),
      ts_dl = Sys.time()
      ) %>% 
    select(ts_dl, everything())
}


gotten_messages <- device_list %>% 
  map(getter_continuing, lk_tbl = lookups_max_ts)


# Count of messages from GET ----
message("\nMessages retrieved:")
map_dbl(gotten_messages, nrow) %>% tibble::enframe() %>% print(n = 100)

# # Fix old format
# map_dfr(gotten_messages, ~distinct(.x, device_id, device_name)) %>% 
#   full_join(read_csv(lookup_path, col_types = cols(.default = "c")))  %>% 
#   write_csv(lookup_path)







# Separate message types from response ----
#   Takes a tibble of responses from `getter`
split_gn <- function(gotten) {
  gotten <- gotten %>% filter(str_detect(device_name, "^CROWN"))
  
  if (nrow(gotten) == 0) {return(NULL)} 
  
  messages <- str_detect(gotten$data, "Gateway|\\{")
  gateways <- str_sub(gotten$data, 4, 4) == "~"
  
  lst <- split(
    gotten,
    -messages + gateways + 2
  )
  # 1: messages, 2: nodes, 3: gateway
  
  new_names <- c("messages", "nodes", "gateway")
  names(lst) <- new_names[as.numeric(names(lst))]
  
  lst
  
}

splitted_messages <- gotten_messages %>% map(split_gn)







# Store raw data from nodes, gateways, and other messages in CSV ----
#  Function takes a named list of these three tibbles from `split_gn`

archive_gn <- function(lst) {
  device_name <- unlist(map(lst, "device_name"))[1]
  device_id <- unlist(map(lst, "device_id"))[1]
  

  writer <- function(dat, name, path = "") {
    fname = paste(device_name, name, ".csv", sep = "_")
    pname = file.path(path, fname)
    if (file.exists(pname)) {cn = F} else {cn = T}

    write_csv(dat, pname, append = T, col_names = cn)
  }
  # TODO update path
  iwalk(lst, writer, path = file.path(holo_path, "archives"))
  
}

splitted_messages %>% walk(archive_gn)






# con <- dbConnect(
#   Postgres(),
#   dbname = "crowndb",
#   host = "crownproject.postgres.database.usgovcloudapi.net",
#   port = 5432,
#   user = "shinyuser@crownproject",
#   password = "CROWNaccess+",
#   sslmode = "require"
# )
# dbListTables(con)
# tbl(con, "wsensor_allocation") %>%
#   collect()


# get node-gateway info ----


#my_auth_token <- gs_auth()
#saveRDS(my_auth_token, "my_auth_token.rds")
message("\n")
gs_auth(token = file.path(holo_path, "my_auth_token.rds"))

# gs_keys from secret.R here


get_allocations <- function(key) {
  raw_gs <- gs_key(key, lookup = T) %>% 
    gs_read(ws = "Sensor_Install", verbose = F) %>% 
    rename(
      node_id = `Node Serial #`, 
      gateway_id = `Gateway Serial #`,
      code = `Farm Code`
      )
  
  compound_flag <- any(str_detect(raw_gs$node_id, ","))
  
  if (compound_flag) {
    raw_gs <- raw_gs %>% 
    separate_rows(node_id, sep = ",[ ]?")
  }
  
  raw_gs %>% 
    mutate(
      node_id = str_sub(node_id, -5, -1),
      gateway_id = str_sub(gateway_id, -3, -1),
      node_id = str_pad(node_id, width = 5, "left", "0"),
      gateway_id = str_pad(gateway_id, width = 3, "left", "0"),
      Subplot = str_extract(Subplot, "[0-9]")
    ) %>% 
    mutate_all(as.character) %>% 
    mutate_at(
      vars(matches("Date")), 
      ~parse_date_time(., orders = c("mdy", "ymd", "mdY", "Ymd"))
    ) 
}

allocation_list <- 
  suppressMessages(map(
    gs_keys,
    possibly(get_allocations, otherwise = NULL)
  )) %>% bind_rows(.id = "state")

get_trt_assignments <- function(key) {
  gs_key(key, lookup = T) %>% 
    gs_read(ws = "Sensor_Allocation", verbose = F) %>% 
    select(matches("Sketch"), matches("Sim")) %>% 
    rename_all(~str_extract(., "^[:alpha:]+")) %>% 
    filter(str_length(Gateway) == 3) %>% 
    gather(key = trt, value = node_id, -Gateway, -Sim) %>% 
    mutate(trt = str_extract(trt, "B|C")) %>% 
    select(sim = Sim, gateway_id = Gateway, trt, node_id)
}

allocation_assignments <- 
  suppressMessages(
    gs_keys[c("NC", "MD")] %>% 
      map_dfr(get_trt_assignments) %>% 
      full_join(allocation_list) %>% 
      filter(!is.na(code))
  )
  



# Do the actual parsing ----

source(file.path(holo_path, "node_parsing.R"))


safely_write_csv_append <- function(x, pname, ...) {
  if (file.exists(pname)) {cn = F} else {cn = T}
  
  write_csv(x, pname, append = T, col_names = cn)
}

# TODO unlikely mode of failure is pulling data outside of
# `Date In` and `Date Out`, should be dealt with at DB level maybe

parse_node_tibble <- function(tbl) {
  if (is.null(tbl)) return(NULL)
  
  parsed_tbl <- tbl %>% 
    select(-data_base64, -ts_dl) %>% 
    mutate(data = map(data, parse_node_string)) %>% 
    unnest()
  
  if (nrow(parsed_tbl) == 0) return(NULL)
  
  suppressMessages(
    parsed_tbl %>% 
      left_join(allocation_assignments %>% select(-sim)) %>% 
      select(-state, -gateway_id, -matches("Date"), -notes)
  )
  
}

t_begin <- Sys.time()

parsed_nodes <- 
  map(splitted_messages, "nodes") %>% 
    map(parse_node_tibble) %>% 
    compact()

# elapsed for node parsing
message("\n# Node parsing, time elapsed:")
print(Sys.time() - t_begin)



parse_gateway_tibble <- function(tbl) {
  if (is.null(tbl)) return(NULL)
  
  parsed_tbl <- tbl %>% 
    select(-data_base64, -ts_dl) %>% 
    mutate(data = map(data, parse_gateway_string)) %>% 
    unnest()
  
  if (nrow(parsed_tbl) == 0) return(NULL)
  
  suppressMessages(
    parsed_tbl %>% 
      left_join(allocation_assignments) %>% 
      select(-state, -node_id, -matches("Date"), -notes)
  )
}

t_begin <- Sys.time()

parsed_gateways <- map(splitted_messages, "gateway") %>% 
  map(parse_gateway_tibble) %>% 
  compact()

# elapsed for gateway parsing
message("\n# Gateway parsing, time elapsed:")
print(Sys.time() - t_begin)


safely_write_csv_join <- function(x, pname, ...) {
  if (file.exists(pname)) {
    
    x <- suppressMessages(
      read_csv(pname, col_types = cols(.default = "c")) %>% 
        full_join(x) %>% 
        distinct()
    )
  }
  
  Sys.sleep(0.5)
  
  write_csv(x, pname)
}

t_begin <- Sys.time()

parsed_nodes %>% 
  iwalk(~safely_write_csv_join(
    .x, pname = glue::glue(holo_path, "/parsed/sensors/{.y}.csv")
    ))

# elapsed for node parsing
message("\n# Nodes joining and writing, time elapsed:")
print(Sys.time() - t_begin)


t_begin <- Sys.time()

parsed_gateways %>% 
  iwalk(~safely_write_csv_join(
    .x, pname = glue::glue(holo_path, "/parsed/gateway/{.y}.csv")
    ))

# elapsed for node parsing
message("\n# Gateways joining and writing, time elapsed:")
print(Sys.time() - t_begin)


message("# Storing timestamps after successful scrape")
lookup_flag <- file.exists(lookup_path)

bind_rows(gotten_messages) %>% 
  group_by(device_id, device_name) %>% 
  summarise(max_ts = max(1L+as.integer(ymd_hms(ts_up)))) %>% 
  mutate(max_ts_human = as_datetime(max_ts)) %>% 
  write_csv(lookup_path, col_names = !lookup_flag, append = lookup_flag)

# Write sensor summary by reading in all data ----
message("\n# Computing summary, parsing timestamps:")
bulk <- 
  tibble(
    depth = c("0","10","40","75"),
    bulk = c(5, 25, 40, 35)
  )

path_2017 <- "C:/Users/Brian.Davis/Google Drive/Workstation/Shared Project Folders/CROWN/DATA/OLD/Parsed Water Data/2017/all_2017_sensors.csv"
path_2018 <- list.files(
  file.path(
    "C:/Users/Brian.Davis/Google Drive/Workstation/Shared Project Folders",
     "/CROWN/DATA/OLD/Parsed Water Data/2018/CombinedData"
  ),
  full.names = T, pattern = ".csv"
)

vwc_summary <- list.files(
  file.path(holo_path, "parsed/sensors"), 
  full.names = T
  ) %>% 
  c(path_2017, path_2018) %>% 
  map(
    ~read_csv(.x, col_types = cols(.default = "c")) %>% 
      rename_all(str_to_lower)
    ) %>% 
  bind_rows() %>% 
  mutate_at(vars(ts_up, timestamp), ymd_hms) %>% 
  mutate(vwc = as.numeric(vwc),
         d = floor_date(timestamp, unit = "day")) %>% 
  filter(!is.na(timestamp)) %>% 
  filter(vwc < 100, vwc > 0, is.finite(vwc)) %>% 
  group_by(node_id, code, trt, d, depth) %>% 
  summarise(vwc_within_depth_1d = mean(vwc)) %>% 
  full_join(bulk) %>% 
  group_by(node_id, code, trt, d) %>% 
  summarise(vwc = sum(vwc_within_depth_1d*bulk)/sum(bulk)) %>% 
  ungroup() %>% 
  mutate(inches = vwc / 2.54,
         trt = str_to_lower(trt))

write_csv(vwc_summary, file.path(holo_path, "vwc_summary.csv"))
gs_upload(
  file.path(holo_path, "vwc_summary.csv"), 
  "CROWN_water_summary", 
  overwrite = T
  )

# vwc_summary %>% filter(is.na(code)) %>% distinct(node_id, .keep_all = T)
# vwc_summary %>% filter(is.na(trt)) %>% distinct(node_id, .keep_all = T)
# TODO 
# post new rows to DB

message(Sys.time())
message("\n-------------\n")