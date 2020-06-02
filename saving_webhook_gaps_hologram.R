source("secret.R")

library(httr)
library(dplyr)
library(dbplyr)
library(DBI)



devices <- RETRY(
  "GET",
  baseurl, path = "api/1/devices", 
  query = list(
    apikey = alondra, 
    tagname = "PSA_GLOBAL", 
    limit = 200, 
    withlocation = T
  )
)

devs <- content(devices)$data %>% 
  purrr::map_int("id")





################





getter_continuing <- function(
  device_id, 
  timestart = as.integer(lubridate::as_datetime("2020-05-17 00:00:00"))
  ) {

  
  query <- list(
    apikey = alondra,
    deviceid = device_id, 
    limit = 2000,
    timestart = timestart
  )
  
  
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
  
  while (content(messages)$continues & tries < 10) {
    earliest <- content(messages)$data %>% 
      purrr::map_chr("logged") %>% 
      lubridate::ymd_hms() %>% 
      as.integer() %>% 
      min()
    
    query$timeend <- earliest
    
    message(
      "__Continue fetching device ", 
      device_id, 
      " before ", 
      lubridate::as_datetime(earliest)
      )
    
    messages <- RETRY(
      "GET",
      baseurl, 
      path = "api/1/csr/rdm",
      query = query
    )
    tries <- tries + 1
    messages_0 <- c(messages_0, content(messages)$data)
  }

  messages_0 %>% 
    purrr::map(~{
      internal_data <- jsonlite::fromJSON(.x$data)
      
      server_timestamp <- lubridate::as_datetime(.x$logged)
      data_column <- list(
        data = internal_data[["data"]],
        deviceName = internal_data[["device_name"]],
        deviceId = internal_data[["device_id"]],
        timestamp = internal_data[["received"]]
      ) %>% 
        jsonlite::toJSON(auto_unbox = T)
      
      tibble(data = data_column, server_timestamp = server_timestamp)
    }) %>% 
    {purrr::quietly(bind_rows)(.)}
}


gotten_messages <- devs %>% 
  purrr::map(purrr::safely(getter_continuing))


successes <- gotten_messages %>% 
  purrr::set_names(devs) %>% 
  purrr::map("result") %>% 
  purrr::map("result")

to_retry <- gotten_messages %>% 
  purrr::set_names(devs) %>% 
  purrr::map("error") %>% 
  purrr::compact() %>% 
  names()

to_investigate <- successes %>% 
  purrr::keep(~is.null(.x)) %>% 
  names()

#
# retried <- to_retry %>% 
#   purrr::map(purrr::safely(getter_continuing))
# 
# successes_r <- retried %>% 
#   purrr::map("result") %>% 
#   purrr::map("result")

#

sessions <- content(devices)$data %>% 
  purrr::map_dfr(~{
    device_id <- .x$id
    lastsession <- .x$lastsession$session_begin %||% NA
    tibble(device_id, lastsession)
  })

sessions %>% 
  filter(device_id %in% to_investigate) %>% 
  arrange(desc(lastsession))

# Only active earlier/never, inactive now

####################################

to_check <- bind_rows(
  successes#,
  #successes_r
)

con_admin <- dbConnect(
  RMariaDB::MariaDB(), 
  username = raw_user,
  password = raw_password,
  dbname = raw_dbname,
  host = raw_host,
  port = raw_port
)

hashes <- tbl(con_admin, "hologram") %>% 
  filter(server_timestamp > lubridate::as_datetime("2020-05-16")) %>% 
  collect() %>% 
  mutate(hash = purrr::map_chr(data, ~jsonlite::fromJSON(.x)$data))


to_push <- to_check %>% 
  mutate(hash = purrr::map_chr(data, ~jsonlite::fromJSON(.x)$data)) %>% 
  filter(!(hash %in% hashes$hash))


to_push_lst <- to_push %>% 
  mutate(uid = row_number()) %>% 
  select(uid, data) %>% 
  purrr::pmap(rawdb_hologram_to_lst)



to_push_nodes_idx <- to_push_lst %>%
  purrr::map_lgl(~stringr::str_count(.x$data, "~") > 10)

to_push_nds <- to_push_lst[to_push_nodes_idx] %>% 
  purrr::map(purrr::safely(parse_nodes))

errs_nd <- to_push_nds %>% 
  purrr::map_lgl(~!is.null(.x$error))

(to_push_lst[to_push_nodes_idx])[errs_nd] %>% 
  purrr::discard(~stringr::str_sub(.x$data, 1, 2) == "VS")



to_push_gws <- to_push_lst[!to_push_nodes_idx] %>% 
  purrr::map(purrr::safely(parse_others))

errs_gw <- to_push_gws %>% 
  purrr::map_lgl(~!is.null(.x$"error"))

# appear to mostly be nodes with no sensors attached,
#   the signal strength makes it longer than a gateway string

(to_push_lst[!to_push_nodes_idx])[errs_gw] %>% 
  purrr::map_chr("data") %>% 
  stringr::str_split("~", simplify = T)



#### ----

sql_to_insert_maria <- sqlAppendTable(
  con_admin,
  "temp_hologram",
  to_push %>% select(data, server_timestamp),
  row.names = F
)

readr::write_file(sql_to_insert_maria, "../gap_data_maria.sql")


anti_join(
  to_push %>% select(data, server_timestamp) %>% 
    mutate(server_timestamp = as.integer(server_timestamp) + 14400),
  tbl(con_admin, "temp_hologram2") %>% collect() %>% 
    mutate(server_timestamp = as.integer(server_timestamp))
)

