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
    "{getwd()}/log/raw_to_shadow_stresscams_{decolonize(Sys.time())}.log"
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
source("utils/parse_stresscams.R")

suppressPackageStartupMessages({
  library(DBI)
  library(dplyr)
  library(dbplyr)
  library(RSQLite)
})


message("Connecting to Raw DB")
con_raw <- etl_connect_raw()
message("Connecting to Shadow DB")
con_sh_st <- etl_connect_shadow("stresscams")


last_gotten_row <- tbl(con_sh_st, "from_raw") %>% 
  summarise(uid = max(rawuid, na.rm = TRUE)) %>% 
  collect() %>% 
  pull(uid)

message(
  glue::glue("Last row in shadow is from Raw uid: {last_gotten_row}")
)


recent_stresscam_rows <- tbl(etl_connect_raw(con_raw), "stresscam_hologram_data") %>% 
  filter(uid > last_gotten_row, uid < last_gotten_row + 200) %>% 
  collect()

loggit(
  "INFO",
  "Collected stresscam records",
  rows = nrow(recent_stresscam_rows)
)

# extract out data and rawdb id into list elements
recent_stresscam_rows_list <- 
  recent_stresscam_rows %>% 
  select(uid, data) %>% 
  purrr::pmap(rawdb_stresscam_to_lst)




# gateways and awake messages
parsed_stresscams <- recent_stresscam_rows_list %>% 
  purrr::map(etl_parse_stresscams) %>% 
  bind_rows() %>% 
  mutate_all(~na_if(., -999))

loggit(
  "INFO",
  "Parsed and stored stresscam rows as local df",
  rows = nrow(parsed_stresscams)
)


message("Writing to shadow DB")
# write rows to shadow DB


rows_aff <- dbWriteTable(
  con_sh_st,
  "stresscam_data",
  parsed_stresscams,
  append = TRUE
)

pushed_count <- tbl(con_sh_st, "stresscam_data") %>% 
  filter(rawuid > last_gotten_row) %>% 
  tally() %>% 
  pull(n)

loggit(
  "INFO",
  "stresscam_data",
  success = rows_aff,
  rows = pushed_count
)




rows_aff <- dbWriteTable(
  con_sh_st,
  "from_raw",
  recent_stresscam_rows %>% 
    select(rawuid = uid),
  append = TRUE
)

loggit(
  "INFO",
  "from_raw",
  success = rows_aff,
  rows = nrow(recent_stresscam_rows)
)



dbDisconnect(con_raw)
dbDisconnect(con_sh_st)

message("Execution end")

set_logfile(logfile = NULL, confirm = F)
