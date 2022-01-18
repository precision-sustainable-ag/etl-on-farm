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

rows_aff <- etl_upsert_stresscams(
  "stresscam_ratings", 
  "stresscam_ratings", 
  rawuids_to_push,
  unicity = "stresscam_device_id_timestamp_utc_key"
)

loggit(
  "INFO",
  "stresscam_ratings",
  rows = rows_aff
)



rows_aff <- etl_mark_pushed(con_sh_st, "from_raw", rawuids_to_push)
loggit(
  "INFO",
  "from_raw",
  rows = rows_aff
)

dbDisconnect(con_sh_st)


message("Execution end")

set_logfile(logfile = NULL)
