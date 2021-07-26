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
    "{getwd()}/log/shadow_to_prod_kobo_{decolonize(Sys.time())}.log"
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
con_sh_f <- etl_connect_shadow("forms")
message("Connecting to Production DB")
con_prod <- etl_connect_prod()

message("TABLE wsensor_install; pulling")
wsi_to_push <- tbl(con_sh_f, "wsensor_install") %>% 
  filter(pushed_to_prod == 0) %>% 
  head(30) %>% 
  select(-sid) %>% 
  collect() %>% 
  mutate_at(
    vars(any_of(c("time_begin", "time_end", "parsed_at"))), 
    lubridate::as_datetime
    )
  
loggit(
  "INFO",
  "Found rows to push; `wsensor_install`",
  rows = nrow(wsi_to_push)
)

message("Pushing to `wsensor_install`")

rows_aff <- etl_insert_form(
  wsi_to_push, 
  "wsensor_install", 
  unicity = "wsensor_install_subplot_code_gateway_serial_no_bare_node_se_key"
  )

loggit(
  "INFO",
  "wsensor_install",
  rows = rows_aff
)

message("Marking rows as pushed in `wsensor_install`")

rows_aff <- etl_mark_pushed(con_sh_f, "wsensor_install", wsi_to_push$rawuid)
loggit(
  "INFO",
  "from_raw",
  rows = rows_aff
)

dbDisconnect(con_sh_f)

message("Execution end")

set_logfile(logfile = NULL)
