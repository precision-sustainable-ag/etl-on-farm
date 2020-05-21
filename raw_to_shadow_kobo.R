message(Sys.time(), "\n\n")

real_time_rq <- httr::GET("example.com")
real_time <- httr::parse_http_date(httr::headers(real_time_rq)$date)

message(
  "Offset of this server and real time is:\n", 
  format(Sys.time() - real_time)
)

source("secret.R")
source("initializers.R")
source("parse_forms.R")

suppressPackageStartupMessages({
  library(DBI)
  library(dplyr)
  library(dbplyr)
  library(RSQLite)
})


message("Connecting to Raw DB")
con_raw <- etl_connect_raw()
message("Connecting to Shadow DB\n")
con_sh_f <- etl_connect_shadow("forms")


message("TABLE wsensor_install")
message(Sys.time())

gotten_wsi <- tbl(con_sh_f, "wsensor_install") %>% 
  select(rawuid) %>% 
  collect() %>% 
  pull()

wsi <- tbl(etl_connect_raw(con_raw), "kobo") %>% 
  filter(
    asset_name == "psa water sensor install",
    !(uid %in% gotten_wsi)
    ) %>% 
  head(30) %>% 
  collect()

message("Found ", nrow(wsi), " forms")

wsi_to_store <- wsi %>% 
  purrr::pmap(rawdb_kobo_to_lst) %>% 
  purrr::map(etl_parse_wsensor_install) %>% 
  dplyr::bind_rows()

dbWriteTable(
  con_sh_f,
  "wsensor_install",
  wsi_to_store,
  append = T
)

message(Sys.time(), " wsensor_install finished\n\n")


dbDisconnect(con_sh_f)
