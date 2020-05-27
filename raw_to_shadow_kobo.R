message(Sys.time(), "\n\n")

x <- na.omit(stringr::str_match(commandArgs(), "--file=(.+)")[,2])
if (length(x)) setwd(dirname(x))

decolonize <- function(tm) {
  tm <- gsub("[-:]", "_", tm)
  gsub(" ", "__", tm)
}

suppressPackageStartupMessages(
  library(loggit)
)

set_logfile(
  glue::glue(
    "{getwd()}/log/raw_to_shadow_kobo_{decolonize(Sys.time())}.log"
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
source("parse_forms.R")

suppressPackageStartupMessages({
  library(DBI)
  library(dplyr)
  library(dbplyr)
  library(RSQLite)
})


message("Connecting to Raw DB")
con_raw <- etl_connect_raw()
message("Connecting to Shadow DB")
con_sh_f <- etl_connect_shadow("forms")


message("TABLE wsensor_install; pulling")

gotten_wsi <- union_all(
  tbl(con_sh_f, "wsensor_install") %>% select(rawuid), 
  tbl(con_sh_f, "needs_help") %>% select(rawuid)
) %>% 
  collect() %>% 
  pull()

wsi <- tbl(etl_connect_raw(con_raw), "kobo") %>% 
  filter(
    asset_name == "psa water sensor install",
    !(uid %in% gotten_wsi)
    ) %>% 
  head(30) %>% 
  collect()

loggit(
  "INFO",
  "Found forms; `psa water sensor install`",
  rows = nrow(wsi)
)

wsi_to_store <- wsi %>% 
  purrr::pmap(rawdb_kobo_to_lst) %>% 
  purrr::map(etl_parse_wsensor_install) %>% 
  dplyr::bind_rows()
message("wsensor_install parsed")


rows_aff <- dbWriteTable(
  con_sh_f,
  "wsensor_install",
  wsi_to_store,
  append = T
)
loggit(
  "INFO",
  "wsensor_install pushed",
  rows = nrow(wsi_to_store)
)



dbDisconnect(con_sh_f)

message("Execution end")

set_logfile(logfile = NULL)
