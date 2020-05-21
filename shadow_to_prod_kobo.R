message(Sys.time(), "\n\n")

source("secret.R")
source("initializers.R")
source("sql_constructors.R")

suppressPackageStartupMessages({
  library(DBI)
  library(dplyr)
  library(dbplyr)
  library(RSQLite)
})

message("Connecting to Shadow DB\n")
con_sh_f <- etl_connect_shadow("forms")
message("Connecting to Production DB\n")
con_prod <- etl_connect_prod()

message("TABLE wsensor_install")
wsi_to_push <- tbl(con_sh_f, "wsensor_install") %>% 
  filter(pushed_to_prod == 0) %>% 
  head(30) %>% 
  select(-sid) %>% 
  collect() %>% 
  mutate_at(
    vars(any_of(c("time_begin", "time_end", "parsed_at"))), 
    lubridate::as_datetime
    )
  
message("Found ", nrow(wsi_to_push), " rows to push\n")

message("Pushing to `wsensor_install`, rows inserted:")

etl_insert_form(
  wsi_to_push, 
  "wsensor_install", 
  unicity = "wsensor_install_subplot_code_gateway_serial_no_bare_node_se_key"
  )

message("\nMarking rows as pushed:")

etl_mark_pushed(con_sh_f, "wsensor_install", wsi_to_push$rawuid)


dbDisconnect(con_sh_f)


message(Sys.time(), "\n\n")
