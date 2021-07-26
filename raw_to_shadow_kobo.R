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
    "{getwd()}/log/raw_to_shadow_kobo_{decolonize(Sys.time())}.log"
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
source("utils/parse_forms.R")
source("utils/forms/water_sensor_install.R")
source("utils/forms/decomp_bag_pre_wt.R")
source("utils/forms/decomp_bag_dry_wt.R")
source("utils/forms/decomp_bag_collect.R")
source("utils/forms/biomass_decomp_bag.R")



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

######

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

######

message("TABLE decomp_biomass_fresh__decomp_bag_pre_wt; pulling")

gotten_dbpw <- union_all(
  tbl(con_sh_f, "decomp_biomass_fresh__decomp_bag_pre_wt") %>% select(rawuid),
  tbl(con_sh_f, "needs_help") %>% select(rawuid)
) %>%
  collect() %>%
  pull()

dbpw <- tbl(etl_connect_raw(con_raw), "kobo") %>%
  filter(
    asset_name == "psa decomp bag pre wt",
    !(uid %in% gotten_dbpw)
  ) %>%
  head(30) %>%
  collect()

loggit(
  "INFO",
  "Found forms; `psa decomp bag pre wt`",
  rows = nrow(dbpw)
)

dbpw_to_store <- dbpw %>%
  purrr::pmap(rawdb_kobo_to_lst) %>%
  purrr::map(etl_parse_decomp_bag_pre_wt) %>%
  dplyr::bind_rows()
message("decomp bag pre wt parsed")


rows_aff <- dbWriteTable(
  con_sh_f,
  "decomp_biomass_fresh__decomp_bag_pre_wt",
  dbpw_to_store,
  append = T
)

loggit(
  "INFO",
  "decomp_biomass_fresh__decomp_bag_pre_wt pushed",
  rows = nrow(dbpw_to_store)
)

######

message("TABLE decomp_biomass_dry__decomp_bag_dry_wt; pulling")

gotten_dbdw <- union_all(
  tbl(con_sh_f, "decomp_biomass_dry__decomp_bag_dry_wt") %>% select(rawuid), 
  tbl(con_sh_f, "needs_help") %>% select(rawuid)
) %>% 
  collect() %>% 
  pull()

dbdw <- tbl(etl_connect_raw(con_raw), "kobo") %>% 
  filter(
    asset_name == "psa decomp bag dry wt",
    !(uid %in% gotten_dbdw)
  ) %>% 
  head(30) %>% 
  collect()

loggit(
  "INFO",
  "Found forms; `psa decomp bag dry wt`",
  rows = nrow(dbdw)
)

dbdw_to_store <- dbdw %>% 
  purrr::pmap(rawdb_kobo_to_lst) %>% 
  purrr::map(etl_parse_decomp_bag_dry_wt) %>% 
  dplyr::bind_rows()
message("decomp bag dry wt parsed")


rows_aff <- dbWriteTable(
  con_sh_f,
  "decomp_biomass_dry__decomp_bag_dry_wt",
  dbdw_to_store,
  append = T
)

loggit(
  "INFO",
  "decomp_biomass_dry__decomp_bag_dry_wt pushed",
  rows = nrow(dbdw_to_store)
)

######

message("TABLE decomp_biomass_dry__decomp_bag_collect; pulling")

gotten_dbc <- union_all(
  tbl(con_sh_f, "decomp_biomass_dry__decomp_bag_collect") %>% select(rawuid), 
  tbl(con_sh_f, "needs_help") %>% select(rawuid)
) %>% 
  collect() %>% 
  pull()

dbc <- tbl(etl_connect_raw(con_raw), "kobo") %>% 
  filter(
    asset_name == "psa decomp bag collect",
    !(uid %in% gotten_dbc)
  ) %>% 
  head(30) %>% 
  collect()

loggit(
  "INFO",
  "Found forms; `psa decomp bag collect`",
  rows = nrow(dbc)
)

dbc_to_store <- dbc %>% 
  purrr::pmap(rawdb_kobo_to_lst) %>% 
  purrr::map(etl_parse_decomp_bag_collect) %>% 
  dplyr::bind_rows()
message("decomp bag collect parsed")


rows_aff <- dbWriteTable(
  con_sh_f,
  "decomp_biomass_dry__decomp_bag_collect",
  dbc_to_store,
  append = T
)

loggit(
  "INFO",
  "decomp_biomass_dry__decomp_bag_collect pushed",
  rows = nrow(dbc_to_store)
)

######

message("TABLE biomass_in_field__biomass_decomp_bag; pulling")

gotten_bif <- union_all(
  tbl(con_sh_f, "biomass_in_field__biomass_decomp_bag") %>% 
    select(rawuid), 
  tbl(con_sh_f, "needs_help") %>% 
    filter(target_tbl == "biomass_in_field__biomass_decomp_bag") %>% 
    select(rawuid) 
) %>% 
  collect() %>% 
  pull()

bif <- tbl(etl_connect_raw(con_raw), "kobo") %>% 
  filter(
    asset_name == "psa biomass decomp bag",
    !(uid %in% gotten_bif)
  ) %>% 
  head(30) %>% 
  collect()

loggit(
  "INFO",
  "Found forms; `psa biomass decomp bag`",
  rows = nrow(bif)
)

bif_to_store <- bif %>% 
  purrr::pmap(rawdb_kobo_to_lst) %>% 
  purrr::map(etl_parse_biomass_decomp_bag__biomass_in_field) %>% 
  dplyr::bind_rows()
message("biomass_in_field parsed")


rows_aff <- dbWriteTable(
  con_sh_f,
  "biomass_in_field__biomass_decomp_bag",
  bif_to_store,
  append = T
)
loggit(
  "INFO",
  "biomass_in_field__biomass_decomp_bag pushed",
  rows = nrow(bif_to_store)
)

######

message("TABLE decomp_biomass_fresh__biomass_decomp_bag; pulling")

gotten_bif <- union_all(
  tbl(con_sh_f, "decomp_biomass_fresh__biomass_decomp_bag") %>% 
    select(rawuid), 
  tbl(con_sh_f, "needs_help") %>% 
    filter(target_tbl == "decomp_biomass_fresh__biomass_decomp_bag") %>% 
    select(rawuid) 
) %>% 
  collect() %>% 
  pull()

bif <- tbl(etl_connect_raw(con_raw), "kobo") %>% 
  filter(
    asset_name == "psa biomass decomp bag",
    !(uid %in% gotten_bif)
  ) %>% 
  head(30) %>% 
  collect()

loggit(
  "INFO",
  "Found forms; `psa biomass decomp bag`",
  rows = nrow(bif)
)

bif_to_store <- bif %>% 
  purrr::pmap(rawdb_kobo_to_lst) %>% 
  purrr::map(etl_parse_biomass_decomp_bag__decomp_biomass_fresh) %>% 
  dplyr::bind_rows()
message("biomass_in_field parsed")


rows_aff <- dbWriteTable(
  con_sh_f,
  "decomp_biomass_fresh__biomass_decomp_bag",
  bif_to_store,
  append = T
)
loggit(
  "INFO",
  "decomp_biomass_fresh__biomass_decomp_bag pushed",
  rows = nrow(bif_to_store)
)

######

dbDisconnect(con_sh_f)

message("Execution end")

set_logfile(logfile = NULL)
