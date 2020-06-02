suppressPackageStartupMessages({
  library(dplyr)
})

source("secret.R")
source("initializers.R")
source("parse_sensor_strings.R")

con_sh_s <- etl_connect_shadow("sensors")

x <- tbl(con_sh_s, "needs_help") %>% 
  filter(fixed == 0) %>% 
  collect() %>% 
  pull(rawuid)

con_raw <- etl_connect_raw()

x_lst <- tbl(etl_connect_raw(con_raw), "hologram") %>% 
  filter(uid %in% x) %>% 
  select(uid, data) %>% 
  collect() %>% 
  purrr::pmap(rawdb_hologram_to_lst) 

nodes_idx <- x_lst %>% 
  purrr::map_lgl(~stringr::str_count(.x$data, "~") > 10)


fixed_gw <- x_lst[!nodes_idx] %>% 
  purrr::map(purrr::safely(parse_others)) %>% 
  purrr::map("result") %>% 
  bind_rows()

# For further investigating
error_gw <- x_lst[!nodes_idx] %>% 
  purrr::map(purrr::safely(parse_others)) %>% 
  purrr::map("error") %>% 
  bind_rows()

fixed_nd <- x_lst[nodes_idx] %>% 
  purrr::map(purrr::safely(parse_nodes)) %>% 
  purrr::map("result")

# For further investigating
error_nd <- x_lst[nodes_idx] %>% 
  purrr::map(purrr::safely(parse_nodes)) %>% 
  purrr::map("error") %>% 
  purrr::compact()


metas <- purrr::map(fixed_nd, "water_node_data") %>% 
  bind_rows() %>% mutate_all(~na_if(., -999))
TDRs <- purrr::map(fixed_nd, "water_sensor_data") %>% 
  bind_rows() %>% mutate_all(~na_if(., -999))
ambs <- purrr::map(fixed_nd, "ambient_sensor_data") %>% 
  bind_rows() %>% mutate_all(~na_if(., -999))

dbWriteTable(
  con_sh_s,
  "water_gateway_data",
  fixed_gw,
  append = TRUE
)


dbWriteTable(
  con_sh_s,
  "water_node_data",
  metas,
  append = TRUE
)

dbWriteTable(
  con_sh_s,
  "water_sensor_data",
  TDRs,
  append = TRUE
)

dbWriteTable(
  con_sh_s,
  "ambient_sensor_data",
  ambs,
  append = TRUE
)


fixed_idx <- glue::glue_collapse(
  c(fixed_gw[["rawuid"]],
    metas[["rawuid"]],
    TDRs[["rawuid"]],
    ambs[["rawuid"]],
    -1) %>% 
    unique(),
  sep = ", "
  )
  
dbExecute(
  con_sh_s,
  glue::glue(
    "
    UPDATE needs_help
    SET fixed = 1
    WHERE rawuid IN ({fixed_idx})
    "
  )
)


dbExecute(
  con_sh_s,
  glue::glue(
    "
    UPDATE from_raw
    SET pushed_to_prod = 0
    WHERE rawuid IN ({fixed_idx})
    "
  )
)



dbDisconnect(con_sh_s)
