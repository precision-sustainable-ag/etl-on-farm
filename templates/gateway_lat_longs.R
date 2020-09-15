source("secret.R")

library(httr)
library(dplyr)
library(dbplyr)
library(DBI)
library(purrr)



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


content(devices)$data %>% 
  map(
    ~list(
      name = .x$name,
      id = .x$id,
      sim = .x$links$cellular[[1]]$sim,
      network_type = .x$lastsession$radio_access_technology,
      tadig = .x$lastsession$tadig,
      network = .x$lastsession$network_name,
      lat = .x$lastsession$latitude,
      long = .x$lastsession$longitude,
      range = .x$lastsession$range
    ) %>% 
      map(~.x %||% NA) %>% 
      as_tibble()
  ) %>% 
  bind_rows() %>% 
  readr::write_csv("latlongs_for_sensors.csv")
