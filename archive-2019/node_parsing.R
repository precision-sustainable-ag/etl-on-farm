suppressPackageStartupMessages({
  library(readr)
  library(purrr)
  library(stringr)
  library(lubridate)
  library(dplyr)
  library(forcats)
})

# ex_lengths <- read_tsv("C:/Users/Baits/Downloads/line_lengths.tsv")

# test_strings <- ex_lengths$data

# Set column names on a possibly NULL matrix ----
safely_set_colnames <- possibly(
  magrittr::set_colnames, 
  otherwise = NULL
)

#safely_matrix <- possibly(matrix, otherwise = NULL)

# Repeat rows to form a matrix, unless NULL ----
rep_as_matrix <- function(vec, num, nms) {
  if (is.null(vec)) return(NULL)
  
  matrix(rep(vec, num), nrow = num, byrow = T) %>% 
    safely_set_colnames(nms)
}

# Map over each node datastring ----
#   assumes certain regex conventions
parse_node_string <- function(s, debug = F) {

  name_list <- list(
    meta = c("node_id", "batt", "encl_temp", "solar_mA", "solar_V", "timestamp"),
    TDR  = c("address", "vwc", "temp", "permittivity", "ec_bulk", "ec_pore"),
    temp = c(
      "t_lb", "t_lb_1h_avg", "t_lb_1h_avg_min", "t_lb_1h_avg_max", 
      "gdd1_ignore", "gdd2_ignore", "t_lb_hrs_obs_ignore"
      ),
    h_t  =  c(
      "t_amb", "t_amb_1h_avg", "t_amb_1h_avg_min", "t_amb_1h_avg_max", 
      "gdd3_ignore", "gdd4_ignore", "t_amb_hrs_obs_ignore"
    ),
    h_h  = c("rh", "rh_1h_avg", "rh_1h_min", "rh_1h_max", "rh_hrs_obs_ignore")
  )
  
  
  addresses <- str_locate_all(s, "~[ABCZabczhtRT]~")[[1]]
  
  chunked_s <- str_sub(s, c(0, addresses[,1]+1), c(addresses[,2]-3, -1))
  
  # TODO check this regex - Correct I think.
  not_garbage <- str_detect(chunked_s, "^[-:.~0-9A-z_]+$")
  clean_chunks <- chunked_s[not_garbage]
  
  # If there's a problem in the metadata, throw out the whole obs
  if (not_garbage[1] == F | !str_detect(s, "^[0-9]")) {return(as_tibble(NULL))}
  
  TDR_indices <- str_detect(clean_chunks, "^[ABCZabcz]~")
  Temp01_indices <- str_detect(clean_chunks, "^[tT]~")
  THum01_indices <- str_detect(clean_chunks, "^[hR]~")
  
  nobs <- max(sum(TDR_indices), sum(Temp01_indices), sum(THum01_indices/2))
  obs <- list()
  
  # obs$meta <- rep_as_matrix(
  #   str_split(clean_chunks[1], "~", simplify = T), 
  #   nobs, name_list$meta
  #   )
  
  obs$meta <- rep(clean_chunks[1], nobs) %>% 
    str_split_fixed("~", 6) %>% 
    safely_set_colnames(name_list$meta)
  
  
  TDR_clean <- str_count(clean_chunks[TDR_indices], "~") == 5
  
  # obs$TDR <- str_split_fixed(
  #   clean_chunks[TDR_indices][TDR_clean], "~", 6
  # ) %>%
  #   safely_set_colnames(name_list$TDR)
  
  obs$TDR <- str_split(
    clean_chunks[TDR_indices][TDR_clean], "~", simplify = T
  ) %>%
    safely_set_colnames(name_list$TDR)
  
  THum <- str_split(clean_chunks[THum01_indices], "~") %>%
    set_names(., map(., length))
  
  obs$hum_hum <- rep_as_matrix(
    THum[["6"]][-1], 
    nobs, name_list$h_h
    )
  
  obs$hum_temp <- rep_as_matrix(
    THum[["8"]][-1], 
    nobs, name_list$h_t
    )
  
  
  temp <- str_split(clean_chunks[Temp01_indices], "~", simplify = T)
  
  obs$temperature <- rep_as_matrix(
    temp[-1], nobs, name_list$temp
    )
  
  
  ret <- lift_dl(cbind)(obs) %>% 
    tibble::as_tibble() %>% 
    mutate_all(list(~na_if(., -99999))) %>% 
    mutate_all(list(~na_if(., -9999))) %>% 
    mutate_all(list(~na_if(., -999)))
  
  if (!is.null(obs$TDR)) {
    ret <- ret %>%     
    mutate(
      depth = fct_collapse(
        factor(address), 
        "0"  = c("z", "Z"), 
        "10" = c("a", "A"), 
        "40" = c("b", "B"), 
        "75" = c("c", "C")
      ),
      depth = as.character(depth)
    )
  }
  
  
  if (debug) {
    return(list(result = ret, garbage = !not_garbage))
  } else {return(ret)}
  
}

# Find bad node data (map over strings) ----
identify_unparseable_nodes <- function(s) {
  p <- parse_node_string(s, debug = T)
  s[is.null(p$result) | nrow(p$result) == 0 | any(p$garbage)]
  
  # TODO does this find all discardable chunks?
}

#map(test_strings, parse_node_string, debug = T)
#microbenchmark::microbenchmark(map(test_strings, parse_node_string))
#map_dfr(test_strings, ~tibble::as_tibble(parse_node_string(.x)))

# All true
# file.path(
#   "C:/Users/Brian.Davis/Google Drive/Workstation/Dropbox/R/PhD projects",
#   "CIG/sensor_scraping/hologram_structure/CROWN NC 17 (85958)_nodes_.csv"
# ) %>% read_csv() %>% pull(data) %>% str_detect("^[-:.~0-9A-z_]+$")

# file.path(
#   "C:/Users/Brian.Davis/Google Drive/Workstation/Dropbox/R/PhD projects",
#   "CIG/sensor_scraping/hologram_structure/CROWN NC 17 (85958)_nodes_.csv"
# ) %>% read_csv() %>% pull(data) %>% 
#   map(parse_node_string) 
# 
# file.path(
#   "C:/Users/Brian.Davis/Google Drive/Workstation/Dropbox/R/PhD projects",
#   "CIG/sensor_scraping/hologram_structure/CROWN NC 17 (85958)_nodes_.csv"
# ) %>% read_csv() %>% pull(data) %>% 
#   map(parse_node_string, debug = T) %>% 
#   map("garbage")
# 
# file.path(
#   "C:/Users/Brian.Davis/Google Drive/Workstation/Dropbox/R/PhD projects",
#   "CIG/sensor_scraping/hologram_structure/CROWN NC 17 (85958)_nodes_.csv"
# ) %>% read_csv() %>% pull(data) %>% c(., "wharbl^") %>% 
#   map(identify_unparseable_nodes)

# Map over each gateway datastring ----
parse_gateway_string <- function(s, debug = T) {
  nms <- c("gateway_id", "timestamp", "batt_mV", "solar_mA", "solar_mV")
  
  not_garbage <- str_detect(s, "^[-:.~0-9A-z_]+$")
  s_clean <- s[not_garbage]
  
  obs <- str_split(s_clean, "~", simplify = T)
  
  safely_set_colnames(obs, nms) %>% 
    as_tibble()
}

# Find bad gateway data (map over strings) ----
identify_unparseable_gateways <- function(s) {
  p <- parse_gateway_string(s)
  s[is.null(p) | nrow(p) == 0]
  
  # TODO does this find all discardable chunks?
}

# file.path(
#   "C:/Users/Brian.Davis/Google Drive/Workstation/Dropbox/R/PhD projects",
#   "CIG/sensor_scraping/hologram_structure/CROWN NC 17 (85958)_gateway_.csv"
# ) %>% read_csv() %>% pull(data) %>% c(., "wharbl^") %>%
#   map(parse_gateway_string)
# 
# file.path(
#   "C:/Users/Brian.Davis/Google Drive/Workstation/Dropbox/R/PhD projects",
#   "CIG/sensor_scraping/hologram_structure/CROWN NC 17 (85958)_gateway_.csv"
# ) %>% read_csv() %>% pull(data) %>% c(., "wharbl^") %>%
#   map(identify_unparseable_gateways)


# It works!

# file.path(
#   "C:/Users/Brian.Davis/Google Drive/Workstation/Dropbox/R/PhD projects",
#   "CIG/sensor_scraping/hologram_structure/CROWN NC 17 (85958)_gateway_.csv"
# ) %>% read_csv() %>% 
#   select(-data_base64) %>% 
#   mutate(data = map(data, parse_gateway_string)) %>% 
#   unnest()




# file.path(
#   "C:/Users/Brian.Davis/Google Drive/Workstation/Dropbox/R/PhD projects",
#   "CIG/sensor_scraping/hologram_structure/CROWN NC 17 (85958)_nodes_.csv"
# ) %>% read_csv() %>% 
#   select(-data_base64) %>% 
#   mutate(data = map(data, parse_node_string)) %>% 
#   unnest()
# 
# file.path(
#   "C:/Users/Brian.Davis/Google Drive/Workstation/Dropbox/R/PhD projects",
#   "CIG/sensor_scraping/hologram_structure/CROWN NC 17 (85958)_nodes_.csv"
# ) %>% read_csv() %>% 
#   select(-data_base64) %>% 
#   mutate(data = map(data, identify_unparseable_nodes)) %>% 
#   unnest()
