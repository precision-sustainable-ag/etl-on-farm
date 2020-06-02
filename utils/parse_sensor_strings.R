safely_set_names <- function(obj, nms) {
  if (!length(obj)) {
    return(NULL)
  } else if (is.matrix(obj)) {
    magrittr::set_colnames(obj, nms)
  } else {
    purrr::set_names(obj, nms)
  }
}

safely_as_tibble <- function(...) {
  x <- dplyr::as_tibble(...)
  if (nrow(x) == 0) x <- NULL
  x
}

rawdb_hologram_to_lst <- function(uid, data) {
  x <- jsonlite::fromJSON(data)
  x$uid <- uid
  
  if (!stringr::str_detect(x$data, "~|awake|update")) {
    x$data <- base64enc::base64decode(x$data) %>% 
      rawToChar()
  }
  
  x
}

addressed_strings_to_char_matrix <- function(string, nms) {

  addr <- stringr::str_sub(string, 1, 1)
  data <- stringr::str_sub(string, 2, -1) %>% 
    stringr::str_split("~", n = length(nms)-1,  simplify = T)
  # length(nms)-1 = number of parts after the address
  
  cbind(addr, data) %>% 
    safely_set_names(nms)
}





# gateways and awake messages

parse_others <- function(elt) {
  err_flag <- FALSE
  chunks <- stringr::str_split(elt$data, "~")[[1]]
  
  if (stringr::str_detect(elt$data, "Gateway awake")) {
    return(NULL)
  }
  
  if (stringr::str_detect(elt$data, "Successful time update")) {
    return(NULL)
  }
  
  # Especially disallow VSE (Steve Evett)
  if (!stringr::str_detect(elt$data, "^V[0-9]")) {
    stop("Unknown string version")
  }
  
  if (
    length(chunks) == 2 &&
    nchar(chunks[1]) == 1 &&
    as.numeric(chunks[2]) < 0
    ) {
    # memory garbage + signal strength
    return(NULL)
  }
  
  if (length(chunks) != 8) {
    stop("Not a data string or incorrect format")
    }

  nms <- c(
    "firmware_version", "project_id", "gateway_serial_no",
    "gw_batt_voltage", "gw_enclosure_temp", "gw_solar_current","gw_solar_voltage",
    "timestamp"
    )
  
  ret <- dplyr::bind_rows(safely_set_names(chunks, nms)) %>% 
    mutate(
      rawuid = elt$uid,
      device_id = elt$deviceId,
      ts_up = elt$timestamp,
      device_name = elt$deviceName
    ) %>% 
    mutate_at(vars(matches("^gw_")), as.numeric) %>% 
    mutate_at(vars(any_of(c("timestamp", "ts_up"))), lubridate::as_datetime)
  
  err_flag <- !isTRUE(as.numeric(ret$gateway_serial_no) > 21000000)
  
  if (err_flag) {
    stop("Possible truncated node data")
  }
  
  ret
}

etl_parse_gws <- function(elt) {
  
  ret <- purrr::safely(parse_others)(elt)
  
  if (!is.null(ret$error)) {
    dbWriteTable(
      etl_connect_shadow("sensors"),
      "needs_help",
      data.frame(rawuid = elt$uid, err = as.character(ret$error)),
      append = TRUE
    )
  }
  
  ret$result
}

parse_nodes <- function(elt) {
  nm_list <- list(
    meta = c(
      "firmware_version", "project_id", "node_serial_no", 
      "nd_batt_voltage", "nd_enclosure_temp", "nd_solar_current","nd_solar_voltage",
      "timestamp"
    ),
    TDR  = c(
      "tdr_address", "tdr_sensor_id", "center_depth",
      "vwc", "soil_temp", "permittivity", "ec_bulk", "ec_pore_water"
    ),
    THum = c(
      "rh_address", "rh_sensor_id", "rh_height",
      "t_amb", "rh"
    ),
    Temp = c(
      "temp_address", "temp_sensor_id", "temp_height", "t_lb"
    )
  )
  
  addresses <- stringr::str_locate_all(elt$data, "~[A-Za-z0-9][0-9]{2}Acclima")[[1]]
  
  chunked_s <- stringr::str_sub(
    elt$data, 
    c(0, addresses[,1] + 1), 
    c(addresses[,1] - 1, -1)
    )
  
  # TODO check this regex - Correct I think.
  not_garbage <- stringr::str_detect(chunked_s, "^[-:.~0-9A-Za-z+_ ]+$")
  clean_chunks <- chunked_s[not_garbage]
  
  # If there's a problem in the metadata, throw out the whole obs
  if (not_garbage[1] == F) {
    stop("Contains Unicode garbage")
  }
  if (!stringr::str_detect(elt$data, "^V[0-9]")) {
    stop("Unknown string version")
  }
  
  
  # even if the node string is corrupted, the last bit should
  # always be SS, provided by gateway
  ss_idx <- length(clean_chunks)
  
  signal <- stringr::str_extract(clean_chunks[ss_idx], "-[.0-9]+$")
  clean_chunks[ss_idx] <- stringr::str_remove(clean_chunks[ss_idx], "~-[.0-9]+$")
  
  # water_node_data ----
  
  meta <- stringr::str_split(clean_chunks[1], "~")[[1]]
  
  if (length(meta) == 8) {
    meta <- purrr::set_names(meta, nm_list$meta)
  } else if (length(meta) == 7) {
    meta <- purrr::set_names(meta, nm_list$meta[c(1, 3:8)])
  }
  
  meta <- dplyr::bind_cols(
    rawuid = elt$uid,
    device_id = elt$deviceId,
    dplyr::bind_rows(meta), 
    ts_up = elt$timestamp,
    signal_strength = as.numeric(signal)
  ) %>% 
    mutate_at(vars(matches("^nd_")), as.numeric) %>% 
    mutate_at(vars(timestamp, ts_up), lubridate::as_datetime)
  
  # TODO this will break if parsing old data (out of season)
  # tosses timestamps more than a day in the future or more than 180 days old
  if (
    get0("real_time", ifnotfound = Sys.time()) + 3600*24 < meta$timestamp ||
    get0("real_time", ifnotfound = Sys.time()) - 180*3600*24 > meta$timestamp
  ) {
    stop("Invalid on-device timestamp:", lubridate::as_date(meta$timestamp))
  }

  
  TDR_indices <- stringr::str_detect(clean_chunks, "Acclima TR31[05]")
  Temp_indices <- stringr::str_detect(clean_chunks, "Acclima Temp")
  THum_indices <- stringr::str_detect(clean_chunks, "Acclima THum")
  
  # water_sensor_data ----

  # 2020 update for 310 sensors, needing addl col in DB
  fv <- stringr::str_extract(meta[["firmware_version"]], "[.0-9]+") %>% 
    lubridate::ymd()
  if (fv > lubridate::ymd("2020-05-28")) {
    nm_list$TDR <- c(nm_list$TDR, "travel_time")
  }
  
  TDR <-  
    addressed_strings_to_char_matrix(
      clean_chunks[TDR_indices],
      nm_list$TDR
    ) %>% 
    as_tibble(.name_repair = "minimal") %>% 
    mutate_at(
      vars(-starts_with("tdr")),
      as.numeric
    ) %>% 
    mutate(
      node_serial_no = meta$node_serial_no,
      timestamp = meta$timestamp,
      ts_up = meta$ts_up,
      device_id = meta$device_id,
      rawuid = meta$rawuid
    )
  
  if (any(nchar(TDR$tdr_sensor_id) > 27)) {
    stop("TDR sensor firmware version too long.")
  }
  
  # ambient_sensor_data ----
  THum <- addressed_strings_to_char_matrix(
    clean_chunks[THum_indices],
    nm_list$THum
  )
  
  # TODO update if there are ever >1 THum or Temp per node
  # because of bug where there may be a duplicated THum with -999 in place of data, 
  #   keep only the first row [1, , drop = F]/head(., 1) of each below
  
  Temp <- addressed_strings_to_char_matrix(
    clean_chunks[Temp_indices],
    nm_list$Temp
  )
  
  ambient <- dplyr::bind_cols(
    safely_as_tibble(head(THum, 1), .name_repair = "minimal"),
    safely_as_tibble(head(Temp, 1), .name_repair = "minimal")
  ) %>% 
    mutate_at(
      vars(ends_with(c("height", "amb", "lb", "temp", "rh"))),
      as.numeric
    ) %>% 
    mutate(
      node_serial_no = meta$node_serial_no,
      timestamp = meta$timestamp,
      ts_up = meta$ts_up,
      device_id = meta$device_id,
      rawuid = meta$rawuid
    )
  
  list(
    water_node_data = meta,
    water_sensor_data = TDR,
    ambient_sensor_data = ambient
  )
} 


etl_parse_nds <- function(elt) {
  
  ret <- purrr::safely(parse_nodes)(elt)
  
  if (!is.null(ret$error)) {
    dbWriteTable(
      etl_connect_shadow("sensors"),
      "needs_help",
      data.frame(rawuid = elt$uid, err = as.character(ret$error)),
      append = TRUE
    )
  }
  
  ret$result
}

