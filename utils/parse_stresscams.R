rawdb_stresscam_to_lst <- function(uid, data) {
  x <- jsonlite::fromJSON(data)
  x$uid <- uid
  
  x
}

iso_8601 <- function(x) {
  format(x, "%Y-%m-%dT%H:%M:%S%z")
}




parse_stresscams <- function(elt) {
  
  canonical_tz <- stringr::str_subset(OlsonNames(), elt$data$ZONE)
  
  if (length(canonical_tz) != 1) {
    stop("Badly matched time zone")
  }
  
  local_time <- lubridate::dmy_hms(
    glue::glue(
      "{elt$data$DATE} {elt$data$TIME}"
    ),
    tz = canonical_tz
  )
  
  utc_time <- local_time %>% lubridate::with_tz("UTC")
  
  if (is.na(local_time)) { stop("Invalid on-device timestamp") }

  probs <- elt$data[stringr::str_detect(names(elt$data), "^P_WS_")]
  probs_string <- jsonlite::toJSON(probs, auto_unbox = T) %>% 
    as.character()
  
  tibble(
    rawuid = elt$uid,
    device_id = as.integer(elt$deviceId),
    dev_id_key = elt$data$DEV_ID,
    firmware_version = elt$data$VER,
    timestamp_utc = as.character(utc_time, usetz = T),
    timestamp_local = as.character(local_time, usetz = T),
    timestamp_zone = canonical_tz,
    ts_up = elt$timestamp,
    cpu_temp = as.numeric(elt$data$CPU_TEMP),
    sd_free = as.numeric(elt$data$SD_free),
    mode = elt$data$MODE,
    crop = elt$data$CROP,
    code = elt$data$FARM,
    rep = as.numeric(elt$data$REP),
    trt = elt$data$TRT,
    file = elt$data$FILE,
    probabilities = probs_string,
    P_WS = as.integer(elt$data$P_WS)
  )
}


etl_parse_stresscams <- function(elt) {
  
  ret <- purrr::safely(parse_stresscams)(elt)
  
  if (!is.null(ret$error)) {
    dbWriteTable(
      etl_connect_shadow("stresscams"),
      "needs_help",
      data.frame(rawuid = elt$uid, err = as.character(ret$error)),
      append = TRUE
    )
  }
  
  ret$result
}


