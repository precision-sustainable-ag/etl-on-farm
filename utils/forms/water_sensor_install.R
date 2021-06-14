parse_wsensor_install <- function(elt) {
  
  if (elt$`__version__` == "vuiiHRr2MJSGzFwSncyLP9") {
    ret <- wsensor_install_1rep(elt)
  } else {
    ret <- wsensor_install_2rep(elt)
  }
  
  if (nrow(ret) == 0) {stop("Some essential item is missing from the form")}
  
  return(ret)
}

wsensor_install_1rep <- function(elt) {
  # timestamp is probably when they sync on Wifi (sent to kobo and webhooked)
  # "Scan_your_bare_node" is bare node serial
  # "What_is_your_Farm_Code" needs to have case coercion and whitespace trim
  # "Scan_your_cover_crop_node" cover node serial
  # "Scan_the_barcode_on_side_of_your_Gateway" gateway serial
  # "start" is time of beginning of form fillout
  # "end" is time of end of form fillout
  # "In_which_subplot_are_ur_sensors_installed" trim to only last char for digit
  # "What_is_the_GPS_loca_on_of_your_bare_node" c(lat, lon, alt_m, acc_m)
  # "What_is_the_GPS_loca_your_cover_crop_node"
  # "Any_issues_to_report_about_your_sensors" # should post these to github
  # "_submitted_by" gives state acct so we can protect against code mistype?
  # "What_day_did_you_install_the_sensors" seems to line up with other timestamps
  # "_submission_time" can be some days after `end`
  
  # "_xform_id_string" is the ID for psa_water_sensor_install
  # "_geolocation" is c(lat, lon) at time of opening? submitting?
  # "__version__" is the hashed version num
  # "_id" matches column in webhook submission (kobo table)
  
  if (elt$`__version__` != "vuiiHRr2MJSGzFwSncyLP9") {
    stop("Incorrect form version, needs inspection")
  }
  
  # handle late form submissions
  begin <- min(
    elt$start %>% lubridate::as_datetime(),
    (elt$`What_day_did_you_install_the_sensors` %||% NA) %>% 
      lubridate::as_datetime(),
    na.rm = T
  )
  
  code <- elt$`What_is_your_Farm_Code` %>% 
    stringr::str_to_upper() %>% 
    stringr::str_trim() %>% 
    stringr::str_extract("^[0-9A-Z]{3}") %>% 
    na.omit() %>% 
    as.character()
  
  assert_active(code, lubridate::year(begin))
  
  
  ret <- tibble(
    rawuid = elt$rawuid,
    parsed_at = Sys.time(),
    code = code,
    subplot = elt$`In_which_subplot_are_ur_sensors_installed` %>% 
      stringr::str_extract("[12]") %>% 
      as.integer(),
    gateway_serial_no = elt$`Scan_the_barcode_on_side_of_your_Gateway`,
    bare_node_serial_no = elt$`Scan_your_bare_node`,
    cover_node_serial_no = elt$`Scan_your_cover_crop_node`,
    time_begin = begin,
    notes = elt$`Any_issues_to_report_about_your_sensors` %||% NA,
    submitted_by = elt$`_submitted_by`
  )
  
  if (nrow(ret) == 0) {stop("Some essential item is missing from the form")}
  
  return(ret)
}



wsensor_install_2rep <- function(elt) {
  # "group_uv9yg82/bare_node_rep2": "18000355",
  # "group_uv9yg82/bare_node_rep1": "18000373",
  # "group_uv9yg82/gps_cover_crop_node_rep1": "32.285082 -81.8630777 29.481088834694983 12.997",
  # "group_uv9yg82/gps_cover_crop_node_rep2": "32.2848972 -81.8632514 38.42488957619054 6.126",
  # "group_uv9yg82/gps_bare_node_rep1": "32.2850888 -81.8630768 26.161320841336558 8.041",
  # "farm_info_group/date_install": "2021-04-12",
  # "group_uv9yg82/gps_bare_node_rep2": "32.2849299 -81.8633825 24.85519306950351 5.262",

  # "group_uv9yg82/cover_crop_node_rep1": "18000379",
  # "farm_info_group/crop": "Corn",
  # "group_vm21f92/form_version": "v4",

  # "end": "2021-04-13T08:26:44.963-04:00",
  # "group_uv9yg82/cover_crop_node_rep2": "18000359",
  # "_submission_time": "2021-04-13T12:26:51",
  # "farm_info_group/crop_planting_date": "2021-03-27",
  # "farm_info_group/initial_or_reinstall": "initial",

  # "barcode_gateway": "21000194",
  # "start": "2021-04-12T12:22:57.720-04:00",
  # "_submitted_by": "ga_psa",
  # "_geolocation": [
  #   32.285082,
  #   -81.8630777
  #   ],
  # "farm_info_group/code": "VMF",

  # "__version__": "vCK5tppVaNkkfWMhtddxEb",
  # "today": "2021-04-12"
  
  
  if (elt$`__version__` != "vCK5tppVaNkkfWMhtddxEb") {
    stop("Incorrect form version, needs inspection")
  }
  
  cd <- elt$`farm_info_group/code` %>% 
    stringr::str_trim() %>% 
    stringr::str_to_upper()
  
  if (!stringr::str_detect(cd, "^[A-Z0-9]{3}$")) {
    stop("Malformed farm code.")
  }
  
  gw <- elt$barcode_gateway
  begin <- elt$start %>% lubridate::as_datetime()
  
  if (is.null(cd) | is.null(gw)) {
    stop("Missing farm code or gateway serial.")
  }
  
  assert_active(cd, lubridate::year(begin))
  
  idx <- stringr::str_detect(names(elt), "node")
  
  node_serials_and_coords <- tibble::enframe(elt[idx]) %>% 
    mutate(
      name = stringr::str_remove(name, "^group.+/"),
      name = stringr::str_replace_all(name, "issues", "notes"),
      value = unlist(value)
    ) %>% 
    tidyr::separate(name, c("column", "subplot"), sep = "_rep") %>% 
    tidyr::pivot_wider(
      values_from = value,
      names_from = column
    ) %>% 
    tidyr::separate(
      gps_cover_crop_node,
      c("cover_lat", "cover_lon"),
      sep = " ",
      extra = "drop"
    ) %>% 
    tidyr::separate(
      gps_bare_node,
      c("bare_lat", "bare_lon"),
      sep = " ",
      extra = "drop"
    ) %>% 
    rename(
      bare_node_serial_no = bare_node,
      cover_node_serial_no = cover_crop_node
    ) 
  
  node_serials_and_coords %>% 
    mutate(
      code = cd,
      gateway_serial_no = gw,
      time_begin = begin,
      rawuid = elt$rawuid,
      parsed_at = Sys.time(),
      submitted_by = elt$`_submitted_by`
    ) %>% 
    arrange(subplot)
  
}



etl_parse_wsensor_install <- function(elt) {
  
  ret <- purrr::safely(parse_wsensor_install)(elt)
  
  if (is.null(ret$result)) {
    dbWriteTable(
      etl_connect_shadow("forms"),
      "needs_help",
      data.frame(
        rawuid = elt$rawuid, 
        target_tbl = "wsensor_install", 
        err = as.character(ret$error)
      ),
      append = TRUE
    )
  }
  
  ret$result
}