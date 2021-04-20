parse_wsensor_install <- function(elt) {
  
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
    stringr::str_extract("[0-9A-Z]{3}") %>% 
    na.omit() %>% 
    as.character()
  
  
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