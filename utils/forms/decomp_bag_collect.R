parse_decomp_bag_collect <- function(elt) {
  
  # _xform_id_string
  # form_version
  # end
  # start
  # _geolocation
  # __version__
  # _submitted_by
  
  # barcode_bag_001
  # barcode_bag_000
  # barcode_bag_003
  # barcode_bag_002
  
  # _submission_time
  # decomp_bag_collect_date
  # _id
  
  
  if (elt$`__version__` != "v94kLXWMrZ8fZRpCFzSn4e") {
    stop("Incorrect form version, needs inspection")
  }
  
  collection_date <- lubridate::ymd(elt$decomp_bag_collect_date)
  
  if (!length(collection_date) | is.na(collection_date)) {
    stop("Missing pickup date")
  }
  
  if (Sys.Date() - collection_date > 5*365) {
    stop("Parsing old data? Pickup date more than 5 years old")
  }
  
  if (Sys.Date() < collection_date) {
    stop("Pickup date listed in the future")
  }
  
  bags <- elt %>% 
    tibble::enframe(value = "barcode_bag") %>% 
    filter(stringr::str_detect(name, "^barcode_bag")) %>% 
    tidyr::unnest(cols = "barcode_bag") %>% 
    select(-name) %>% 
    mutate(recovery_date = collection_date)
  
  barcode_flag <- stringr::str_detect(
    bags$barcode_bag, 
    "^[A-Z]{3}-[12]-[AB]-[0-5]$"
  )
  
  if (any(!barcode_flag) | any(is.na(barcode_flag))) {
    stop("Malformed barcode(s)")
  }
  
  ret <- bags %>%
    tidyr::separate(
      barcode_bag,
      c("code", "subplot", "subsample", "time"),
      sep = "-"
    ) %>% 
    mutate(
      rawuid = elt$rawuid,
      parsed_at = Sys.time(),
      submitted_by = elt$`_submitted_by`
    ) %>%
    select(rawuid, parsed_at, everything())
  
  if (nrow(ret) == 0) {stop("Some essential item is missing from the form")}
  
  return(ret)
}

etl_parse_decomp_bag_collect <- function(elt) {
  
  ret <- purrr::safely(parse_decomp_bag_collect)(elt)
  
  if (is.null(ret$result)) {
    dbWriteTable(
      etl_connect_shadow("forms"),
      "needs_help",
      data.frame(
        rawuid = elt$rawuid, 
        target_tbl = "decomp_biomass_dry__decomp_bag_collect", 
        err = as.character(ret$error)
      ),
      append = TRUE
    )
  }
  
  ret$result
}







