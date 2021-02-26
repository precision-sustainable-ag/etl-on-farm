parse_decomp_bag_pre_wt <- function(elt) {
  
  # _id               ## matches column in webhook submission (kobo table)
  # _notes
  # _xform_id_string  ## ID for "psa decomp bag pre wt"
  # form_version      ## Sarah's versioning
  # end               ## UTC timestamp (could be months later)
  # start             ## UTC timestamp (as soon as first field filled)
  
  # group_014/pre_bag_wt_grams_014  ## will be out of order!
  # group_016/pre_bag_wt_grams_016
  # group_016/barcode_bag_016
  # group_014/barcode_bag_014
  # ...
  
  # __version__       ## hashed version num
  # today             ## selected date YMD
  # _geolocation      ## c(lat, lon) at time of submitting?
  # _submitted_by     ## state acct so we can protect against code mistype eventually?
  
  
  # _submission_time  ## UTC timestamp (can be long after "end")
  
  if (elt$`__version__` != "vZvVu9PdqRHLeeJGbGha3z") {
    stop("Incorrect form version, needs inspection")
  }
  
  
  weights <- elt %>% 
    tibble::enframe() %>% 
    filter(stringr::str_detect(name, "^group")) %>% 
    tidyr::unnest(cols = "value") %>% 
    tidyr::separate(name, c("group", "var"), sep = "/") %>% 
    mutate(
      var = stringr::str_remove_all(var, "_[0-9]+$")
    ) %>% 
    tidyr::pivot_wider(values_from = value, names_from = var)
  
  barcode_flag <- stringr::str_detect(
    weights$barcode_bag, 
    "^[A-Z]{3}-[12]-[AB]-[0-5]$"
  )
  
  na_flag <- any(is.na(weights$pre_bag_wt_grams))
  
  if (any(!barcode_flag) | any(is.na(barcode_flag))) {
    stop("Malformed barcode(s)")
  }
  
  if (na_flag) {
    stop("Missing weight(s)")
  }
  
  weights %>%
    tidyr::separate(
      barcode_bag,
      c("code", "subplot", "subsample", "time"),
      sep = "-"
    ) %>%
    rename(empty_bag_wt = pre_bag_wt_grams) %>%
    select(-group) %>%
    mutate(
      rawuid = elt$rawuid,
      parsed_at = Sys.time(),
      notes = unlist(elt$`_notes`) %||% "",
      submitted_by = elt$`_submitted_by`
    ) %>%
    select(rawuid, parsed_at, everything())
}



etl_parse_decomp_bag_pre_wt <- function(elt) {
  
  ret <- purrr::safely(parse_decomp_bag_pre_wt)(elt)
  
  if (is.null(ret$result)) {
    dbWriteTable(
      etl_connect_shadow("forms"),
      "needs_help",
      data.frame(
        rawuid = elt$rawuid, 
        target_tbl = "decomp_biomass_fresh__decomp_bag_pre_wt", 
        err = as.character(ret$error)
      ),
      append = TRUE
    )
  }
  
  ret$result
}


