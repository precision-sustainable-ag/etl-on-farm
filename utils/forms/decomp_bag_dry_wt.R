parse_decomp_bag_dry_wt <- function(elt) {
  
  #  "_id"                                      
  #  "_notes"    
  
  #  "dry_wt_group_001/dry_wt_001"              
  #  "dry_wt_group/dry_wt"                      
  #  "dry_wt_group_001/decomp_bag_001"          
  #  "dry_wt_group/decomp_bag" 
  
  #  "_submitted_by"                            
  #  "_xform_id_string"                         
  #  "end"                                      
  #  "_submission_time"     
  #  "start"                                    
  #  "_geolocation"           
  #  "_version_"                                
  #  "__version__"    
  
  if (elt$`__version__` != "vDqdEsDav5K6hSRHrgYJmM") {
    stop("Incorrect form version, needs inspection")
  }
  
  
  weights <- elt %>% 
    tibble::enframe() %>% 
    filter(stringr::str_detect(name, "^dry_wt_group")) %>% 
    tidyr::unnest(cols = "value") %>% 
    tidyr::separate(name, c("group", "var"), sep = "/") %>% 
    mutate(
      var = stringr::str_remove_all(var, "_[0-9]+$")
    ) %>% 
    tidyr::pivot_wider(values_from = value, names_from = var)

  barcode_flag <- stringr::str_detect(
    weights$decomp_bag,
    "^[A-Z]{3}-[12]-[AB]-[0-5]$"
  )

  na_flag <- any(is.na(weights$dry_wt))

  if (any(!barcode_flag) | any(is.na(barcode_flag))) {
    stop("Malformed barcode(s)")
  }

  if (na_flag) {
    stop("Missing weight(s)")
  }

  weights %>%
    tidyr::separate(
      decomp_bag,
      c("code", "subplot", "subsample", "time"),
      sep = "-"
    ) %>%
    rename(dry_biomass_wt = dry_wt) %>%
    select(-group) %>%
    mutate(
      rawuid = elt$rawuid,
      parsed_at = Sys.time(),
      notes = unlist(elt$`_notes`) %||% "",
      submitted_by = elt$`_submitted_by`
    ) %>%
    select(rawuid, parsed_at, everything())
}

x %>% 
  purrr::pmap(rawdb_kobo_to_lst) %>% 
  purrr::map(purrr::safely(parse_decomp_bag_dry_wt)) %>% 
  purrr::map("result") %>% 
  dplyr::bind_rows()



etl_parse_decomp_bag_dry_wt <- function(elt) {
  
  ret <- purrr::safely(parse_decomp_bag_dry_wt)(elt)
  
  if (is.null(ret$result)) {
    dbWriteTable(
      etl_connect_shadow("forms"),
      "needs_help",
      data.frame(
        rawuid = elt$rawuid, 
        target_tbl = "decomp_biomass_dry__decomp_bag_dry_wt", 
        err = as.character(ret$error)
      ),
      append = TRUE
    )
  }
  
  ret$result
}



