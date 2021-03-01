parse_biomass_decomp_bag__biomass_in_field <- function(elt) {
  
  # "group_000/fresh_bag_wt_grams_000": "111.1",
  # "code": "EPF",
  # "group_023/barcode_bag_023": "EPF-2-B-3",
  # "group_000/barcode_bag_000": "EPF-1-A-0",
  # "group_011/fresh_bag_wt_grams_011": "153.5",
  # "sub1_average_fresh_wt": "579.3499999999999",
  # "group_017/barcode_bag_017": "EPF-2-A-5",
  # "group_016/fresh_bag_wt_grams_016": "158.2",
  # "group_007/fresh_bag_wt_grams_007": "153.1",
  # "fresh_wt_b1": "723.8",
  # "group_015/fresh_bag_wt_grams_015": "160.4",
  # "group_018/fresh_bag_wt_grams_018": "158.1",
  # "group_008/barcode_bag_008": "EPF-1-A-3",
  # "fresh_wt_a1": "434.9",
  # "group_006/barcode_bag_006": "EPF-1-B-3",
  # "group_015/barcode_bag_015": "EPF-2-B-4",
  # "bag_a2": "0.0",
  # "group_014/fresh_bag_wt_grams_014": "158.7",
  # "sub1_legume": "less_than_40_legume",
  # "end": "2020-04-10T16:10:12.230-04:00",
  # "group_013/barcode_bag_013": "EPF-2-B-0",
  # "group_018/barcode_bag_018": "EPF-2-B-1",
  # "target2_bag_wt": "101.29",
  # "bag_b1": "0.0",
  # "group_011/barcode_bag_011": "EPF-1-B-4",
  # "group_016/barcode_bag_016": "EPF-2-A-1",
  # "group_002/barcode_bag_002": "EPF-1-A-4",
  # "group_022/barcode_bag_022": "EPF-2-A-4",
  # "start": "2020-04-10T15:36:15.834-04:00",
  # "group_010/barcode_bag_010": "EPF-1-B-5",
  # "bag_a1": "0.0",
  # "group_009/fresh_bag_wt_grams_009": "152.7",
  # "__version__": "vyYD9Bt3WSjzKorvPhLvLh",
  # "group_019/fresh_bag_wt_grams_019": "159.4",
  # "group_001/fresh_bag_wt_grams_001": "112.0",
  # "group_003/barcode_bag_003": "EPF-1-B-2",
  # "group_005/barcode_bag_005": "EPF-1-A-5",
  # "fresh_wt_b2": "665.5",
  # "group_012/fresh_bag_wt_grams_012": "119.7",
  # "group_004/barcode_bag_004": "EPF-1-A-1",
  # "group_012/barcode_bag_012": "EPF-2-A-0",
  # "group_014/barcode_bag_014": "EPF-2-A-3",
  # "group_020/fresh_bag_wt_grams_020": "160.5",
  # "_submitted_by": "md_psa",
  # "group_009/barcode_bag_009": "EPF-1-B-1",
  # "group_017/fresh_bag_wt_grams_017": "154.0",
  # "group_005/fresh_bag_wt_grams_005": "153.8",
  # "group_013/fresh_bag_wt_grams_013": "120.4",
  # "group_004/fresh_bag_wt_grams_004": "148.9",
  # "bag_b2": "0.0",
  # "group_022/fresh_bag_wt_grams_022": "156.7",
  # "group_003/fresh_bag_wt_grams_003": "151.2",
  # "group_007/barcode_bag_007": "EPF-1-A-2",
  # "group_021/barcode_bag_021": "EPF-2-B-5",
  # "sub2_legume": "less_than_40_legume",
  # "group_006/fresh_bag_wt_grams_006": "154.8",
  # "_submission_time": "2020-04-13T16:14:09",
  # "group_019/barcode_bag_019": "EPF-2-A-2",
  # "drilled_broad": "broadcast",
  # "target1_bag_wt": "96.56",
  # "group_001/barcode_bag_001": "EPF-1-B-0",
  # "group_002/fresh_bag_wt_grams_002": "154.2",
  # "group_020/barcode_bag_020": "EPF-2-B-2",
  # "group_023/fresh_bag_wt_grams_023": "159.1",
  # "group_021/fresh_bag_wt_grams_021": "157.8",
  # "fresh_wt_a2": "550.0",
  # "group_008/fresh_bag_wt_grams_008": "153.1",
  # "sub2_average_fresh_wt": "607.75",
  # "group_010/fresh_bag_wt_grams_010": "154.4"  
  
  if (elt$`__version__` != "vyYD9Bt3WSjzKorvPhLvLh") {
    #stop("Incorrect form version, needs inspection")
  }
  
  
  weights_long <- elt %>% 
    tibble::enframe() %>% 
    filter(stringr::str_detect(name, "^fresh|legume$")) %>% 
    tidyr::unnest(cols = "value") %>% 
    mutate(
      subplot = stringr::str_extract(name, "[12]"),
      name = stringr::str_remove(name, "[12]")
    ) 
  
  if (nrow(weights_long) != 6) {
    stop("Missing weights or legume estimate.")
  }
  
  weights <- weights_long %>% 
    tidyr::pivot_wider(
      id_cols = subplot,
      names_from = name,
      values_from = value
    ) %>%
    mutate(
      legumes_40 = stringr::str_detect(
        sub_legume, "more"
      ) && !stringr::str_detect(
        sub_legume, "less"
      ),
      legumes_40 = as.integer(legumes_40)
    ) %>% 
    select(-sub_legume)
  
  wts_flag <- is.na(c(weights$fresh_wt_a, weights$fresh_wt_b))
  
  if (any(wts_flag)) {
    stop("Missing fresh weight(s).")
  }
  
  bags_long <- elt %>% 
    tibble::enframe() %>% 
    filter(stringr::str_detect(name, "^bag")) %>% 
    tidyr::unnest(cols = "value") %>% 
    mutate(
      subplot = stringr::str_extract(name, "[12]"),
      name = stringr::str_remove(name, "[12]")
    )
  
  if (nrow(bags_long) != 4) {
    stop("Missing empty bag weight(s).")
  }
  
  bags <- bags_long %>%
    group_by(subplot) %>% 
    summarise(bag_wt = mean(as.numeric(value)))
  
  if (any(is.na(bags$bag_wt))) {
    stop("Malformed/empty bag weight(s).")
  }
  
  if (is.null(elt$code) | !stringr::str_detect(elt$code, "^[A-Z]{3}$")) {
    stop("Missing/malformed farm code.")
  }
  
  full_join(
    weights, bags
  ) %>% 
    mutate(code = elt$code) %>% 
    mutate(
      rawuid = elt$rawuid,
      parsed_at = Sys.time(),
      submitted_by = elt$`_submitted_by`
    ) %>%
    select(rawuid, parsed_at, code, everything())
}



etl_parse_biomass_decomp_bag__biomass_in_field <- function(elt) {
  
  ret <- purrr::safely(parse_biomass_decomp_bag__biomass_in_field)(elt)
  
  if (is.null(ret$result)) {
    dbWriteTable(
      etl_connect_shadow("forms"),
      "needs_help",
      data.frame(
        rawuid = elt$rawuid, 
        target_tbl = "biomass_in_field__biomass_decomp_bag", 
        err = as.character(ret$error)
      ),
      append = TRUE
    )
  }
  
  ret$result
}



parse_biomass_decomp_bag__decomp_biomass_fresh <- function(elt) {
  
  # "group_000/fresh_bag_wt_grams_000": "111.1",
  # "code": "EPF",
  # "group_023/barcode_bag_023": "EPF-2-B-3",
  # "group_000/barcode_bag_000": "EPF-1-A-0",
  # "group_011/fresh_bag_wt_grams_011": "153.5",
  # "sub1_average_fresh_wt": "579.3499999999999",
  # "group_017/barcode_bag_017": "EPF-2-A-5",
  # "group_016/fresh_bag_wt_grams_016": "158.2",
  # "group_007/fresh_bag_wt_grams_007": "153.1",
  # "fresh_wt_b1": "723.8",
  # "group_015/fresh_bag_wt_grams_015": "160.4",
  # "group_018/fresh_bag_wt_grams_018": "158.1",
  # "group_008/barcode_bag_008": "EPF-1-A-3",
  # "fresh_wt_a1": "434.9",
  # "group_006/barcode_bag_006": "EPF-1-B-3",
  # "group_015/barcode_bag_015": "EPF-2-B-4",
  # "bag_a2": "0.0",
  # "group_014/fresh_bag_wt_grams_014": "158.7",
  # "sub1_legume": "less_than_40_legume",
  # "end": "2020-04-10T16:10:12.230-04:00",
  # "group_013/barcode_bag_013": "EPF-2-B-0",
  # "group_018/barcode_bag_018": "EPF-2-B-1",
  # "target2_bag_wt": "101.29",
  # "bag_b1": "0.0",
  # "group_011/barcode_bag_011": "EPF-1-B-4",
  # "group_016/barcode_bag_016": "EPF-2-A-1",
  # "group_002/barcode_bag_002": "EPF-1-A-4",
  # "group_022/barcode_bag_022": "EPF-2-A-4",
  # "start": "2020-04-10T15:36:15.834-04:00",
  # "group_010/barcode_bag_010": "EPF-1-B-5",
  # "bag_a1": "0.0",
  # "group_009/fresh_bag_wt_grams_009": "152.7",
  # "__version__": "vyYD9Bt3WSjzKorvPhLvLh",
  # "group_019/fresh_bag_wt_grams_019": "159.4",
  # "group_001/fresh_bag_wt_grams_001": "112.0",
  # "group_003/barcode_bag_003": "EPF-1-B-2",
  # "group_005/barcode_bag_005": "EPF-1-A-5",
  # "fresh_wt_b2": "665.5",
  # "group_012/fresh_bag_wt_grams_012": "119.7",
  # "group_004/barcode_bag_004": "EPF-1-A-1",
  # "group_012/barcode_bag_012": "EPF-2-A-0",
  # "group_014/barcode_bag_014": "EPF-2-A-3",
  # "group_020/fresh_bag_wt_grams_020": "160.5",
  # "_submitted_by": "md_psa",
  # "group_009/barcode_bag_009": "EPF-1-B-1",
  # "group_017/fresh_bag_wt_grams_017": "154.0",
  # "group_005/fresh_bag_wt_grams_005": "153.8",
  # "group_013/fresh_bag_wt_grams_013": "120.4",
  # "group_004/fresh_bag_wt_grams_004": "148.9",
  # "bag_b2": "0.0",
  # "group_022/fresh_bag_wt_grams_022": "156.7",
  # "group_003/fresh_bag_wt_grams_003": "151.2",
  # "group_007/barcode_bag_007": "EPF-1-A-2",
  # "group_021/barcode_bag_021": "EPF-2-B-5",
  # "sub2_legume": "less_than_40_legume",
  # "group_006/fresh_bag_wt_grams_006": "154.8",
  # "_submission_time": "2020-04-13T16:14:09",
  # "group_019/barcode_bag_019": "EPF-2-A-2",
  # "drilled_broad": "broadcast",
  # "target1_bag_wt": "96.56",
  # "group_001/barcode_bag_001": "EPF-1-B-0",
  # "group_002/fresh_bag_wt_grams_002": "154.2",
  # "group_020/barcode_bag_020": "EPF-2-B-2",
  # "group_023/fresh_bag_wt_grams_023": "159.1",
  # "group_021/fresh_bag_wt_grams_021": "157.8",
  # "fresh_wt_a2": "550.0",
  # "group_008/fresh_bag_wt_grams_008": "153.1",
  # "sub2_average_fresh_wt": "607.75",
  # "group_010/fresh_bag_wt_grams_010": "154.4"  
  
  if (elt$`__version__` != "vyYD9Bt3WSjzKorvPhLvLh") {
    #stop("Incorrect form version, needs inspection")
  }
  
  
  weights <- elt  %>% 
    tibble::enframe() %>% 
    filter(stringr::str_detect(name, "^group")) %>% 
    tidyr::unnest(cols = "value") %>% 
    tidyr::separate(name, c("group", "var"), sep = "/") %>% 
    mutate(
      var = stringr::str_remove_all(var, "_[0-9]+$")
    ) %>%
    tidyr::pivot_wider(values_from = value, names_from = var) %>% 
    select(-group)
  
  if (is.null(weights$barcode_bag)) {
    stop("Malformed/missing barcode(s)")
  }
  
  barcode_flag <- stringr::str_detect(
    weights$barcode_bag, 
    "^[A-Z]{3}-[12]-[AB]-[0-5]$"
  )
  
  if (any(!barcode_flag) | any(is.na(barcode_flag))) {
    stop("Malformed/missing barcode(s)")
  }
  
  ret <- weights %>%
    tidyr::separate(
      barcode_bag,
      c("code", "subplot", "subsample", "time"),
      sep = "-"
    ) %>% 
    rename(fresh_biomass_wt = fresh_bag_wt_grams) %>% 
    mutate(
      rawuid = elt$rawuid,
      parsed_at = Sys.time(),
      submitted_by = elt$`_submitted_by`
    ) %>%
    select(rawuid, parsed_at, everything())
  
  if (any(is.na(ret$fresh_biomass_wt))) {stop("Missing weight(s)")}
  
  if (nrow(ret) == 0) {stop("Some essential item is missing from the form")}
  
  return(ret)
}



etl_parse_biomass_decomp_bag__decomp_biomass_fresh <- function(elt) {
  
  ret <- purrr::safely(parse_biomass_decomp_bag__decomp_biomass_fresh)(elt)
  
  if (is.null(ret$result)) {
    dbWriteTable(
      etl_connect_shadow("forms"),
      "needs_help",
      data.frame(
        rawuid = elt$rawuid, 
        target_tbl = "decomp_biomass_fresh__biomass_decomp_bag", 
        err = as.character(ret$error)
      ),
      append = TRUE
    )
  }
  
  ret$result
}



