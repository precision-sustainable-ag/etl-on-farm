sanitize_ndjson <- function(s) {
  s %>% 
    paste(collapse = "\n") %>% 
    stringr::str_replace_all("\n|\r", "<br>") %>% 
    stringr::str_replace_all(":", "-") %>% 
    stringr::str_replace_all("\t", "&emsp;&emsp;")
}


# construct equality logic subclause ID matching rows
ternary_equals_clauser <- function(temp, target, nms) {
  glue::glue(
    '(
    {temp}."{nms}" = {target}."{nms}" OR 
      ({temp}."{nms}" IS NULL AND {target}."{nms}" IS NULL)
    )'
  ) %>% 
    glue::glue_collapse(sep = " AND ")
}

# construct query to perform antijoin and then insert result
insert_from_antijoin <- function(temptable, targettable, nms_both) {
  
  cols <- glue::glue('"{nms_both}"') %>% 
    glue::glue_collapse(sep = ", ")
  
  where_clause <- ternary_equals_clauser(
    temptable, targettable, nms_both
  )
  
  glue::glue(
    "INSERT INTO {targettable} ({cols})
    SELECT {cols} FROM {temptable}
    WHERE NOT EXISTS (
      SELECT 1 from {targettable}
      WHERE (
        {where_clause}
      )
    )"
  )
  
}

upsert_do_nothing <- function(temptable, targettable, nms_both, unicity_nm) {
  
  cols <- glue::glue('"{nms_both}"') %>% 
    glue::glue_collapse(sep = ", ")
  
  if (missing(unicity_nm) || is.null(unicity_nm)) {
    conflict_clause <- ";"
  } else {
    conflict_clause <- glue::glue(
      "ON CONFLICT ON CONSTRAINT {unicity_nm}
      DO NOTHING;"
    )
  }
  
  glue::glue(
    "
    INSERT INTO {targettable} ({cols})
    SELECT {cols} FROM {temptable}
    {conflict_clause}
    "
  )
  
}

# Pull specified rows, push to temp, perform antijoin and insert
# etl_push_sensors <- function(shadow_tb, prod_tb, rawuids, unicity = NULL) {
#   prod_con <- etl_connect_prod()
#   shad_con <- etl_connect_shadow("sensors")
#   
#   from_shadow <- tbl(shad_con, shadow_tb) %>% 
#     filter(rawuid %in% rawuids) %>%
#     select(-sid) %>% 
#     collect() 
#   
#   if (!all(unicity %in% names(from_shadow))) {
#     stop("Unicity constraint cols not found in shadow database")
#   } else if (is.null(unicity)) {
#     unicity <- names(from_shadow)
#   }
#   
#   to_push <- from_shadow %>% 
#     distinct_at(vars(matches(unicity)), .keep_all = TRUE) %>% 
#     mutate_at(
#       vars(matches(c("ts_up", "timestamp"))),
#       ~lubridate::as_datetime(.)
#     )
#   
#   temp_tb <- glue::glue("temp_{prod_tb}")
#   
#   dbWriteTable(
#     prod_con, temp_tb, to_push, 
#     temporary = TRUE, overwrite = TRUE
#     )
#   
#   nms_temp <- names(to_push)
#   nms_prod <- dbListFields(prod_con, prod_tb)
#   
#   nms_match <- intersect(nms_temp, nms_prod)
#   
#   query <- insert_from_antijoin(
#     temp_tb, prod_tb, nms_match
#   )
#   
#   rows_affected <- dbExecute(prod_con, query)
#   
#   # temp tables should disappear when connection closes
#   # hoping this is the source of hanging
#   # dbRemoveTable(prod_con, temp_tb)
#   
#   dbDisconnect(prod_con)
#   dbDisconnect(shad_con)
#   
#   rows_affected
# }

# Pull specified rows from shadow (matching rawuids)
#   Push them into temp table in prod
#   Find matching names between temp table and target table
#   Construct upsert query that inserts matching cols
#     UNLESS that violates name of unicity constraint (from pgAdmin)
#     in that case skip the row
etl_upsert_sensors <- function(shadow_tb, prod_tb, rawuids, unicity = NULL) {
  prod_con <- etl_connect_prod()
  shad_con <- etl_connect_shadow("sensors")
  
  from_shadow <- tbl(shad_con, shadow_tb) %>% 
    filter(rawuid %in% rawuids) %>%
    select(-sid) %>% 
    collect() %>% 
    mutate_at(
      vars(matches(c("ts_up", "timestamp"))),
      ~lubridate::as_datetime(.)
    )
  
  temp_tb <- glue::glue("temp_{prod_tb}")
  
  dbWriteTable(
    prod_con, temp_tb, from_shadow, 
    temporary = TRUE, overwrite = TRUE
  )
  
  nms_temp <- names(from_shadow)
  nms_prod <- dbListFields(prod_con, prod_tb)
  
  nms_match <- intersect(nms_temp, nms_prod)
  
  query <- upsert_do_nothing(
    temp_tb, prod_tb, nms_match, unicity
  )
  
  rows_affected <- purrr::safely(dbExecute)(prod_con, query)
  
  dbDisconnect(prod_con)
  dbDisconnect(shad_con)
  
  if (is.null(rows_affected$error)) {
    return(rows_affected$result)
  } else {
    stop(sanitize_ndjson(rows_affected$error))
  }
  
}


# Like the sensor upsert, except pass in a local DF instead of the name of
#   the shadow DB table. The forms require some reparsing, esp of times
#   before pushing
etl_insert_form <- function(local_tb, prod_tb, unicity = NULL) {
  prod_con <- etl_connect_prod()
  shad_con <- etl_connect_shadow("forms")
  
  temp_tb <- glue::glue("temp_{prod_tb}")
  
  dbWriteTable(
    prod_con, temp_tb, local_tb, 
    temporary = TRUE, overwrite = TRUE
  )
  
  nms_temp <- names(local_tb)
  nms_prod <- dbListFields(prod_con, prod_tb)
  
  nms_match <- intersect(nms_temp, nms_prod)
  
  query <- upsert_do_nothing(
    temp_tb, prod_tb, nms_match, unicity
  )
  
  rows_affected <- purrr::safely(dbExecute)(prod_con, query)
  
  dbDisconnect(prod_con)
  dbDisconnect(shad_con)
  
  if (is.null(rows_affected$error)) {
    return(rows_affected$result)
  } else {
    stop(sanitize_ndjson(rows_affected$error))
  }
}

etl_mark_pushed <- function(conn, tbl_nm, idx) {
  if (!length(idx)) {return(0)}
  
  indices <- glue::glue_collapse(idx, sep = ", ")
  
  rows_affected <- purrr::safely(dbExecute)(
    conn,
    glue::glue(
      "
      UPDATE {tbl_nm}
      SET pushed_to_prod = 1
      WHERE rawuid IN ({indices})
      "
    )
  )
  
  if (is.null(rows_affected$error)) {
    return(rows_affected$result)
  } else {
    stop(sanitize_ndjson(rows_affected$error))
  }
}
