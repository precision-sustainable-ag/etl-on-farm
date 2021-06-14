`%||%` <- purrr::`%||%`

rawdb_kobo_to_lst <- function(uid, id, asset_name, data, timestamp) {
  x <- jsonlite::fromJSON(data)
  x$rawuid <- uid
  x$asset_name <- asset_name
  x$timestamp <- timestamp
  x
}


assert_active <- function(cd, yr) {
  con <- etl_connect_prod()
  on.exit(dbDisconnect(con))
  
  yr <- as.character(yr)
  
  active <- tbl(con, "site_information") %>% 
    filter(year == yr) %>% 
    filter(code == cd) %>% 
    collect() %>% 
    nrow()

  if (!active) {
    stop("Code supplied is not active in the form year.")
  }
}
