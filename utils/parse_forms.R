`%||%` <- purrr::`%||%`

rawdb_kobo_to_lst <- function(uid, id, asset_name, data, timestamp) {
  x <- jsonlite::fromJSON(data)
  x$rawuid <- uid
  x$asset_name <- asset_name
  x$timestamp <- timestamp
  x
}


