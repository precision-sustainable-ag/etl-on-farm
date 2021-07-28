message(Sys.time(), "\n\n")

x <- na.omit(stringr::str_match(commandArgs(), "--file=(.+)")[,2])
if (length(x)) setwd(dirname(x))

decolonize <- function(s) {
  s <- stringr::str_replace_all(s, "[[:punct:]]", "_") 
  stringr::str_replace_all(s, "[[:space:]]", "__")
}

suppressPackageStartupMessages(
  library(loggit)
)

set_logfile(
  glue::glue(
    "{getwd()}/log/prefill_rows_in_prod_{decolonize(Sys.time())}.log"
  )
)
set_timestamp_format("%Y-%m-%dT%H:%M:%OS4%z")

message("Execution start")

real_time_rq <- httr::RETRY("GET", "example.com")
real_time <- httr::parse_http_date(httr::headers(real_time_rq)$date)

off <- Sys.time() - real_time
loggit(
  "INFO",
  glue::glue("Offset of this server and real time is {format(off)}"),
  offset_s = as.double(off, units = "secs")
)

source("secret.R")
source("utils/initializers.R")

suppressPackageStartupMessages({
  library(DBI)
  library(dplyr)
  library(dbplyr)
  library(purrr)
})

# Do the work ----

fill_rows_create <- function(con, tbl_nm, preset_cols = list()) {
  
  enrolled_codes <- tbl(con, "site_information") %>% 
    pull(code)
  
  filled_codes <- tbl(con, tbl_nm) %>% 
    distinct(code) %>% 
    pull(code)
  
  to_fill_codes <- setdiff(enrolled_codes, filled_codes)
  
  message(
    glue::glue("Found {length(to_fill_codes)} sites for `{tbl_nm}`")
  )
  
  to_fill_tbl <- expand.grid(
    c(
      list(code = to_fill_codes),
      rev(preset_cols)
    ),
    stringsAsFactors = F
  ) 
  
  to_fill_tbl %>% 
    arrange(code)
}

tidy_safe_results <- function(elt) {
  list(
    result = elt$result %||% NA,
    error = elt$error %||% NA
  )
}

# Wrap any errors ----

etl_fill_rows <- function(con, tbl_nm, preset_cols = list()) {
  
  x <- fill_rows_create(con, tbl_nm, preset_cols)
  
  y <- purrr::safely(dbAppendTable)(
    con, tbl_nm, x,
    row.names = NULL, append = T
  )
  
  if (!is.null(y$error)) {
    stop(as.character(y$error))
  } else {
    message(
      glue::glue("Successfully filled {y$result} rows into `{tbl_nm}`")
      )
  }
  
  return(y)
}


# Specify structure ----

table_row_dictionary <- list(
  "biomass_in_field" = list(
    subplot = 1:2
  ),
  "biomass_nir" = list(
    subplot = 1:2
  ),
  "decomp_biomass_ash" = list(
    subplot = 1:2,
    subsample = c("A", "B"),
    time = 0:5
  ),
  "decomp_biomass_cn" = list(
    subplot = 1:2,
    subsample = c("A", "B"),
    time = 0:5
  ),
  "decomp_biomass_dry" = list(
    subplot = 1:2,
    subsample = c("A", "B"),
    time = 0:5
  ),
  "decomp_biomass_fresh" = list(
    subplot = 1:2,
    subsample = c("A", "B"),
    time = 0:5
  ),
  "gps_corners" = list(
    subplot = 1:2,
    treatment = c("B", "C"),
    corner_index = 1:4
  )
)

# Actually run ----

admin_con <- etl_connect_prod()
table_row_results <- purrr::imap(
  table_row_dictionary,
  ~etl_fill_rows(admin_con, .y, .x)
)

outlog <- table_row_results %>% 
  purrr::map(tidy_safe_results) %>% 
  jsonlite::toJSON()

jsonlite::prettify(outlog)

loggit::loggit(
  "INFO",
  "inserted_rows",
  list(
    data = stringr::str_remove_all(
      jsonlite::base64_enc(outlog), 
      "\n"
      )
    )
  )

dbDisconnect(admin_con)

message("Execution end")

set_logfile(logfile = NULL, confirm = F)