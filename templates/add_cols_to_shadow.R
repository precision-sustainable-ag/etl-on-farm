# Use this function to edit existing shadow tables
#   DON'T FORGET!
#   You need to update the table definition in `initializers.R`

alter_table <- function(con, tab, col_n, col_t, default = NULL, not_null = F) {
  
  stopifnot(
    stringr::str_detect(tab, "^[a-z_]+$"),
    stringr::str_detect(col_n, "^[a-z_]+$"),
    stringr::str_detect(col_t, "^TEXT|INTEGER|REAL$")
  )
  
  nn <- if (!is.null(default) & not_null) {
    "NOT NULL"
  } else {""}
  
  if (!is.null(default)) {
    val <- switch(
      col_t,
      "TEXT" = glue::glue("'{default}'"),
      "INTEGER" = as.integer(default),
      "REAL" = as.numeric(default)
    )
    
    stopifnot(
      !is.na(val),
      stringr::str_detect(val, "--|;", negate = T)
    )
    
    default <- paste("DEFAULT", val)
  } else {
    default <- ""
  }
  
  
  query <- glue::glue(
    "
    ALTER TABLE `{tab}`
    ADD COLUMN `{col_n}` {col_t} {nn} {default};
    "
    )
  
  print(query)
  
  conf <- askYesNo("Is this the expected query?")
  
  if (!isTRUE(conf)) {
    stop("Malformed query; aborted.")
  }
  
  
  dbBegin(con)
  dbExecute(
    con,
    query
  )
  
  print(tbl(con, tab), width = Inf)
  
  conf <- askYesNo("Is this the expected output?")
  
  if (isTRUE(conf)) {
    dbCommit(con)
  } else {
    dbRollback(con)
    stop("Malformed table; aborted.")
  }
  
} 


con <- etl_connect_shadow("forms")

tbl(con, "wsensor_install")

alter_table(
  con,
  "wsensor_install",
  "bare_lon",
  "REAL"
)

alter_table(
  con,
  "wsensor_install",
  "bare_lat",
  "REAL"
)

alter_table(
  con,
  "wsensor_install",
  "cover_lon",
  "REAL"
)

alter_table(
  con,
  "wsensor_install",
  "cover_lat",
  "REAL"
)
