etl_connect_raw <- function(con = NULL) {
  if (!exists("raw_user")) {source("secret.R")}
  
  if (!is.null(con)) {
    x <- purrr::safely(dbListTables)(con)
    if (is.null(x$error)) return(con)
  }
  
  raw_ip <- iptools::hostname_to_ip(raw_host)[[1]][1]
  
  dbConnect(
    RMariaDB::MariaDB(), 
    username = raw_user,
    password = raw_password,
    dbname = raw_dbname,
    host = raw_ip,
    port = raw_port,
    program_name = "R_Puller_Parser_v0.1",
    connect_timeout = 10
  )
}

etl_connect_prod <- function(con = NULL) {
  if (!exists("pg_user")) {source("secret.R")}
  
  if (!is.null(con)) {
    x <- purrr::safely(dbListTables)(con)
    if (is.null(x$error)) return(con)
  }
  
  pg_ip <- iptools::hostname_to_ip(pg_host)[[1]][1]
  
  
  dbConnect(
    RPostgres::Postgres(), 
    user = pg_user,
    password = pg_password,
    dbname = pg_dbname,
    host = pg_ip,
    port = pg_port,
    sslmode = "require",
    application_name = "R_Puller_Parser_v0.1",
    connect_timeout = 5
  )
}

etl_connect_shadow <- function(dbname = NULL) {
  if (is.null(dbname)) {stop("dbname must be either `sensors` or `forms`")}
  
  dbConnect(
    RSQLite::SQLite(),
    dbname = glue::glue("./db/{dbname}.db"),
    flags = RSQLite::SQLITE_RW,
    bigint = "integer"
  )
}

etl_summarise_shadow <- function(con = NULL) {
  tbls <- dbListTables(con)
  rows <- purrr::map_int(
    tbls, 
    ~tbl(con, .x) %>% dplyr::count() %>% collect() %>% pull(n)
    )
  
  tibble::tibble(tbls, rows)
}


etl_init_shadow_sensors <- function(reset = F) {
  if (file.exists("./db/sensors.db") && !reset) {
    stop("Sensor database already exists. Need to reset?")
  }
  
  if (file.exists("./db/sensors.db") && reset) {
    confirmation <- askYesNo(
      "Are you sure you want to clear the shadow sensors DB?\n",
      FALSE
      )
    
    stopifnot(confirmation)
    
    file.create("./db/sensors.db")
  }
  
  if (!file.exists("./db/sensors.db")) {file.create("./db/sensors.db")}
  
  con_sh <- etl_connect_shadow(dbname = "sensors")
  
  # Keep track of pulled rows
  dbExecute(
    con_sh,
    "CREATE TABLE from_raw (
      sid INTEGER PRIMARY KEY AUTOINCREMENT,
      rawuid INTEGER,
      pushed_to_prod INTEGER DEFAULT 0
    );"
  )
  
  dbWriteTable(
    con_sh,
    "from_raw",
    data.frame(rawuid = 0),
    append = TRUE
  )
  
  # Keep track of errored rows
  dbExecute(
    con_sh,
    "CREATE TABLE needs_help (
      sid INTEGER PRIMARY KEY AUTOINCREMENT,
      rawuid INTEGER,
      err TEXT,
      fixed INTEGER DEFAULT 0
    );"
  )
  
  # Store gateway msgs
  dbExecute(
    con_sh,
    "CREATE TABLE water_gateway_data (
      sid INTEGER PRIMARY KEY AUTOINCREMENT,
      rawuid INTEGER,
      device_id INTEGER,
      firmware_version TEXT,
      project_id TEXT,
      gateway_serial_no TEXT,
      timestamp INTEGER,
      ts_up INTEGER,
      gw_batt_voltage REAL,
      gw_enclosure_temp REAL,
      gw_solar_current REAL,
      gw_solar_voltage REAL,
      device_name TEXT
    );"
  )
  
  # Store node device data
  dbExecute(
    con_sh,
    "CREATE TABLE water_node_data (
      sid INTEGER PRIMARY KEY AUTOINCREMENT,
      rawuid INTEGER,
      device_id INTEGER,
      firmware_version TEXT,
      project_id TEXT,
      node_serial_no TEXT,
      timestamp INTEGER,
      ts_up INTEGER,
      nd_batt_voltage REAL,
      nd_enclosure_temp REAL,
      nd_solar_current REAL,
      nd_solar_voltage REAL,
      signal_strength REAL
    );"
  )
  
  # Store node TDR data
  dbExecute(
    con_sh,
    "CREATE TABLE water_sensor_data (
      sid INTEGER PRIMARY KEY AUTOINCREMENT,
      rawuid INTEGER,
      device_id INTEGER,
      node_serial_no TEXT,
      timestamp INTEGER,
      ts_up INTEGER,
      tdr_sensor_id TEXT,
      tdr_address TEXT,
      center_depth REAL,
      vwc REAL,
      soil_temp REAL,
      permittivity REAL,
      ec_bulk REAL,
      ec_pore_water REAL
    );"
  )
  
  # Store node ambient data
  dbExecute(
    con_sh,
    "CREATE TABLE ambient_sensor_data (
      sid INTEGER PRIMARY KEY AUTOINCREMENT,
      rawuid INTEGER,
      device_id INTEGER,
      node_serial_no TEXT,
      timestamp INTEGER,
      ts_up INTEGER,
      rh_sensor_id TEXT,
      rh_address TEXT,
      rh_height REAL,
      t_amb REAL,
      rh REAL,
      temp_sensor_id TEXT,
      temp_address TEXT,
      temp_height REAL,
      t_lb REAL
    );"
  )
  
  # Store Hologram device data
  dbExecute(
    con_sh,
    "CREATE TABLE hologram_metadata (
      sid INTEGER PRIMARY KEY AUTOINCREMENT,
      rawuid INTEGER,
      device_name TEXT,
      device_id INTEGER,
      link_id TEXT,
      org_id TEXT,
      device_state TEXT,
      gateway_serial_no TEXT,
      time_begin INTEGER,
      time_end INTEGER
    );"
  )
  
  result <- etl_summarise_shadow(con_sh)
  
  dbDisconnect(con_sh)
  
  return(result)
}



etl_init_shadow_forms <- function(reset = F) {
  # each table should be a separate constructor function
  # each table should have `parsed_at` timestamp so we can record changes
  
  if (file.exists("./db/forms.db") && !reset) {
    stop("Forms database already exists. Need to reset?")
  }
  
  if (file.exists("./db/forms.db") && reset) {
    confirmation <- askYesNo(
      "Are you sure you want to clear the shadow forms DB?\n",
      FALSE
    )
    
    stopifnot(confirmation)
    
    file.create("./db/forms.db")
  }
  
  if (!file.exists("./db/forms.db")) {file.create("./db/forms.db")}
  
  
  con_sh <- etl_connect_shadow(dbname = "forms")
  
  # Keep track of errored rows
  dbExecute(
    con_sh,
    "CREATE TABLE needs_help (
      sid INTEGER PRIMARY KEY AUTOINCREMENT,
      rawuid INTEGER,
      target_tbl TEXT,
      err TEXT,
      fixed INTEGER DEFAULT 0
    );"
  )
  
  
  result <- etl_summarise_shadow(con_sh)
  dbDisconnect(con_sh)
  
  return(result)
}

etl_create_shadow_forms_wsensor_install <- function(reset = F) {
  if (!file.exists("./db/forms.db")) {
    stop("Forms database does not exist; first run `etl_init_shadow_forms()`")
  }
  
  con_sh <- etl_connect_shadow(dbname = "forms")
  tbls <- dbListTables(con_sh)
  existing <- "wsensor_install" %in% tbls
  
  if (reset && existing) {
    confirmation <- askYesNo(
      "Are you sure you want to clear the shadow forms DB?\n",
      FALSE
    )
    
    stopifnot(confirmation)
    dbRemoveTable(con_sh, "wsensor_install")
  }
  
  if (!reset && existing) {
    stop("Table already exists. Need to reset?")
  }
  
  
  # Keep track of errored rows
  dbExecute(
    con_sh,
    "CREATE TABLE wsensor_install (
      sid INTEGER PRIMARY KEY AUTOINCREMENT,
      rawuid INTEGER,
      parsed_at INTEGER,
      code TEXT,
      subplot INTEGER,
      gateway_serial_no TEXT,
      bare_node_serial_no TEXT,
      cover_node_serial_no TEXT,
      time_begin INTEGER,
      time_end INTEGER,
      notes TEXT,
      submitted_by TEXT,
      pushed_to_prod INTEGER DEFAULT 0
    );"
  )
  
  result <- etl_summarise_shadow(con_sh)
  dbDisconnect(con_sh)
  
  return(result)
}



# Execute initialization:
# etl_init_shadow_sensors()
# etl_init_shadow_forms()
# etl_create_shadow_forms_wsensor_install()




# library(RSQLite)
# con <- dbConnect(SQLite(), "dump_test.db")
# dbWriteTable(con, "iris", head(iris))
# system2(
#   "sqlite3", 
#   args = c("/Users/baits/Documents/R/dump_test.db", "'.dump iris'"), 
#   #"'.dump'"     # ".dump"     # "/".dump/""
#   stdout = TRUE
# )
