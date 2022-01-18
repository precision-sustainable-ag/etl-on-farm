quickly <- function(.f, timeout = 5, otherwise = NULL, quiet = FALSE) {
  # lifted from purrr::possibly
  fname <- deparse(rlang::expr({{.f}}))  
  # rlang::quo_text may die but looks nicer than deparse
  
  .f <- purrr::as_mapper(.f)
  force(otherwise)
  function(...) {
    tryCatch(
      R.utils::withTimeout(
        .f(...),
        timeout = timeout,
        onTimeout = "error"
      ), 
      TimeoutException = function(e) {
        if (!quiet) message("`", fname, "` took too long: >", timeout, "s")
      },
      error = function(e) {
        if (!quiet) message("Error: ", e$message)
        otherwise
      }, 
      interrupt = function(e) {
        stop("Terminated by user", call. = FALSE)
      })
  }
}

etl_connect_raw <- function(con = NULL) {
  if (!exists("raw_user")) {source("secret.R")}
  
  # if (!is.null(con)) {
  #   x <- purrr::safely(dbListTables)(con)
  #   if (is.null(x$error)) return(con)
  # }
  
  if (!is.null(con)) {
    x <- quickly(dbListTables)(con)
    if (!is.null(x)) return(con)
  }
  
  # raw_ip <- curl::nslookup(raw_host)
  
  dbConnect(
    RMariaDB::MariaDB(), 
    username = raw_user,
    password = raw_password,
    dbname = raw_dbname,
    host = raw_host, # raw_ip,
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
  
  pg_ip <- curl::nslookup(pg_host)
  
  
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
      tower_signal_strength INTEGER,
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
      ec_pore_water REAL,
      travel_time REAL
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

etl_init_shadow_stresscams <- function(reset = F) {
  if (file.exists("./db/stresscams.db") && !reset) {
    stop("Stresscam database already exists. Need to reset?")
  }
  
  if (file.exists("./db/stresscams.db") && reset) {
    confirmation <- askYesNo(
      "Are you sure you want to clear the shadow stresscams DB?\n",
      FALSE
    )
    
    stopifnot(confirmation)
    
    file.create("./db/stresscams.db")
  }
  
  if (!file.exists("./db/stresscams.db")) {file.create("./db/stresscams.db")}
  
  con_sh <- etl_connect_shadow(dbname = "stresscams")
  
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
  
  # Store stresscam info
  dbExecute(
    con_sh,
    "CREATE TABLE stresscam_data (
      sid INTEGER PRIMARY KEY AUTOINCREMENT,
      rawuid INTEGER,
      device_id INTEGER,
      dev_id_key TEXT,
      firmware_version TEXT,
      timestamp_utc TEXT,
      timestamp_local TEXT,
      timestamp_zone TEXT,
      ts_up TEXT,
      cpu_temp REAL,
      sd_free REAL,
      mode TEXT,
      crop TEXT,
      code TEXT,
      rep INTEGER,
      trt TEXT,
      file TEXT,
      -- P_WS_0 REAL,
      -- P_WS_1 REAL,
      -- P_WS_2 REAL,
      -- P_WS_3 REAL,
      -- P_WS_4 REAL,
      -- P_WS_5 REAL,
      probabilities TEXT,
      P_WS REAL
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
      pushed_to_prod INTEGER DEFAULT 0,
      bare_lat REAL,
      bare_lon REAL,
      cover_lat REAL,
      cover_lon REAL
    );"
  )
  
  result <- etl_summarise_shadow(con_sh)
  dbDisconnect(con_sh)
  
  return(result)
}


etl_create_shadow_forms_decomp_biomass_fresh__decomp_bag_pre_wt <- function(reset = F) {
  if (!file.exists("./db/forms.db")) {
    stop("Forms database does not exist; first run `etl_init_shadow_forms()`")
  }
  
  con_sh <- etl_connect_shadow(dbname = "forms")
  tbls <- dbListTables(con_sh)
  existing <- "decomp_biomass_fresh__decomp_bag_pre_wt" %in% tbls
  
  if (reset && existing) {
    confirmation <- askYesNo(
      "Are you sure you want to clear the shadow forms DB?\n",
      FALSE
    )
    
    stopifnot(confirmation)
    dbRemoveTable(con_sh, "decomp_biomass_fresh__decomp_bag_pre_wt")
  }
  
  if (!reset && existing) {
    stop("Table already exists. Need to reset?")
  }
  
  
  # Keep track of errored rows
  dbExecute(
    con_sh,
    "CREATE TABLE decomp_biomass_fresh__decomp_bag_pre_wt (
      sid INTEGER PRIMARY KEY AUTOINCREMENT,
      rawuid INTEGER,
      parsed_at INTEGER,
      code TEXT,
      subplot INTEGER,
      subsample TEXT,
      time INTEGER,
      empty_bag_wt REAL,
      notes TEXT,
      submitted_by TEXT,
      pushed_to_prod INTEGER DEFAULT 0
    );"
  )
  
  result <- etl_summarise_shadow(con_sh)
  dbDisconnect(con_sh)
  
  return(result)
}

etl_cron <- function(
  scriptfile, 
  clear = F, debug = F, 
  at = format(Sys.time(), "%H:%M"),
  freq = "hourly",
  where = getwd()
  ) {
  
  where <- tools::file_path_as_absolute(where)
  
  if (clear) {
    cronR::cron_rm(id = scriptfile, dry_run = debug)
  } else { 
    cmd <- cronR::cron_rscript(
      scriptfile,
      rscript_log = glue::glue(
        "{getwd()}/rscript_log/{scriptfile}.log"
      ),
      rscript_args = where,
      log_append = F
    )
    
    cronR::cron_add(
      cmd,
      frequency = freq,
      at = at,
      id = scriptfile,
      tags = c("ETL", "PSAOF"),
      dry_run = debug
    )
  }

}


etl_create_shadow_forms_decomp_biomass_dry__decomp_bag_dry_wt <- function(reset = F) {
  if (!file.exists("./db/forms.db")) {
    stop("Forms database does not exist; first run `etl_init_shadow_forms()`")
  }
  
  con_sh <- etl_connect_shadow(dbname = "forms")
  tbls <- dbListTables(con_sh)
  existing <- "decomp_biomass_dry__decomp_bag_dry_wt" %in% tbls
  
  if (reset && existing) {
    confirmation <- askYesNo(
      "Are you sure you want to clear the shadow forms DB?\n",
      FALSE
    )
    
    stopifnot(confirmation)
    dbRemoveTable(con_sh, "decomp_biomass_dry__decomp_bag_dry_wt")
  }
  
  if (!reset && existing) {
    stop("Table already exists. Need to reset?")
  }
  
  
  # Keep track of errored rows
  dbExecute(
    con_sh,
    "CREATE TABLE decomp_biomass_dry__decomp_bag_dry_wt (
      sid INTEGER PRIMARY KEY AUTOINCREMENT,
      rawuid INTEGER,
      parsed_at INTEGER,
      code TEXT,
      subplot INTEGER,
      subsample TEXT,
      time INTEGER,
      dry_biomass_wt REAL,
      notes TEXT,
      submitted_by TEXT,
      pushed_to_prod INTEGER DEFAULT 0
    );"
  )
  
  result <- etl_summarise_shadow(con_sh)
  dbDisconnect(con_sh)
  
  return(result)
}

etl_create_shadow_forms_decomp_biomass_dry__decomp_bag_collect <- function(reset = F) {
  if (!file.exists("./db/forms.db")) {
    stop("Forms database does not exist; first run `etl_init_shadow_forms()`")
  }
  
  con_sh <- etl_connect_shadow(dbname = "forms")
  tbls <- dbListTables(con_sh)
  existing <- "decomp_biomass_dry__decomp_bag_collect" %in% tbls
  
  if (reset && existing) {
    confirmation <- askYesNo(
      "Are you sure you want to clear the shadow forms DB?\n",
      FALSE
    )
    
    stopifnot(confirmation)
    dbRemoveTable(con_sh, "decomp_biomass_dry__decomp_bag_collect")
  }
  
  if (!reset && existing) {
    stop("Table already exists. Need to reset?")
  }
  
  
  # Keep track of errored rows
  dbExecute(
    con_sh,
    "CREATE TABLE decomp_biomass_dry__decomp_bag_collect (
      sid INTEGER PRIMARY KEY AUTOINCREMENT,
      rawuid INTEGER,
      parsed_at INTEGER,
      code TEXT,
      subplot INTEGER,
      subsample TEXT,
      time INTEGER,
      recovery_date INTEGER,
      notes TEXT,
      submitted_by TEXT,
      pushed_to_prod INTEGER DEFAULT 0
    );"
  )
  
  result <- etl_summarise_shadow(con_sh)
  dbDisconnect(con_sh)
  
  return(result)
}


etl_create_shadow_forms_biomass_in_field__biomass_decomp_bag <- function(reset = F) {
  if (!file.exists("./db/forms.db")) {
    stop("Forms database does not exist; first run `etl_init_shadow_forms()`")
  }
  
  con_sh <- etl_connect_shadow(dbname = "forms")
  tbls <- dbListTables(con_sh)
  existing <- "biomass_in_field__biomass_decomp_bag" %in% tbls
  
  if (reset && existing) {
    confirmation <- askYesNo(
      "Are you sure you want to clear the shadow forms DB?\n",
      FALSE
    )
    
    stopifnot(confirmation)
    dbRemoveTable(con_sh, "biomass_in_field__biomass_decomp_bag")
  }
  
  if (!reset && existing) {
    stop("Table already exists. Need to reset?")
  }
  
  
  # Keep track of errored rows
  dbExecute(
    con_sh,
    "CREATE TABLE biomass_in_field__biomass_decomp_bag (
      sid INTEGER PRIMARY KEY AUTOINCREMENT,
      rawuid INTEGER,
      parsed_at INTEGER,
      code TEXT,
      subplot INTEGER,
      fresh_wt_a REAL,
      fresh_wt_b REAL,
      bag_wt REAL,
      legumes_40 INTEGER,
      notes TEXT,
      submitted_by TEXT,
      pushed_to_prod INTEGER DEFAULT 0
    );"
  )
  
  result <- etl_summarise_shadow(con_sh)
  dbDisconnect(con_sh)
  
  return(result)
}


etl_create_shadow_forms_decomp_biomass_fresh__biomass_decomp_bag <- function(reset = F) {
  if (!file.exists("./db/forms.db")) {
    stop("Forms database does not exist; first run `etl_init_shadow_forms()`")
  }
  
  con_sh <- etl_connect_shadow(dbname = "forms")
  tbls <- dbListTables(con_sh)
  existing <- "decomp_biomass_fresh__biomass_decomp_bag" %in% tbls
  
  if (reset && existing) {
    confirmation <- askYesNo(
      "Are you sure you want to clear the shadow forms DB?\n",
      FALSE
    )
    
    stopifnot(confirmation)
    dbRemoveTable(con_sh, "decomp_biomass_fresh__biomass_decomp_bag")
  }
  
  if (!reset && existing) {
    stop("Table already exists. Need to reset?")
  }
  
  
  # Keep track of errored rows
  dbExecute(
    con_sh,
    "CREATE TABLE decomp_biomass_fresh__biomass_decomp_bag (
      sid INTEGER PRIMARY KEY AUTOINCREMENT,
      rawuid INTEGER,
      parsed_at INTEGER,
      code TEXT,
      subplot INTEGER,
      subsample TEXT,
      time INTEGER,
      fresh_biomass_wt REAL,
      notes TEXT,
      submitted_by TEXT,
      pushed_to_prod INTEGER DEFAULT 0
    );"
  )
  
  result <- etl_summarise_shadow(con_sh)
  dbDisconnect(con_sh)
  
  return(result)
}



# SETUP PROJECT: ----
# -- Install libraries:
#   - First entry only needed on dev machine, not server
# renv::init()
#   - On every commit run:
# renv::snapshot()
#   - On clone run:
# renv::restore()

# -- Create untracked dirs
# if (!dir.exists("./log")) dir.create("./log")
# if (!dir.exists("./db")) dir.create("./db")
# if (!dir.exists("./rscript_log")) dir.create("./rscript_log")


# -- Execute initialization:
# etl_init_shadow_sensors()
# etl_init_shadow_stresscams()
# etl_init_shadow_forms()
# etl_create_shadow_forms_wsensor_install()
# etl_create_shadow_forms_decomp_biomass_fresh__decomp_bag_pre_wt()
# etl_create_shadow_forms_decomp_biomass_dry__decomp_bag_dry_wt()
# etl_create_shadow_forms_decomp_biomass_dry__decomp_bag_collect()
# etl_create_shadow_forms_biomass_in_field__biomass_decomp_bag()
# etl_create_shadow_forms_decomp_biomass_fresh__biomass_decomp_bag()



# -- Spin up cron jobs:
#  - Run once after cloning into server 
#  - (don't forget to make log dirs)
# etl_cron("raw_to_shadow_hologram.R", at = "19:09")
# etl_cron("shadow_to_prod_hologram.R", at = "19:14")
# etl_cron("raw_to_shadow_stresscams.R", at = "19:19")
# etl_cron("shadow_to_prod_stresscams.R", at = "19:24")
# etl_cron("raw_to_shadow_kobo.R", at = "19:19")
# etl_cron("shadow_to_prod_kobo.R", at = "19:24")
# etl_cron("shadow_backup.R", at = "20:29", freq = "daily") 
# etl_cron("prefill_rows.R", at = "19:29")

#  - To remove jobs later:
# cronR::cron_ls()
# cronR::cron_njobs()
# etl_cron("shadow_backup.R", clear = T)
#  - To clear all:
# cronR::cron_clear()

