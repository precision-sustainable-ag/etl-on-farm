# Sensor data

# Get list of uids from Shadow.imported_from_hologram
# Calculate max(uid)
# Query Raw.hologram where uid > our_max
# extract datafield
# parse JSON
# extract string, parse, decode, tabularize
# store in Shadow.hologram
# update Shadow.imported_from_hologram
# catch errors in Shadow.retry_hologram
# keep a list of Raw.uid and string
#   pass the Raw.uid along with parsed rows into Production (maybe???)
#   Then if batched flash data comes in at end of season, I can filter
#     for unseen strings, so I don't have to overwrite any rows in Production

library(DBI)
library(dplyr)
library(dbplyr)
raw_con <- dbConnect(
  RMariaDB::MariaDB(), 
  username = raw_user,
  password = raw_password,
  dbname = raw_dbname,
  host = raw_host,
  port = raw_port
  )


# library(RSQLite)
# con <- dbConnect(SQLite(), "dump_test.db")
# dbWriteTable(con, "iris", head(iris))
# system2("sqlite3", args = c("/Users/baits/Documents/R/dump_test.db", "\".dump\""))