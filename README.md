# Hologram Puller/Parser

----

Prototype for processing sensor data.

The first version:

- Called API from Hologram
- Stored device list
- Stored latest timestamps for each device
- Archived raw strings
- Parsed raw strings into tabular format
- Archived data as CSVs

The next version will:

- Call API from the Raw DB (MySQL server which does its own API calls)
  - Keep track of which Raw rows have been parsed into Shadow DB
  - We will have a table in Shadow for each table in Raw that only has a record of parsed-row `uid`s, so we can subset Raw using `>max(uid)`
- Extract raw strings from the response object in each row
- Parse raw strings
- Push into Shadow