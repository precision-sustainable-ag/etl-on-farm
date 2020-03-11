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
  - We will have a table in Shadow for each table in Raw that only has a record of parsed-row `uid`s, so we can subset Raw using `>max(uid)` (`import_from_*`)
  - We will have a table to do try-catch style errors (`try_again_*`) to store anything that has unexpected values
  - That way all rows will be pulled only when new **or** when explicitly batched from the retry table
- Extract raw strings from the response object in each row
- Parse raw strings
- Push into Shadow

Then the tech dashboard will use the `Publish` function to push from Shadow to Published DB.
  - Except for `TST`, `DEV`, `DV1`, `TS1`, etc for testing (which will otherwise propagate through all functions)