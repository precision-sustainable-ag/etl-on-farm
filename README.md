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

~~The next version will:~~

- ~~Call API from the Raw DB (MySQL server which does its own API calls)~~
  - ~~Keep track of which Raw rows have been parsed into Shadow DB~~
  - ~~We will have a table in Shadow for each table in Raw that only has a record of parsed-row `uid`s, so we can subset Raw using `>max(uid)` (`import_from_*`)~~
  - ~~We will have a table to do try-catch style errors (`try_again_*`) to store anything that has unexpected values~~
  - ~~That way all rows will be pulled only when new **or** when explicitly batched from the retry table~~
- ~~Extract raw strings from the response object in each row~~
- ~~Parse raw strings~~
- ~~Push into Shadow~~

~~Then the tech dashboard will use the `Publish` function to push from Shadow to Published DB.~~

  - ~~Except for `TST`, `DEV`, `DV1`, `TS1`, etc for testing (which will otherwise propagate through all functions)~~
  
----

## Major rewrite:

New architecture:

 1. Webhooks push to Raw DB (MySQL or Mongo)
 2. Parsing script pulls from Raw
  - We will have a table in Shadow DB (see below) for each table (assets/forms) in Raw that only has a column of parsed-row `uid`s and a column of assets, so we can subset Raw using `GROUP BY asset` + `>max(uid)` (`imported_from_*`)
  - We will have a table to do try-catch style errors (`try_again_*`) to store anything that has unexpected values
  - That way all rows will be pulled only when new **or** when explicitly batched from the retry table
 3. Extract raw strings from the response object in each row
 4. Parse raw strings
 5. Push into SQLite/flatfiles (Shadow DB)
 6. Collate Shadow into appropriate forms to mirror tables in Production DB
 7. Push row updates into Published, with `Validated = FALSE` column
 
Then, the Tech Dashboard will have the permissions for Shepherd users to "Publish" rows in the Production DB (change `Validated = TRUE`). Additionally the State Lead users will have the ability to Suggest Changes to data values, which the Shepherds can resolve in an issue tracker, which will edit the Production DB.

**Additional important note:** The `Export` CSV button in the Tech Dashboard needs to filter out rows matching `TST`, `DEV`, `DV1`, `TS1`, etc for testing. Power users who query the Production DB directly **MUST KNOW**:

 1. Ignore those testing rows
 2. Don't rely on "unpublished" rows (`Validated = FALSE`)
