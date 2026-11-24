### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Apps, sheets and charts

Each app is ingested both as a container, which holds its tables, sheets and charts, and as a dashboard with the `App` subtype. The app dashboard links to the app's sheets, and to the app's tables that no chart reads, so lineage runs from every table to the app, through a chart and its sheet or directly.

- In a published app, users' private sheets are skipped; base and community sheets are ingested.
- Charts inside a container object's tabs are ingested as charts of the sheet. An untitled chart takes its tab label as its title.
- Objects with no data, such as buttons, filter panes and variable inputs, are skipped.
- On-prem, with `ingest_unpublished_apps: true` in the `on_prem` block, apps not published to a stream are ingested under a stream named `Unpublished`. `space_pattern` filters it like any other stream, so an allow list has to include `Unpublished` to ingest them.
- On-prem, owners are ingested by their Qlik user id, which for Windows directories is the account name, so they join with users ingested from Active Directory or LDAP by username.

To keep only apps and their tables, set `include_sheets_and_charts: false`. Apps are then read without their sheets, which is faster, and each app dashboard links to all of its tables. With stale metadata removal enabled, the sheets and charts an earlier run ingested are soft-deleted on the next run. They are usually most of the entities a run ingests, so dropping them can exceed `stateful_ingestion.fail_safe_threshold` (75% by default), and the run then skips stale removal and reports a failure. Raise the threshold to 100 for the first run with the option off, and set it back afterwards.

#### Lineage from load scripts

On-prem, the connector always reads each app's load script. On Qlik Cloud it reads the script when the app's data model has no tables, or when some of its tables have no upstream lineage, for example apps loaded through the data load editor. Tables already traced by the data model are not added again from the script.

A `SELECT` that runs after `LIB CONNECT TO`, `ODBC CONNECT TO` or `OLEDB CONNECT TO` becomes a Qlik table with lineage to the tables it reads, as long as its data connection resolves to a platform and the SQL can be parsed. An unlabelled `SELECT` table is named after the table that follows `FROM`, cut to 32 characters, as Qlik names it. `--` comments in SQL are ignored.

##### Data connections

The upstream platform and database come from the data connection:

- A connection named in `LIB CONNECT TO` resolves through `data_connection_to_platform_instance`, or through a data connection the connector can list, whose platform comes from its connector type or connection string. The platform is never guessed from the connection name, so an unlisted name without a mapping does not resolve.
- On Qlik Cloud, a space-prefixed name resolves to the connection in that space. A name without a space prefix resolves only to the connection with that name in the app's own space (the personal space for personal apps), even when another space holds a connection with that name. Map the name in `data_connection_to_platform_instance` when the app's own connection is not visible to the API key.
- Connection names and the keys of `data_connection_to_platform_instance` and `file_connection_to_storage` match in any case, as Qlik matches them. The exact spelling is tried first; a spelling in another case matches only when a single connection or key has it.
- An inline `ODBC CONNECT TO` or `OLEDB CONNECT TO` connection string resolves from its driver or provider. Only the properties that name where the data lives (driver, provider, server, host, port, data source, database, catalog, schema, DSN, warehouse and account) are kept when an inline connection string is reported or ingested, and URLs in them lose their user name, password and query string.

Use `data_connection_to_platform_instance` when the platform or database cannot be inferred, or when the upstream source uses a platform instance or env. Set `database` and `default_schema` for tables the SQL doesn't qualify:

```yaml
data_connection_to_platform_instance:
  "My SQL Server connection":
    platform: mssql
    database: sales_db
    default_schema: dbo
  "My Databricks connection":
    platform: databricks
    database: main
```

On SQL Server, Snowflake, PostgreSQL, Redshift, Databricks, BigQuery, MySQL, MariaDB and Oracle, a table whose database or schema is neither in the SQL nor resolved for its connection is left out of lineage and reported as a warning.

##### Script variables

Variables set with `SET`, or with `LET` and a quoted string, are expanded in connection names and SQL from where the script sets them. A variable the visible script doesn't set, such as one computed with `LET` from an expression or set in a script pulled in with `$(Include=...)` or `$(Must_Include=...)`, takes the value the app's last reload left it, and otherwise the value given in `script_variables`. An app value that starts with `=` is an expression and is not used, so a `script_variables` value for the same name is used instead.

The app only keeps a variable's last value, so these variables stay unknown, even after an include:

- the variable of a `FOR` or `FOR EACH` loop, inside the loop
- a variable set with `LET` from an expression inside a loop or an `IF` or `SWITCH` branch, until the loop or block ends (see below)
- a variable set with `LET` more than once, including in a `SUB` called more than once
- a variable whose `IF` branches disagree, until the block and any loop or block around it ends
- a `SUB` parameter the script gives no literal value or sets with `LET` from an expression
- a `SET` or `LET` that copies or reads one of these

After an `IF` or `SWITCH` block that is not inside a loop or another block, a variable it set, or one known before it that an include in one branch could change, takes the app's value once the script does not set it again, as after a loop. After a block or a loop, if the app doesn't have that variable, it stays unknown, and neither its `script_variables` value nor an empty value is used. A `SUB` parameter is local to the call, so it never takes the app's value.

Use `script_variables` for variables neither the app nor its visible script has, for example one set to nothing after the reload, which deletes it, or a date-based folder. They're used in QVD and file paths, connection names, SQL and chart expressions:

```yaml
script_variables:
  vSchema: sales
  vMonth: "2026-10"
```

##### Control flow

Conditions are not evaluated:

- A connection set inside an `IF` or `SWITCH` block is kept after the block only when every branch sets the same one. A variable keeps the value every branch gives it, otherwise taking the app's value as described above. An `IF` without `ELSE` or a `SWITCH` without `DEFAULT` can run no branch, so the value from before the block must match as well.
- A `SUB` body is read where the script calls it, with the connection and variables in effect at that `CALL`. A `SUB` that is never called adds no tables. Quoted or numeric arguments of a `CALL` are bound to the `SUB` parameters for that call, and any other argument leaves its parameter unknown.
- A `CALL` of a `SUB` the script does not define makes the connection and all variables unknown, as an [included script that is not read](#included-scripts-qlik-cloud) does. After either, the table loaded last is unknown too, so a `Concatenate` or `Join` without a table name is skipped until another table is loaded.
- A `SELECT` that runs while the connection is unknown is skipped.
- Nothing after an unconditional `EXIT SCRIPT` is read.
- Rows added with `Concatenate (Table)` or fields added with `Join (Table)` belong to the named table, and without a table name to the table loaded just before.

##### Included scripts (Qlik Cloud)

A script pulled in with `$(Include=...)` or `$(Must_Include=...)` is read into the app's script where it is included, together with the scripts it includes. It is read when:

- its path is a `lib://` path that the app's variables expand. Those hold the values the app's last reload left, over any `script_variables` in the recipe; an empty value or an `=` expression keeps the recipe's value. A path that uses a variable the script sets more than once, or inside an `IF`, `SWITCH`, loop or `SUB`, or that is a loop variable or `SUB` parameter, is not read.
- the API key's user has **Can view** in the space that holds the file.
- it is nested no more than 10 levels deep, and does not include itself directly or through another included script.
- the app's script with everything it includes stays under 5,000,000 characters.

On-prem, included scripts are not read. An included script that is not read makes the connection and all variables unknown after the include, and the tables, QVDs and files it loads get no lineage from the script. On Qlik Cloud, a file in such an app's lineage graph that its own script does not load is only added to the table's lineage, never removed.

##### Fields and column-level lineage

A `LOAD` stacked on a `SELECT` (a preceding load) gives the table its field names: `amount AS [Net Sales]` becomes field `Net Sales` with column lineage from `amount`, and an expression such as `Year(order_date) AS Year` gets column lineage from every column it reads.

- `LOAD *` keeps the `SELECT` columns. So does a preceding load with a field list item that is a `$(variable)` on its own, aliased or not (such as `LOAD $(vFields)` or `$(vField) AS Name`), or whose field name is a `$(variable)` (such as `amount AS [$(vName)]`), since the variable may hold any of them. A field aliased from such a variable, `Name` here, gets no column lineage, and a field named by a variable is left out. The fields the load does name are still added.
- A variable inside a field's expression, as in `amount * $(vRate) AS Net`, only changes that field's value.
- Unquoted field names may start with `%`, as Qlik key fields often do.
- A field from an expression without `AS` is left out, since Qlik names it after the expression text.
- Source columns are matched by exact name first and then in any case, since the SQL parser may change their case. When two columns differ only in case, a name in a third case gets no column lineage.
- A `CrossTable` load keeps column lineage for its qualifier fields (the first fields, one unless its third argument says otherwise). Its data field gets lineage from the columns it turns into rows, and its attribute field, which holds column names, gets none. An unaliased expression in its field list still counts among the fields, so the qualifiers are counted correctly, but it is not ingested as a field.
- A `Generic` load, or a `CrossTable` whose field list is not known (such as `LOAD *`), gets table lineage only.

#### Lineage through QVDs and other Qlik datasets (Qlik Cloud)

When an app's data model has no tables (the app is listed under `apps_without_load_model` in the report), the connector reads the app's tables from Qlik Cloud's lineage graph. Each table is ingested as `{app id}.{table name}`, the same name a load script table gets, and links upstream to the Qlik datasets it loads, such as QVD files, and to the app tables it loads from, including another app's tables, as a `BINARY` load reads them, with column lineage between their fields.

A table of an app the run does not ingest is left out. A `BINARY` load of an app that has a data model links to a data model table of that app when the table's name matches exactly one table in the source app's lineage graph. A lineage graph table that matches no data model table is reported in a warning with the app ID and the table names; lineage from it, by a load or a `STORE`, is only added, and edges from earlier runs are kept. A table that the load script parser also finds with a `SELECT` is ingested once, with the upstreams from both. The tables' fields and their types come from the app's data model in the Engine, as its last reload left them. A load script table whose name the parser can't read from its statement, such as an unlabelled `SELECT` from a subquery or a `SELECT` labelled with a variable it can't expand (`[$(vTable)]:`), takes the name of the one data model table with exactly its columns; otherwise it is ingested as `table_<n>`. Label such loads in the script to name them for certain. A `SELECT` with an unbracketed variable label it can't expand (`$(vTable): SQL SELECT ...`) is not ingested.

Each Qlik dataset links upstream to the app tables that `STORE` into it, so lineage runs from an ETL app, through the QVDs it writes, to the apps that load them. QVD datasets have the `QVD` subtype and are named without the `.qvd` extension; the dataset URN still holds the file name. Script apps (data preparation scripts) are read like analytics apps, and the QVDs they store into are linked to their tables.

- Only datasets in spaces allowed by `space_pattern`, and visible to the API key, are ingested as Qlik datasets. Give the API key's user access to the spaces that hold the QVDs to see the whole chain.
- A Qlik dataset that an app loads but the API key can't list, such as a QVD in a space its user can't see, is named from the app's upstream lineage graph. So is a file the load script can't name from its storage or by its connection and path (see [Lineage from files on cloud storage](#lineage-from-files-on-azure-storage-amazon-s3-and-google-cloud-storage-qlik-cloud)). It is ingested as a Qlik Sense dataset named by Qlik's label for it, without a schema, with the Qlik ID (QRI) in its URN, and links upstream to the tables of the apps Qlik saw store into it, when the run ingests those apps. These edges are only added, never removed; remove an edge in DataHub when an app no longer stores into the dataset. The graph is read, in two requests, only for apps that load a dataset or file the run can't otherwise name. A dataset listed in a space that `space_pattern` leaves out stays out. A dataset or file that only the field level of the graph names is not named. Once its space or storage is visible, the dataset gets its usual URN and the QRI-named one is no longer emitted.
- With `include_column_lineage` (on by default), the connector also reads the field level of the lineage graph for each table and for each dataset an app stores into. This adds column-level lineage between app tables and datasets, and gives a table the script drops after storing it its field names. It costs one request per table. A field loaded under a new name is linked only when its source field's name is known this run.

A table's lineage is replaced each run, as its app owns it. When something it loads could not be read this run, such as the list of datasets or the app's load script, the lineage read this run is added to the table's existing lineage instead, and the run reports a warning.

A dataset is not built by one app, so only its edges from Qlik app tables are replaced: the connector reads the dataset's current lineage from DataHub, keeps every other edge, such as lineage added in DataHub, and drops edges from app tables that no longer store into it. When a storing app could not be read, or DataHub's current lineage can't be read, the storing tables are only added. If Qlik Cloud has no lineage graph for an app whose last read had tables, the app is treated as partly read rather than as having lost its tables.

#### Lineage from files on Azure Storage, Amazon S3 and Google Cloud Storage (Qlik Cloud)

Qlik Cloud's lineage graph names each file an app loads from an Azure Storage, Amazon S3 or Google Cloud Storage data connection by a hash of the connection's container or bucket and of the file's path, so the file itself can't be read from the graph. The connector links such a file to its dataset in DataHub (`abs`, `s3` or `gcs`) when the app's load script reads it with `LOAD ... FROM [lib://<connection>/<path>]`, and the hash of that path is the one in the graph. This applies to apps whose tables come from the lineage graph, listed under `apps_without_load_model`.

- Paths built from variables are tried both as the script's own `SET` and `LET` statements expand them and with the app's variables as its last reload left them, including those set in an included script, since Qlik's lineage graph records the paths of the last reload.
- Mapping loads and loads that add to another table count too.
- Files are also resolved when the list of Qlik datasets could not be read.

The connector also needs to know where the connection's files are. It reads the container or bucket from the data connection when the API key's user can see it (**Can consume data** in the connection's space), and then also checks it against the graph. Otherwise, set it in `file_connection_to_storage`, keyed by the connection name as load scripts write it, with the `platform_instance` and `env` of the source that ingests the storage so the URNs join:

```yaml
file_connection_to_storage:
  "Sales Space:Lake":
    storage_uri: abfss://landing@salesaccount.dfs.core.windows.net/qlik
    platform_instance: sales_lake
```

`storage_uri` takes an `abfss://<container>@<account>.dfs.core.windows.net/<folder>` (or `abfs://`, or Azure HTTPS) URI that names the container, an `s3://<bucket>/<folder>` URI or a `gs://<bucket>/<folder>` URI. The scheme may be in any case.

A file is named `<container or bucket>/<folder>/<path>`, as the `abs`, `s3` and `gcs` sources name a file when their `path_specs` ingest files individually. An Azure file's name does not include its storage account, so two accounts with a container of the same name give the same URN; with several storage accounts, set `platform_instance` per connection in `file_connection_to_storage`, matching the `abs` recipe of each account. Names keep the case of the path the script reads; set `convert_urns_to_lowercase: true` on a mapping when the storage source is run with that option.

When neither is known, a file on the connection is ingested as a Qlik Sense dataset, so the apps that store it and the apps that load it still join through it, but it does not join the storage's own dataset. It is named by space, connection and path: a connection the script names without its space (`lib://<connection>/<path>`) is the app's own space's, as Qlik Cloud resolves it, so it gets the same dataset as `lib://<space>:<connection>/<path>`. Its URN is `<space>:<connection>/<path>` in lowercase, with the path normalised (no leading, trailing or doubled slashes, and `/` for `\`). Its name is the file name (without `.qvd` for a QVD). Its qualified name is `lib://<space>:<connection>/<path>` with that path; when apps spell it in different case, the spelling that sorts first is used, so it is the same every run. It sits in its space (or, when the run doesn't ingest that space, under the space's name) under its connection and folders, and it has subtype `QVD` for a `.qvd` file and `Qlik Dataset` otherwise, with no schema. As the URN holds the space name, renaming a space gives its files new datasets, and documentation, tags and other metadata added in DataHub to the old ones are not carried over; stale-metadata removal drops the old ones. Such a file is only ingested when an app's lineage graph reads it in the same run; a file that apps only `STORE` into may never have been written, so it is left out and listed under `unread_stored_files` in the report. Edges from the apps that store into it are only added. Once the storage is mapped, the file links to that dataset instead, and stale-metadata removal drops the Qlik Sense one. Files on a connection named without its space in an app in a personal space, and on a space's `DataFiles` connection, which holds Qlik datasets, are not named this way. A file on such a connection that an app loads is named from the app's lineage graph instead (see [Lineage through QVDs and other Qlik datasets](#lineage-through-qvds-and-other-qlik-datasets-qlik-cloud)), and a `STORE` into it is not linked from the script. When the data connections could not be listed, no file is named either way, as the connection may be one whose storage the list would give; the table keeps the file lineage of the last run.

A file is linked only when one connection could hold it:

- When the script reads the same path through another connection whose storage is not known, and the file can't be named by its space and connection (a connection named without its space in a personal-space app), the file is not named from the script; it may be named from the app's lineage graph as above.
- When two connections known only from `storage_uri` could hold it, such as the prod and dev branches of an `IF`, the one whose Azure container path or Google Cloud Storage bucket matches the file's QRI is linked, and otherwise neither is.
- When the data connections or an app's variables could not be read, files that are not resolved keep their lineage from the last run.

The other way, a file an app writes with `STORE <table> INTO [lib://<connection>/<path>]` onto a connection whose storage is known gets the stored table as an upstream, with column lineage for the fields it writes into text and Parquet files (as the `STORE` format says, whatever the file is named), so an app that loads the file later links through it. QVD, text (`(txt)`) and Parquet (`(parquet)`) stores are linked; a `STORE` with no format writes a QVD. The `abs`, `s3` and `gcs` sources don't ingest QVD files, so a stored QVD's dataset only carries this table-level lineage and joins the Qlik apps that load it; text and Parquet files join the datasets those sources ingest. A file Qlik catalogues as a dataset in a space the recipe ingests is not linked on storage, as apps that load it link to that Qlik dataset, which already gets the stored table from its own lineage graph; while the Qlik datasets can't be listed, stored files are not linked. Stores are only linked from tables the app ingests, and only from apps whose load script the connector reads (see [Lineage from load scripts](#lineage-from-load-scripts)). When the script stores the same table into different files from different branches of an `IF` or `SWITCH`, such as a prod and a dev path, which file it wrote can't be told, so none of them is linked and the stores are listed under `qvd_stores_in_competing_branches` in the report; two names of one connection, such as with and without its space, write the same file. Other apps may store into the same file, so the connector only replaces the app's own edges: it reads the file's current lineage from DataHub and drops edges from the app's tables that it no longer stores into the file, including files it stored into in the last run, which the app's checkpoint records when stateful ingestion is on. These edges are written at the end of the run, and an edge from a table another app also has, such as a table both load from one source, is never dropped, as that app may be what stores it; while some app's tables or part of the run could not be read, no edges are dropped. When an app's stores aren't all known this run, because its load script, its variables or the data connections could not be read, a store's path or connection can't be resolved, the script includes another script, or stores compete, its edges are only added. `file_stores_linked` in the report counts the linked stores. An on-prem recipe that maps the same storage in `file_connection_to_storage` gives its QVDs the same datasets, so lineage joins across the two (see [QVD paths](#qvd-paths)).

#### Lineage through QVDs (on-prem)

On Qlik Sense Enterprise on Windows, QVD lineage comes from the load scripts. A table loaded with `LOAD ... FROM <file> (qvd)` links upstream to that QVD, and a QVD written with `STORE <table> INTO <file>` links upstream to the stored table, so lineage runs from an ETL app, through the QVDs it writes, to the apps that load them. Each QVD is a dataset with the `QVD` subtype, named after its file without the extension.

Each QVD is placed in `Folder` containers that follow its path, one per server, share or drive and folder (`nas` / `qvd` / `raw` for `\\nas\qvd\raw\orders.qvd`), as the S3, ABS and GCS sources place files, so QVDs are browsed by where they are stored.

A QVD's schema comes from the table stored into it, or, when no app stores it, from the fields apps load from it. Column-level lineage follows the fields of each `LOAD` and `STORE` field list; a `LOAD *` leaves the table's fields unknown.

##### QVD paths

A QVD is identified by its file path. A `lib://` path is resolved through the folder of its data connection, and file system paths, which only legacy mode allows, are used as they are. Paths are matched without regard to case or slash direction, so two connections to the same folder find the same QVD. When apps reach one share by different names, such as a short and a fully qualified host name, map them with `qvd_folder_aliases`:

```yaml
on_prem:
  qvd_folder_aliases:
    "//qlik-sense/qvd": "//qlik-sense.corp.local/qvd"
```

A connection that is not a folder connection, such as an Azure Storage or Amazon S3 connection, has no folder in Qlik, so its QVDs are unresolved. Map it in `file_connection_to_storage` with the `storage_uri` of the container or bucket folder it points at. Its QVDs then take the `abs`, `s3` or `gcs` dataset that Qlik Cloud gives the same file, `<container or bucket>/<folder>/<path>`, so lineage joins across a Qlik Cloud and an on-prem recipe that map the same storage. The connector writes only table-level lineage to and from these files, as they have no schema in DataHub, and never removes them as stale, as they are not Qlik datasets. When an app read in a run no longer stores into such a file, its edge into the file is removed. A folder connection listed here takes the storage dataset instead of a `qlik-sense` QVD dataset. `storage_qvds_resolved` in the report counts the QVDs linked this way, once for each app that uses them:

```yaml
file_connection_to_storage:
  "Lake - Azure":
    storage_uri: abfss://qlik@lakeaccount.dfs.core.windows.net/qvd
    convert_urns_to_lowercase: true
```

Set `convert_urns_to_lowercase` as for [Qlik Cloud files](#lineage-from-files-on-azure-storage-amazon-s3-and-google-cloud-storage-qlik-cloud), the same way in both recipes.

- Variables in a QVD path, including those set with `LET` or in an included script such as a connection prefix, are expanded with the app's variables as its last reload left them.
- A `lib://` path may separate the connection from the path with a backslash.
- As in Qlik, a variable the app doesn't have expands to nothing in a connection name, such as an empty connection prefix in `lib://$(vPrefix)QVD Data/`, but only when what is left names a folder or storage connection and the visible script doesn't set the variable in `IF` or `SWITCH` branches that disagree.
- Quotes a `SET` variable carries into a path are dropped.
- Variables that stay unknown in load scripts (see [Script variables](#script-variables)) are not expanded in paths either. After a loop, a variable it set takes the app's value again, or stays unknown if the app doesn't have it, and a `SUB` parameter stays unknown until the call ends. Assignments in comments, in a `SUB` that is never called and after `EXIT SCRIPT` are not counted.

QVDs of the connections in `qvd_ignored_connections` (by default the monitoring apps' `ServerLogFolder` and `ArchivedLogsFolder`) and the Engine's `BUFFER` QVDs are not ingested. When an app's load script can't be read, the QVDs it writes are unknown, so QVD schemas and lineage are kept from the last run.

#### App data models (on-prem)

On-prem, the connector opens each app without its data by default, so table fields come only from the `SELECT` and `LOAD` field lists in the load scripts, and fields have no types. Set `read_data_model: true` in the `on_prem` block to open each app with its data and read its data model instead. Every table then gets the fields and types the last reload left in it, including tables loaded with `LOAD *` and from QVDs, and charts link to exactly the tables and fields they use.

This is much slower, about a minute per large app, and reads one app at a time unless `max_workers` is set. The Engine keeps an app it opened with its data in memory until the engine's app cache time, set in the QMC, has passed since its last session, as it does for an app users open ([Qlik support article](https://community.qlik.com/t5/Official-Support-Articles/Why-does-the-Qlik-Sense-Memory-usage-remain-high/ta-p/1805698)). Schedule such runs outside peak hours. When opening an app with its data is denied, as section access can deny it to the service user, or times out while the data loads, the app is read without its data instead and listed in the run's "Qlik apps read without their data" warning. With stateful ingestion, an app with a `BINARY` statement whose last read ingested BINARY tables then also counts as partly read (see [Lineage through BINARY loads](#lineage-through-binary-loads-on-prem)).

#### Lineage through BINARY loads (on-prem)

A `BINARY` statement loads another app's data model into an app. On Qlik Sense Enterprise it loads from a `.qvf` file, not from an app id ([Qlik help](https://help.qlik.com/en-US/sense/November2025/Subsystems/Hub/Content/Sense_Hub/Scripting/ScriptRegularStatements/Binary.htm)), so the file doesn't say which app it holds. The tables it brings in are the tables of the app's data model that the load script doesn't create itself, so BINARY lineage needs `read_data_model: true`. A table the script renames another table to (`RENAME TABLE`) is the script's. Qlik names a table `Orders-1` when the script loads a table under a name already taken, but a real table can have the same form, such as `Orders-2024`. A table named like that, whose name before the last `-<number>` is a table the script loads, is the file's only when the source app's data model has a table of exactly that name; otherwise it gets no lineage and is listed in `binary_loads_without_lineage`. Each of the other tables is ingested as an app table and linked upstream:

- To the tables of the same name of the app the file was exported from, when `binary_file_to_app` maps the file to that app's id, with column-level lineage between their fields.
  - Only tables that app ingests are linked, and only when the run reads it or keeps it from its last read. An app only ingests tables it loads with SQL, from a QVD or with `BINARY`, or stores into a QVD, so a table it builds with `RESIDENT` or `INLINE` or from another file is not linked.
  - A table the source app's data model doesn't have is the script's own, and gets the lineage of its own loads only. With an include or a `SUB` the script doesn't define, that lineage is only added to.
  - A table the script also loads into under the same name is still linked. It is not linked when the script drops it with `DROP TABLE` first, or loads it with a `Replace` prefix, which in a full reload drops the table and loads it again.
  - When the script drops or replaces a table it doesn't name, such as an unlabelled `Replace LOAD` or `DROP TABLE $(vTable)` with a variable the connector can't expand, no table is linked, because any of them could be that table.
  - When the script includes another script with `$(Include)` or `$(Must_Include)`, or calls a `SUB` it doesn't define, the tables are still linked, but their lineage is only added to, never replaced. The included script isn't read and could drop or reload a table. The run reports this as "Qlik BINARY tables may be changed by an included script".
  - If the source app isn't read or kept, or has the table but doesn't ingest it, the table's lineage from earlier runs is kept and only added to. The same holds for column lineage when the source app's data model can't be read and its last read didn't record its fields.
- Otherwise, to the file. On a connection mapped in `file_connection_to_storage`, that is the storage's dataset, such as an `abs` or `s3` file. On a folder connection or a file system path, it is a dataset with the `Qlik App File` subtype, named and placed in folders as a QVD is (see [QVD paths](#qvd-paths)).
  - The file's tables are not known here, so a table the script also loads under the same name is not linked.
  - When the script also creates tables it doesn't name, none are ingested, because any data model table could be one of those. Such tables come from an unlabelled `LOAD`, a `Generic` load, or a table labelled with a variable the connector can't expand (`$(vTable): LOAD ...`). The same applies when the script includes another script or calls a `SUB` it doesn't define, because the file's tables can't be told apart from tables the included script creates. Map the file in `binary_file_to_app` instead.

A BINARY load listed in [`binary_loads_without_lineage`](#binary_loads_without_lineage-in-the-report-on-prem) for one of the first six reasons there ingests none of its tables.

```yaml
on_prem:
  read_data_model: true
  binary_file_to_app:
    "lib://Apps/Sales Model.qvf": "<id of the Sales Model app>"
```

A `binary_file_to_app` path matches the path as the script names it, or the file path it resolves to, ignoring case and slash direction, and app ids match whatever their case. BINARY loads that get no lineage are listed in `binary_loads_without_lineage` in the report, with the reason.

With stateful ingestion and `read_data_model: true`, when an app's data model can't be loaded, or a BINARY path uses a variable and the app's variables can't be read, and the app's last read ingested BINARY tables, the app counts as partly read (see [Partly read apps](#partly-read-apps)), so its last read is kept rather than its BINARY tables removed. After `max_partial_reads_kept` such runs, the app is ingested without its BINARY tables, which stale metadata removal then soft-deletes, and the run reports "Qlik BINARY tables are no longer kept" as a warning, not a failure. An app whose data model was never read, for example because section access denied its data from the first run, is ingested without BINARY tables and is not partly read.

#### Chart input fields

Each chart lists the Qlik table fields it reads as input fields, and its inputs are narrowed to the tables those fields belong to. Fields are taken from the chart's dimensions and measure expressions, including master measures. A field shared by several tables, such as a key field, is listed for each of them.

- A name in an expression counts only when it matches a field of one of the app's ingested tables. Names match case-sensitively, as Qlik field names do, except for the columns of tables traced from a load script, whose case the SQL parser may change. For those tables, the fields are the columns the SQL parser found, under the names a preceding load gives them.
- Tables loaded with `MAPPING` are only used by mapping operations such as `ApplyMap`, `Map ... Using`, `Rename Field` and `MapSubstring`, and are never chart inputs.
- A derived field such as `Date.autoCalendar.Year` or `[Date].autoCalendar.Year` counts as its base field `Date`, unless a field named exactly `Date.autoCalendar.Year` exists, which then takes precedence. When neither is known, the chart keeps all of the app's tables as inputs.
- Values in set modifiers, such as `"Net*"` and `"East"` in `{<Region={"Net*"}+{"East"}>}`, are search strings and are not matched.
- A `$(variable)` is expanded with the app variable's definition as the app was last saved or reloaded, including variables set in included scripts, so a chart whose dimension is `$(vGrain)` reads the field the variable names. A variable a user changes through a variable input or button is read with its saved value.
- Comments in expressions are ignored.

A chart keeps all of the app's tables as inputs when a measure cannot be read, or when an expression uses a variable whose definition starts with `=` or that the app does not have. A chart that reads no field of the app, such as a text object or a chart whose fields were dropped by the load script, has no input tables. A chart whose measures could not be read this time keeps the input fields from its last read, while a chart that uses a deleted master measure lists the fields its other dimensions and measures read.

#### Large sites

##### Parallel reads and rate limits

The Engine loads each app's data when the connector opens the app, on Qlik Cloud and on-prem unless `read_data_model` is false, so every worker holds an app in memory. Raise `max_workers` with care.

- `max_workers` sets how many apps and datasets are read at the same time, across all spaces and streams. It defaults to 1, and to 4 on-prem when `read_data_model` is false. Each worker holds its own Engine session under the configured user.
- On Qlik Cloud, a rate-limited (HTTP 429) REST request is retried up to 5 times. Each retry waits for the `Retry-After` time when Qlik sends one, and otherwise backs off for 0, 2, 4, 8 and 16 seconds. A request made while listing or reading a space, item or app that is still rate-limited after that fails, and that space, item or app is reported as unread or partly read, which keeps stale metadata removal from deleting its metadata.
- On Qlik Cloud, a rate-limited Engine connection is not retried and leaves its app unread.
- On-prem, while the Engine refuses connections, as it does while its service restarts, app reads retry every 15 seconds until the Engine has refused them for 5 minutes in a row. After that, each app whose connection is refused is reported as partly read straight away, until a connection attempt ends in anything other than a refusal, which starts a new 5 minutes. The run reports the longest time the Engine refused connections as an info entry.
- Only refusals are waited for this way. When the Engine host is down or a firewall drops the connection, each app read tries to connect 3 times, waiting up to `timeout_seconds` each time, before the app is reported as partly read.

##### Partly read apps

With stateful ingestion and stale metadata removal enabled, each run records which entities every app produced.

- When an app is only partly read and its last read was recorded, nothing from the partial read is emitted and the run reports a warning. The app's metadata stays as of its last complete read until a read of it completes.
- This lasts for at most as many consecutive runs as the top-level `max_partial_reads_kept` option sets (3 by default). After that, the partial read is ingested so the app's current state appears, and the run reports a failure, which skips stale metadata removal for that run. A complete read resets the count.
- When its last read was not recorded, for example on the first run or when the last checkpoint could not be saved, the partial read is emitted and the run reports a failure, which keeps stale metadata removal from deleting anything in that run.
- A failed run keeps the recorded last read of the apps it did not reach.
- A complete read records only what it produced, so sheets and charts removed in Qlik are soft-deleted.

The checkpoint holds every entity URN of every app, alongside the stale metadata removal state. On-prem, it also holds the data model field lists of the apps named in `binary_file_to_app`. If it grows past `stateful_ingestion.max_checkpoint_state_size` (16 MB by default), it is not saved and the run reports a warning.

##### Skipping unchanged apps (on-prem)

With `skip_unchanged_apps: true` in the `on_prem` block and stateful ingestion enabled, later runs only open apps that changed in the Engine. This is on-prem only, because the Repository Service reports when each app, sheet, story and master item was last modified, and Qlik Cloud has no equivalent listing.

```yaml
max_workers: 4
on_prem:
  # server and certificates as above
  skip_unchanged_apps: true
stateful_ingestion:
  enabled: true
  remove_stale_metadata: true
```

An app is read again when:

- the app or any of its sheets, stories or master items is saved
- it is reloaded, published or moves stream
- its owner or a sheet's owner changes
- the recipe or a data connection changes, or the connector is upgraded
- the last read of it failed
- its app container in DataHub is missing or does not carry the fingerprint of that read (stored as the container's `ingestionFingerprint` custom property, which is visible in the UI)
- any entity that read produced is missing from DataHub or soft-deleted

Entities of a skipped app stay in place and are not soft-deleted. Aspects that failed to write for an entity that exists are not detected. To read every app again, turn `skip_unchanged_apps` off for one run; `stateful_ingestion.ignore_old_state: true` also does that, but skips stale metadata removal for that run.

Column-level lineage is resolved against the upstream schemas in DataHub at read time. After ingesting an upstream source for the first time, turn `skip_unchanged_apps` off for one run so every app is read again.

### Limitations

- On-prem ingestion does not ingest Qlik Cloud datasets. From cloud storage connections it only links QVDs, and only for connections mapped in `file_connection_to_storage`.
- Scripts pulled in with `$(Include=...)` are read only on Qlik Cloud, in the cases listed in [Included scripts](#included-scripts-qlik-cloud), and a `SUB` the script calls but does not define is not read.
- A table loaded with `RESIDENT` from another script table gets no lineage.
- On Qlik Cloud, a file loaded through a variable that varies as the script runs, as in a `FOR EACH` loop over `FileList(...)`, is not named from the app's last value, so the table's file lineage is only added to.
- Database tables in Qlik Cloud's lineage graph are not linked, as Qlik identifies them by an opaque hash; their lineage comes from the load script instead.
- Qlik answers "Forbidden" for datasets and spaces the API key's user cannot access. Those datasets are only named from the lineage graph of the apps that load them, without their schema.
- A file named from the lineage graph and also resolved from another app's load script is ingested as two datasets until its storage is set in `file_connection_to_storage`.
- `file_connection_to_storage` does not accept `wasb://` and `wasbs://` URIs.
- On-prem, an unlabelled `LOAD` from a QVD is skipped, as Qlik names its table.
- On-prem with `read_data_model: true`, apps whose section access denies the service user their data are read without their data: their tables get fields only from the load script, and the apps are listed in the "Qlik apps read without their data" warning.

### Troubleshooting

Most lineage gaps show up as a warning or as an entry in the ingestion report. The sections below are named after them.

#### "Unable to fully read Qlik app"

The app could not be read completely, for example because the Engine timed out or refused the connection. See [Partly read apps](#partly-read-apps) for what is kept. On-prem, check that the Engine (`engine_port`, 4747 by default) is running and reachable, and raise `timeout_seconds` if reads time out. On Qlik Cloud, lower `max_workers` if reads are rate-limited. On-prem, an app with a `BINARY` statement whose last read ingested BINARY tables is also partly read when its data model can't be loaded, for example because section access denies its data to the service user, or when its BINARY path uses a variable and its variables can't be read. After `max_partial_reads_kept` runs its BINARY tables are let go instead. Give the service user access to the app's data in section access to keep them.

#### "Unable to read Qlik app load script"

On Qlik Cloud, the API key's user needs to be the app's Owner, or have **Can manage** or **Can operate** in the managed space. When the Engine denies access to the script, the app is still ingested, and its charts get their inputs from the data model. Charts of an app whose data model or load script could not be read for another reason keep the inputs from their last read. Any other failure to read the script marks the app as partly read and reports a failure for the run, which keeps stale metadata removal from deleting its lineage.

#### `unresolved_data_connections` in the report

A `SELECT` ran through a connection that resolves to no platform, so it was skipped. Add the connection name to `data_connection_to_platform_instance` with its `platform`, or give the API key's user **Can consume data** in the connection's space.

#### "Qlik load script table is missing its database or schema"

The SQL doesn't qualify a table, and its connection has no database or schema. Set `database` and `default_schema` for the connection in `data_connection_to_platform_instance`.

#### "Unable to parse Qlik load script SQL" or "Qlik load script SQL uses a variable that is not set"

A `SELECT` that cannot be parsed is skipped, for example one that uses an unexpanded variable in a table name. Set the variables the warning lists as `settable` in `script_variables`; no script sets them. Variables listed as set at run time, such as a loop variable, can't be set this way.

#### "Some Qlik QVD loads have no lineage", or `unresolved_qvd_paths` and `unlabelled_qvd_loads` in the report (on-prem)

`unresolved_qvd_paths` lists paths the connector can't resolve to one file, such as a wildcard, a variable the app doesn't have, a path relative to the `Directory` statement, or a `lib://` path of a connection that isn't a folder connection and isn't in `file_connection_to_storage`. Set missing variables in `script_variables`, map one share reached by different names with `qvd_folder_aliases`, and map storage connections in `file_connection_to_storage`.

`unlabelled_qvd_loads` lists QVDs loaded without a table label. Qlik names such a table itself, so it is not linked to the QVD. Give the `LOAD` a label, such as `Orders: LOAD * FROM [lib://QVD Data/orders.qvd] (qvd);`.

#### `binary_loads_without_lineage` in the report (on-prem)

Each entry gives the app, and the file or source app, with the reason its BINARY load wasn't linked:

- "the path names no single file": the path uses a variable the app doesn't have, a wildcard, or a path relative to the `Directory` statement. Set the variables in `script_variables`, or use a `lib://` or absolute path.
- "the app's data model was not read": set `read_data_model: true`. If it is set, the app's data couldn't be opened; see "Qlik apps read without their data".
- "binary_file_to_app maps it to the app itself": map the file to the id of the app it was exported from.
- "the script creates tables it doesn't name": map the file in `binary_file_to_app`.
- "the script drops, replaces or renames to tables it doesn't name": label the `Replace` load, or set the variable that names the dropped or renamed table in `script_variables`.
- "its data connection is not a known folder connection": map the connection in `file_connection_to_storage`, or the file in `binary_file_to_app`.
- "app ... was not read or kept this run": include the source app in the run, and fix the errors that stopped it from being read.
- "app ... ingests no table ...": the source app doesn't ingest that table, and its data model fields aren't known. Check that `binary_file_to_app` names the app the file was exported from.
- "app ... doesn't ingest table ...": the source app's data model has the table, but the app builds it without SQL, a QVD or `BINARY`, for example with `RESIDENT` or `INLINE`, so it is not ingested and there is no dataset to link to. `binary_file_to_app` is not the cause.
- "table ... may be a script table Qlik renamed": a table such as `Orders-2024` beside an `Orders` load in the script could be Qlik's renamed `Orders` load or a table from the file. Map the file in `binary_file_to_app` and include the source app in the run, so its data model decides.
- "table ... is not in app ...'s data model, so it is taken as a script table Qlik renamed": the table is the script's own, and its lineage is not known from the script. Label the load with a name not already taken to ingest it with lineage.

#### "Qlik BINARY tables may be changed by an included script" (on-prem)

These apps load another app with a mapped `BINARY` statement and also include scripts the connector doesn't read, or call a `SUB` the script doesn't define. Their BINARY tables are linked to the source app, but the lineage is only added to, so edges written by earlier runs stay. Remove an edge in DataHub when the table no longer comes from that app.

#### `unresolved_file_connections` in the report, or "Qlik file connections have no known storage" (Qlik Cloud)

Apps read or store files through these connections, but their storage is not known. Their files are linked only between Qlik apps, by space, connection and path (see [Lineage from files on cloud storage](#lineage-from-files-on-azure-storage-amazon-s3-and-google-cloud-storage-qlik-cloud)). A personal-space app's file on a connection named without its space is named from the loading app's lineage graph when the app's tables come from the graph, and is otherwise left out of lineage; a `STORE` into it is not linked. For an Azure Storage, Amazon S3 or Google Cloud Storage connection, add it to `file_connection_to_storage` with its `storage_uri`, or give the API key's user **Can consume data** in the connection's space. Other connectors, such as SFTP, Dropbox or SharePoint, can't be mapped; a file an app's lineage graph places on one of those is not listed here.

#### "Unable to read Qlik upstream lineage graph" (Qlik Cloud)

The upstream lineage graph of these apps could not be read, so sources only the graph names, such as QVDs in spaces the API key's user can't see, are left out of their tables' lineage, and the tables' lineage is only added to this run. Datasets the last run named from the graph are kept, as they are when the datasets could not be listed or a listed QVD has no QRI. The graph is read again on the next run.

#### "Unable to read included Qlik scripts" (Qlik Cloud)

Each entry gives the path of an included file, the app and space that include it, and why it could not be read: an Engine error, an include cycle, nesting more than 10 levels deep, or the size limit. A path without a space, such as `lib://DataFiles/common.qvs`, resolves in the space of the app that includes it. For an access error, give the API key's user **Can view** in the space that holds the file. Until then, what the script loads is missing from lineage, and the app's lineage is only added to.

#### "Unable to read included Qlik scripts" (Qlik Cloud)

Each entry gives the path of an included file, the app and space that include it, and why it could not be read: an Engine error, an include cycle, nesting more than 10 levels deep, or the size limit. A path without a space, such as `lib://DataFiles/common.qvs`, resolves in the space of the app that includes it. For an access error, give the API key's user **Can view** in the space that holds the file. Until then, what the script loads is missing from lineage, and the app's lineage is only added to.

#### `apps_without_lineage_graph` in the report, or "Qlik Cloud has no lineage graph for app" (Qlik Cloud)

Qlik Cloud has no lineage graph for these apps, and their last read, if any, had no tables, so their tables come from the load script only. Qlik also has no graph for an app whose lineage it has not computed yet, such as right after a reload. When there is no last read to compare with, the warning is reported and the lineage of the app's tables is only added to for that run.

#### `apps_change_check_failed` in the report, or "Unable to check whether Qlik apps changed" (on-prem)

With `skip_unchanged_apps`, these apps could not be checked for changes, for example because the Repository Service or DataHub did not answer, so they were read in full. Nothing is lost; if the check keeps failing, the entry says why.

#### "Lineage of Qlik app table only added to"

Something the table loads could not be read this run, so the lineage read this run is added to the table's existing lineage instead of replacing it. The warning gives the reason; fix that and the next run replaces the lineage.

#### "Qlik app checkpoint is too large to save"

Raise `stateful_ingestion.max_checkpoint_state_size`.
