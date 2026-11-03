### Capabilities

#### Processes and deployments

The source plugin queries `ComponentMetadata` for current, non-deleted components of type `process`. For each process it reads the component XML to get the description, then emits:

- A DataFlow keyed by the component ID, with the process name as its display name and the custom properties `componentId`, `version`, `branchName`, `modifiedDate`, `folderFullPath` and `deployments`.
- One DataJob with the ID `process`, with the lineage described below.

Active deployments come from `DeployedPackage`, joined to environment names. `environment_pattern` controls which environments count. With `include_undeployed_processes: false` (the default), a process that has no active deployment in an allowed environment is skipped, and stateful ingestion soft-deletes it if it was ingested before.

#### Execution history

With `include_execution_history: true` (the default), the source plugin reads `ExecutionRecord` for the last `execution_history_days` days (7 by default, at most 30) in one query for the whole account. For each ingested process it keeps the `max_executions_per_process` most recent executions (50 by default) and emits each one as a DataProcessInstance run of the process DataJob, with the custom properties `status`, `atomName`, `executionType`, `parentExecutionId`, `topLevelExecutionId`, `inboundDocumentCount`, `outboundDocumentCount` and `inboundErrorDocumentCount`. Manual executions (`exec_manual`) are typed as ad hoc runs and all others as scheduled runs. A run started by a Process Call shape (`sub_process`) takes the type of its top-level run and is linked to the run that called it as its parent instance. Boomi leaves `parentExecutionId` empty on these runs in practice, so the top-level run stands in for the caller. The link is only made when the calling run is ingested in the same run of the source plugin.

Each run gets a start event at `executionTime`. Finished runs also get an end event at `executionTime` plus `executionDuration`, or at `executionTime` when Boomi reports no duration:

| Boomi status                                             | Run result       |
| -------------------------------------------------------- | ---------------- |
| `COMPLETE`, `COMPLETE_WARN`                              | Success          |
| `ERROR`, `ABORTED`                                       | Failure          |
| `DISCARDED`                                              | Skipped          |
| `INPROCESS`, `STARTED`, `ENQUEUED`, `ABORTED_RECOVERING` | Start event only |

Any other status gets a start event only and is counted in `executions_unknown_status`. Executions of processes that were not ingested (filtered or undeployed) are skipped and counted in `executions_process_not_emitted`. `environment_pattern` does not filter executions directly, because an execution record names the runtime (atom) it ran on, not the environment. Runs follow the ingested processes, so a process skipped because it is only deployed to denied environments gets no runs.

Boomi purges execution history after the runtime's purge setting, 30 days by default, so runs older than that cannot be ingested. Schedule ingestion more often than the purge interval to keep a continuous history. The window query pages through every execution in the account, so busy accounts with a long window need more API calls.

#### Lineage

With `include_lineage: true` (the default), the source plugin reads the shapes in each process. For every Connector shape it reads the connection and operation components, and for the legacy Database connector the database profile too. A read operation adds the dataset to the DataJob's inputs and a write adds it to the outputs. Read or write comes from the operation, not from the shape's action:

- `DatabaseGetAction` and `SalesforceGetAction` read, and the matching `SendAction` elements write.
- For other connectors, the `customOperationType` or `operationType` decides. `QUERY` and `GET` read. `CREATE`, `INSERT`, `UPSERT`, `UPDATE`, `DELETE` and `EXECUTE` write. Anything else is skipped and reported as an unknown operation direction.

Listen shapes, which receive documents pushed to the runtime, are skipped.

| Connector         | `subType`                          | Platform                                                                                                                          | Table                                                                                                      |
| ----------------- | ---------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| Database (legacy) | `database`                         | From the driver: SQL Server, Oracle, MySQL, PostgreSQL, DB2, SAP HANA and Snowflake. With a custom driver, from the URL template. | The profile's SQL, parsed with the connection's database as the default. Without SQL, the profile's table. |
| Database V2       | `officialboomi-X3979C-dbv2da-prod` | From the JDBC URL: `sqlserver`, `mysql`, `mariadb`, `postgresql`, `oracle`, `snowflake`, `redshift` and `db2`.                    | The operation's query, parsed, or else its object type ID. The connection's schema is the default schema.  |
| Salesforce        | `salesforce`                       | `salesforce`                                                                                                                      | The operation's object name, with its case kept.                                                           |
| Other, JDBC URL   | Any other subType                  | From a connection field whose value is a JDBC URL, such as Snowflake (`officialboomi-X3979C-snowfl-prod`).                        | A SQL field in the operation, parsed, or else its object type ID. Experimental.                            |
| Other, mapped     | Any subType you map                | From `connection_type_platform_map` or `connection_to_platform`.                                                                  | A SQL field in the operation, parsed, or else its object type ID. Experimental.                            |

The database is taken from the driver settings or from the JDBC URL where the URL names one (`databaseName` for SQL Server, the path for MySQL, MariaDB, PostgreSQL, Redshift and DB2, and `db` for Snowflake). A legacy Database connection with a custom driver (`driverId="custom"`) stores its URL as a template, such as `jdbc:postgresql://{0}:{1}/{2}{3}`, and keeps the host, port, database name and additional options as separate settings. The source plugin fills `{0}` to `{3}` from those settings before reading the platform and database from the URL. For MySQL and MariaDB, which have no schema level, the Database V2 `schemaName` is used as the database.

For connectors other than the three above, the source plugin looks at every field of the connection and uses one whose value starts with `jdbc:`, whatever the field is called. The platform, database and schema come from that URL, so a Snowflake connection with `jdbc:snowflake://acct.snowflakecomputing.com/?db=ANALYTICS&schema=PUBLIC` resolves an operation on `ORDERS` to `analytics.public.orders` without any mapping. An object type ID that is already qualified keeps its own database and schema. A connection with more than one different JDBC URL is skipped and counted under `ambiguous_jdbc_url`. In the operation, a field whose ID contains `sql` or `query` and whose value starts with a SQL statement is parsed for tables in place of the object type ID; an operation with more than one such statement is skipped and counted under `ambiguous_sql`.

The order is: `connection_to_platform`, then the connector-specific parsing above, then a JDBC URL in the connection's fields, then `connection_type_platform_map`. If `connection_type_platform_map` names a different platform than the JDBC URL, the mapped platform is used without the URL's database and schema, and a warning is raised.

Unqualified table names in SQL get the connection's database but no schema, because the schema they resolve to depends on the database's search path. For PostgreSQL and SQL Server, set `default_schema` in `connection_to_platform` so the URNs match the ones from the database's own source. Oracle URLs carry a SID or service name, which DataHub's Oracle source does not put in URNs, so Oracle tables get no database unless you set one. Table names are lowercased by default. The source plugin never builds a URN from a host name.

##### Mapping connections

Use `connection_to_platform` when the URNs have to match another source that sets a platform instance, or when a connection does not carry the database or schema. Keys are connection component IDs, which you can copy from the connection's URL in Boomi. An entry wins over everything read from the connection:

```yaml
connection_to_platform:
  "1a2b3c4d-0000-0000-0000-000000000001":
    platform: mssql
    platform_instance: erp
    database: my_db
    default_schema: dbo
    # env defaults to the source's env.
    convert_urns_to_lowercase: true
```

Use `connection_type_platform_map` to give every connection of a connector type a platform. This is how connectors without built-in support whose connections hold no JDBC URL get lineage. The operation's object type ID becomes the table name, qualified with the database and schema from `connection_to_platform` when the connection has an entry. This generic path is experimental: the object type IDs of these connectors have not been checked against live accounts, so check the URNs it produces before you rely on them.

```yaml
connection_type_platform_map:
  "officialboomi-X3979C-example-prod": snowflake
connection_to_platform:
  "1a2b3c4d-0000-0000-0000-000000000002":
    platform: snowflake
    database: analytics
    default_schema: public
```

##### Not supported

SAP (`sapjco`), HTTP (`http`), FTP (`ftp`), SFTP (`officialboomi-X3979C-sftpv2-prod`, `officialboomi-X3979C-sftpvx-prod`), Disk (`disk-sdk`) and mail connectors address function modules, URLs, files and mailboxes rather than tables, so by default they get no lineage and are counted per subType in `lineage_skipped_subtypes`. An entry in `connection_type_platform_map` for the subType, or in `connection_to_platform` for the connection, overrides this and uses the operation's object type ID as the table name. Any other connector that is not mapped and has no JDBC URL in its connection is counted under `unsupported_connector` in `lineage_edges_unresolved`, with one warning per subType.

When an operation has no table or SQL cannot be parsed, the edge is skipped rather than guessed, and counted by reason in `lineage_edges_unresolved`. When a connection, operation or profile cannot be read, the process's inputs and outputs are left out of this run altogether, so the lineage stored by an earlier run is kept instead of being replaced by a partial set. These processes are listed in `lineage_processes_incomplete`.

##### Process calls

A Process Call shape adds the called process's DataJob to the caller's input DataJobs, but only when the called process is ingested in the same run. Calls to filtered or undeployed processes are counted in `process_calls_child_not_emitted`.

The API responses and component XML this relies on were validated against a live Boomi account, with the legacy Database connector (custom PostgreSQL driver), Database V2 (MySQL), Salesforce, HTTP Client, Disk and SFTP connectors, and a Process Call.

#### Folders

Every folder that matches `folder_pattern` is emitted as a container, together with its parent folders so the browse path stays complete. `folder_pattern` is matched against the full folder path, for example `Acme/Production/Orders`. Processes in folders that do not match are skipped. If a process's folder is not in the folder listing, the path is read from the process component instead. When no path is available, the process is kept if `folder_pattern` only has deny entries and skipped if it has an allow list.

#### Branches

If Branch and Merge is enabled on the account, set `branch_name` (usually `main`). A branch copy of a component keeps the original component ID, so the source plugin keeps only the most recently modified copy of each component.

#### Rate limiting

Boomi allows 10 Platform API requests per second per account and answers with HTTP 503 when the limit is exceeded. The source plugin caps itself at `max_requests_per_second` (8 by default) and retries 429, 500, 502, 503 and 504 responses with exponential backoff. Lower the cap if other API clients share the account.

The source plugin makes one request per process to read its component XML, so an account with 1,000 processes needs a little over two minutes at the default rate. Lineage adds one request for each distinct connection, operation and database profile; components shared between processes are read once per run.

### Limitations

- Lineage is table level only. Column lineage from Map components is not extracted.
- Lineage is read from the current component versions, not the version deployed to each environment.
- Connectors without built-in support, such as Amazon S3 and NetSuite, only get lineage when their connection holds a JDBC URL, or through `connection_type_platform_map` or `connection_to_platform`. These generic paths are experimental.
- Salesforce connections do not name the org, so Salesforce datasets from different orgs only stay apart if you set `platform_instance` in `connection_to_platform`.
- Execution history is limited to what Boomi still retains, 30 days by default.
- A process is one DataFlow across all environments. The version deployed to each environment is only shown in the `deployments` property.
- Owners are only emitted as user URNs. If your DataHub users are not keyed by email, the owners do not link to existing users.

### Troubleshooting

#### HTTP 401 on every request

Check that `username` is the plain user name (the source plugin adds the `BOOMI_TOKEN.` prefix), that the token has not been revoked, and that `account_id` matches the account the token belongs to.

#### Warning "Cannot read Boomi deployments"

The token's role lacks the privilege to query `DeployedPackage`. The run continues, ingests every process and leaves out the `deployments` property. Grant **Packaged Component Deployment** to restore deployment filtering.

#### Warning "Cannot read Boomi execution history"

The token's role lacks **View Results**, so `ExecutionRecord` returns HTTP 403. The run continues without run history. Grant **View Results**, or set `include_execution_history: false` to skip the query.

If the account has no runtimes (Atoms) the token can see, Boomi rejects the execution query with HTTP 400. The source plugin treats that as no execution history, sets `executions_no_runtimes` in the report and does not warn.

#### Warning "Unsupported Boomi connector for lineage"

A process uses a connector the source plugin has no lineage support for, and its connection holds no JDBC URL. The warning names the subType. Add it to `connection_type_platform_map` if its operations have an object type ID that is a table name, or ignore the warning.

#### Lineage URNs do not match the ingested tables

Compare the platform instance, database, schema and casing of the URNs with the ones from the source that ingested the tables, and set them for that connection in `connection_to_platform`.

#### Fewer processes than expected

Check the `processes_filtered` and `processes_undeployed_skipped` lists in the ingestion report. Undeployed processes are skipped unless `include_undeployed_processes` is `true`, and folder restrictions on the role hide processes entirely.
