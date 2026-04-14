# Snowflake DMF Assertions [BETA]

The DataHub Open Assertion Compiler allows you to define your Data Quality assertions in a simple YAML format, and then compile them to be executed by Snowflake Data Metric Functions.
Once compiled, you'll be able to register the compiled DMFs in your Snowflake environment, and extract their results them as part of your normal ingestion process for DataHub.
Results of Snowflake DMF assertions will be reported as normal Assertion Results, viewable on a historical timeline in the context
of the table with which they are associated.

## Prerequisites

- You must have a Snowflake Enterprise account, where the DMFs feature is enabled.
- You must have the necessary permissions to provision DMFs in your Snowflake environment (see below)
- You must have the necessary permissions to query the DMF results in your Snowflake environment (see below)
- You must have DataHub instance with Snowflake metadata ingested. If you do not have existing snowflake ingestion, refer [Snowflake Quickstart Guide](https://docs.datahub.com/docs/quick-ingestion-guides/snowflake/overview) to get started.
- You must have DataHub CLI installed and run [`datahub init`](https://docs.datahub.com/docs/cli/#init).

### Permissions

_Permissions required for registering DMFs_

According to the latest Snowflake docs, here are the permissions the service account performing the
DMF registration and ingestion must have:

| Privilege                    | Object           | Notes                                                                                                                      |
| ---------------------------- | ---------------- | -------------------------------------------------------------------------------------------------------------------------- |
| USAGE                        | Database, schema | Database and schema where snowflake DMFs will be created. This is configured in compile command described below.           |
| CREATE FUNCTION              | Schema           | This privilege enables creating new DMF in schema configured in compile command.                                           |
| EXECUTE DATA METRIC FUNCTION | Account          | This privilege enables you to control which roles have access to server-agnostic compute resources to call the system DMF. |
| USAGE                        | Database, schema | These objects are the database and schema that contain the referenced table in the query.                                  |
| OWNERSHIP                    | Table            | This privilege enables you to associate a DMF with a referenced table.                                                     |
| USAGE                        | DMF              | This privilege enables calling the DMF in schema configured in compile command.                                            |

and the roles that must be granted:

| Role                       | Notes              |
| -------------------------- | ------------------ |
| SNOWFLAKE.DATA_METRIC_USER | To use System DMFs |

_Permissions required for running DMFs (scheduled DMFs run with table owner's role)_

Because scheduled DMFs run with the role of the table owner, the table owner must have the following privileges:

| Privilege                    | Object           | Notes                                                                                                                      |
| ---------------------------- | ---------------- | -------------------------------------------------------------------------------------------------------------------------- |
| USAGE                        | Database, schema | Database and schema where snowflake DMFs will be created. This is configured in compile command described below.           |
| USAGE                        | DMF              | This privilege enables calling the DMF in schema configured in compile power.                                              |
| EXECUTE DATA METRIC FUNCTION | Account          | This privilege enables you to control which roles have access to server-agnostic compute resources to call the system DMF. |

and the roles that must be granted:

| Role                       | Notes              |
| -------------------------- | ------------------ |
| SNOWFLAKE.DATA_METRIC_USER | To use System DMFs |

_Permissions required for querying DMF results_

In addition, the service account that will be executing DataHub Ingestion, and querying the DMF results, must have been granted the following system application roles:

| Role                           | Notes                       |
| ------------------------------ | --------------------------- |
| DATA_QUALITY_MONITORING_VIEWER | Query the DMF results table |

To learn more about Snowflake DMFs and the privileges required to provision and query them, see the [Snowflake documentation](https://docs.snowflake.com/en/user-guide/data-quality-intro).

_Example: Granting Permissions_

```sql
-- setup permissions to <assertion-registration-role> to create DMFs and associate DMFs with table
grant usage on database "<dmf-database>" to role "<assertion-service-role>"
grant usage on schema "<dmf-database>.<dmf-schema>" to role "<assertion-service-role>"
grant create function on schema "<dmf-database>.<dmf-schema>" to role "<assertion-service-role>"
-- grant ownership + rest of permissions to <assertion-service-role>
grant role "<table-owner-role>" to role "<assertion-service-role>"

-- setup permissions for <table-owner-role> to run DMFs on schedule
grant usage on database "<dmf-database>" to role "<table-owner-role>"
grant usage on schema "<dmf-database>.<dmf-schema>" to role "<table-owner-role>"
grant usage on all functions in "<dmf-database>.<dmf-schema>" to role "<table-owner-role>"
grant usage on future functions in "<dmf-database>.<dmf-schema>" to role "<table-owner-role>"
grant database role SNOWFLAKE.DATA_METRIC_USER to role "<table-owner-role>"
grant execute data metric function on account to role "<table-owner-role>"

-- setup permissions for <datahub-role> to query DMF results
grant application role SNOWFLAKE.DATA_QUALITY_MONITORING_VIEWER to role "<datahub_role>"
```

## Supported Assertion Types

The following assertion types are currently supported by the DataHub Snowflake DMF Assertion Compiler:

- [Freshness](/docs/managed-datahub/observe/freshness-assertions.md)
- [Volume](/docs/managed-datahub/observe/volume-assertions.md)
- [Column](/docs/managed-datahub/observe/column-assertions.md)
- [Custom SQL](/docs/managed-datahub/observe/custom-sql-assertions.md)

Note that Schema Assertions are not currently supported.

## Creating Snowflake DMF Assertions

The process for declaring and running assertions backend by Snowflake DMFs consists of a few steps, which will be outlined
in the following sections.

### Step 1. Define your Data Quality assertions using Assertion YAML files

See the section **Declaring Assertions in YAML** below for examples of how to define assertions in YAML.

### Step 2. Register your assertions with DataHub

Use the DataHub CLI to register your assertions with DataHub, so they become visible in the DataHub UI:

```bash
datahub assertions upsert -f examples/library/assertions_configuration.yml
```

### Step 3. Compile the assertions into Snowflake DMFs using the DataHub CLI

Next, we'll use the `assertions compile` command to generate the SQL code for the Snowflake DMFs,
which can then be registered in Snowflake.

```bash
datahub assertions compile -f examples/library/assertions_configuration.yml -p snowflake -x DMF_SCHEMA=<db>.<schema-where-DMF-should-live>
```

Two files will be generated as output of running this command:

- `dmf_definitions.sql`: This file contains the SQL code for the DMFs that will be registered in Snowflake.
- `dmf_associations.sql`: This file contains the SQL code for associating the DMFs with the target tables in Snowflake.

By default in a folder called `target`. You can use config option `-o <output_folder>` in `compile` command to write these compile artifacts in another folder.

Each of these artifacts will be important for the next steps in the process.

_dmf_definitions.sql_

This file stores the SQL code for the DMFs that will be registered in Snowflake, generated
from your YAML assertion definitions during the compile step.

```sql
-- Example dmf_definitions.sql

-- Start of Assertion 5c32eef47bd763fece7d21c7cbf6c659

            CREATE or REPLACE DATA METRIC FUNCTION
            test_db.datahub_dmfs.datahub__5c32eef47bd763fece7d21c7cbf6c659 (ARGT TABLE(col_date DATE))
            RETURNS NUMBER
            COMMENT = 'Created via DataHub for assertion urn:li:assertion:5c32eef47bd763fece7d21c7cbf6c659 of type volume'
            AS
            $$
            select case when metric <= 1000 then 1 else 0 end from (select count(*) as metric from TEST_DB.PUBLIC.TEST_ASSERTIONS_ALL_TIMES )
            $$;

-- End of Assertion 5c32eef47bd763fece7d21c7cbf6c659
....
```

_dmf_associations.sql_

This file stores the SQL code for associating with the target table,
along with scheduling the generated DMFs to run on at particular times.

```sql
-- Example dmf_associations.sql

-- Start of Assertion 5c32eef47bd763fece7d21c7cbf6c659

            ALTER TABLE TEST_DB.PUBLIC.TEST_ASSERTIONS_ALL_TIMES SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
            ALTER TABLE TEST_DB.PUBLIC.TEST_ASSERTIONS_ALL_TIMES ADD DATA METRIC FUNCTION test_db.datahub_dmfs.datahub__5c32eef47bd763fece7d21c7cbf6c659 ON (col_date);

-- End of Assertion 5c32eef47bd763fece7d21c7cbf6c659
....
```

### Step 4. Register the compiled DMFs in your Snowflake environment

Next, you'll need to run the generated SQL from the files output in Step 3 in Snowflake.

You can achieve this either by running the SQL files directly in the Snowflake UI, or by using the SnowSQL CLI tool:

```bash
snowsql -f dmf_definitions.sql
snowsql -f dmf_associations.sql
```

::: NOTE
Scheduling Data Metric Function on table incurs Serverless Credit Usage in Snowflake. Refer [Billing and Pricing](https://docs.snowflake.com/en/user-guide/data-quality-intro#billing-and-pricing) for more details.
Please ensure you DROP Data Metric Function created via dmf_associations.sql if the assertion is no longer in use.
:::

### Step 5. Run ingestion to report the results back into DataHub

Once you've registered the DMFs, they will be automatically executed, either when the target table is updated or on a fixed
schedule.

To report the results of the generated Data Quality assertions back into DataHub, you'll need to run the DataHub ingestion process with a special configuration
flag: `include_assertion_results: true`:

```yaml
# Your DataHub Snowflake Recipe
source:
  type: snowflake
  config:
    # ...
    include_assertion_results: True
    # ...
```

During ingestion we will query for the latest DMF results stored in Snowflake, convert them into DataHub Assertion Results, and report the results back into DataHub during your ingestion process
either via CLI or the UI visible as normal assertions.

`datahub ingest -c snowflake.yml`

## Ingesting External (User-Created) DMFs

In addition to DataHub-created DMFs, you can also ingest results from your own custom Snowflake Data Metric Functions. "External" here means DMFs that were created directly in Snowflake without using DataHub's assertion compiler - they exist outside of DataHub's management.

### Why Use External DMFs?

You might want to ingest external DMFs if:

- **Pre-existing DMFs**: You already have DMFs in Snowflake that were created before adopting DataHub, and you want to see their results in DataHub without recreating them
- **Custom logic**: You need DMF logic that isn't supported by DataHub's assertion compiler (e.g., complex multi-table checks)
- **Team workflows**: Different teams manage DMFs directly in Snowflake, but you want centralized visibility in DataHub
- **Gradual adoption**: You want to start monitoring existing data quality checks in DataHub before fully migrating to DataHub-managed assertions

### Enabling External DMF Ingestion

To ingest external DMFs, add the `include_externally_managed_dmfs` flag to your Snowflake recipe:

```yaml
source:
  type: snowflake
  config:
    # ... connection config ...

    # Enable assertion results ingestion (required)
    include_assertion_results: true

    # Enable external DMF ingestion (new)
    include_externally_managed_dmfs: true

    # Time window for assertion results
    start_time: "-7 days"
```

Both flags must be enabled for external DMF ingestion to work.

### Requirements for External DMFs

**External DMFs must return `1` for SUCCESS and `0` for FAILURE.**

DataHub interprets the `VALUE` column from Snowflake's `DATA_QUALITY_MONITORING_RESULTS` table as:

- `VALUE = 1` → Assertion **PASSED**
- `VALUE = 0` → Assertion **FAILED**

This is because DataHub cannot interpret arbitrary return values (e.g., "100 null rows" - is that good or bad?). You must build the pass/fail logic into your DMF.

::: warning What if my DMF returns other values?
If your DMF returns values other than 0 or 1, DataHub will mark the assertion result as **ERROR**:

- `VALUE = 1` → **PASSED**
- `VALUE = 0` → **FAILED**
- `VALUE != 0 and VALUE != 1` (e.g., 5, 100, -1) → **ERROR**

The ERROR state indicates that the DMF is not configured correctly for DataHub ingestion. You can identify these cases by:

1. Checking the ingestion logs for warnings like: `DMF 'my_dmf' returned invalid value 100. Expected 1 (pass) or 0 (fail). Marking as ERROR.`
2. Looking for assertions with ERROR status in the DataHub UI
   :::

#### Example: Writing External DMFs Correctly

**WRONG** - Returns raw count (DataHub can't interpret this):

```sql
CREATE DATA METRIC FUNCTION my_null_check(ARGT TABLE(col VARCHAR))
RETURNS NUMBER AS
$$
  SELECT COUNT(*) FROM ARGT WHERE col IS NULL
$$;
-- Returns: 0, 5, 100, etc. - DataHub can't determine pass/fail!
```

**CORRECT** - Returns 1 (pass) or 0 (fail):

```sql
CREATE DATA METRIC FUNCTION my_null_check(ARGT TABLE(col VARCHAR))
RETURNS NUMBER AS
$$
  SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
  FROM ARGT WHERE col IS NULL
$$;
-- Returns: 1 if no nulls (pass), 0 if has nulls (fail)
```

**CORRECT** - With threshold:

```sql
CREATE DATA METRIC FUNCTION my_null_check_threshold(ARGT TABLE(col VARCHAR))
RETURNS NUMBER AS
$$
  SELECT CASE WHEN COUNT(*) <= 10 THEN 1 ELSE 0 END
  FROM ARGT WHERE col IS NULL
$$;
-- Returns: 1 if ≤10 nulls (pass), 0 if >10 nulls (fail)
```

### How External DMFs Differ from DataHub-Created DMFs

| Aspect             | DataHub-Created DMFs                               | External DMFs                             |
| ------------------ | -------------------------------------------------- | ----------------------------------------- |
| **Naming**         | Prefixed with `datahub__`                          | Any name                                  |
| **Definition**     | Created via `datahub assertions compile`           | Created manually in Snowflake             |
| **Assertion Type** | Based on YAML definition (Freshness, Volume, etc.) | CUSTOM                                    |
| **Source**         | NATIVE (defined in DataHub)                        | EXTERNAL                                  |
| **URN Generation** | Extracted from DMF name (`datahub__<guid>`)        | Generated from Snowflake's `REFERENCE_ID` |

### How External DMFs Appear in DataHub UI

External DMFs appear in DataHub with:

- **Assertion Type**: CUSTOM
- **Source**: EXTERNAL
- **Platform Instance**: Snowflake platform instance (if configured)
- **Description**: "External Snowflake DMF: {dmf_name}"
- **Custom Properties**:
  - `snowflake_dmf_name`: The DMF function name
  - `snowflake_reference_id`: Snowflake's unique identifier for the DMF-table binding
  - `snowflake_dmf_columns`: Comma-separated list of columns the DMF operates on

You can view external DMF assertions in the **Quality** tab of the associated dataset in the DataHub UI. They will show pass/fail history alongside any DataHub-created assertions.

## Caveats

- Currently, Snowflake supports at most 1000 DMF-table associations at the moment so you can not define more than 1000 assertions for snowflake.
- Currently, Snowflake does not allow JOIN queries or non-deterministic functions in DMF definition so you can not use these in SQL for SQL assertion or in filters section.
- Currently, all DMFs scheduled on a table must follow same exact schedule, so you can not set assertions on same table to run on different schedules.
- Currently, DMFs are only supported for regular tables and not dynamic or external tables.

## FAQ

Coming soon!
