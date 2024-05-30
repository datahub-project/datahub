## Snowflake DMF Assertions [BETA]

The DataHub Open Assertion Compiler allows you to define your Data Quality assertions in a simple YAML format, and then compile them to be executed by Snowflake Data Metric Functions.
Once compiled, you'll be able to register the compiled DMFs in your Snowflake environment, and extract their results them as part of your normal ingestion process for DataHub.
Results of Snowflake DMF assertions will be reported as normal Assertion Results, viewable on a historical timeline in the context
of the table with which they are associated.

### Prerequisites

- You must have a Snowflake Enterprise account, where the DMFs feature is enabled.
- You must have the necessary permissions to provision DMFs in your Snowflake environment (see below)
- You must have the necessary permissions to query the DMF results in your Snowflake environment (see below) 

#### Provisioning DMFs

According to the latest Snowflake docs, here are the permissions the service account performing the
DMF registration and ingestion must have:

| Privilege                    | Object           | Notes                                                                                                                      |
|------------------------------|------------------|----------------------------------------------------------------------------------------------------------------------------|
| EXECUTE DATA METRIC FUNCTION | Account          | This privilege enables you to control which roles have access to server-agnostic compute resources to call the system DMF. |
| USAGE                        | Database, schema | These objects are the database and schema that contain the referenced table in the query.                                  |
| USAGE                        | Database, schema | Database and schema where snowflake DMFs will be created. This is configured in `compile` command described below.         |
| USAGE                        | DMF              | This privilege enables you to use the registered DMF                                                                       |
| OWNERSHIP                    | Table            | This privilege enables you to associate a DMF with a referenced table.                                                     |
| CREATE FUNCTION              | Schema           | This privilege enables creating new DMF in schema.                                                                         |


#### Querying DMF Results

In addition, the service account that will be executing DataHub Ingestion, and querying the DMF results, must have been granted the following system application roles:

| Role                           | Notes                       |
|--------------------------------|-----------------------------|
| DATA_QUALITY_MONITORING_VIEWER | Query the DMF results table |

To learn more about Snowflake DMFs and the privileges required to provision and query them, see the [Snowflake documentation](https://docs.snowflake.com/en/user-guide/data-quality-intro).

### Supported Assertion Types

The following assertion types are currently supported by the DataHub Snowflake DMF Assertion Compiler:

- [Freshness](/docs/managed-datahub/observe/freshness-assertions.md)
- [Volume](/docs/managed-datahub/observe/volume-assertions.md)
- [Column](/docs/managed-datahub/observe/column-assertions.md)
- [Custom SQL](/docs/managed-datahub/observe/custom-sql-assertions.md)

Note that Schema Assertions are not currently supported.

### Creating Snowflake DMF Assertions

The process for declaring and running assertions backend by Snowflake DMFs consists of a few steps, which will be outlined
in the following sections.


#### Step 1. Define your Data Quality assertions using Assertion YAML files

See the section **Declaring Assertions in YAML** below for examples of how to define assertions in YAML.


#### Step 2. Register your assertions with DataHub

Use the DataHub CLI to register your assertions with DataHub, so they become visible in the DataHub UI:

```bash
datahub assertions upsert -f examples/library/assertions_configuration.yml
```


#### Step 3. Compile the assertions into Snowflake DMFs using the DataHub CLI

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
# Example dmf_definitions.sql

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
# Example dmf_associations.sql

-- Start of Assertion 5c32eef47bd763fece7d21c7cbf6c659

            ALTER TABLE TEST_DB.PUBLIC.TEST_ASSERTIONS_ALL_TIMES SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
            ALTER TABLE TEST_DB.PUBLIC.TEST_ASSERTIONS_ALL_TIMES ADD DATA METRIC FUNCTION test_db.datahub_dmfs.datahub__5c32eef47bd763fece7d21c7cbf6c659 ON (col_date);

-- End of Assertion 5c32eef47bd763fece7d21c7cbf6c659
....
```


#### Step 4. Register the compiled DMFs in your Snowflake environment

Next, you'll need to run the generated SQL from the files output in Step 3 in Snowflake.

You can achieve this either by running the SQL files directly in the Snowflake UI, or by using the SnowSQL CLI tool:

```bash
snowsql -f dmf_definitions.sql
snowsql -f dmf_associations.sql
```


#### Step 5. Run ingestion to report the results back into DataHub

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


### FAQ

Coming soon!