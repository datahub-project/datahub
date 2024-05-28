# DataHub's Open Data Quality Assertions Specification

DataHub is developing an open-source Data Quality Assertions Specification & Compiler that will allow you to declare data quality checks / expectations / assertions using in a simple, universal
YAML-based format, and then compile them to into artifacts that can be registered or directly executed by 3rd party Data Quality tools like [Snowflake DMFs](https://docs.snowflake.com/en/user-guide/data-quality-intro), 
dbt tests, Great Expectations or Acryl Cloud natively. 

Ultimately, our goal is to provide an framework-agnostic, highly-portable format for defining Data Quality checks, making it seamless to swap out the underlying
assertion engine without service disruption for end consumers of the results of these data quality checks in catalogging tools like DataHub. 

## Integrations

Currently, the DataHub Open Assertions Specification supports the following integrations:

- Snowflake via [Snowflake DMFs](https://docs.snowflake.com/en/user-guide/data-quality-intro)

And is looking for contributions to build out support for the following integrations:

- [Looking for Contributions] dbt tests
- [Looking for Contributions] Great Expectation checks

Below, we'll look at how to define assertions in YAML, and then provide an usage overview for each support integration.

## The Specification: Declaring Data Quality Assertions in YAML

The following assertion types are currently supported by the DataHub YAML Assertion spec:

- [Freshness](/docs/managed-datahub/observe/freshness-assertions.md) 
- [Volume](/docs/managed-datahub/observe/volume-assertions.md)
- [Column](/docs/managed-datahub/observe/column-assertions.md)
- [Custom SQL](/docs/managed-datahub/observe/custom-sql-assertions.md)
- [Schema](/docs/managed-datahub/observe/schema-assertions.md)

Each assertion type aims to validate a different aspect of a warehouse or table table, from
structure to size to column integrity to custom metrics. 

In this section, we'll go over examples of defining each. 

### Freshness Assertions

Freshness Assertions allow you to verify that your data was updated within the expected timeframe.
Below you'll find examples of defining different types of freshness assertions via YAML. 

<TODO Mayuri Help> 

### Volume Assertions

Volume Assertions allow you to verify that the number of records in your dataset meets your expectations.
Below you'll find examples of defining different types of volume assertions via YAML.

<TODO Mayuri Help> 

### Column Assertions

Column Assertions allow you to verify that the values in a column meet your expectations.
Below you'll find examples of defining different types of column assertions via YAML.

<TODO Mayuri Help> 

### Custom SQL Assertions

Custom SQL Assertions allow you to define custom SQL queries to verify your data meets your expectations.
Below you'll find examples of defining different types of custom SQL assertions via YAML.

<TODO Mayuri Help> 

### Schema Assertions

Schema Assertions allow you to define custom SQL queries to verify your data meets your expectations.
Below you'll find examples of defining different types of custom SQL assertions via YAML.

<TODO Mayuri Help> 

## Snowflake

The DataHub Open Assertion Compiler allows you to define your Data Quality assertions in a simple YAML format, and then compile them to be executed by Snowflake Data Metric Functions.
Once compiled, you'll be able to register the compiled DMFs in your Snowflake environment, and extract their results them as part of your normal ingestion process for DataHub.
Results of Snowflake DMF assertions will be reported as normal Assertion Results, viewable on a historical timeline in the context
of the table with which they are associated.

### Prerequisites

- You must have a Snowflake Enterprise account, where the DMFs feature is enabled.
- You must have the necessary permissions to create and run DMFs in your Snowflake environment.
- You must have the necessary permissions to query the DMF results in your Snowflake environment.

To learn more about Snowflake DMFs, see the [Snowflake documentation](https://docs.snowflake.com/en/user-guide/data-quality-intro).

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
datahub assertions compile -f examples/library/assertions_configuration.yml -p snowflake
```

This will generate <MAYURI TODO>.


#### Step 4. Register the compiled DMFs in your Snowflake environment

<TODO - Mayuri what should this step look like?>


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

This will query the DMF results store in Snowflake, convert them into DataHub Assertion Results, and report the results back into DataHub:

`datahub ingest -c snowflake.yml`

## dbt test

Seeking contributions!

## Great Expectations

Seeking contributions!

## Soda SQL 

Seeking contributions!