# Assertions

:::note Contract Monitoring Support
Currently we support Snowflake, Redshift, BigQuery, and Databricks for out-of-the-box contract monitoring as part of Acryl Observe.
:::

An assertion is **a data quality test that finds data that violates a specified rule.** 
Assertions serve as the building blocks of [Data Contracts](/docs/managed-datahub/observe/data-contract.md) – this is how we verify the contract is met.

## How to Create and Run Assertions

Data quality tests (a.k.a. assertions) can be created and run by Acryl or ingested from a 3rd party tool.

### Acryl Observe

For Acryl-provided assertion runners, we can deploy an agent in your environment to hit your sources and DataHub. Acryl Observe offers out-of-the-box evaluation of the following kinds of assertions:

- [Freshness](/docs/managed-datahub/observe/freshness-assertions.md) (SLAs)
- [Volume](/docs/managed-datahub/observe/volume-assertions.md)
- [Custom SQL](/docs/managed-datahub/observe/custom-sql-assertions.md)
- [Column](/docs/managed-datahub/observe/column-assertions.md)

These can be defined through the DataHub API or the UI. 

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/assertions/assertion-ui.png"/>
</p>

### 3rd Party Runners

You can integrate 3rd party tools as follows:

- [DBT Test](/docs/generated/ingestion/sources/dbt.md#integrating-with-dbt-test)
- [Great Expectations](../../../metadata-ingestion/integration_docs/great-expectations.md)

If you opt for a 3rd party tool, it will be your responsibility to ensure the assertions are run based on the Data Contract spec stored in DataHub. With 3rd party runners, you can get the Assertion Change events by subscribing to our Kafka topic using the [DataHub Actions Framework](/docs/actions/README.md).


## Alerts

Beyond the ability to see the results of the assertion checks (and history of the results) both on the physical asset’s page in the DataHub UI and as the result of DataHub API calls, you can also get notified via [Slack messages](/docs/managed-datahub/slack/saas-slack-setup.md) (DMs or to a team channel) based on your [subscription](https://youtu.be/VNNZpkjHG_I?t=79) to an assertion change event. In the future, we’ll also provide the ability to subscribe directly to contracts.

With Acryl Observe, you can get the Assertion Change event by getting API events via [AWS EventBridge](/docs/managed-datahub/operator-guide/setting-up-events-api-on-aws-eventbridge.md) (the availability and simplicity of setup of each solution dependent on your current Acryl setup – chat with your Acryl representative to learn more).


## Cost

We provide a plethora of ways to run your assertions, aiming to allow you to use the cheapest possible means to do so and/or the most accurate means to do so, depending on your use case. For example, for Freshness (SLA) assertions, it is relatively cheap to use either their Audit Log or Information Schema as a means to run freshness checks, and we support both of those as well as Last Modified Column, High Watermark Column, and DataHub Operation ([see the docs for more details](/docs/managed-datahub/observe/freshness-assertions.md#3-change-source)).

## Execution details - Where and How

There are a few ways DataHub Cloud assertions can be executed:
1. Directly query the source system:
  a. `Information Schema` tables are used by default to power cheap, fast checks on a table's freshness or row count.
  b. `Audit log` or `Operation log` tables can be used to granularly monitor table operations.
  c. The table itself can also be queried directly. This is useful for freshness checks referencing `last_updated` columns, row count checks targetting a subset of the data, and column value checks. We offer several optimizations to reduce query costs for these checks.
2. Reference DataHub profiling information
  a. `Operation`s that are reported via ingestion or our SDKs can power monitoring table freshness.
  b. `DatasetProfile` and `SchemaFieldProfile` ingested or reported via SDKs can power monitoring table metrics and column metrics.

### Privacy: Execute In-Network, avoid exposing data externally
As a part of DataHub Cloud, we offer a [Remote Executor](/docs/managed-datahub/operator-guide/setting-up-remote-ingestion-executor.md) deployment model. If this model is used, assertions will execute within your network, and only the results will be sent back to DataHub Cloud. Neither your actual credentials, nor your source data will leave your network.

### Source system selection
Assertions will execute queries using the same source system that was used to initially ingest the table.
There are some scenarios where customers may have multiple ingestion sources for, i.e. a BigQuery table. In this case, by default the executor will take the ingestion source that was used to ingest the table's `DatasetProperties`. This behavior can be modified by your customer success rep.
