# Assertions

_Note: currently we support Snowflake, Databricks, Redshift, and BigQuery for out-of-the-box contract monitoring as part of Acryl Observe._


## What is an Assertion

An assertion is a data quality test that finds data that violate one or more specified rules. These serve as the backbone of Data Contracts – this is how we verify the contract is met. 

## How to Create and Run Assertions

Data quality tests (a.k.a. assertions) can be run by either Acryl or ingested from a 3rd party tool. 

### 3rd Party Runners

You can integrate 3rd party tools as follows:

- [DBT Test](https://datahubproject.io/docs/generated/ingestion/sources/dbt#integrating-with-dbt-test)
- [Great Expectations](https://datahubproject.io/docs/metadata-ingestion/integration_docs/great-expectations/)

****

If you opt for a 3rd party tool, it will be your responsibility to ensure the assertions are run based on the Data Contract spec stored in DataHub. With 3rd party runners, you can get the Assertion Change events by subscribing to our Kafka topic using the [DataHub Actions Framework](https://datahubproject.io/docs/actions). 


### Acryl Observe

For Acryl-provided assertion runners, we can deploy an agent in your environment to hit your sources and DataHub. Acryl Observe offers out-of-the-box evaluation of the following kinds of assertions:

- [Freshness](https://datahubproject.io/docs/managed-datahub/observe/freshness-assertions) (SLAs)
- [Volume](https://datahubproject.io/docs/managed-datahub/observe/volume-assertions)
- [Custom SQL](https://datahubproject.io/docs/managed-datahub/observe/custom-sql-assertions)
- [Column](https://datahubproject.io/docs/managed-datahub/observe/column-assertions)

****

These can be defined through the DataHub API or the UI. With Acryl Observe, you can get the Assertion Change event by getting API events via [AWS EventBridge](https://datahubproject.io/docs/managed-datahub/operator-guide/setting-up-events-api-on-aws-eventbridge/) (the availability and simplicity of setup of each solution dependent on your current Acryl setup – chat with your Acryl representative to learn more).

****

Assertions UI example

![](https://lh7-us.googleusercontent.com/Cbo_rujT4QBYGzqMhxk7wNsaiWw6_04biMtC4-qPg-WJdP1VvwKOBcwpQg4j34WfWOvHuCmldP7-GUh-v9Y1YVGYyr1A4qzyolqAT7rC7pU1_0RhrtHDRvWiUZIXh9tB92_4rYkSDNCK6eykMb4Vels)


## Alerts

Beyond the ability to see the results of the assertion checks (and history of the results) both on the physical asset’s page in the DataHub UI and as the result of DataHub API calls, you can also get notified via [slack messages](https://datahubproject.io/docs/managed-datahub/saas-slack-setup/) (DMs or to a team channel) based on your [subscription](https://youtu.be/VNNZpkjHG_I?t=79) to an assertion change event. In the future, we’ll also provide the ability to subscribe directly to contracts.


## Cost

We provide a plethora of ways to run your assertions, aiming to allow you to use the cheapest possible means to do so and/or the most accurate means to do so, depending on your use case. For example, for Freshness (SLA) assertions, it is relatively cheap to use either their Audit Log or Information Schema as a means to run freshness checks, and we support both of those as well as Last Modified Column, High Watermark Column, and DataHub Operation ([see the docs for more details](https://datahubproject.io/docs/managed-datahub/observe/freshness-assertions/#3-change-source)).
