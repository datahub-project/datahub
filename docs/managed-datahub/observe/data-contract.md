# Data Contracts

## What Is a Data Contract

The definition of a Data Contract is consisted of the following:

- **Verifiable** : based on the actual physical data asset, not its metadata (e.g., schema checks, column-level data checks, and operational SLA-s but not documentation, ownership, and tags).
- **A set of assertions** : The actual checks against the physical asset to determine a contract’s status (schema, freshness, volume, custom, and column)
- **Producer oriented** : One contract per physical data asset, owned by the producer.


<details>
<summary>Consumer Oriented Data contracts</summary>
We’ve gone with producer-oriented contracts to keep the number of contracts manageable and because we expect consumers to desire a lot of overlap in a given physical asset’s contract. Although, we've heard feedback that consumer-oriented data contracts meet certain needs that producer-oriented contracts do not. For example, having one contract per consumer all on the same physical data asset would allow each consumer to get alerts only when the assertions they care about are violated.We welcome feedback on this in slack!
</details>

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/data_contracts/validated-data-contracts-ui.png"/>
</p>

## Data Contract and Assertions

Another way to word our vision of data contracts is **A bundle of verifiable assertions on physical data assets representing a public producer commitment.** 
These can be all the assertions on an asset or only the subset you want publicly promised to consumers. Data Contracts allow you to **promote a selected group of your assertions** as a public promise: if this subset of assertions is not met, the Data Contract is failing.

See docs on [assertions](/docs/managed-datahub/observe/assertions.md) for more details on the types of assertions and how to create and run them.

:::note Ownership 
the owner of the physical data asset is also the owner of the contract and can accept proposed changes and make changes themselves to the contract.
:::


## How to Create Data Contracts

Data Contracts can be created via DataHub CLI (YAML), API, or UI.

### DataHub CLI using YAML

For creation via CLI, it’s a simple CLI upsert command that you can integrate into your CI/CD system to publish your Data Contracts and any change to them.

1. Define your data contract.
```yaml
# id: sample_data_contract # Optional: if not provided, an id will be generated
entity: urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)
version: 1
freshness:
  type: cron
  cron: "4 8 * * 1-5"
data_quality:
  - type: unique
    column: field_foo
## here's an example of how you'd define the schema 
# schema:
#   type: json-schema
#   json-schema:
#     type: object
#     properties:
#       field_foo:
#         type: string
#         native_type: VARCHAR(100)
#       field_bar:
#         type: boolean
#         native_type: boolean
#       field_documents:
#         type: array
#         items:
#           type: object
#           properties:
#             docId:
#               type: object
#               properties:
#                 docPolicy:
#                   type: object 
#                   properties:
#                     policyId:
#                       type: integer
#                     fileId:
#                       type: integer
#     required:
#       - field_bar
#       - field_documents

```
2. Use the CLI to create the contract by running the below command.

```shell
datahub datacontract upsert -f contract_definition.yml
```

3. Now you can see your contract on the UI.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/data_contracts/data-contracts-ui.png"/>
</p>


### UI

1. Navigate to the Dataset Profile for the dataset you wish to create a contract for
2. Under the **Validations** > **Data Contracts** tab, click **Create**.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/data_contracts/create-data-contract-ui.png"/>
</p>


3. Select the assertions you wish to be included in the Data Contract.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/data_contracts/select-assertions.png"/>
</p>


:::note Create Data Contracts via UI
Please note that when creating a Data Contract via UI, the Freshness, Schema, and Data Quality assertions are expected to have been created already.
:::
4. Now you can see it in the UI.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/data_contracts/contracts-created.png"/>
</p>


### API

:::note
API guide on creating data contract is coming soon!
:::

## How to Run Data Contracts

Running Data Contracts is dependent on running the contract’s assertions and getting the results on Datahub. Using Acryl Observe (available on SAAS), you can schedule assertions on Datahub itself. Otherwise, you can run your assertions outside of Datahub and have the results published back to Datahub. 

Datahub integrates nicely with DBT Test and Great Expectations, as described below. For other 3rd party assertion runners, you’ll need to use our APIs to publish the assertion results back to our platform.

### DBT Test

During DBT Ingestion, we pick up the dbt `run_results` file, which contains the dbt test run results, and translate it into assertion runs. [See details here.](/docs/generated/ingestion/sources/dbt.md#module-dbt)

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/data_contracts/dbt-test.png"/>
</p>



### Great Expectations

For Great Expectations, you can integrate the **DataHubValidationAction** directly into your Great Expectations Checkpoint in order to have the assertion (aka. expectation) results to Datahub. [See the guide here](../../../metadata-ingestion/integration_docs/great-expectations.md).

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/data_contracts/gx-test.png"/>
</p>
