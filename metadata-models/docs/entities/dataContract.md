# Data Contract

A Data Contract is an agreement between a data asset's producer and consumer that defines expectations and guarantees about the quality, structure, and operational characteristics of data. Data Contracts serve as formal commitments that help establish trust and reliability in data pipelines by making explicit what data consumers can expect from data producers.

Data Contracts in DataHub are built on top of [assertions](./assertion.md) and represent a curated set of verifiable guarantees about a physical data asset. They are producer-oriented, meaning each physical data asset has one contract owned by its producer, which declares the standards and SLAs that consumers can rely on.

## Identity

Data Contracts are identified by a single unique string identifier:

- The unique contract id: A string identifier that uniquely identifies the contract. This can be any string value and is often auto-generated based on the entity being contracted.

The URN structure for a Data Contract is: `urn:li:dataContract:<contract-id>`

Example URNs:

- `urn:li:dataContract:my-critical-dataset-contract`
- `urn:li:dataContract:a1b2c3d4-e5f6-7890-abcd-ef1234567890`

When creating Data Contracts programmatically, the contract ID can be explicitly specified, or it can be auto-generated based on the entity being contracted. The auto-generation creates a stable, deterministic ID using a GUID derived from the entity URN, ensuring that contracts are reproducible across multiple runs.

## Important Capabilities

Data Contracts provide three main types of guarantees through assertions:

### Contract Properties

The `dataContractProperties` aspect defines the core characteristics of a contract, including:

- **Entity Association**: The URN of the entity (typically a dataset) that this contract applies to. Currently, DataHub supports contracts for datasets, with future support planned for data products and other entity types.
- **Schema Contracts**: Assertions that define expectations about the logical schema of the data asset. Schema contracts ensure that the structure of your data remains consistent and matches what consumers expect.
- **Freshness Contracts**: Assertions that define operational SLAs for data freshness. These contracts specify how up-to-date the data should be, helping consumers understand the recency guarantees they can rely on.
- **Data Quality Contracts**: Assertions that define quality expectations at the table or column level. These include constraints on data completeness, accuracy, validity, and custom quality checks.
- **Raw Contract**: An optional YAML-formatted string representation of the contract definition, useful for storing the original contract specification.

Each contract type (schema, freshness, data quality) contains references to assertion entities, which define the actual validation logic and evaluation criteria.

The following code snippet shows how to create a basic Data Contract with schema, freshness, and data quality assertions.

<details>
<summary>Python SDK: Create a Data Contract</summary>

```python
{{ inline /metadata-ingestion/examples/library/datacontract_create_basic.py show_path_as_comment }}
```

</details>

### Schema Contracts

Schema contracts ensure that the structure of your data asset matches expectations. They are particularly important for:

- Preventing breaking changes in data pipelines
- Ensuring backward compatibility for downstream consumers
- Documenting the expected structure of data assets
- Validating that schema changes are intentional and controlled

A schema contract references a schema assertion entity, which contains the actual schema specification and validation logic. The assertion can be created using DataHub's built-in assertion framework or by integrating with external tools like dbt or Great Expectations.

<details>
<summary>Python SDK: Add a schema contract to an existing Data Contract</summary>

```python
{{ inline /metadata-ingestion/examples/library/datacontract_add_schema_contract.py show_path_as_comment }}
```

</details>

### Freshness Contracts

Freshness contracts define SLAs for how recent data should be. They help answer questions like:

- How often should this dataset be updated?
- What is the maximum acceptable data staleness?
- When can consumers expect new data to arrive?

Freshness contracts are critical for time-sensitive applications where stale data can lead to incorrect decisions or missed opportunities. They typically specify thresholds like "data should be no more than 2 hours old" or "data should be updated at least once per day."

<details>
<summary>Python SDK: Add a freshness contract to an existing Data Contract</summary>

```python
{{ inline /metadata-ingestion/examples/library/datacontract_add_freshness_contract.py show_path_as_comment }}
```

</details>

### Data Quality Contracts

Data quality contracts define expectations about the quality characteristics of your data. These can include:

- **Completeness**: No null values in critical columns, or null percentage below a threshold
- **Validity**: Values match expected formats, ranges, or patterns
- **Accuracy**: Values meet business rules and constraints
- **Consistency**: Relationships between columns are maintained
- **Custom checks**: Business-specific validation logic

Unlike schema and freshness contracts (which are typically singular per dataset), a Data Contract can contain multiple data quality assertions, each targeting different aspects of data quality.

<details>
<summary>Python SDK: Add data quality contracts to an existing Data Contract</summary>

```python
{{ inline /metadata-ingestion/examples/library/datacontract_add_quality_contract.py show_path_as_comment }}
```

</details>

### Contract Status

The `dataContractStatus` aspect tracks the current state of the contract. A contract can be in one of two states:

- **ACTIVE**: The contract is active and enforced. Violations should trigger alerts or block data pipelines.
- **PENDING**: The contract is pending implementation. It may be used for visibility or planning purposes but is not yet enforced.

The contract status can also include custom properties for additional metadata about the contract's state.

<details>
<summary>Python SDK: Update the status of a Data Contract</summary>

```python
{{ inline /metadata-ingestion/examples/library/datacontract_update_status.py show_path_as_comment }}
```

</details>

### Tags and Glossary Terms

Like other DataHub entities, Data Contracts can have tags and glossary terms attached to them. These help with:

- Categorizing contracts by domain, criticality, or other dimensions
- Linking contracts to business glossary terms for better understanding
- Filtering and searching for contracts in the DataHub UI

Tags and terms on Data Contracts follow the same patterns as other entities, using the `globalTags` and `glossaryTerms` aspects.

### Structured Properties

Data Contracts support structured properties, which allow you to attach custom, strongly-typed metadata to contracts. This is useful for:

- Adding contract-specific metadata like SLA tier, enforcement level, or review status
- Integrating with external contract management systems
- Capturing business context and ownership information

Structured properties are defined at the platform level and can be applied to any Data Contract entity.

## Integration Points

### Relationship to Assertions

Data Contracts are built on top of the [assertion](./assertion.md) entity. Each contract contains references to assertion URNs that define the actual validation logic. This separation allows:

- Reusing assertions across multiple contracts
- Managing assertion logic independently from contract definitions
- Evaluating assertions outside the context of a contract
- Tracking assertion results and history

The relationship between contracts and assertions is established through the `ContractFor` relationship (contract to entity) and `IncludesSchemaAssertion`, `IncludesFreshnessAssertion`, and `IncludesDataQualityAssertion` relationships (contract to assertions).

### Relationship to Datasets

Currently, Data Contracts are primarily associated with [dataset](./dataset.md) entities. The `dataContractProperties` aspect includes an `entity` field that references the dataset URN. This relationship is captured using the `ContractFor` relationship type.

A dataset can have one active Data Contract at a time, though the contract can be updated or replaced. Consumers can query a dataset to retrieve its associated contract and understand the guarantees they can expect.

### Integration with Data Quality Tools

Data Contracts integrate with external data quality and testing tools:

- **dbt Tests**: dbt test results can be ingested into DataHub as assertions, which can then be referenced in Data Contracts. This allows you to use dbt's testing framework while managing contracts in DataHub.
- **Great Expectations**: Great Expectations checkpoints can publish assertion results to DataHub using the DataHubValidationAction, making expectation suites part of your Data Contracts.
- **Custom Tools**: Any external system can publish assertion results to DataHub via API, allowing you to build Data Contracts on top of your existing data quality infrastructure.

### GraphQL API

Data Contracts are accessible through DataHub's GraphQL API, which provides:

- **Upsert Operations**: Create or update contracts using the `upsertDataContract` mutation
- **Query Operations**: Retrieve contract details, including all associated assertions
- **Contract Evaluation**: Check contract status by evaluating all associated assertions
- **Entity Resolution**: Navigate from datasets to their contracts and vice versa

The GraphQL API is particularly useful for integrating Data Contracts into CI/CD pipelines, custom UIs, or workflow orchestration systems.

### REST API

Data Contracts can be created, read, updated, and deleted using DataHub's REST API. The standard entity CRUD operations apply:

- `POST /entities` - Create or update a Data Contract entity
- `GET /entities/urn:li:dataContract:<id>` - Retrieve a Data Contract by URN
- `DELETE /entities/urn:li:dataContract:<id>` - Remove a Data Contract

Aspects can be individually updated using the aspect-specific endpoints, allowing fine-grained control over contract properties and status.

## Notable Exceptions

### Producer vs Consumer Orientation

DataHub Data Contracts are producer-oriented, meaning each physical data asset has one contract owned by the producer. This design choice keeps contracts manageable and ensures clear ownership.

However, this may not fit all use cases. Some organizations prefer consumer-oriented contracts where each consumer defines their own expectations for a shared data asset. While DataHub doesn't directly support consumer-oriented contracts, you can achieve similar functionality by:

- Creating multiple assertions on the same dataset with different owners
- Using tags or structured properties to indicate which consumers care about which assertions
- Building custom workflows that evaluate subsets of assertions for different consumers

### Contract Enforcement

Data Contracts in DataHub define expectations but do not automatically enforce them. Enforcement depends on:

- Setting up assertion evaluation (either using DataHub's built-in capabilities or external tools)
- Configuring alerts and notifications when contracts are violated
- Integrating contract checks into data pipelines and CI/CD workflows

DataHub provides the framework for defining and tracking contracts, but actual enforcement requires additional integration work specific to your data infrastructure.

### Assertion Lifecycle

When you delete a Data Contract, the associated assertions are not automatically deleted. This is by design - assertions can exist independently and may be used by other contracts or monitored separately.

If you want to remove both a contract and its assertions, you must delete them separately. This ensures that assertion definitions and their historical results are preserved even when contracts change.

### YAML-Based Contract Definitions

DataHub supports defining Data Contracts in YAML files using the `DataContract` Python model. This provides a simpler, declarative way to define contracts that can be version-controlled and reviewed like code.

The YAML format is particularly useful for:

- Defining contracts as part of infrastructure-as-code workflows
- Storing contract definitions alongside dbt models or other data pipeline code
- Reviewing contract changes through standard code review processes

However, YAML-based contracts are converted to the underlying MCP (Metadata Change Proposal) format when ingested, so all operations ultimately use the same underlying entity and aspect structure.
