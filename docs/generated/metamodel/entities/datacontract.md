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


**Python SDK: Create a Data Contract**

```python
# Inlined from /metadata-ingestion/examples/library/datacontract_create_basic.py
# metadata-ingestion/examples/library/datacontract_create_basic.py
import logging
import os

from datahub.emitter.mce_builder import make_assertion_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DataContractPropertiesClass,
    DataContractStateClass,
    DataContractStatusClass,
    DataQualityContractClass,
    FreshnessContractClass,
    SchemaContractClass,
    StatusClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

dataset_urn = make_dataset_urn(platform="snowflake", name="purchases", env="PROD")

schema_assertion_urn = make_assertion_urn("schema-assertion-for-purchases")
freshness_assertion_urn = make_assertion_urn("freshness-assertion-for-purchases")
quality_assertion_urn_1 = make_assertion_urn("quality-assertion-for-purchases-1")
quality_assertion_urn_2 = make_assertion_urn("quality-assertion-for-purchases-2")

contract_urn = "urn:li:dataContract:purchases-contract"

properties_aspect = DataContractPropertiesClass(
    entity=dataset_urn,
    schema=[SchemaContractClass(assertion=schema_assertion_urn)],
    freshness=[FreshnessContractClass(assertion=freshness_assertion_urn)],
    dataQuality=[
        DataQualityContractClass(assertion=quality_assertion_urn_1),
        DataQualityContractClass(assertion=quality_assertion_urn_2),
    ],
)

status_aspect = StatusClass(removed=False)

contract_status_aspect = DataContractStatusClass(state=DataContractStateClass.ACTIVE)

rest_emitter = DatahubRestEmitter(
    gms_server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
    token=os.getenv("DATAHUB_GMS_TOKEN"),
)

for event in MetadataChangeProposalWrapper.construct_many(
    entityUrn=contract_urn,
    aspects=[
        properties_aspect,
        status_aspect,
        contract_status_aspect,
    ],
):
    rest_emitter.emit(event)

log.info(f"Created data contract {contract_urn}")

```



### Schema Contracts

Schema contracts ensure that the structure of your data asset matches expectations. They are particularly important for:

- Preventing breaking changes in data pipelines
- Ensuring backward compatibility for downstream consumers
- Documenting the expected structure of data assets
- Validating that schema changes are intentional and controlled

A schema contract references a schema assertion entity, which contains the actual schema specification and validation logic. The assertion can be created using DataHub's built-in assertion framework or by integrating with external tools like dbt or Great Expectations.


**Python SDK: Add a schema contract to an existing Data Contract**

```python
# Inlined from /metadata-ingestion/examples/library/datacontract_add_schema_contract.py
# metadata-ingestion/examples/library/datacontract_add_schema_contract.py
import logging

from datahub.emitter.mce_builder import make_assertion_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    DataContractPropertiesClass,
    SchemaContractClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

contract_urn = "urn:li:dataContract:purchases-contract"

graph = DataHubGraph(config=DatahubClientConfig(server="http://localhost:8080"))

contract_properties = graph.get_aspect(
    entity_urn=contract_urn,
    aspect_type=DataContractPropertiesClass,
)

if not contract_properties:
    log.error(f"Contract {contract_urn} not found")
    exit(1)

new_schema_assertion_urn = make_assertion_urn("new-schema-assertion")

existing_schema = contract_properties.schema or []
existing_schema.append(SchemaContractClass(assertion=new_schema_assertion_urn))

contract_properties.schema = existing_schema

event = MetadataChangeProposalWrapper(
    entityUrn=contract_urn,
    aspect=contract_properties,
)

rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)

log.info(f"Added schema contract to {contract_urn}")

```



### Freshness Contracts

Freshness contracts define SLAs for how recent data should be. They help answer questions like:

- How often should this dataset be updated?
- What is the maximum acceptable data staleness?
- When can consumers expect new data to arrive?

Freshness contracts are critical for time-sensitive applications where stale data can lead to incorrect decisions or missed opportunities. They typically specify thresholds like "data should be no more than 2 hours old" or "data should be updated at least once per day."


**Python SDK: Add a freshness contract to an existing Data Contract**

```python
# Inlined from /metadata-ingestion/examples/library/datacontract_add_freshness_contract.py
# metadata-ingestion/examples/library/datacontract_add_freshness_contract.py
import logging

from datahub.emitter.mce_builder import make_assertion_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    DataContractPropertiesClass,
    FreshnessContractClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

contract_urn = "urn:li:dataContract:purchases-contract"

graph = DataHubGraph(config=DatahubClientConfig(server="http://localhost:8080"))

contract_properties = graph.get_aspect(
    entity_urn=contract_urn,
    aspect_type=DataContractPropertiesClass,
)

if not contract_properties:
    log.error(f"Contract {contract_urn} not found")
    exit(1)

new_freshness_assertion_urn = make_assertion_urn("new-freshness-assertion")

existing_freshness = contract_properties.freshness or []
existing_freshness.append(FreshnessContractClass(assertion=new_freshness_assertion_urn))

contract_properties.freshness = existing_freshness

event = MetadataChangeProposalWrapper(
    entityUrn=contract_urn,
    aspect=contract_properties,
)

rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)

log.info(f"Added freshness contract to {contract_urn}")

```



### Data Quality Contracts

Data quality contracts define expectations about the quality characteristics of your data. These can include:

- **Completeness**: No null values in critical columns, or null percentage below a threshold
- **Validity**: Values match expected formats, ranges, or patterns
- **Accuracy**: Values meet business rules and constraints
- **Consistency**: Relationships between columns are maintained
- **Custom checks**: Business-specific validation logic

Unlike schema and freshness contracts (which are typically singular per dataset), a Data Contract can contain multiple data quality assertions, each targeting different aspects of data quality.


**Python SDK: Add data quality contracts to an existing Data Contract**

```python
# Inlined from /metadata-ingestion/examples/library/datacontract_add_quality_contract.py
# metadata-ingestion/examples/library/datacontract_add_quality_contract.py
import logging

from datahub.emitter.mce_builder import make_assertion_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    DataContractPropertiesClass,
    DataQualityContractClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

contract_urn = "urn:li:dataContract:purchases-contract"

graph = DataHubGraph(config=DatahubClientConfig(server="http://localhost:8080"))

contract_properties = graph.get_aspect(
    entity_urn=contract_urn,
    aspect_type=DataContractPropertiesClass,
)

if not contract_properties:
    log.error(f"Contract {contract_urn} not found")
    exit(1)

new_quality_assertion_urn_1 = make_assertion_urn("completeness-check-user-id")
new_quality_assertion_urn_2 = make_assertion_urn("validity-check-email-format")

existing_quality = contract_properties.dataQuality or []
existing_quality.extend(
    [
        DataQualityContractClass(assertion=new_quality_assertion_urn_1),
        DataQualityContractClass(assertion=new_quality_assertion_urn_2),
    ]
)

contract_properties.dataQuality = existing_quality

event = MetadataChangeProposalWrapper(
    entityUrn=contract_urn,
    aspect=contract_properties,
)

rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)

log.info(f"Added data quality contracts to {contract_urn}")

```



### Contract Status

The `dataContractStatus` aspect tracks the current state of the contract. A contract can be in one of two states:

- **ACTIVE**: The contract is active and enforced. Violations should trigger alerts or block data pipelines.
- **PENDING**: The contract is pending implementation. It may be used for visibility or planning purposes but is not yet enforced.

The contract status can also include custom properties for additional metadata about the contract's state.


**Python SDK: Update the status of a Data Contract**

```python
# Inlined from /metadata-ingestion/examples/library/datacontract_update_status.py
# metadata-ingestion/examples/library/datacontract_update_status.py
import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DataContractStateClass,
    DataContractStatusClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

contract_urn = "urn:li:dataContract:purchases-contract"

contract_status_aspect = DataContractStatusClass(state=DataContractStateClass.ACTIVE)

event = MetadataChangeProposalWrapper(
    entityUrn=contract_urn,
    aspect=contract_status_aspect,
)

rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)

log.info(f"Updated status of data contract {contract_urn} to ACTIVE")

```



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



## Technical Reference Guide

The sections above provide an overview of how to use this entity. The following sections provide detailed technical information about how metadata is stored and represented in DataHub.

**Aspects** are the individual pieces of metadata that can be attached to an entity. Each aspect contains specific information (like ownership, tags, or properties) and is stored as a separate record, allowing for flexible and incremental metadata updates.

**Relationships** show how this entity connects to other entities in the metadata graph. These connections are derived from the fields within each aspect and form the foundation of DataHub's knowledge graph.

### Reading the Field Tables

Each aspect's field table includes an **Annotations** column that provides additional metadata about how fields are used:

- **⚠️ Deprecated**: This field is deprecated and may be removed in a future version. Check the description for the recommended alternative
- **Searchable**: This field is indexed and can be searched in DataHub's search interface
- **Searchable (fieldname)**: When the field name in parentheses is shown, it indicates the field is indexed under a different name in the search index. For example, `dashboardTool` is indexed as `tool`
- **→ RelationshipName**: This field creates a relationship to another entity. The arrow indicates this field contains a reference (URN) to another entity, and the name indicates the type of relationship (e.g., `→ Contains`, `→ OwnedBy`)

Fields with complex types (like `Edge`, `AuditStamp`) link to their definitions in the [Common Types](#common-types) section below.

### Aspects

#### dataContractProperties
Information about a data contract



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| entity | string | ✓ | The entity that this contract is associated with. Currently, we only support Dataset contracts, b... | → ContractFor |
| schema | SchemaContract[] |  | An optional set of schema contracts. If this is a dataset contract, there will only be one. | → IncludesSchemaAssertion |
| freshness | FreshnessContract[] |  | An optional set of FRESHNESS contracts. If this is a dataset contract, there will only be one. | → IncludesFreshnessAssertion |
| dataQuality | DataQualityContract[] |  | An optional set of Data Quality contracts, e.g. table and column level contract constraints. | → IncludesDataQualityAssertion |
| rawContract | string |  | YAML-formatted contract definition |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataContractProperties"
  },
  "name": "DataContractProperties",
  "namespace": "com.linkedin.datacontract",
  "fields": [
    {
      "Relationship": {
        "entityTypes": [
          "dataset"
        ],
        "name": "ContractFor"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "entity",
      "doc": "The entity that this contract is associated with. Currently, we only support Dataset contracts, but\nin the future we may also support Data Product level contracts."
    },
    {
      "Relationship": {
        "/*/assertion": {
          "entityTypes": [
            "assertion"
          ],
          "name": "IncludesSchemaAssertion"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "SchemaContract",
            "namespace": "com.linkedin.datacontract",
            "fields": [
              {
                "Relationship": {
                  "entityTypes": [
                    "assertion"
                  ],
                  "name": "IncludesSchemaAssertion"
                },
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "assertion",
                "doc": "The assertion representing the schema contract."
              }
            ],
            "doc": "Expectations for a logical schema"
          }
        }
      ],
      "name": "schema",
      "default": null,
      "doc": "An optional set of schema contracts. If this is a dataset contract, there will only be one."
    },
    {
      "Relationship": {
        "/*/assertion": {
          "entityTypes": [
            "assertion"
          ],
          "name": "IncludesFreshnessAssertion"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "FreshnessContract",
            "namespace": "com.linkedin.datacontract",
            "fields": [
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "assertion",
                "doc": "The assertion representing the SLA contract."
              }
            ],
            "doc": "A contract pertaining to the operational SLAs of a physical data asset"
          }
        }
      ],
      "name": "freshness",
      "default": null,
      "doc": "An optional set of FRESHNESS contracts. If this is a dataset contract, there will only be one."
    },
    {
      "Relationship": {
        "/*/assertion": {
          "entityTypes": [
            "assertion"
          ],
          "name": "IncludesDataQualityAssertion"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "DataQualityContract",
            "namespace": "com.linkedin.datacontract",
            "fields": [
              {
                "Relationship": {
                  "entityTypes": [
                    "assertion"
                  ],
                  "name": "IncludesDataQualityAssertion"
                },
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "assertion",
                "doc": "The assertion representing the Data Quality contract.\nE.g. a table or column-level assertion."
              }
            ],
            "doc": "A data quality contract pertaining to a physical data asset\nData Quality contracts are used to make assertions about data quality metrics for a physical data asset"
          }
        }
      ],
      "name": "dataQuality",
      "default": null,
      "doc": "An optional set of Data Quality contracts, e.g. table and column level contract constraints."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "rawContract",
      "default": null,
      "doc": "YAML-formatted contract definition"
    }
  ],
  "doc": "Information about a data contract"
}
```





#### dataContractStatus
Information about the status of a data contract



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| customProperties | map | ✓ | Custom property bag. | Searchable |
| state | DataContractState | ✓ | The latest state of the data contract | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataContractStatus"
  },
  "name": "DataContractStatus",
  "namespace": "com.linkedin.datacontract",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "fieldType": "TEXT",
          "queryByDefault": true
        }
      },
      "type": {
        "type": "map",
        "values": "string"
      },
      "name": "customProperties",
      "default": {},
      "doc": "Custom property bag."
    },
    {
      "Searchable": {},
      "type": {
        "type": "enum",
        "symbolDocs": {
          "ACTIVE": "The data contract is active.",
          "PENDING": "The data contract is pending implementation."
        },
        "name": "DataContractState",
        "namespace": "com.linkedin.datacontract",
        "symbols": [
          "ACTIVE",
          "PENDING"
        ]
      },
      "name": "state",
      "doc": "The latest state of the data contract"
    }
  ],
  "doc": "Information about the status of a data contract"
}
```





#### status
The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.
This aspect is used to represent soft deletes conventionally.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| removed | boolean | ✓ | Whether the entity has been removed (soft-deleted). Kept for backward compatibility. When lifecyc... | Searchable |
| lifecycleStage | string |  | The lifecycle stage of the entity, referencing a lifecycleStageType entity. When null, the entity... | Searchable |
| lifecycleLastUpdated | [AuditStamp](#auditstamp) |  | Attribution for the lifecycle stage transition — who moved the entity into its current stage and ... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "status"
  },
  "name": "Status",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "removed",
      "default": false,
      "doc": "Whether the entity has been removed (soft-deleted).\nKept for backward compatibility. When lifecycleStage is set to a stage\nwith hideInSearch=true, this field is NOT automatically synced \u2014 the\nsearch layer uses lifecycleStage settings directly."
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD",
        "queryByDefault": false
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "lifecycleStage",
      "default": null,
      "doc": "The lifecycle stage of the entity, referencing a lifecycleStageType entity.\nWhen null, the entity is in its default active state (visible in search).\nWhen set, the referenced lifecycle stage's settings determine behavior\n(e.g., hideInSearch=true excludes the entity from default search results).\n\nUsers can override default filtering by explicitly filtering on this field."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "AuditStamp",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "long",
              "name": "time",
              "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "actor",
              "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "impersonator",
              "default": null,
              "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "message",
              "default": null,
              "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
            }
          ],
          "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
        }
      ],
      "name": "lifecycleLastUpdated",
      "default": null,
      "doc": "Attribution for the lifecycle stage transition \u2014 who moved the entity\ninto its current stage and when. Populated automatically by the\nsetLifecycleStage mutation; should be set by any code path that\nwrites the lifecycleStage field."
    }
  ],
  "doc": "The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.\nThis aspect is used to represent soft deletes conventionally."
}
```





#### structuredProperties
Properties about an entity governed by StructuredPropertyDefinition



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| properties | StructuredPropertyValueAssignment[] | ✓ | Custom property bag. |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "structuredProperties"
  },
  "name": "StructuredProperties",
  "namespace": "com.linkedin.structured",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "StructuredPropertyValueAssignment",
          "namespace": "com.linkedin.structured",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "propertyUrn",
              "doc": "The property that is being assigned a value."
            },
            {
              "type": {
                "type": "array",
                "items": [
                  "string",
                  "double"
                ]
              },
              "name": "values",
              "doc": "The value assigned to the property."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "AuditStamp",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "impersonator",
                      "default": null,
                      "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "message",
                      "default": null,
                      "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                    }
                  ],
                  "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
                }
              ],
              "name": "created",
              "default": null,
              "doc": "Audit stamp containing who created this relationship edge and when"
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "lastModified",
              "default": null,
              "doc": "Audit stamp containing who last modified this relationship edge and when"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "structuredPropertyAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "structuredPropertyAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "structuredPropertyAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ]
        }
      },
      "name": "properties",
      "doc": "Custom property bag."
    }
  ],
  "doc": "Properties about an entity governed by StructuredPropertyDefinition"
}
```





### Common Types

These types are used across multiple aspects in this entity.

#### AuditStamp

Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage.

**Fields:**

- `time` (long): When did the resource/association/sub-resource move into the specific lifecyc...
- `actor` (string): The entity (e.g. a member URN) which will be credited for moving the resource...
- `impersonator` (string?): The entity (e.g. a service URN) which performs the change on behalf of the Ac...
- `message` (string?): Additional context around how DataHub was informed of the particular change. ...


### Relationships

#### Outgoing
These are the relationships stored in this entity's aspects
- ContractFor

   - Dataset via `dataContractProperties.entity`
- IncludesSchemaAssertion

   - Assertion via `dataContractProperties.schema`
   - Assertion via `dataContractProperties.schema.assertion`
- IncludesFreshnessAssertion

   - Assertion via `dataContractProperties.freshness`
- IncludesDataQualityAssertion

   - Assertion via `dataContractProperties.dataQuality`
   - Assertion via `dataContractProperties.dataQuality.assertion`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
