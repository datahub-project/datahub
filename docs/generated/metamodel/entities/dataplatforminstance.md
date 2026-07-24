# Data Platform Instance

A Data Platform Instance represents a specific deployment or instance of a data platform. While a [dataPlatform](./dataPlatform.md) represents a technology type (e.g., MySQL, Snowflake, BigQuery), a dataPlatformInstance represents a particular running instance of that platform (e.g., "production-mysql-cluster", "dev-snowflake-account", "analytics-bigquery-project").

This entity is crucial for organizations that run multiple instances of the same platform technology across different environments, regions, or organizational units. It enables DataHub to distinguish between assets from different platform instances and provides a way to organize and manage platform-level metadata and credentials.

## Identity

Data Platform Instances are identified by two components:

- **Platform**: The URN of the data platform technology (e.g., `urn:li:dataPlatform:snowflake`)
- **Instance**: A unique string identifier for this specific instance (e.g., "prod-us-west-2", "dev-cluster-01")

The complete URN follows the pattern:

```
urn:li:dataPlatformInstance:(urn:li:dataPlatform:<platform>,<instance_id>)
```

### Examples

- `urn:li:dataPlatformInstance:(urn:li:dataPlatform:mysql,production-mysql-01)`
  - A production MySQL database cluster
- `urn:li:dataPlatformInstance:(urn:li:dataPlatform:snowflake,acme-prod-account)`
  - A production Snowflake account
- `urn:li:dataPlatformInstance:(urn:li:dataPlatform:bigquery,analytics-project)`
  - A BigQuery project used for analytics
- `urn:li:dataPlatformInstance:(urn:li:dataPlatform:iceberg,data-lake-warehouse)`
  - An Iceberg warehouse instance

## Important Capabilities

### Platform Instance Properties

The `dataPlatformInstanceProperties` aspect contains descriptive metadata about the platform instance:

- **name**: A display-friendly name for the instance (searchable, supports autocomplete)
- **description**: Documentation explaining the purpose, usage, or characteristics of this instance
- **customProperties**: Key-value pairs for additional custom metadata
- **externalUrl**: A link to external documentation or management console for this instance

This aspect helps users understand what each platform instance represents and how it should be used.


**Python SDK: Create a platform instance with properties**

```python
# Inlined from /metadata-ingestion/examples/library/platform_instance_create.py
# metadata-ingestion/examples/library/platform_instance_create.py
import os

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DataPlatformInstancePropertiesClass

# Create the platform instance URN
platform_instance_urn = builder.make_dataplatform_instance_urn(
    platform="mysql", instance="production-mysql-cluster"
)

# Define properties for the platform instance
platform_properties = DataPlatformInstancePropertiesClass(
    name="Production MySQL Cluster",
    description="Primary MySQL database cluster serving production workloads in US West region",
    customProperties={
        "region": "us-west-2",
        "environment": "production",
        "cluster_size": "3-node",
        "version": "8.0.35",
    },
    externalUrl="https://cloud.mysql.com/console/clusters/prod-cluster",
)

# Create metadata change proposal
platform_instance_mcp = MetadataChangeProposalWrapper(
    entityUrn=platform_instance_urn,
    aspect=platform_properties,
)

# Emit metadata to DataHub
gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
emitter = DatahubRestEmitter(gms_server=gms_server, token=token)
emitter.emit_mcp(platform_instance_mcp)

print(f"Created platform instance: {platform_instance_urn}")

```



### Iceberg Warehouse Configuration

DataHub can serve as an Iceberg catalog, managing Iceberg tables through platform instances. The `icebergWarehouseInfo` aspect stores the configuration needed to manage an Iceberg warehouse:

- **dataRoot**: S3 path to the root location for table storage
- **clientId**: URN reference to the AWS access key ID secret
- **clientSecret**: URN reference to the AWS secret access key secret
- **region**: AWS region where the warehouse is located
- **role**: IAM role ARN used for credential vending
- **tempCredentialExpirationSeconds**: Expiration time for temporary credentials
- **env**: Environment/fabric type (PROD, DEV, QA, etc.)

This enables DataHub to manage Iceberg tables as a REST catalog, handling metadata operations and credential vending for data access.

The `datahub iceberg` CLI provides commands to create, update, list, and delete Iceberg warehouses. See the [Iceberg integration documentation](https://datahubproject.io/docs/generated/ingestion/sources/iceberg) for details.

### Ownership and Tags

Like other DataHub entities, platform instances support:

- **ownership**: Track who owns or manages this platform instance
- **globalTags**: Apply tags for categorization (e.g., "production", "pci-compliant", "deprecated")
- **institutionalMemory**: Add links to runbooks, documentation, or related resources

These aspects enable governance and discoverability of platform instances.


**Python SDK: Add metadata to a platform instance**

```python
# Inlined from /metadata-ingestion/examples/library/platform_instance_add_metadata.py
# metadata-ingestion/examples/library/platform_instance_add_metadata.py
import time

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    GlobalTagsClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    TagAssociationClass,
)

# Create the platform instance URN
platform_instance_urn = builder.make_dataplatform_instance_urn(
    platform="snowflake", instance="acme-prod-account"
)

# Add ownership
owners = [
    OwnerClass(
        owner=builder.make_user_urn("data-platform-team"),
        type=OwnershipTypeClass.TECHNICAL_OWNER,
    ),
    OwnerClass(
        owner=builder.make_user_urn("john.doe"),
        type=OwnershipTypeClass.DATAOWNER,
    ),
]

ownership_mcp = MetadataChangeProposalWrapper(
    entityUrn=platform_instance_urn,
    aspect=OwnershipClass(owners=owners),
)

# Add tags
tags = GlobalTagsClass(
    tags=[
        TagAssociationClass(tag=builder.make_tag_urn("production")),
        TagAssociationClass(tag=builder.make_tag_urn("pci-compliant")),
        TagAssociationClass(tag=builder.make_tag_urn("tier-1")),
    ]
)

tags_mcp = MetadataChangeProposalWrapper(
    entityUrn=platform_instance_urn,
    aspect=tags,
)

# Add institutional memory (links)
links = InstitutionalMemoryClass(
    elements=[
        InstitutionalMemoryMetadataClass(
            url="https://wiki.company.com/snowflake-prod-runbook",
            description="Production Snowflake Runbook",
            createStamp=AuditStampClass(
                time=int(time.time() * 1000), actor=builder.make_user_urn("datahub")
            ),
        ),
        InstitutionalMemoryMetadataClass(
            url="https://wiki.company.com/snowflake-access-guide",
            description="How to request access to production Snowflake",
            createStamp=AuditStampClass(
                time=int(time.time() * 1000), actor=builder.make_user_urn("datahub")
            ),
        ),
    ]
)

links_mcp = MetadataChangeProposalWrapper(
    entityUrn=platform_instance_urn,
    aspect=links,
)

# Emit all metadata changes
emitter = DatahubRestEmitter("http://localhost:8080")
emitter.emit_mcp(ownership_mcp)
emitter.emit_mcp(tags_mcp)
emitter.emit_mcp(links_mcp)

print(f"Added ownership, tags, and links to: {platform_instance_urn}")

```



### Status and Deprecation

Platform instances can be marked with status information:

- **status**: Indicates if the instance is active or has been removed
- **deprecation**: Mark instances as deprecated when they are being phased out, with optional decommission date and migration notes

This helps communicate lifecycle information about platform instances to users.

## Code Examples

### Creating a Platform Instance

The most common way to create platform instances is through the ingestion framework, which automatically creates them when the `platform_instance` configuration is specified in source configs. However, you can also create them programmatically:


**Python SDK: Create a data platform instance**

```python
# Inlined from /metadata-ingestion/examples/library/platform_instance_create.py
# metadata-ingestion/examples/library/platform_instance_create.py
import os

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DataPlatformInstancePropertiesClass

# Create the platform instance URN
platform_instance_urn = builder.make_dataplatform_instance_urn(
    platform="mysql", instance="production-mysql-cluster"
)

# Define properties for the platform instance
platform_properties = DataPlatformInstancePropertiesClass(
    name="Production MySQL Cluster",
    description="Primary MySQL database cluster serving production workloads in US West region",
    customProperties={
        "region": "us-west-2",
        "environment": "production",
        "cluster_size": "3-node",
        "version": "8.0.35",
    },
    externalUrl="https://cloud.mysql.com/console/clusters/prod-cluster",
)

# Create metadata change proposal
platform_instance_mcp = MetadataChangeProposalWrapper(
    entityUrn=platform_instance_urn,
    aspect=platform_properties,
)

# Emit metadata to DataHub
gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
emitter = DatahubRestEmitter(gms_server=gms_server, token=token)
emitter.emit_mcp(platform_instance_mcp)

print(f"Created platform instance: {platform_instance_urn}")

```



### Attaching Platform Instance to Datasets

When ingesting metadata, the `dataPlatformInstance` aspect links datasets to their platform instance. This is typically done by ingestion connectors but can also be done manually:


**Python SDK: Attach platform instance to a dataset**

```python
# Inlined from /metadata-ingestion/examples/library/dataset_attach_platform_instance.py
from datahub.sdk import DataHubClient, DatasetUrn

client = DataHubClient.from_env()

dataset = client.entities.get(
    DatasetUrn(
        platform="mysql",
        name="production-mysql-cluster.ecommerce.customers",
        env="PROD",
    )
)

dataset._set_platform_instance(platform="mysql", instance="production-mysql-cluster")

client.entities.update(dataset)

print("Attached platform instance 'production-mysql-cluster'")
print(f"to dataset {dataset.urn}")

```



### Querying Platform Instances

You can retrieve platform instance information using the REST API or GraphQL:


**Python SDK: Query platform instance via REST API**

```python
# Inlined from /metadata-ingestion/examples/library/platform_instance_query.py
# metadata-ingestion/examples/library/platform_instance_query.py
import json
import os

import datahub.emitter.mce_builder as builder
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Create a DataHub graph client
gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
config = DatahubClientConfig(server=gms_server, token=token)
graph = DataHubGraph(config)

# Create the platform instance URN
platform_instance_urn = builder.make_dataplatform_instance_urn(
    platform="mysql", instance="production-mysql-cluster"
)

# Check if the platform instance exists
if graph.exists(platform_instance_urn):
    print(f"Platform instance exists: {platform_instance_urn}\n")

    # Get the full entity with all aspects
    entity = graph.get_entity_semityped(platform_instance_urn)

    # Access the key aspect to get platform and instance ID
    if "dataPlatformInstanceKey" in entity:
        key_aspect = entity["dataPlatformInstanceKey"]
        print(f"Platform: {key_aspect.platform}")
        print(f"Instance ID: {key_aspect.instance}\n")

    # Access properties
    if "dataPlatformInstanceProperties" in entity:
        props = entity["dataPlatformInstanceProperties"]
        print(f"Name: {props.name}")
        print(f"Description: {props.description}")
        if props.customProperties:
            print("Custom Properties:")
            for key, value in props.customProperties.items():
                print(f"  {key}: {value}")
        if props.externalUrl:
            print(f"External URL: {props.externalUrl}")
        print()

    # Access ownership
    if "ownership" in entity:
        ownership = entity["ownership"]
        print("Owners:")
        for owner in ownership.owners:
            print(f"  - {owner.owner} ({owner.type})")
        print()

    # Access tags
    if "globalTags" in entity:
        global_tags = entity["globalTags"]
        print("Tags:")
        for tag_association in global_tags.tags:
            print(f"  - {tag_association.tag}")
        print()

    # Access institutional memory (links)
    if "institutionalMemory" in entity:
        institutional_memory = entity["institutionalMemory"]
        print("Links:")
        for element in institutional_memory.elements:
            print(f"  - {element.description}: {element.url}")
        print()

    # Get raw aspects using REST API for complete data
    raw_entity = graph.get_entity_raw(
        entity_urn=platform_instance_urn,
        aspects=[
            "dataPlatformInstanceKey",
            "dataPlatformInstanceProperties",
            "ownership",
            "globalTags",
            "institutionalMemory",
            "deprecation",
            "status",
        ],
    )

    print("Raw entity data:")
    print(json.dumps(raw_entity, indent=2))

else:
    print(f"Platform instance does not exist: {platform_instance_urn}")

```




**REST API: Fetch platform instance entity**

```bash
curl 'http://localhost:8080/entities/urn%3Ali%3AdataPlatformInstance%3A(urn%3Ali%3AdataPlatform%3Amysql%2Cproduction-cluster)'
```




**GraphQL: Search for Iceberg warehouses**

```graphql
query {
  search(
    input: {
      type: DATA_PLATFORM_INSTANCE
      query: "dataPlatform:iceberg"
      start: 0
      count: 10
    }
  ) {
    searchResults {
      entity {
        ... on DataPlatformInstance {
          urn
          platform {
            name
          }
          instanceId
          properties {
            name
            description
          }
        }
      }
    }
  }
}
```



## Integration Points

### Relationship to Other Entities

Platform instances are referenced by many entities through the `dataPlatformInstance` aspect:

- **Datasets**: Associate datasets with their platform instance, enabling filtering and organization by instance
- **Charts**: BI tool charts can be linked to the specific instance they query
- **Dashboards**: Dashboards are associated with platform instances
- **Data Jobs**: ETL/pipeline jobs reference the platform instance they run on
- **Data Flows**: Pipeline definitions can be associated with platform instances
- **ML Models**: Models can track which platform instance they were trained on
- **Containers**: Database schemas, folders, and other containers reference their instance
- **Assertions**: Data quality assertions can be scoped to specific instances

This creates a powerful organizational dimension across all data assets.

### Ingestion Framework Integration

Most DataHub ingestion sources support a `platform_instance` configuration parameter. When specified, the connector automatically attaches the platform instance to all ingested entities:

```yaml
source:
  type: mysql
  config:
    host_port: "mysql.prod.company.com:3306"
    platform_instance: "production-mysql-cluster"
    # ... other config
```

The platform instance is then used to:

- Distinguish assets from different instances of the same platform
- Enable instance-level filtering in the UI
- Support multi-tenant or multi-region deployments
- Organize metadata by deployment environment

### Usage in Dataset Naming

For platforms that support multiple instances, the platform instance is often incorporated into dataset names to ensure uniqueness. For example:

- Without instance: `urn:li:dataset:(urn:li:dataPlatform:mysql,db.schema.table,PROD)`
- With instance: `urn:li:dataset:(urn:li:dataPlatform:mysql,prod-cluster.db.schema.table,PROD)`

This ensures that tables with the same name across different instances have distinct URNs.

### Iceberg Catalog Integration

When DataHub serves as an Iceberg REST catalog, platform instances represent Iceberg warehouses. Each warehouse configuration includes:

- Storage credentials for S3 access
- IAM role configuration for credential vending
- Warehouse root location in object storage
- Environment designation

DataHub manages the lifecycle of Iceberg tables within these warehouses, handling:

- Table creation and metadata storage
- Temporary credential generation for read/write access
- Table discovery and lineage tracking
- Schema evolution

See the `datahub iceberg` CLI commands for managing Iceberg warehouses as platform instances.

## Notable Exceptions

### Internal vs. External Use

Data Platform Instances are categorized as "internal" entities in DataHub's entity registry, meaning they are primarily used for organization and metadata management rather than being primary discovery targets. Users typically interact with datasets, dashboards, and other assets rather than directly browsing platform instances.

However, platform instances are searchable and can be viewed in the DataHub UI when investigating asset organization or platform-level configurations.

### Platform Instance vs. Environment

Platform instances are distinct from the environment/fabric concept used in entity URNs (PROD, DEV, QA, etc.). While environment is a required part of many entity identifiers, platform instance is optional and provides a finer-grained organizational dimension.

A single platform instance typically corresponds to one environment, but you can have multiple instances within the same environment (e.g., "prod-us-west", "prod-us-east", "prod-eu-central" all in PROD environment).

### Automatic Instance Creation

Platform instances are typically created implicitly during ingestion rather than being explicitly defined beforehand. When an ingestion source references a platform instance that doesn't exist, DataHub will automatically create a basic platform instance entity. You can then enrich it with additional metadata like properties, ownership, and tags.

### Limited GraphQL Search

Unlike primary entities like datasets and dashboards, platform instances have limited search functionality in GraphQL. The `search` query with `type: DATA_PLATFORM_INSTANCE` is supported, but some advanced search features may not be fully implemented. REST API access provides full functionality.

### Immutable Key Components

Once created, a platform instance's key components (platform URN and instance ID) cannot be changed. If you need to rename an instance, you must create a new platform instance entity and migrate references from the old one.



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

#### dataPlatformInstanceProperties
Properties associated with a Data Platform Instance



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| customProperties | map | ✓ | Custom property bag. | Searchable |
| externalUrl | string |  | URL where the reference exist | Searchable |
| name | string |  | Display name of the Data Platform Instance | Searchable |
| description | string |  | Documentation of the Data Platform Instance | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataPlatformInstanceProperties"
  },
  "name": "DataPlatformInstanceProperties",
  "namespace": "com.linkedin.dataplatforminstance",
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
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "java": {
        "class": "com.linkedin.common.url.Url",
        "coercerClass": "com.linkedin.common.url.UrlCoercer"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "externalUrl",
      "default": null,
      "doc": "URL where the reference exist"
    },
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldNameAliases": [
          "_entityName"
        ],
        "fieldType": "WORD_GRAM",
        "searchLabel": "entityName",
        "searchTier": 1
      },
      "type": [
        "null",
        "string"
      ],
      "name": "name",
      "default": null,
      "doc": "Display name of the Data Platform Instance"
    },
    {
      "Searchable": {
        "fieldType": "TEXT",
        "hasValuesFieldName": "hasDescription",
        "searchTier": 2
      },
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Documentation of the Data Platform Instance"
    }
  ],
  "doc": "Properties associated with a Data Platform Instance"
}
```





#### ownership
Ownership information of an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| owners | Owner[] | ✓ | List of owners of the entity. |  |
| ownerTypes | map |  | Ownership type to Owners map, populated via mutation hook. | Searchable |
| lastModified | [AuditStamp](#auditstamp) | ✓ | Audit stamp containing who last modified the record and when. A value of 0 in the time field indi... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "ownership"
  },
  "name": "Ownership",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Owner",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "corpuser",
                  "corpGroup"
                ],
                "name": "OwnedBy"
              },
              "Searchable": {
                "addToFilters": true,
                "fieldName": "owners",
                "fieldType": "URN",
                "filterNameOverride": "Owned By",
                "hasValuesFieldName": "hasOwners",
                "queryByDefault": false,
                "searchTier": 2
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "owner",
              "doc": "Owner URN, e.g. urn:li:corpuser:ldap, urn:li:corpGroup:group_name, and urn:li:multiProduct:mp_name\n(Caveat: only corpuser is currently supported in the frontend.)"
            },
            {
              "deprecated": true,
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "BUSINESS_OWNER": "A person or group who is responsible for logical, or business related, aspects of the asset.",
                  "CONSUMER": "A person, group, or service that consumes the data\nDeprecated! Use TECHNICAL_OWNER or BUSINESS_OWNER instead.",
                  "CUSTOM": "Set when ownership type is unknown or a when new one is specified as an ownership type entity for which we have no\nenum value for. This is used for backwards compatibility",
                  "DATAOWNER": "A person or group that is owning the data\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "DATA_STEWARD": "A steward, expert, or delegate responsible for the asset.",
                  "DELEGATE": "A person or a group that overseas the operation, e.g. a DBA or SRE.\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "DEVELOPER": "A person or group that is in charge of developing the code\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "NONE": "No specific type associated to the owner.",
                  "PRODUCER": "A person, group, or service that produces/generates the data\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "STAKEHOLDER": "A person or a group that has direct business interest\nDeprecated! Use TECHNICAL_OWNER, BUSINESS_OWNER, or STEWARD instead.",
                  "TECHNICAL_OWNER": "person or group who is responsible for technical aspects of the asset."
                },
                "deprecatedSymbols": {
                  "CONSUMER": true,
                  "DATAOWNER": true,
                  "DELEGATE": true,
                  "DEVELOPER": true,
                  "PRODUCER": true,
                  "STAKEHOLDER": true
                },
                "name": "OwnershipType",
                "namespace": "com.linkedin.common",
                "symbols": [
                  "CUSTOM",
                  "TECHNICAL_OWNER",
                  "BUSINESS_OWNER",
                  "DATA_STEWARD",
                  "NONE",
                  "DEVELOPER",
                  "DATAOWNER",
                  "DELEGATE",
                  "PRODUCER",
                  "CONSUMER",
                  "STAKEHOLDER"
                ],
                "doc": "Asset owner types"
              },
              "name": "type",
              "doc": "The type of the ownership"
            },
            {
              "Relationship": {
                "entityTypes": [
                  "ownershipType"
                ],
                "name": "ownershipType"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "typeUrn",
              "default": null,
              "doc": "The type of the ownership\nUrn of type O"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "OwnershipSource",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": {
                        "type": "enum",
                        "symbolDocs": {
                          "AUDIT": "Auditing system or audit logs",
                          "DATABASE": "Database, e.g. GRANTS table",
                          "FILE_SYSTEM": "File system, e.g. file/directory owner",
                          "ISSUE_TRACKING_SYSTEM": "Issue tracking system, e.g. Jira",
                          "MANUAL": "Manually provided by a user",
                          "OTHER": "Other sources",
                          "SERVICE": "Other ownership-like service, e.g. Nuage, ACL service etc",
                          "SOURCE_CONTROL": "SCM system, e.g. GIT, SVN"
                        },
                        "name": "OwnershipSourceType",
                        "namespace": "com.linkedin.common",
                        "symbols": [
                          "AUDIT",
                          "DATABASE",
                          "FILE_SYSTEM",
                          "ISSUE_TRACKING_SYSTEM",
                          "MANUAL",
                          "SERVICE",
                          "SOURCE_CONTROL",
                          "OTHER"
                        ]
                      },
                      "name": "type",
                      "doc": "The type of the source"
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "url",
                      "default": null,
                      "doc": "A reference URL for the source"
                    }
                  ],
                  "doc": "Source/provider of the ownership information"
                }
              ],
              "name": "source",
              "default": null,
              "doc": "Source information for the ownership"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "ownerAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "ownerAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "ownerAttributionDates",
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
          ],
          "doc": "Ownership information"
        }
      },
      "name": "owners",
      "doc": "List of owners of the entity."
    },
    {
      "Searchable": {
        "/$key": {
          "fieldType": "MAP_ARRAY",
          "queryByDefault": false
        }
      },
      "type": [
        {
          "type": "map",
          "values": {
            "type": "array",
            "items": "string"
          }
        },
        "null"
      ],
      "name": "ownerTypes",
      "default": {},
      "doc": "Ownership type to Owners map, populated via mutation hook."
    },
    {
      "type": {
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
      },
      "name": "lastModified",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "Audit stamp containing who last modified the record and when. A value of 0 in the time field indicates missing data."
    }
  ],
  "doc": "Ownership information of an entity."
}
```





#### globalTags
Tag aspect used for applying tags to an entity



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| tags | TagAssociation[] | ✓ | Tags associated with a given entity | Searchable, → TaggedWith |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "globalTags"
  },
  "name": "GlobalTags",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Relationship": {
        "/*/tag": {
          "entityTypes": [
            "tag"
          ],
          "name": "TaggedWith"
        }
      },
      "Searchable": {
        "/*/tag": {
          "addToFilters": true,
          "boostScore": 0.5,
          "fieldName": "tags",
          "fieldType": "URN",
          "filterNameOverride": "Tagged With",
          "hasValuesFieldName": "hasTags",
          "queryByDefault": true,
          "searchTier": 2
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "TagAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.TagUrn"
              },
              "type": "string",
              "name": "tag",
              "doc": "Urn of the applied tag"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "context",
              "default": null,
              "doc": "Additional context about the association"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "tagAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "tagAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "tagAttributionDates",
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
          ],
          "doc": "Properties of an applied tag. For now, just an Urn. In the future we can extend this with other properties, e.g.\npropagation parameters."
        }
      },
      "name": "tags",
      "doc": "Tags associated with a given entity"
    }
  ],
  "doc": "Tag aspect used for applying tags to an entity"
}
```





#### institutionalMemory
Institutional memory of an entity. This is a way to link to relevant documentation and provide description of the documentation. Institutional or tribal knowledge is very important for users to leverage the entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| elements | InstitutionalMemoryMetadata[] | ✓ | List of records that represent institutional memory of an entity. Each record consists of a link,... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "institutionalMemory"
  },
  "name": "InstitutionalMemory",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "InstitutionalMemoryMetadata",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.url.Url",
                "coercerClass": "com.linkedin.common.url.UrlCoercer"
              },
              "type": "string",
              "name": "url",
              "doc": "Link to an engineering design document or a wiki page."
            },
            {
              "type": "string",
              "name": "description",
              "doc": "Description of the link."
            },
            {
              "type": {
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
              },
              "name": "createStamp",
              "doc": "Audit stamp associated with creation of this record"
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "updateStamp",
              "default": null,
              "doc": "Audit stamp associated with updation of this record"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "InstitutionalMemoryMetadataSettings",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "boolean",
                      "name": "showInAssetPreview",
                      "default": false,
                      "doc": "Show record in asset preview like on entity header and search previews"
                    }
                  ],
                  "doc": "Settings related to a record of InstitutionalMemoryMetadata"
                }
              ],
              "name": "settings",
              "default": null,
              "doc": "Settings for this record"
            }
          ],
          "doc": "Metadata corresponding to a record of institutional memory."
        }
      },
      "name": "elements",
      "doc": "List of records that represent institutional memory of an entity. Each record consists of a link, description, creator and timestamps associated with that record."
    }
  ],
  "doc": "Institutional memory of an entity. This is a way to link to relevant documentation and provide description of the documentation. Institutional or tribal knowledge is very important for users to leverage the entity."
}
```





#### deprecation
Deprecation status of an entity



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| deprecated | boolean | ✓ | Whether the entity is deprecated. | Searchable |
| decommissionTime | long |  | The time user plan to decommission this entity. |  |
| note | string | ✓ | Additional information about the entity deprecation plan, such as the wiki, doc, RB. |  |
| actor | string | ✓ | The user URN which will be credited for modifying this deprecation content. |  |
| replacement | string |  |  |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "deprecation"
  },
  "name": "Deprecation",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "BOOLEAN",
        "filterNameOverride": "Deprecated",
        "weightsPerFieldValue": {
          "true": 0.5
        }
      },
      "type": "boolean",
      "name": "deprecated",
      "doc": "Whether the entity is deprecated."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "decommissionTime",
      "default": null,
      "doc": "The time user plan to decommission this entity."
    },
    {
      "type": "string",
      "name": "note",
      "doc": "Additional information about the entity deprecation plan, such as the wiki, doc, RB."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "actor",
      "doc": "The user URN which will be credited for modifying this deprecation content."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "replacement",
      "default": null
    }
  ],
  "doc": "Deprecation status of an entity"
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





#### icebergWarehouseInfo
An Iceberg warehouse location and credentails whose read/writes are governed by datahub catalog.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| dataRoot | string | ✓ | Path of the root for the backing store of the tables in the warehouse. |  |
| clientId | string | ✓ | clientId to be used to authenticate with storage hosting this warehouse |  |
| clientSecret | string | ✓ | client secret to authenticate with storage hosting this warehouse |  |
| region | string | ✓ | region where the warehouse is located. |  |
| role | string |  |  |  |
| tempCredentialExpirationSeconds | int |  |  |  |
| env | FabricType | ✓ |  |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "icebergWarehouseInfo"
  },
  "name": "IcebergWarehouseInfo",
  "namespace": "com.linkedin.dataplatforminstance",
  "fields": [
    {
      "type": "string",
      "name": "dataRoot",
      "doc": "Path of the root for the backing store of the tables in the warehouse."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "clientId",
      "doc": "clientId to be used to authenticate with storage hosting this warehouse"
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "clientSecret",
      "doc": "client secret to authenticate with storage hosting this warehouse"
    },
    {
      "type": "string",
      "name": "region",
      "doc": "region where the warehouse is located."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "role",
      "default": null
    },
    {
      "type": [
        "null",
        "int"
      ],
      "name": "tempCredentialExpirationSeconds",
      "default": null
    },
    {
      "type": {
        "type": "enum",
        "symbolDocs": {
          "CERT": "Designates certification fabrics",
          "CORP": "Designates corporation fabrics",
          "DEV": "Designates development fabrics",
          "EI": "Designates early-integration fabrics",
          "NON_PROD": "Designates non-production fabrics",
          "PRD": "Alternative Prod spelling",
          "PRE": "Designates pre-production fabrics",
          "PROD": "Designates production fabrics",
          "QA": "Designates quality assurance fabrics",
          "RVW": "Designates review fabrics",
          "SANDBOX": "Designates sandbox fabrics",
          "SBX": "Alternative spelling for sandbox",
          "SIT": "System Integration Testing",
          "STG": "Designates staging fabrics",
          "TEST": "Designates testing fabrics",
          "TST": "Alternative Test spelling",
          "UAT": "Designates user acceptance testing fabrics"
        },
        "name": "FabricType",
        "namespace": "com.linkedin.common",
        "symbols": [
          "DEV",
          "TEST",
          "QA",
          "UAT",
          "EI",
          "PRE",
          "STG",
          "NON_PROD",
          "PROD",
          "CORP",
          "RVW",
          "PRD",
          "TST",
          "SIT",
          "SBX",
          "SANDBOX",
          "CERT"
        ],
        "doc": "Fabric group type"
      },
      "name": "env"
    }
  ],
  "doc": "An Iceberg warehouse location and credentails whose read/writes are governed by datahub catalog."
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
- OwnedBy

   - Corpuser via `ownership.owners.owner`
   - CorpGroup via `ownership.owners.owner`
- ownershipType

   - OwnershipType via `ownership.owners.typeUrn`
- TaggedWith

   - Tag via `globalTags.tags`
#### Incoming
These are the relationships stored in other entity's aspects
- PartOfSlackWorkspace

   - Corpuser via `slackUserInfo.slackInstance`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
