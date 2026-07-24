# DataProcess

> **DEPRECATED**: This entity is deprecated and should not be used for new implementations.
>
> **Use [dataFlow](./dataFlow.md) and [dataJob](./dataJob.md) instead.**
>
> The `dataProcess` entity was an early attempt to model data processing tasks but has been superseded by the more robust and flexible `dataFlow` and `dataJob` entities which better represent the hierarchical nature of modern data pipelines.

## Deprecation Notice

The `dataProcess` entity was deprecated to provide a clearer separation between:

- **DataFlow**: Represents the overall pipeline/workflow (e.g., an Airflow DAG, dbt project, Spark application)
- **DataJob**: Represents individual tasks within a pipeline (e.g., an Airflow task, dbt model, Spark job)

This two-level hierarchy better matches how modern orchestration systems organize data processing work and provides more flexibility for lineage tracking, ownership assignment, and operational monitoring.

### Why was it deprecated?

The original `dataProcess` entity had several limitations:

1. **No hierarchical structure**: It couldn't represent the relationship between a pipeline and its constituent tasks
2. **Limited orchestrator support**: The flat structure didn't map well to DAG-based orchestration platforms like Airflow, Prefect, or Dagster
3. **Unclear semantics**: It was ambiguous whether a dataProcess represented a whole pipeline or a single task
4. **Poor lineage modeling**: Without task-level granularity, lineage relationships were less precise

The new `dataFlow` and `dataJob` model addresses these limitations by providing a clear parent-child relationship that mirrors real-world data processing architectures.

## Identity (Historical Reference)

DataProcess entities were identified by three components:

- **Name**: The process name (typically an ETL job name)
- **Orchestrator**: The workflow management platform (e.g., `airflow`, `azkaban`)
- **Origin (Fabric)**: The environment where the process runs (PROD, DEV, etc.)

The URN structure was:

```
urn:li:dataProcess:(<name>,<orchestrator>,<origin>)
```

### Example URNs

```
urn:li:dataProcess:(customer_etl_job,airflow,PROD)
urn:li:dataProcess:(sales_aggregation,azkaban,DEV)
```

## Important Capabilities (Historical Reference)

### DataProcessInfo Aspect

The `dataProcessInfo` aspect captured inputs and outputs of the process:

- **Inputs**: Array of dataset URNs consumed by the process
- **Outputs**: Array of dataset URNs produced by the process

This established basic lineage relationships through "Consumes" relationships with datasets.

### Common Aspects

Like other entities, dataProcess supported:

- **Ownership**: Assigning owners to processes
- **Status**: Marking processes as removed
- **Global Tags**: Categorization and classification
- **Institutional Memory**: Links to documentation

## Migration Guide

### When to use DataFlow vs DataJob

**Use DataFlow when representing:**

- Airflow DAGs
- dbt projects
- Prefect flows
- Dagster pipelines
- Azkaban workflows
- Any container of related data processing tasks

**Use DataJob when representing:**

- Airflow tasks within a DAG
- dbt models within a project
- Prefect tasks within a flow
- Dagster ops/assets within a pipeline
- Individual processing steps

**Use both together:**

- Create a DataFlow for the pipeline
- Create DataJobs for each task within that pipeline
- Link DataJobs to their parent DataFlow

### Conceptual Mapping

| DataProcess Concept | New Model Equivalent       | Notes                            |
| ------------------- | -------------------------- | -------------------------------- |
| Process with tasks  | DataFlow + DataJobs        | Split into two entities          |
| Process name        | DataFlow flowId            | Becomes the parent identifier    |
| Single-step process | DataFlow + 1 DataJob       | Still requires both entities     |
| Orchestrator        | DataFlow orchestrator      | Same concept, better modeling    |
| Origin/Fabric       | DataFlow cluster           | Often matches environment        |
| Inputs/Outputs      | DataJob dataJobInputOutput | Moved to job level for precision |

### Migration Steps

To migrate from `dataProcess` to `dataFlow`/`dataJob`:

1. **Identify your process structure**: Determine if your dataProcess represents a pipeline (has multiple steps) or a single task

2. **Create a DataFlow**: This represents the overall pipeline/workflow

   - Use the same orchestrator value
   - Use the process name as the flow ID
   - Use a cluster identifier (often matches the origin/fabric)

3. **Create DataJob(s)**: Create one or more jobs within the flow

   - For single-step processes: create one job named after the process
   - For multi-step processes: create a job for each step
   - Link each job to its parent DataFlow

4. **Migrate lineage**: Move input/output dataset relationships from the process level to the job level

5. **Migrate metadata**: Transfer ownership, tags, and documentation to the appropriate entity (typically the DataFlow for pipeline-level metadata, or specific DataJobs for task-level metadata)

### Migration Examples

**Example 1: Simple single-task process**

Old dataProcess:

```
urn:li:dataProcess:(daily_report,airflow,PROD)
```

New structure:

```
DataFlow: urn:li:dataFlow:(airflow,daily_report,prod)
DataJob:  urn:li:dataJob:(urn:li:dataFlow:(airflow,daily_report,prod),daily_report_task)
```

**Example 2: Multi-step ETL pipeline**

Old dataProcess:

```
urn:li:dataProcess:(customer_pipeline,airflow,PROD)
```

New structure:

```
DataFlow: urn:li:dataFlow:(airflow,customer_pipeline,prod)
DataJob:  urn:li:dataJob:(urn:li:dataFlow:(airflow,customer_pipeline,prod),extract_customers)
DataJob:  urn:li:dataJob:(urn:li:dataFlow:(airflow,customer_pipeline,prod),transform_customers)
DataJob:  urn:li:dataJob:(urn:li:dataFlow:(airflow,customer_pipeline,prod),load_customers)
```

## Code Examples

### Querying Existing DataProcess Entities

If you need to query existing dataProcess entities for migration purposes:


**Python SDK: Query a dataProcess entity**

```python
# Inlined from /metadata-ingestion/examples/library/dataprocess_query_deprecated.py
"""
Example: Query an existing (deprecated) dataProcess entity for migration purposes.

This example shows how to read a deprecated dataProcess entity from DataHub
to understand its structure before migrating it to dataFlow and dataJob entities.

Note: This is only for reading existing data. Do NOT create new dataProcess entities.
Use dataFlow and dataJob instead for all new implementations.
"""

from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create emitter to read from DataHub
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

# URN of the deprecated dataProcess entity to query
dataprocess_urn = "urn:li:dataProcess:(customer_etl_job,airflow,PROD)"

# Fetch the entity using the REST API
try:
    entity = emitter._session.get(
        f"{emitter._gms_server}/entities/{dataprocess_urn}"
    ).json()

    print(f"Found dataProcess: {dataprocess_urn}")
    print("\n=== Entity Aspects ===")

    # Extract key information for migration
    if "aspects" in entity:
        aspects = entity["aspects"]

        # Key aspect (identity)
        if "dataProcessKey" in aspects:
            key = aspects["dataProcessKey"]
            print("\nIdentity:")
            print(f"  Name: {key.get('name')}")
            print(f"  Orchestrator: {key.get('orchestrator')}")
            print(f"  Origin (Fabric): {key.get('origin')}")

        # Core process information
        if "dataProcessInfo" in aspects:
            info = aspects["dataProcessInfo"]
            print("\nProcess Info:")
            if "inputs" in info:
                print(f"  Input Datasets: {len(info['inputs'])}")
                for inp in info["inputs"]:
                    print(f"    - {inp}")
            if "outputs" in info:
                print(f"  Output Datasets: {len(info['outputs'])}")
                for out in info["outputs"]:
                    print(f"    - {out}")

        # Ownership information
        if "ownership" in aspects:
            ownership = aspects["ownership"]
            print("\nOwnership:")
            for owner in ownership.get("owners", []):
                print(f"  - {owner['owner']} (type: {owner.get('type', 'UNKNOWN')})")

        # Tags
        if "globalTags" in aspects:
            tags = aspects["globalTags"]
            print("\nTags:")
            for tag in tags.get("tags", []):
                print(f"  - {tag['tag']}")

        # Status
        if "status" in aspects:
            status = aspects["status"]
            print(f"\nStatus: {status.get('removed', False)}")

    print("\n=== Migration Recommendation ===")
    print("Replace this dataProcess with:")
    print(
        f"  DataFlow URN: urn:li:dataFlow:({key.get('orchestrator')},{key.get('name')},{key.get('origin', 'PROD').lower()})"
    )
    print(
        f"  DataJob URN: urn:li:dataJob:(urn:li:dataFlow:({key.get('orchestrator')},{key.get('name')},{key.get('origin', 'PROD').lower()}),main_task)"
    )
    print("\nSee dataprocess_migrate_to_flow_job.py for migration code examples.")

except Exception as e:
    print(f"Error querying dataProcess: {e}")
    print("\nThis is expected if the entity doesn't exist.")
    print("DataProcess is deprecated - use dataFlow and dataJob instead.")

```



### Creating Equivalent DataFlow and DataJob (Recommended)

Instead of using dataProcess, create the modern equivalent:


**Python SDK: Create DataFlow and DataJob to replace dataProcess**

```python
# Inlined from /metadata-ingestion/examples/library/dataprocess_migrate_to_flow_job.py
"""
Example: Migrate from deprecated dataProcess to modern dataFlow and dataJob entities.

This example shows how to create the modern dataFlow and dataJob entities
that replace the deprecated dataProcess entity.

The dataProcess entity used a flat structure:
  dataProcess -> inputs/outputs

The new model uses a hierarchical structure:
  dataFlow (pipeline) -> dataJob (task) -> inputs/outputs

This provides better organization and more precise lineage tracking.
"""

from datahub.metadata.urns import DatasetUrn
from datahub.sdk import DataFlow, DataHubClient, DataJob

client = DataHubClient.from_env()

# Old dataProcess would have been:
# urn:li:dataProcess:(customer_etl_job,airflow,PROD)
# with inputs and outputs at the process level

# New approach: Create a DataFlow for the pipeline
dataflow = DataFlow(
    platform="airflow",  # Same as the old 'orchestrator' field
    name="customer_etl_job",  # Same as the old 'name' field
    platform_instance="prod",  # Based on old 'origin' field
    description="ETL pipeline for customer data processing",
)

# Create DataJob(s) within the flow
# For a simple single-task process, create one job
# For complex multi-step processes, create multiple jobs
datajob = DataJob(
    name="customer_etl_task",  # Task name within the flow
    flow=dataflow,  # Link to parent flow
    description="Main ETL task for customer data",
    # Inputs and outputs now live at the job level for precise lineage
    inlets=[
        DatasetUrn(platform="mysql", name="raw_db.customers", env="PROD"),
        DatasetUrn(platform="mysql", name="raw_db.orders", env="PROD"),
    ],
    outlets=[
        DatasetUrn(platform="postgres", name="analytics.customer_summary", env="PROD"),
    ],
)

# Upsert both entities
client.entities.upsert(dataflow)
client.entities.upsert(datajob)

print("Successfully migrated from dataProcess to dataFlow + dataJob!")
print(f"DataFlow URN: {dataflow.urn}")
print(f"DataJob URN: {datajob.urn}")
print("\nKey improvements over dataProcess:")
print("- Clear separation between pipeline (DataFlow) and task (DataJob)")
print("- Support for multi-step pipelines with multiple DataJobs")
print("- More precise lineage at the task level")
print("- Better integration with modern orchestration platforms")

```



### Complete Migration Example


**Python SDK: Full migration from dataProcess to dataFlow/dataJob**

```python
# Inlined from /metadata-ingestion/examples/library/dataprocess_full_migration.py
"""
Example: Complete migration from dataProcess to dataFlow/dataJob with metadata preservation.

This example demonstrates a full migration path that:
1. Reads an existing deprecated dataProcess entity
2. Extracts all its metadata (inputs, outputs, ownership, tags)
3. Creates equivalent dataFlow and dataJob entities
4. Preserves all metadata relationships

Use this as a template for migrating multiple dataProcess entities in bulk.
"""

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    TagAssociationClass,
)
from datahub.sdk import DataFlow, DataHubClient, DataJob

# Initialize clients
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
client = DataHubClient.from_env()

# Step 1: Define the dataProcess to migrate
old_dataprocess_urn = "urn:li:dataProcess:(sales_pipeline,airflow,PROD)"

print(f"Migrating: {old_dataprocess_urn}")

try:
    # Step 2: Fetch the existing dataProcess entity
    entity = rest_emitter._session.get(
        f"{rest_emitter._gms_server}/entities/{old_dataprocess_urn}"
    ).json()

    aspects = entity.get("aspects", {})

    # Extract identity information
    key = aspects.get("dataProcessKey", {})
    name = key.get("name", "unknown_process")
    orchestrator = key.get("orchestrator", "unknown")
    origin = key.get("origin", "PROD")

    # Extract process info
    process_info = aspects.get("dataProcessInfo", {})
    input_datasets = process_info.get("inputs", [])
    output_datasets = process_info.get("outputs", [])

    # Extract ownership
    ownership_aspect = aspects.get("ownership", {})
    owners = ownership_aspect.get("owners", [])

    # Extract tags
    tags_aspect = aspects.get("globalTags", {})
    tags = tags_aspect.get("tags", [])

    print("\n=== Extracted Metadata ===")
    print(f"Name: {name}")
    print(f"Orchestrator: {orchestrator}")
    print(f"Environment: {origin}")
    print(f"Inputs: {len(input_datasets)} datasets")
    print(f"Outputs: {len(output_datasets)} datasets")
    print(f"Owners: {len(owners)}")
    print(f"Tags: {len(tags)}")

    # Step 3: Create the new DataFlow
    dataflow = DataFlow(
        platform=orchestrator,
        name=name,
        platform_instance=origin.lower(),
        description=f"Migrated from dataProcess {name}",
    )

    # Step 4: Create the DataJob(s)
    # For simplicity, creating one job. In practice, you might split into multiple jobs.
    datajob = DataJob(
        name=f"{name}_main",
        flow=dataflow,
        description=f"Main task for {name}",
        inlets=[inp for inp in input_datasets],  # These should be dataset URNs
        outlets=[out for out in output_datasets],  # These should be dataset URNs
    )

    # Step 5: Upsert the entities
    client.entities.upsert(dataflow)
    client.entities.upsert(datajob)

    print("\n=== Created New Entities ===")
    print(f"DataFlow: {dataflow.urn}")
    print(f"DataJob: {datajob.urn}")

    # Step 6: Migrate ownership to DataFlow
    if owners:
        ownership_to_add = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=owner.get("owner"),
                    type=getattr(OwnershipTypeClass, owner.get("type", "DATAOWNER")),
                )
                for owner in owners
            ]
        )
        rest_emitter.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=str(dataflow.urn),
                aspect=ownership_to_add,
            )
        )
        print(f"Migrated {len(owners)} owner(s) to DataFlow")

    # Step 7: Migrate tags to DataFlow
    if tags:
        tags_to_add = GlobalTagsClass(
            tags=[TagAssociationClass(tag=tag.get("tag")) for tag in tags]
        )
        rest_emitter.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=str(dataflow.urn),
                aspect=tags_to_add,
            )
        )
        print(f"Migrated {len(tags)} tag(s) to DataFlow")

    print("\n=== Migration Complete ===")
    print("Next steps:")
    print("1. Verify the new entities in DataHub UI")
    print("2. Update any downstream systems to reference the new URNs")
    print("3. Consider soft-deleting the old dataProcess entity")

except Exception as e:
    print(f"Error during migration: {e}")
    print("\nCommon issues:")
    print("- DataProcess entity doesn't exist (already migrated or never created)")
    print("- Network connectivity to DataHub GMS")
    print("- Permission issues writing to DataHub")

```



## Integration Points

### Historical Usage

The dataProcess entity was previously used by:

1. **Early ingestion connectors**: Original Airflow, Azkaban connectors before they migrated to dataFlow/dataJob
2. **Custom integrations**: User-built integrations that haven't been updated
3. **Legacy metadata**: Historical data in existing DataHub instances

### Modern Replacements

All modern DataHub connectors use dataFlow and dataJob:

- **Airflow**: DAGs → DataFlow, Tasks → DataJob
- **dbt**: Projects → DataFlow, Models → DataJob
- **Prefect**: Flows → DataFlow, Tasks → DataJob
- **Dagster**: Pipelines → DataFlow, Ops/Assets → DataJob
- **Fivetran**: Connectors → DataFlow, Sync operations → DataJob
- **AWS Glue**: Jobs → DataFlow, Steps → DataJob
- **Azure Data Factory**: Pipelines → DataFlow, Activities → DataJob

### DataProcessInstance

Note that `dataProcessInstance` is **NOT deprecated**. It represents a specific execution/run of either:

- A dataJob (recommended)
- A legacy dataProcess (for backward compatibility)

DataProcessInstance continues to be used for tracking pipeline run history, status, and runtime information.

## Notable Exceptions

### Timeline for Removal

- **Deprecated**: Early 2021 (with introduction of dataFlow/dataJob)
- **Status**: Still exists in the entity registry for backward compatibility
- **Current State**: No active ingestion sources create dataProcess entities
- **Removal**: No specific timeline, maintained for existing data

### Reading Existing Data

The dataProcess entity remains readable through all DataHub APIs for backward compatibility. Existing dataProcess entities in your instance will continue to function and display in the UI.

### No New Writes Recommended

While technically possible to create new dataProcess entities, it is **strongly discouraged**. All new integrations should use dataFlow and dataJob.

### Upgrade Path

There is no automatic migration tool. Organizations with significant dataProcess data should:

1. Use the Python SDK to query existing dataProcess entities
2. Create equivalent dataFlow and dataJob entities
3. Preserve URN mappings for lineage continuity
4. Consider soft-deleting old dataProcess entities once migration is verified

### GraphQL API

The dataProcess entity is minimally exposed in the GraphQL API. Modern GraphQL queries and mutations focus on dataFlow and dataJob entities.

## Additional Resources

- [DataFlow Entity Documentation](./dataFlow.md)
- [DataJob Entity Documentation](./dataJob.md)
- [Lineage Documentation](../../../features/feature-guides/lineage.md)
- [Airflow Integration Guide](../../../lineage/airflow.md)



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

#### dataProcessKey
Key for a Data Process



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| name | string | ✓ | Process name i.e. an ETL job name | Searchable |
| orchestrator | string | ✓ | Standardized Orchestrator where data process is defined. TODO: Migrate towards something that can... | Searchable |
| origin | FabricType | ✓ | Fabric type where dataset belongs to or where it was generated. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataProcessKey"
  },
  "name": "DataProcessKey",
  "namespace": "com.linkedin.metadata.key",
  "fields": [
    {
      "Searchable": {
        "boostScore": 4.0,
        "enableAutocomplete": true,
        "fieldType": "WORD_GRAM"
      },
      "type": "string",
      "name": "name",
      "doc": "Process name i.e. an ETL job name"
    },
    {
      "Searchable": {
        "enableAutocomplete": true,
        "fieldType": "TEXT_PARTIAL"
      },
      "type": "string",
      "name": "orchestrator",
      "doc": "Standardized Orchestrator where data process is defined.\nTODO: Migrate towards something that can be validated like DataPlatform urn"
    },
    {
      "Searchable": {
        "fieldType": "TEXT_PARTIAL",
        "queryByDefault": false
      },
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
      "name": "origin",
      "doc": "Fabric type where dataset belongs to or where it was generated."
    }
  ],
  "doc": "Key for a Data Process"
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





#### dataProcessInfo
The inputs and outputs of this data process



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| inputs | string[] |  | the inputs of the data process | Searchable, → Consumes |
| outputs | string[] |  | the outputs of the data process | Searchable, → Consumes |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataProcessInfo"
  },
  "name": "DataProcessInfo",
  "namespace": "com.linkedin.dataprocess",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataset"
          ],
          "isLineage": true,
          "name": "Consumes"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "inputs",
          "fieldType": "URN",
          "numValuesFieldName": "numInputDatasets",
          "queryByDefault": false
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "inputs",
      "default": null,
      "doc": "the inputs of the data process"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataset"
          ],
          "isLineage": true,
          "name": "Consumes"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "outputs",
          "fieldType": "URN",
          "numValuesFieldName": "numOutputDatasets",
          "queryByDefault": false
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "outputs",
      "default": null,
      "doc": "the outputs of the data process"
    }
  ],
  "doc": "The inputs and outputs of this data process"
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





#### testResults
Information about a Test Result



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| failing | [TestResult](#testresult)[] | ✓ | Results that are failing | Searchable, → IsFailing |
| passing | [TestResult](#testresult)[] | ✓ | Results that are passing | Searchable, → IsPassing |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "testResults"
  },
  "name": "TestResults",
  "namespace": "com.linkedin.test",
  "fields": [
    {
      "Relationship": {
        "/*/test": {
          "entityTypes": [
            "test"
          ],
          "name": "IsFailing"
        }
      },
      "Searchable": {
        "/*/test": {
          "fieldName": "failingTests",
          "fieldType": "URN",
          "hasValuesFieldName": "hasFailingTests",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "TestResult",
          "namespace": "com.linkedin.test",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "test",
              "doc": "The urn of the test"
            },
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "FAILURE": " The Test Failed",
                  "SUCCESS": " The Test Succeeded"
                },
                "name": "TestResultType",
                "namespace": "com.linkedin.test",
                "symbols": [
                  "SUCCESS",
                  "FAILURE"
                ]
              },
              "name": "type",
              "doc": "The type of the result"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "testDefinitionMd5",
              "default": null,
              "doc": "The md5 of the test definition that was used to compute this result.\nSee TestInfo.testDefinition.md5 for more information."
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
              "name": "lastComputed",
              "default": null,
              "doc": "The audit stamp of when the result was computed, including the actor who computed it."
            }
          ],
          "doc": "Information about a Test Result"
        }
      },
      "name": "failing",
      "doc": "Results that are failing"
    },
    {
      "Relationship": {
        "/*/test": {
          "entityTypes": [
            "test"
          ],
          "name": "IsPassing"
        }
      },
      "Searchable": {
        "/*/test": {
          "fieldName": "passingTests",
          "fieldType": "URN",
          "hasValuesFieldName": "hasPassingTests",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.test.TestResult"
      },
      "name": "passing",
      "doc": "Results that are passing"
    }
  ],
  "doc": "Information about a Test Result"
}
```





#### subTypes
Sub Types. Use this aspect to specialize a generic Entity
e.g. Making a Dataset also be a View or also be a LookerExplore



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| typeNames | string[] | ✓ | The names of the specific types. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "subTypes"
  },
  "name": "SubTypes",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldType": "KEYWORD",
          "filterNameOverride": "Sub Type",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "typeNames",
      "doc": "The names of the specific types."
    }
  ],
  "doc": "Sub Types. Use this aspect to specialize a generic Entity\ne.g. Making a Dataset also be a View or also be a LookerExplore"
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

#### TestResult

Information about a Test Result

**Fields:**

- `test` (string): The urn of the test
- `type` (TestResultType): The type of the result
- `testDefinitionMd5` (string?): The md5 of the test definition that was used to compute this result. See Test...
- `lastComputed` (AuditStamp?): The audit stamp of when the result was computed, including the actor who comp...


### Relationships

#### Outgoing
These are the relationships stored in this entity's aspects
- OwnedBy

   - Corpuser via `ownership.owners.owner`
   - CorpGroup via `ownership.owners.owner`
- ownershipType

   - OwnershipType via `ownership.owners.typeUrn`
- Consumes

   - Dataset via `dataProcessInfo.inputs`
   - Dataset via `dataProcessInfo.outputs`
- IsFailing

   - Test via `testResults.failing`
- IsPassing

   - Test via `testResults.passing`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
