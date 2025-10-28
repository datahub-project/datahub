# DataFlow

A DataFlow represents a high-level data processing pipeline or workflow orchestrated by systems like Apache Airflow, Azkaban, Prefect, Dagster, or similar workflow management platforms. DataFlows serve as containers for related DataJobs, representing the overall execution context and organization of data processing tasks.

## Identity

DataFlows are uniquely identified by three components:

- **Orchestrator**: The workflow management platform that executes the flow (e.g., `airflow`, `azkaban`, `prefect`, `dagster`)
- **Flow ID**: A unique identifier for the workflow within the orchestrator (typically the DAG name, pipeline name, or workflow identifier)
- **Cluster**: The execution environment or cluster where the flow runs (e.g., `prod`, `staging`, `dev`)

The URN structure follows this pattern:

```
urn:li:dataFlow:(<orchestrator>,<flowId>,<cluster>)
```

### URN Examples

**Apache Airflow DAG in production:**

```
urn:li:dataFlow:(airflow,daily_sales_pipeline,prod)
```

**Prefect flow in staging:**

```
urn:li:dataFlow:(prefect,customer_analytics,staging)
```

**Azkaban workflow in development:**

```
urn:li:dataFlow:(azkaban,data_quality_checks,dev)
```

## Important Capabilities

### Core DataFlow Information

DataFlows maintain essential metadata about the pipeline through the `dataFlowInfo` aspect:

- **Name**: The display name of the flow (may differ from flow_id)
- **Description**: Detailed explanation of what the flow does and its purpose
- **Project**: Optional namespace or project the flow belongs to
- **Created/Modified Timestamps**: When the flow was created and last modified in the source system
- **External URL**: Link to the flow in the orchestration platform's UI
- **Custom Properties**: Key-value pairs for additional platform-specific metadata
- **Environment**: The fabric type (PROD, DEV, STAGING, etc.)

### Editable Properties

The `editableDataFlowProperties` aspect allows users to modify certain properties through the DataHub UI without interfering with ingestion from source systems:

- **Description**: User-edited documentation that takes precedence over ingested descriptions

This separation ensures that edits made in DataHub are preserved and not overwritten by subsequent ingestion runs.

### Version Information

The `versionInfo` aspect tracks versioning details for the flow:

- **Version**: An identifier like a git commit hash or md5 hash
- **Version Type**: The type of version identifier being used (e.g., "git", "md5")

This is particularly useful for tracking changes to pipeline code and correlating pipeline versions with their execution history.

### Relationship with DataJobs

DataFlows act as parent entities for DataJobs. Each DataJob's identity includes a reference to its parent DataFlow through the `flow` field in the DataJobKey. This creates a hierarchical relationship:

```
DataFlow (Pipeline)
  └─ DataJob (Task 1)
  └─ DataJob (Task 2)
  └─ DataJob (Task 3)
```

This structure mirrors how workflow orchestrators organize tasks within DAGs or pipelines.

### Incidents Tracking

The `incidentsSummary` aspect provides visibility into data quality or operational issues:

- **Active Incidents**: Currently unresolved incidents affecting this flow
- **Resolved Incidents**: Historical record of incidents that have been addressed
- **Incident Details**: Type, priority, creation time, and resolution time for each incident

This enables DataHub to serve as a centralized incident management system for data pipelines.

## Code Examples

### Creating a DataFlow

<details>
<summary>Python SDK: Create a basic DataFlow</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataflow_create.py show_path_as_comment }}
```

</details>

### Creating a DataFlow with Comprehensive Metadata

<details>
<summary>Python SDK: Create a DataFlow with owners, tags, and custom properties</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataflow_comprehensive.py show_path_as_comment }}
```

</details>

### Reading DataFlow Metadata

<details>
<summary>Python SDK: Read a DataFlow and access its properties</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataflow_read.py show_path_as_comment }}
```

</details>

### Adding Tags and Terms to a DataFlow

<details>
<summary>Python SDK: Add tags and glossary terms to a DataFlow</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataflow_add_tags_terms.py show_path_as_comment }}
```

</details>

### Adding Ownership to a DataFlow

<details>
<summary>Python SDK: Set owners for a DataFlow</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataflow_add_ownership.py show_path_as_comment }}
```

</details>

### Querying DataFlow via REST API

DataFlows can be queried using the standard DataHub REST APIs:

<details>
<summary>REST API: Fetch a DataFlow entity</summary>

```bash
# Get a complete DataFlow snapshot
curl 'http://localhost:8080/entities/urn%3Ali%3AdataFlow%3A(airflow,daily_sales_pipeline,prod)'
```

Response includes all aspects:

```json
{
  "urn": "urn:li:dataFlow:(airflow,daily_sales_pipeline,prod)",
  "aspects": {
    "dataFlowKey": {
      "orchestrator": "airflow",
      "flowId": "daily_sales_pipeline",
      "cluster": "prod"
    },
    "dataFlowInfo": {
      "name": "Daily Sales Pipeline",
      "description": "Processes daily sales data and updates aggregates",
      "project": "analytics",
      "externalUrl": "https://airflow.company.com/dags/daily_sales_pipeline"
    },
    "ownership": { ... },
    "globalTags": { ... }
  }
}
```

</details>

### Creating DataFlow with DataJobs

<details>
<summary>Python SDK: Create a DataFlow with associated DataJobs</summary>

```python
{{ inline /metadata-ingestion/examples/library/datajob_create_full.py show_path_as_comment }}
```

</details>

## Integration Points

### Relationship with DataJobs

DataFlows have a parent-child relationship with DataJobs through the `IsPartOf` relationship. This is the primary integration point:

```python
from datahub.sdk import DataFlow, DataJob, DataHubClient

# DataJob automatically links to its parent DataFlow
flow = DataFlow(platform="airflow", name="my_dag")
job = DataJob(name="extract_data", flow=flow)
```

### Relationship with Datasets

While DataFlows don't directly reference datasets, their child DataJobs establish lineage relationships with datasets through:

- **Inlets**: Input datasets consumed by jobs
- **Outlets**: Output datasets produced by jobs

This creates indirect lineage from DataFlows to datasets through their constituent jobs.

### Orchestrator Platform Integration

Common orchestrators that produce DataFlow entities:

- **Apache Airflow**: Each DAG becomes a DataFlow, tasks become DataJobs
- **Prefect**: Flows are DataFlows, tasks are DataJobs
- **Dagster**: Jobs/Pipelines are DataFlows, ops/solids are DataJobs
- **Azkaban**: Flows are DataFlows, jobs are DataJobs
- **Fivetran**: Connectors are represented as DataFlows with sync operations as DataJobs

### DataProcessInstance Tracking

DataFlow executions can be tracked using DataProcessInstance entities, which record:

- Start and end times of flow runs
- Success/failure status
- Input and output datasets for specific runs

This enables tracking of pipeline run history and troubleshooting failures.

### GraphQL Integration

The DataFlow entity is exposed through DataHub's GraphQL API with full support for:

- Querying flow metadata
- Browsing flows by orchestrator and cluster
- Searching flows by name, description, and project
- Updating flow properties (ownership, tags, terms, etc.)

Key GraphQL resolvers:

- `dataFlow`: Fetch a single DataFlow by URN
- `searchAcrossEntities`: Search for DataFlows with filters
- `updateDataFlow`: Modify DataFlow properties

## Notable Exceptions

### Cluster vs Environment

While DataFlows use the `cluster` field in their URN for identification, they also have an `env` field in the `dataFlowInfo` aspect. These serve different purposes:

- **Cluster**: Part of the identity, typically matches the environment but can represent specific deployment instances
- **Environment (env)**: A semantic indicator of the fabric type (PROD, DEV, STAGING) that's searchable and filterable

In most cases, these should align (e.g., cluster="prod" and env="PROD"), but they can diverge when multiple production clusters exist or when representing complex deployment topologies.

### Platform vs Orchestrator

Throughout the codebase, you'll see both terms used:

- In the URN structure and key aspect, the field is called `orchestrator`
- In Python SDK and some APIs, it's referred to as `platform`

These are synonymous and refer to the same concept: the workflow management system executing the flow.

### Legacy API vs SDK

Two APIs exist for creating DataFlows:

1. **Legacy API** (`datahub.api.entities.datajob.DataFlow`): Uses the older emitter pattern
2. **Modern SDK** (`datahub.sdk.DataFlow`): Preferred approach with cleaner interfaces

New code should use the modern SDK (imported from `datahub.sdk`), though both are maintained for backward compatibility.

### DataFlow vs DataPipeline

In some contexts, you might see references to "data pipelines" in documentation or UI. These are informal terms that refer to DataFlows. The formal entity type in the metadata model is `dataFlow`, not `dataPipeline`.
