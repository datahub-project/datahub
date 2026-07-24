# DataJob

Data jobs represent individual units of data processing work within a data pipeline or workflow. They are the tasks, steps, or operations that transform, move, or process data as part of a larger data flow. Examples include Airflow tasks, dbt models, Spark jobs, Databricks notebooks, and similar processing units in orchestration systems.

## Identity

Data jobs are identified by two pieces of information:

- The data flow (pipeline/workflow) that they belong to: this is represented as a URN pointing to the parent `dataFlow` entity. The data flow defines the orchestrator (e.g., `airflow`, `spark`, `dbt`), the flow ID (e.g., the DAG name or pipeline name), and the cluster where it runs.
- The unique job identifier within that flow: this is a string that uniquely identifies the task within its parent flow (e.g., task name, step name, model name).

The URN structure for a data job is: `urn:li:dataJob:(urn:li:dataFlow:(<orchestrator>,<flow_id>,<cluster>),<job_id>)`

### Examples

**Airflow task:**

```
urn:li:dataJob:(urn:li:dataFlow:(airflow,daily_etl_dag,prod),transform_customer_data)
```

**dbt model:**

```
urn:li:dataJob:(urn:li:dataFlow:(dbt,analytics_project,prod),staging.stg_customers)
```

**Spark job:**

```
urn:li:dataJob:(urn:li:dataFlow:(spark,data_processing_pipeline,PROD),aggregate_sales_task)
```

**Databricks notebook:**

```
urn:li:dataJob:(urn:li:dataFlow:(databricks,etl_workflow,production),process_events_notebook)
```

## Important Capabilities

### Job Information (dataJobInfo)

The `dataJobInfo` aspect captures the core properties of a data job:

- **Name**: Human-readable name of the job (searchable with autocomplete)
- **Description**: Detailed description of what the job does
- **Type**: The type of job (e.g., SQL, Python, Spark, etc.)
- **Flow URN**: Reference to the parent data flow
- **Created/Modified timestamps**: When the job was created or last modified in the source system
- **Environment**: The fabric/environment where the job runs (PROD, DEV, QA, etc.)
- **Custom properties**: Additional key-value properties specific to the source system
- **External references**: Links to external documentation or definitions (e.g., GitHub links)

### Input/Output Lineage (dataJobInputOutput)

The `dataJobInputOutput` aspect defines the data lineage relationships for the job:

- **Input datasets**: Datasets consumed by the job during processing (via `inputDatasetEdges`)
- **Output datasets**: Datasets produced by the job (via `outputDatasetEdges`)
- **Input data jobs**: Other data jobs that this job depends on (via `inputDatajobEdges`)
- **Input dataset fields**: Specific schema fields consumed from input datasets
- **Output dataset fields**: Specific schema fields produced in output datasets
- **Fine-grained lineage**: Column-level lineage mappings showing which upstream fields contribute to downstream fields

This aspect establishes the critical relationships that enable DataHub to build and visualize data lineage graphs across your entire data ecosystem.

### Editable Properties (editableDataJobProperties)

The `editableDataJobProperties` aspect stores documentation edits made through the DataHub UI:

- **Description**: User-edited documentation that complements or overrides the ingested description
- **Change audit stamps**: Tracks who made edits and when

This separation ensures that manual edits in the UI are preserved and not overwritten by ingestion pipelines.

### Ownership

Like other entities, data jobs support ownership through the `ownership` aspect. Owners can be users or groups with various ownership types (DATAOWNER, PRODUCER, DEVELOPER, etc.). This helps identify who is responsible for maintaining and troubleshooting the job.

### Tags and Glossary Terms

Data jobs can be tagged and associated with glossary terms:

- **Tags** (`globalTags` aspect): Used for categorization, classification, or operational purposes (e.g., PII, critical, deprecated)
- **Glossary terms** (`glossaryTerms` aspect): Link jobs to business terminology and concepts from your glossary

### Domains and Applications

Data jobs can be organized into:

- **Domains** (`domains` aspect): Business domains or data domains for organizational structure
- **Applications** (`applications` aspect): Associated with specific applications or systems

### Structured Properties and Forms

Data jobs support:

- **Structured properties**: Custom typed properties defined by your organization
- **Forms**: Structured documentation forms for consistency

## Code Examples

### Creating a Data Job

The simplest way to create a data job is using the Python SDK v2:


**Python SDK: Create a basic data job**

```python
# Inlined from /metadata-ingestion/examples/library/datajob_create_basic.py
# metadata-ingestion/examples/library/datajob_create_basic.py
from datahub.metadata.urns import DataFlowUrn, DatasetUrn
from datahub.sdk import DataHubClient, DataJob

client = DataHubClient.from_env()

datajob = DataJob(
    name="transform_customer_data",
    flow_urn=DataFlowUrn(
        orchestrator="airflow",
        flow_id="daily_etl_pipeline",
        cluster="prod",
    ),
    description="Transforms raw customer data into analytics-ready format",
    inlets=[
        DatasetUrn(platform="postgres", name="raw.customers", env="PROD"),
        DatasetUrn(platform="postgres", name="raw.addresses", env="PROD"),
    ],
    outlets=[
        DatasetUrn(platform="snowflake", name="analytics.dim_customers", env="PROD"),
    ],
)

client.entities.upsert(datajob)
print(f"Created data job: {datajob.urn}")

```



### Adding Tags, Terms, and Ownership

Common metadata can be added to data jobs to enhance discoverability and governance:


**Python SDK: Add tags, terms, and ownership to a data job**

```python
# Inlined from /metadata-ingestion/examples/library/datajob_add_tags_terms_ownership.py
# metadata-ingestion/examples/library/datajob_add_tags_terms_ownership.py
from datahub.metadata.urns import (
    CorpUserUrn,
    DataFlowUrn,
    DataJobUrn,
    GlossaryTermUrn,
    TagUrn,
)
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

datajob_urn = DataJobUrn(
    job_id="transform_customer_data",
    flow=DataFlowUrn(
        orchestrator="airflow", flow_id="daily_etl_pipeline", cluster="prod"
    ),
)

datajob = client.entities.get(datajob_urn)

datajob.add_tag(TagUrn("Critical"))
datajob.add_tag(TagUrn("ETL"))

datajob.add_term(GlossaryTermUrn("CustomerData"))
datajob.add_term(GlossaryTermUrn("DataTransformation"))

datajob.add_owner(CorpUserUrn("data_engineering_team"))
datajob.add_owner(CorpUserUrn("john.doe"))

client.entities.update(datajob)

print(f"Added tags, terms, and ownership to {datajob_urn}")

```



### Updating Job Properties

You can update job properties like descriptions using the low-level APIs:


**Python SDK: Update data job description**

```python
# Inlined from /metadata-ingestion/examples/library/datajob_update_description.py
# metadata-ingestion/examples/library/datajob_update_description.py
from datahub.sdk import DataFlowUrn, DataHubClient, DataJobUrn

client = DataHubClient.from_env()

dataflow_urn = DataFlowUrn(
    orchestrator="airflow", flow_id="daily_etl_pipeline", cluster="prod"
)
datajob_urn = DataJobUrn(flow=dataflow_urn, job_id="transform_customer_data")

datajob = client.entities.get(datajob_urn)
datajob.set_description(
    "This job performs critical customer data transformation. "
    "It joins raw customer records with address information and applies "
    "data quality rules before loading into the analytics warehouse."
)

client.entities.update(datajob)

print(f"Updated description for {datajob_urn}")

```



### Querying Data Job Information

Retrieve data job information via the REST API:


**REST API: Query a data job**

```python
# Inlined from /metadata-ingestion/examples/library/datajob_query_rest.py
# metadata-ingestion/examples/library/datajob_query_rest.py
import json
from urllib.parse import quote

import requests

datajob_urn = "urn:li:dataJob:(urn:li:dataFlow:(airflow,daily_etl_pipeline,prod),transform_customer_data)"

gms_server = "http://localhost:8080"
url = f"{gms_server}/entities/{quote(datajob_urn, safe='')}"

response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    print(json.dumps(data, indent=2))

    if "aspects" in data:
        aspects = data["aspects"]

        if "dataJobInfo" in aspects:
            job_info = aspects["dataJobInfo"]["value"]
            print(f"\nJob Name: {job_info.get('name')}")
            print(f"Description: {job_info.get('description')}")
            print(f"Type: {job_info.get('type')}")

        if "dataJobInputOutput" in aspects:
            lineage = aspects["dataJobInputOutput"]["value"]
            print(f"\nInput Datasets: {len(lineage.get('inputDatasetEdges', []))}")
            print(f"Output Datasets: {len(lineage.get('outputDatasetEdges', []))}")

        if "ownership" in aspects:
            ownership = aspects["ownership"]["value"]
            print(f"\nOwners: {len(ownership.get('owners', []))}")
            for owner in ownership.get("owners", []):
                print(f"  - {owner.get('owner')} ({owner.get('type')})")

        if "globalTags" in aspects:
            tags = aspects["globalTags"]["value"]
            print("\nTags:")
            for tag in tags.get("tags", []):
                print(f"  - {tag.get('tag')}")
else:
    print(f"Failed to retrieve data job: {response.status_code}")
    print(response.text)

```



### Adding Lineage to Data Jobs

Data jobs are often used to define lineage relationships. See the existing lineage examples:


**Python SDK: Add lineage using DataJobPatchBuilder**

```python
# Inlined from /metadata-ingestion/examples/library/datajob_add_lineage_patch.py
from datahub.emitter.mce_builder import (
    make_data_job_urn,
    make_dataset_urn,
    make_schema_field_urn,
)
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass as FineGrainedLineage,
    FineGrainedLineageDownstreamTypeClass as FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamTypeClass as FineGrainedLineageUpstreamType,
)
from datahub.specific.datajob import DataJobPatchBuilder

# Create DataHub Client
datahub_client = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Create DataJob URN
datajob_urn = make_data_job_urn(
    orchestrator="airflow", flow_id="dag_abc", job_id="task_456"
)

# Create DataJob Patch to Add Lineage
patch_builder = DataJobPatchBuilder(datajob_urn)
patch_builder.add_input_dataset(
    make_dataset_urn(platform="kafka", name="SampleKafkaDataset", env="PROD")
)
patch_builder.add_output_dataset(
    make_dataset_urn(platform="hive", name="SampleHiveDataset", env="PROD")
)
patch_builder.add_input_datajob(
    make_data_job_urn(orchestrator="airflow", flow_id="dag_abc", job_id="task_123")
)
patch_builder.add_input_dataset_field(
    make_schema_field_urn(
        parent_urn=make_dataset_urn(
            platform="hive", name="fct_users_deleted", env="PROD"
        ),
        field_path="user_id",
    )
)
patch_builder.add_output_dataset_field(
    make_schema_field_urn(
        parent_urn=make_dataset_urn(
            platform="hive", name="fct_users_created", env="PROD"
        ),
        field_path="user_id",
    )
)

# Update column-level lineage through the Data Job
lineage1 = FineGrainedLineage(
    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
    upstreams=[
        make_schema_field_urn(make_dataset_urn("postgres", "raw_data.users"), "user_id")
    ],
    downstreamType=FineGrainedLineageDownstreamType.FIELD,
    downstreams=[
        make_schema_field_urn(
            make_dataset_urn("postgres", "analytics.user_metrics"),
            "user_id",
        )
    ],
    transformOperation="IDENTITY",
    confidenceScore=1.0,
)
patch_builder.add_fine_grained_lineage(lineage1)
patch_builder.remove_fine_grained_lineage(lineage1)
# Replaces all existing fine-grained lineages
patch_builder.set_fine_grained_lineages([lineage1])

patch_mcps = patch_builder.build()

# Emit DataJob Patch
for patch_mcp in patch_mcps:
    datahub_client.emit(patch_mcp)

```




**Python SDK: Define fine-grained lineage through a data job**

```python
# Inlined from /metadata-ingestion/examples/library/lineage_emitter_datajob_finegrained.py
import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
)
from datahub.metadata.schema_classes import DataJobInputOutputClass


def datasetUrn(tbl):
    return builder.make_dataset_urn("postgres", tbl)


def fldUrn(tbl, fld):
    return builder.make_schema_field_urn(datasetUrn(tbl), fld)


# Lineage of fields output by a job
# bar.c1          <-- unknownFunc(bar2.c1, bar4.c1)
# bar.c2          <-- myfunc(bar3.c2)
# {bar.c3,bar.c4} <-- unknownFunc(bar2.c2, bar2.c3, bar3.c1)
# bar.c5          <-- unknownFunc(bar3)
# {bar.c6,bar.c7} <-- unknownFunc(bar4)
# bar2.c9 has no upstream i.e. its values are somehow created independently within this job.

# Note that the semantic of the "transformOperation" value is contextual.
# In above example, it is regarded as some kind of UDF; but it could also be an expression etc.

fineGrainedLineages = [
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
        upstreams=[fldUrn("bar2", "c1"), fldUrn("bar4", "c1")],
        downstreamType=FineGrainedLineageDownstreamType.FIELD,
        downstreams=[fldUrn("bar", "c1")],
    ),
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
        upstreams=[fldUrn("bar3", "c2")],
        downstreamType=FineGrainedLineageDownstreamType.FIELD,
        downstreams=[fldUrn("bar", "c2")],
        confidenceScore=0.8,
        transformOperation="myfunc",
    ),
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
        upstreams=[fldUrn("bar2", "c2"), fldUrn("bar2", "c3"), fldUrn("bar3", "c1")],
        downstreamType=FineGrainedLineageDownstreamType.FIELD_SET,
        downstreams=[fldUrn("bar", "c3"), fldUrn("bar", "c4")],
        confidenceScore=0.7,
    ),
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.DATASET,
        upstreams=[datasetUrn("bar3")],
        downstreamType=FineGrainedLineageDownstreamType.FIELD,
        downstreams=[fldUrn("bar", "c5")],
    ),
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.DATASET,
        upstreams=[datasetUrn("bar4")],
        downstreamType=FineGrainedLineageDownstreamType.FIELD_SET,
        downstreams=[fldUrn("bar", "c6"), fldUrn("bar", "c7")],
    ),
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.NONE,
        upstreams=[],
        downstreamType=FineGrainedLineageDownstreamType.FIELD,
        downstreams=[fldUrn("bar2", "c9")],
    ),
]

# The lineage of output col bar.c9 is unknown. So there is no lineage for it above.
# Note that bar2 is an input as well as an output dataset, but some fields are inputs while other fields are outputs.

dataJobInputOutput = DataJobInputOutputClass(
    inputDatasets=[datasetUrn("bar2"), datasetUrn("bar3"), datasetUrn("bar4")],
    outputDatasets=[datasetUrn("bar"), datasetUrn("bar2")],
    inputDatajobs=None,
    inputDatasetFields=[
        fldUrn("bar2", "c1"),
        fldUrn("bar2", "c2"),
        fldUrn("bar2", "c3"),
        fldUrn("bar3", "c1"),
        fldUrn("bar3", "c2"),
        fldUrn("bar4", "c1"),
    ],
    outputDatasetFields=[
        fldUrn("bar", "c1"),
        fldUrn("bar", "c2"),
        fldUrn("bar", "c3"),
        fldUrn("bar", "c4"),
        fldUrn("bar", "c5"),
        fldUrn("bar", "c6"),
        fldUrn("bar", "c7"),
        fldUrn("bar", "c9"),
        fldUrn("bar2", "c9"),
    ],
    fineGrainedLineages=fineGrainedLineages,
)

dataJobLineageMcp = MetadataChangeProposalWrapper(
    entityUrn=builder.make_data_job_urn("spark", "Flow1", "Task1"),
    aspect=dataJobInputOutput,
)

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

# Emit metadata!
emitter.emit_mcp(dataJobLineageMcp)

```



## Integration Points

### Relationship with DataFlow

Every data job belongs to exactly one `dataFlow` entity, which represents the parent pipeline or workflow. The data flow captures:

- The orchestrator/platform (Airflow, Spark, dbt, etc.)
- The flow/pipeline/DAG identifier
- The cluster or environment where it executes

This hierarchical relationship allows DataHub to organize jobs within their workflows and understand the execution context.

### Relationship with Datasets

Data jobs establish lineage by defining:

- **Consumes** relationships with input datasets
- **Produces** relationships with output datasets

These relationships are the foundation of DataHub's lineage graph. When a job processes data, it creates a connection between upstream sources and downstream outputs, enabling impact analysis and data discovery.

### Relationship with DataProcessInstance

While `dataJob` represents the definition of a processing task, `dataProcessInstance` represents a specific execution or run of that job. Process instances capture:

- Runtime information (start time, end time, duration)
- Status (success, failure, running)
- Input/output datasets for that specific run
- Error messages and logs

This separation allows you to track both the static definition of a job and its dynamic runtime behavior.

### GraphQL Resolvers

The DataHub GraphQL API provides rich query capabilities for data jobs:

- **DataJobType**: Main type for querying data job information
- **DataJobRunsResolver**: Resolves execution history and run information
- **DataFlowDataJobsRelationshipsMapper**: Maps relationships between flows and jobs
- **UpdateLineageResolver**: Handles lineage updates for jobs

### Ingestion Sources

Data jobs are commonly ingested from:

- **Airflow**: Tasks and DAGs with lineage extraction
- **dbt**: Models as data jobs with SQL-based lineage
- **Spark**: Job definitions with dataset dependencies
- **Databricks**: Notebooks and workflows
- **Dagster**: Ops and assets as processing units
- **Prefect**: Tasks and flows
- **AWS Glue**: ETL jobs
- **Azure Data Factory**: Pipeline activities
- **Looker**: LookML models and derived tables

These connectors automatically extract job definitions, lineage, and metadata from the source systems.

## Notable Exceptions

### DataHub Ingestion Jobs

DataHub's own ingestion pipelines are represented as data jobs with special aspects:

- **datahubIngestionRunSummary**: Tracks ingestion run statistics, entities processed, warnings, and errors
- **datahubIngestionCheckpoint**: Maintains state for incremental ingestion

These aspects are specific to DataHub's internal ingestion framework and are not used for general-purpose data jobs.

### Job Status Deprecation

The `status` field in `dataJobInfo` is deprecated in favor of the `dataProcessInstance` model. Instead of storing job status on the job definition itself, create separate process instance entities for each execution with their own status information. This provides a cleaner separation between job definitions and runtime execution history.

### Subtype Usage

The `subTypes` aspect allows you to classify jobs into categories:

- SQL jobs
- Python jobs
- Notebook jobs
- Container jobs
- Custom job types

This helps with filtering and organizing jobs in the UI and API queries.



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

#### dataJobKey
Key for a Data Job



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| flow | string | ✓ | Standardized data processing flow urn representing the flow for the job | Searchable (dataFlow), → IsPartOf |
| jobId | string | ✓ | Unique Identifier of the data job | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataJobKey"
  },
  "name": "DataJobKey",
  "namespace": "com.linkedin.metadata.key",
  "fields": [
    {
      "Relationship": {
        "entityTypes": [
          "dataFlow"
        ],
        "name": "IsPartOf"
      },
      "Searchable": {
        "fieldName": "dataFlow",
        "fieldType": "URN_PARTIAL",
        "queryByDefault": false
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "flow",
      "doc": "Standardized data processing flow urn representing the flow for the job"
    },
    {
      "Searchable": {
        "enableAutocomplete": true,
        "fieldType": "WORD_GRAM"
      },
      "type": "string",
      "name": "jobId",
      "doc": "Unique Identifier of the data job"
    }
  ],
  "doc": "Key for a Data Job"
}
```





#### dataJobInfo
Information about a Data processing job



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| customProperties | map | ✓ | Custom property bag. | Searchable |
| externalUrl | string |  | URL where the reference exist | Searchable |
| name | string | ✓ | Job name | Searchable |
| description | string |  | Job description | Searchable |
| type | union |  | Datajob type *NOTE**: AzkabanJobType is deprecated. Please use strings instead. |  |
| flowUrn | string |  | DataFlow urn that this job is part of |  |
| created | [TimeStamp](#timestamp) |  | A timestamp documenting when the asset was created in the source Data Platform (not on DataHub) | Searchable |
| lastModified | [TimeStamp](#timestamp) |  | A timestamp documenting when the asset was last modified in the source Data Platform (not on Data... | Searchable |
| status | JobStatus |  | Status of the job - Deprecated for Data Process Instance model. | ⚠️ Deprecated |
| env | FabricType |  | Environment for this job | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataJobInfo"
  },
  "name": "DataJobInfo",
  "namespace": "com.linkedin.datajob",
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
      "type": "string",
      "name": "name",
      "doc": "Job name"
    },
    {
      "Searchable": {
        "fieldType": "TEXT",
        "hasValuesFieldName": "hasDescription",
        "sanitizeRichText": true,
        "searchTier": 2
      },
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Job description"
    },
    {
      "type": [
        {
          "type": "enum",
          "symbolDocs": {
            "COMMAND": "The command job type is one of the basic built-in types. It runs multiple UNIX commands using java processbuilder.\nUpon execution, Azkaban spawns off a process to run the command.",
            "GLUE": "Glue type is for running AWS Glue job transforms.",
            "HADOOP_JAVA": "Runs a java program with ability to access Hadoop cluster.\nhttps://azkaban.readthedocs.io/en/latest/jobTypes.html#java-job-type",
            "HADOOP_SHELL": "In large part, this is the same Command type. The difference is its ability to talk to a Hadoop cluster\nsecurely, via Hadoop tokens.",
            "HIVE": "Hive type is for running Hive jobs.",
            "PIG": "Pig type is for running Pig jobs.",
            "SQL": "SQL is for running Presto, mysql queries etc"
          },
          "name": "AzkabanJobType",
          "namespace": "com.linkedin.datajob.azkaban",
          "symbols": [
            "COMMAND",
            "HADOOP_JAVA",
            "HADOOP_SHELL",
            "HIVE",
            "PIG",
            "SQL",
            "GLUE"
          ],
          "doc": "The various types of support azkaban jobs"
        },
        "string"
      ],
      "name": "type",
      "doc": "Datajob type\n*NOTE**: AzkabanJobType is deprecated. Please use strings instead."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.DataFlowUrn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "flowUrn",
      "default": null,
      "doc": "DataFlow urn that this job is part of"
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "createdAt",
          "fieldType": "DATETIME",
          "searchLabel": "createdAt"
        }
      },
      "type": [
        "null",
        {
          "type": "record",
          "name": "TimeStamp",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "long",
              "name": "time",
              "doc": "When did the event occur"
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "actor",
              "default": null,
              "doc": "Optional: The actor urn involved in the event."
            }
          ],
          "doc": "A standard event timestamp"
        }
      ],
      "name": "created",
      "default": null,
      "doc": "A timestamp documenting when the asset was created in the source Data Platform (not on DataHub)"
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "lastModifiedAt",
          "fieldType": "DATETIME",
          "searchLabel": "lastModifiedAt"
        }
      },
      "type": [
        "null",
        "com.linkedin.common.TimeStamp"
      ],
      "name": "lastModified",
      "default": null,
      "doc": "A timestamp documenting when the asset was last modified in the source Data Platform (not on DataHub)"
    },
    {
      "deprecated": "Use Data Process Instance model, instead",
      "type": [
        "null",
        {
          "type": "enum",
          "symbolDocs": {
            "COMPLETED": "Jobs with successful completion.",
            "FAILED": "Jobs that have failed.",
            "IN_PROGRESS": "Jobs currently running.",
            "SKIPPED": "Jobs that have been skipped.",
            "STARTING": "Jobs being initialized.",
            "STOPPED": "Jobs that have stopped.",
            "STOPPING": "Jobs being stopped.",
            "UNKNOWN": "Jobs with unknown status (either unmappable or unavailable)"
          },
          "name": "JobStatus",
          "namespace": "com.linkedin.datajob",
          "symbols": [
            "STARTING",
            "IN_PROGRESS",
            "STOPPING",
            "STOPPED",
            "COMPLETED",
            "FAILED",
            "UNKNOWN",
            "SKIPPED"
          ],
          "doc": "Job statuses"
        }
      ],
      "name": "status",
      "default": null,
      "doc": "Status of the job - Deprecated for Data Process Instance model."
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "KEYWORD",
        "filterNameOverride": "Environment",
        "queryByDefault": false
      },
      "type": [
        "null",
        {
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
        }
      ],
      "name": "env",
      "default": null,
      "doc": "Environment for this job"
    }
  ],
  "doc": "Information about a Data processing job"
}
```





#### dataJobInputOutput
Information about the inputs and outputs of a Data processing job



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| inputDatasets | string[] | ✓ | Input datasets consumed by the data job during processing Deprecated! Use inputDatasetEdges instead. | ⚠️ Deprecated, Searchable, → Consumes |
| inputDatasetEdges | [Edge](#edge)[] |  | Input datasets consumed by the data job during processing | Searchable, → Consumes |
| outputDatasets | string[] | ✓ | Output datasets produced by the data job during processing Deprecated! Use outputDatasetEdges ins... | ⚠️ Deprecated, Searchable, → Produces |
| outputDatasetEdges | [Edge](#edge)[] |  | Output datasets produced by the data job during processing | Searchable, → Produces |
| inputDatajobs | string[] |  | Input datajobs that this data job depends on Deprecated! Use inputDatajobEdges instead. | ⚠️ Deprecated, → DownstreamOf |
| inputDatajobEdges | [Edge](#edge)[] |  | Input datajobs that this data job depends on | → DownstreamOf |
| inputDatasetFields | string[] |  | Fields of the input datasets used by this job | Searchable, → Consumes |
| outputDatasetFields | string[] |  | Fields of the output datasets this job writes to | Searchable, → Produces |
| fineGrainedLineages | FineGrainedLineage[] |  | Fine-grained column-level lineages Not currently supported in the UI Use UpstreamLineage aspect f... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataJobInputOutput",
    "schemaVersion": 2
  },
  "name": "DataJobInputOutput",
  "namespace": "com.linkedin.datajob",
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
      "deprecated": true,
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "inputDatasets",
      "doc": "Input datasets consumed by the data job during processing\nDeprecated! Use inputDatasetEdges instead."
    },
    {
      "Relationship": {
        "/*/destinationUrn": {
          "createdActor": "inputDatasetEdges/*/created/actor",
          "createdOn": "inputDatasetEdges/*/created/time",
          "entityTypes": [
            "dataset"
          ],
          "isLineage": true,
          "name": "Consumes",
          "properties": "inputDatasetEdges/*/properties",
          "updatedActor": "inputDatasetEdges/*/lastModified/actor",
          "updatedOn": "inputDatasetEdges/*/lastModified/time"
        }
      },
      "Searchable": {
        "/*/destinationUrn": {
          "fieldName": "inputDatasetEdges",
          "fieldType": "URN",
          "numValuesFieldName": "numInputDatasets",
          "queryByDefault": false
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "Edge",
            "namespace": "com.linkedin.common",
            "fields": [
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": [
                  "null",
                  "string"
                ],
                "name": "sourceUrn",
                "default": null,
                "doc": "Urn of the source of this relationship edge.\nIf not specified, assumed to be the entity that this aspect belongs to."
              },
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "destinationUrn",
                "doc": "Urn of the destination of this relationship edge."
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
                "type": [
                  "null",
                  {
                    "type": "map",
                    "values": "string"
                  }
                ],
                "name": "properties",
                "default": null,
                "doc": "A generic properties bag that allows us to store specific information on this graph edge."
              }
            ],
            "doc": "A common structure to represent all edges to entities when used inside aspects as collections\nThis ensures that all edges have common structure around audit-stamps and will support PATCH, time-travel automatically."
          }
        }
      ],
      "name": "inputDatasetEdges",
      "default": null,
      "doc": "Input datasets consumed by the data job during processing"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataset"
          ],
          "isLineage": true,
          "isUpstream": false,
          "name": "Produces"
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
      "deprecated": true,
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "outputDatasets",
      "doc": "Output datasets produced by the data job during processing\nDeprecated! Use outputDatasetEdges instead."
    },
    {
      "Relationship": {
        "/*/destinationUrn": {
          "createdActor": "outputDatasetEdges/*/created/actor",
          "createdOn": "outputDatasetEdges/*/created/time",
          "entityTypes": [
            "dataset"
          ],
          "isLineage": true,
          "isUpstream": false,
          "name": "Produces",
          "properties": "outputDatasetEdges/*/properties",
          "updatedActor": "outputDatasetEdges/*/lastModified/actor",
          "updatedOn": "outputDatasetEdges/*/lastModified/time"
        }
      },
      "Searchable": {
        "/*/destinationUrn": {
          "fieldName": "outputDatasetEdges",
          "fieldType": "URN",
          "numValuesFieldName": "numOutputDatasets",
          "queryByDefault": false
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "com.linkedin.common.Edge"
        }
      ],
      "name": "outputDatasetEdges",
      "default": null,
      "doc": "Output datasets produced by the data job during processing"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataJob"
          ],
          "isLineage": true,
          "name": "DownstreamOf"
        }
      },
      "deprecated": true,
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "inputDatajobs",
      "default": null,
      "doc": "Input datajobs that this data job depends on\nDeprecated! Use inputDatajobEdges instead."
    },
    {
      "Relationship": {
        "/*/destinationUrn": {
          "createdActor": "inputDatajobEdges/*/created/actor",
          "createdOn": "inputDatajobEdges/*/created/time",
          "entityTypes": [
            "dataJob"
          ],
          "isLineage": true,
          "name": "DownstreamOf",
          "properties": "inputDatajobEdges/*/properties",
          "updatedActor": "inputDatajobEdges/*/lastModified/actor",
          "updatedOn": "inputDatajobEdges/*/lastModified/time"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "com.linkedin.common.Edge"
        }
      ],
      "name": "inputDatajobEdges",
      "default": null,
      "doc": "Input datajobs that this data job depends on"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "schemaField"
          ],
          "name": "Consumes"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "inputFields",
          "fieldType": "URN",
          "numValuesFieldName": "numInputFields",
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
      "name": "inputDatasetFields",
      "default": null,
      "doc": "Fields of the input datasets used by this job"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "schemaField"
          ],
          "name": "Produces"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "outputFields",
          "fieldType": "URN",
          "numValuesFieldName": "numOutputFields",
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
      "name": "outputDatasetFields",
      "default": null,
      "doc": "Fields of the output datasets this job writes to"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "FineGrainedLineage",
            "namespace": "com.linkedin.dataset",
            "fields": [
              {
                "type": {
                  "type": "enum",
                  "symbolDocs": {
                    "DATASET": " Indicates that this lineage is originating from upstream dataset(s)",
                    "FIELD_SET": " Indicates that this lineage is originating from upstream field(s)",
                    "NONE": " Indicates that there is no upstream lineage i.e. the downstream field is not a derived field"
                  },
                  "name": "FineGrainedLineageUpstreamType",
                  "namespace": "com.linkedin.dataset",
                  "symbols": [
                    "FIELD_SET",
                    "DATASET",
                    "NONE"
                  ],
                  "doc": "The type of upstream entity in a fine-grained lineage"
                },
                "name": "upstreamType",
                "doc": "The type of upstream entity"
              },
              {
                "Searchable": {
                  "/*": {
                    "fieldName": "fineGrainedUpstreams",
                    "fieldType": "URN",
                    "hasValuesFieldName": "hasFineGrainedUpstreams",
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
                "name": "upstreams",
                "default": null,
                "doc": "Upstream entities in the lineage"
              },
              {
                "type": {
                  "type": "enum",
                  "symbolDocs": {
                    "FIELD": " Indicates that the lineage is for a single, specific, downstream field",
                    "FIELD_SET": " Indicates that the lineage is for a set of downstream fields"
                  },
                  "name": "FineGrainedLineageDownstreamType",
                  "namespace": "com.linkedin.dataset",
                  "symbols": [
                    "FIELD",
                    "FIELD_SET"
                  ],
                  "doc": "The type of downstream field(s) in a fine-grained lineage"
                },
                "name": "downstreamType",
                "doc": "The type of downstream field(s)"
              },
              {
                "type": [
                  "null",
                  {
                    "type": "array",
                    "items": "string"
                  }
                ],
                "name": "downstreams",
                "default": null,
                "doc": "Downstream fields in the lineage"
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "transformOperation",
                "default": null,
                "doc": "The transform operation applied to the upstream entities to produce the downstream field(s)"
              },
              {
                "type": "float",
                "name": "confidenceScore",
                "default": 1.0,
                "doc": "The confidence in this lineage between 0 (low confidence) and 1 (high confidence)"
              },
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": [
                  "null",
                  "string"
                ],
                "name": "query",
                "default": null,
                "doc": "The query that was used to generate this lineage.\nPresent only if the lineage was generated from a detected query."
              },
              {
                "type": [
                  "null",
                  {
                    "type": "enum",
                    "symbolDocs": {
                      "EXACT": "The reference matched an existing entity exactly, including URN casing.",
                      "NORMALIZED": "The reference was case-normalized to match an existing entity whose URN uses\ndifferent casing (i.e. the reference was rewritten to heal a casing mismatch).",
                      "UNRESOLVED": "The reference is on a configured upstream platform but could not be resolved to\na single existing entity \u2014 either no entity matched (under any casing), or the\ncasing was ambiguous (multiple entities share the case-insensitive form). The\nreference was left unchanged; this flags potentially broken lineage."
                    },
                    "name": "LineageMatchType",
                    "namespace": "com.linkedin.dataset",
                    "symbols": [
                      "EXACT",
                      "NORMALIZED",
                      "UNRESOLVED"
                    ],
                    "doc": "How an upstream lineage reference's URN was resolved against the entities stored\nin DataHub. Populated by the lineage URN casing normalization processor for\nreferences on a configured upstream platform; absent when the reference is out of\nscope (platform not configured, feature disabled) or was ingested before the\nfeature was enabled.\n\nThis verdict reflects DataHub's knowledge AT THE TIME THE LINEAGE EDGE WAS\nINGESTED, not the current state of the graph. It is a point-in-time record and is\nnot re-evaluated automatically: e.g. a reference recorded as UNRESOLVED (its target\ndid not exist yet) keeps that value even after the target is later ingested and the\nedge in fact resolves exactly \u2014 the verdict only refreshes when the referencing\nsource is re-ingested."
                  }
                ],
                "name": "matchType",
                "default": null,
                "doc": "Aggregate of how the upstream field references' URNs were resolved against the\nentities stored in DataHub. Set by the lineage URN casing normalization processor:\nNORMALIZED if any field was rewritten to heal a casing mismatch, else UNRESOLVED if\nany could not be resolved, else EXACT. Absent when no reconciliation was performed\n(out of scope). Reflects DataHub's knowledge at ingestion time and is not\nre-evaluated later; see LineageMatchType."
              }
            ],
            "doc": "A fine-grained lineage from upstream fields/datasets to downstream field(s)"
          }
        }
      ],
      "name": "fineGrainedLineages",
      "default": null,
      "doc": "Fine-grained column-level lineages\nNot currently supported in the UI\nUse UpstreamLineage aspect for datasets to express Column Level Lineage for the UI"
    }
  ],
  "doc": "Information about the inputs and outputs of a Data processing job"
}
```





#### editableDataJobProperties
Stores editable changes made to properties. This separates changes made from
ingestion pipelines and edits in the UI to avoid accidental overwrites of user-provided data by ingestion pipelines



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| created | [AuditStamp](#auditstamp) | ✓ | An AuditStamp corresponding to the creation of this resource/association/sub-resource. A value of... |  |
| lastModified | [AuditStamp](#auditstamp) | ✓ | An AuditStamp corresponding to the last modification of this resource/association/sub-resource. I... |  |
| deleted | [AuditStamp](#auditstamp) |  | An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically,... |  |
| description | string |  | Edited documentation of the data job | Searchable (editedDescription) |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "editableDataJobProperties"
  },
  "name": "EditableDataJobProperties",
  "namespace": "com.linkedin.datajob",
  "fields": [
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
      "name": "created",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "An AuditStamp corresponding to the creation of this resource/association/sub-resource. A value of 0 for time indicates missing data."
    },
    {
      "type": "com.linkedin.common.AuditStamp",
      "name": "lastModified",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "An AuditStamp corresponding to the last modification of this resource/association/sub-resource. If no modification has happened since creation, lastModified should be the same as created. A value of 0 for time indicates missing data."
    },
    {
      "type": [
        "null",
        "com.linkedin.common.AuditStamp"
      ],
      "name": "deleted",
      "default": null,
      "doc": "An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically, deleted MUST have a later timestamp than creation. It may or may not have the same time as lastModified depending upon the resource/association/sub-resource semantics."
    },
    {
      "Searchable": {
        "fieldName": "editedDescription",
        "fieldType": "TEXT",
        "sanitizeRichText": true,
        "searchTier": 2
      },
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Edited documentation of the data job "
    }
  ],
  "doc": "Stores editable changes made to properties. This separates changes made from\ningestion pipelines and edits in the UI to avoid accidental overwrites of user-provided data by ingestion pipelines"
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





#### browsePaths
Shared aspect containing Browse Paths to be indexed for an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| paths | string[] | ✓ | A list of valid browse paths for the entity.  Browse paths are expected to be forward slash-separ... | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "browsePaths"
  },
  "name": "BrowsePaths",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "fieldName": "browsePaths",
          "fieldType": "BROWSE_PATH"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "paths",
      "doc": "A list of valid browse paths for the entity.\n\nBrowse paths are expected to be forward slash-separated strings. For example: 'prod/snowflake/datasetName'"
    }
  ],
  "doc": "Shared aspect containing Browse Paths to be indexed for an entity."
}
```





#### glossaryTerms
Related business terms information



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| terms | GlossaryTermAssociation[] | ✓ | The related business terms |  |
| auditStamp | [AuditStamp](#auditstamp) | ✓ | Audit stamp containing who reported the related business term |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "glossaryTerms"
  },
  "name": "GlossaryTerms",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "GlossaryTermAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "glossaryTerm"
                ],
                "name": "TermedWith"
              },
              "Searchable": {
                "addToFilters": true,
                "fieldName": "glossaryTerms",
                "fieldType": "URN",
                "filterNameOverride": "Glossary Term",
                "hasValuesFieldName": "hasGlossaryTerms",
                "includeSystemModifiedAt": true,
                "systemModifiedAtFieldName": "termsModifiedAt"
              },
              "java": {
                "class": "com.linkedin.common.urn.GlossaryTermUrn"
              },
              "type": "string",
              "name": "urn",
              "doc": "Urn of the applied glossary term"
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "actor",
              "default": null,
              "doc": "The user URN which will be credited for adding associating this term to the entity"
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
                  "fieldName": "termAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "termAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "termAttributionDates",
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
          "doc": "Properties of an applied glossary term."
        }
      },
      "name": "terms",
      "doc": "The related business terms"
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
      "name": "auditStamp",
      "doc": "Audit stamp containing who reported the related business term"
    }
  ],
  "doc": "Related business terms information"
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





#### dataPlatformInstance
The specific instance of the data platform that this entity belongs to



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| platform | string | ✓ | Data Platform | Searchable |
| instance | string |  | Instance of the data platform (e.g. db instance) | Searchable (platformInstance) |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataPlatformInstance"
  },
  "name": "DataPlatformInstance",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "URN",
        "filterNameOverride": "Platform"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "platform",
      "doc": "Data Platform"
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldName": "platformInstance",
        "fieldType": "URN",
        "filterNameOverride": "Platform Instance"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "instance",
      "default": null,
      "doc": "Instance of the data platform (e.g. db instance)"
    }
  ],
  "doc": "The specific instance of the data platform that this entity belongs to"
}
```





#### browsePathsV2
Shared aspect containing a Browse Path to be indexed for an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| path | BrowsePathEntry[] | ✓ | A valid browse path for the entity. This field is provided by DataHub by default. This aspect is ... | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "browsePathsV2"
  },
  "name": "BrowsePathsV2",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*/id": {
          "fieldName": "browsePathV2",
          "fieldType": "BROWSE_PATH_V2"
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "BrowsePathEntry",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "string",
              "name": "id",
              "doc": "The ID of the browse path entry. This is what gets stored in the index.\nIf there's an urn associated with this entry, id and urn will be the same"
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "urn",
              "default": null,
              "doc": "Optional urn pointing to some entity in DataHub"
            }
          ],
          "doc": "Represents a single level in an entity's browsePathV2"
        }
      },
      "name": "path",
      "doc": "A valid browse path for the entity. This field is provided by DataHub by default.\nThis aspect is a newer version of browsePaths where we can encode more information in the path.\nThis path is also based on containers for a given entity if it has containers.\n\nThis is stored in elasticsearch as unit-separator delimited strings and only includes platform specific folders or containers.\nThese paths should not include high level info captured elsewhere ie. Platform and Environment."
    }
  ],
  "doc": "Shared aspect containing a Browse Path to be indexed for an entity."
}
```





#### domains
Links from an Asset to its Domains



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| domains | string[] | ✓ | The Domains attached to an Asset | Searchable, → AssociatedWith |
| domainAssociations | DomainAssociation[] |  | Additional per-domain association metadata such as attribution and propagation source. A superset... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "domains",
    "schemaVersion": 2
  },
  "name": "Domains",
  "namespace": "com.linkedin.domain",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "domain"
          ],
          "name": "AssociatedWith"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "domains",
          "fieldType": "URN",
          "filterNameOverride": "Domain",
          "hasValuesFieldName": "hasDomain"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "domains",
      "doc": "The Domains attached to an Asset"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "DomainAssociation",
            "namespace": "com.linkedin.domain",
            "fields": [
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "domain",
                "doc": "Urn of the associated domain. Corresponds to an entry in the parallel domains array."
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
                    "fieldName": "domainAttributionActors",
                    "fieldType": "URN",
                    "queryByDefault": false
                  },
                  "/source": {
                    "fieldName": "domainAttributionSources",
                    "fieldType": "URN",
                    "queryByDefault": false
                  },
                  "/time": {
                    "fieldName": "domainAttributionDates",
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
                "doc": "Information about who, why, and how this domain was applied.\nsourceDetail may carry flags such as 'propagated'='true' when set via glossary tree propagation."
              }
            ],
            "doc": "Properties of an applied domain association."
          }
        }
      ],
      "name": "domainAssociations",
      "default": null,
      "doc": "Additional per-domain association metadata such as attribution and propagation source.\nA superset of the domains field; entries correspond by domain URN.\nInitial migration handled by the DomainsMigrationMutator;\nthe two fields are kept in sync via the DomainsSyncMutationHook."
    }
  ],
  "doc": "Links from an Asset to its Domains"
}
```





#### applications
Links from an Asset to its Applications



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| applications | string[] | ✓ | The Applications attached to an Asset | Searchable, → AssociatedWith |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "applications"
  },
  "name": "Applications",
  "namespace": "com.linkedin.application",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "application"
          ],
          "name": "AssociatedWith"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "applications",
          "fieldType": "URN",
          "filterNameOverride": "Application",
          "hasValuesFieldName": "hasApplication"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "applications",
      "doc": "The Applications attached to an Asset"
    }
  ],
  "doc": "Links from an Asset to its Applications"
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





#### versionInfo
Information about a Data processing job



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| customProperties | map | ✓ | Custom property bag. | Searchable |
| externalUrl | string |  | URL where the reference exist | Searchable |
| version | string | ✓ | The version which can indentify a job version like a commit hash or md5 hash |  |
| versionType | string | ✓ | The type of the version like git hash or md5 hash |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "versionInfo"
  },
  "name": "VersionInfo",
  "namespace": "com.linkedin.datajob",
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
      "type": "string",
      "name": "version",
      "doc": "The version which can indentify a job version like a commit hash or md5 hash"
    },
    {
      "type": "string",
      "name": "versionType",
      "doc": "The type of the version like git hash or md5 hash"
    }
  ],
  "doc": "Information about a Data processing job"
}
```





#### container
Link from an asset to its parent container



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| container | string | ✓ | The parent container of an asset | Searchable, → IsPartOf |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "container"
  },
  "name": "Container",
  "namespace": "com.linkedin.container",
  "fields": [
    {
      "Relationship": {
        "entityTypes": [
          "container"
        ],
        "name": "IsPartOf"
      },
      "Searchable": {
        "addToFilters": true,
        "fieldName": "container",
        "fieldType": "URN",
        "filterNameOverride": "Container",
        "hasValuesFieldName": "hasContainer"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "container",
      "doc": "The parent container of an asset"
    }
  ],
  "doc": "Link from an asset to its parent container"
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





#### forms
Forms that are assigned to this entity to be filled out



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| incompleteForms | [FormAssociation](#formassociation)[] | ✓ | All incomplete forms assigned to the entity. | Searchable |
| completedForms | [FormAssociation](#formassociation)[] | ✓ | All complete forms assigned to the entity. | Searchable |
| verifications | FormVerificationAssociation[] | ✓ | Verifications that have been applied to the entity via completed forms. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "forms"
  },
  "name": "Forms",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*/completedPrompts/*/id": {
          "fieldName": "incompleteFormsCompletedPromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/completedPrompts/*/lastModified/time": {
          "fieldName": "incompleteFormsCompletedPromptResponseTimes",
          "fieldType": "DATETIME",
          "queryByDefault": false
        },
        "/*/incompletePrompts/*/id": {
          "fieldName": "incompleteFormsIncompletePromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/urn": {
          "fieldName": "incompleteForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "FormAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "urn",
              "doc": "Urn of the applied form"
            },
            {
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "FormPromptAssociation",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "string",
                      "name": "id",
                      "doc": "The id for the prompt. This must be GLOBALLY UNIQUE."
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
                      "doc": "The last time this prompt was touched for the entity (set, unset)"
                    },
                    {
                      "type": [
                        "null",
                        {
                          "type": "record",
                          "name": "FormPromptFieldAssociations",
                          "namespace": "com.linkedin.common",
                          "fields": [
                            {
                              "type": [
                                "null",
                                {
                                  "type": "array",
                                  "items": {
                                    "type": "record",
                                    "name": "FieldFormPromptAssociation",
                                    "namespace": "com.linkedin.common",
                                    "fields": [
                                      {
                                        "type": "string",
                                        "name": "fieldPath",
                                        "doc": "The field path on a schema field."
                                      },
                                      {
                                        "type": "com.linkedin.common.AuditStamp",
                                        "name": "lastModified",
                                        "doc": "The last time this prompt was touched for the field on the entity (set, unset)"
                                      }
                                    ],
                                    "doc": "Information about the status of a particular prompt for a specific schema field\non an entity."
                                  }
                                }
                              ],
                              "name": "completedFieldPrompts",
                              "default": null,
                              "doc": "A list of field-level prompt associations that are not yet complete for this form."
                            },
                            {
                              "type": [
                                "null",
                                {
                                  "type": "array",
                                  "items": "com.linkedin.common.FieldFormPromptAssociation"
                                }
                              ],
                              "name": "incompleteFieldPrompts",
                              "default": null,
                              "doc": "A list of field-level prompt associations that are complete for this form."
                            }
                          ],
                          "doc": "Information about the field-level prompt associations on a top-level prompt association."
                        }
                      ],
                      "name": "fieldAssociations",
                      "default": null,
                      "doc": "Optional information about the field-level prompt associations."
                    }
                  ],
                  "doc": "Information about the status of a particular prompt.\nNote that this is where we can add additional information about individual responses:\nactor, timestamp, and the response itself."
                }
              },
              "name": "incompletePrompts",
              "default": [],
              "doc": "A list of prompts that are not yet complete for this form."
            },
            {
              "type": {
                "type": "array",
                "items": "com.linkedin.common.FormPromptAssociation"
              },
              "name": "completedPrompts",
              "default": [],
              "doc": "A list of prompts that have been completed for this form."
            }
          ],
          "doc": "Properties of an applied form."
        }
      },
      "name": "incompleteForms",
      "doc": "All incomplete forms assigned to the entity."
    },
    {
      "Searchable": {
        "/*/completedPrompts/*/id": {
          "fieldName": "completedFormsCompletedPromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/completedPrompts/*/lastModified/time": {
          "fieldName": "completedFormsCompletedPromptResponseTimes",
          "fieldType": "DATETIME",
          "queryByDefault": false
        },
        "/*/incompletePrompts/*/id": {
          "fieldName": "completedFormsIncompletePromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/urn": {
          "fieldName": "completedForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.common.FormAssociation"
      },
      "name": "completedForms",
      "doc": "All complete forms assigned to the entity."
    },
    {
      "Searchable": {
        "/*/form": {
          "fieldName": "verifiedForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "FormVerificationAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "form",
              "doc": "The urn of the form that granted this verification."
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "lastModified",
              "default": null,
              "doc": "An audit stamp capturing who and when verification was applied for this form."
            }
          ],
          "doc": "An association between a verification and an entity that has been granted\nvia completion of one or more forms of type 'VERIFICATION'."
        }
      },
      "name": "verifications",
      "default": [],
      "doc": "Verifications that have been applied to the entity via completed forms."
    }
  ],
  "doc": "Forms that are assigned to this entity to be filled out"
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





#### incidentsSummary
Summary related incidents on an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| resolvedIncidents | string[] | ✓ | Resolved incidents for an asset Deprecated! Use the richer resolvedIncidentsDetails instead. | ⚠️ Deprecated |
| activeIncidents | string[] | ✓ | Active incidents for an asset Deprecated! Use the richer activeIncidentsDetails instead. | ⚠️ Deprecated |
| resolvedIncidentDetails | [IncidentSummaryDetails](#incidentsummarydetails)[] | ✓ | Summary details about the set of resolved incidents | Searchable, → ResolvedIncidents |
| activeIncidentDetails | [IncidentSummaryDetails](#incidentsummarydetails)[] | ✓ | Summary details about the set of active incidents | Searchable, → ActiveIncidents |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "incidentsSummary"
  },
  "name": "IncidentsSummary",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "deprecated": true,
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "resolvedIncidents",
      "default": [],
      "doc": "Resolved incidents for an asset\nDeprecated! Use the richer resolvedIncidentsDetails instead."
    },
    {
      "deprecated": true,
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "activeIncidents",
      "default": [],
      "doc": "Active incidents for an asset\nDeprecated! Use the richer activeIncidentsDetails instead."
    },
    {
      "Relationship": {
        "/*/urn": {
          "entityTypes": [
            "incident"
          ],
          "name": "ResolvedIncidents"
        }
      },
      "Searchable": {
        "/*/createdAt": {
          "fieldName": "resolvedIncidentCreatedTimes",
          "fieldType": "DATETIME"
        },
        "/*/priority": {
          "fieldName": "resolvedIncidentPriorities",
          "fieldType": "COUNT"
        },
        "/*/resolvedAt": {
          "fieldName": "resolvedIncidentResolvedTimes",
          "fieldType": "DATETIME"
        },
        "/*/type": {
          "fieldName": "resolvedIncidentTypes",
          "fieldType": "KEYWORD"
        },
        "/*/urn": {
          "fieldName": "resolvedIncidents",
          "fieldType": "URN",
          "hasValuesFieldName": "hasResolvedIncidents",
          "numValuesFieldName": "numResolvedIncidents",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "IncidentSummaryDetails",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "urn",
              "doc": "The urn of the incident"
            },
            {
              "type": "string",
              "name": "type",
              "doc": "The type of an incident"
            },
            {
              "type": "long",
              "name": "createdAt",
              "doc": "The time at which the incident was raised in milliseconds since epoch."
            },
            {
              "type": [
                "null",
                "long"
              ],
              "name": "resolvedAt",
              "default": null,
              "doc": "The time at which the incident was marked as resolved in milliseconds since epoch. Null if the incident is still active."
            },
            {
              "type": [
                "null",
                "int"
              ],
              "name": "priority",
              "default": null,
              "doc": "The priority of the incident"
            }
          ],
          "doc": "Summary statistics about incidents on an entity."
        }
      },
      "name": "resolvedIncidentDetails",
      "default": [],
      "doc": "Summary details about the set of resolved incidents"
    },
    {
      "Relationship": {
        "/*/urn": {
          "entityTypes": [
            "incident"
          ],
          "name": "ActiveIncidents"
        }
      },
      "Searchable": {
        "/*/createdAt": {
          "fieldName": "activeIncidentCreatedTimes",
          "fieldType": "DATETIME"
        },
        "/*/priority": {
          "fieldName": "activeIncidentPriorities",
          "fieldType": "COUNT"
        },
        "/*/type": {
          "fieldName": "activeIncidentTypes",
          "fieldType": "KEYWORD"
        },
        "/*/urn": {
          "addHasValuesToFilters": true,
          "fieldName": "activeIncidents",
          "fieldType": "URN",
          "hasValuesFieldName": "hasActiveIncidents",
          "numValuesFieldName": "numActiveIncidents",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.common.IncidentSummaryDetails"
      },
      "name": "activeIncidentDetails",
      "default": [],
      "doc": "Summary details about the set of active incidents"
    }
  ],
  "doc": "Summary related incidents on an entity."
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





#### dataTransformLogic
Information about a Query against one or more data assets (e.g. Tables or Views).



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| transforms | DataTransform[] | ✓ | List of transformations applied |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataTransformLogic"
  },
  "name": "DataTransformLogic",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "DataTransform",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "QueryStatement",
                  "namespace": "com.linkedin.query",
                  "fields": [
                    {
                      "type": "string",
                      "name": "value",
                      "doc": "The query text"
                    },
                    {
                      "type": {
                        "type": "enum",
                        "symbolDocs": {
                          "SQL": "A SQL Query",
                          "UNKNOWN": "Unknown query language"
                        },
                        "name": "QueryLanguage",
                        "namespace": "com.linkedin.query",
                        "symbols": [
                          "SQL",
                          "UNKNOWN"
                        ]
                      },
                      "name": "language",
                      "default": "SQL",
                      "doc": "The language of the Query, e.g. SQL."
                    }
                  ],
                  "doc": "A query statement against one or more data assets."
                }
              ],
              "name": "queryStatement",
              "default": null,
              "doc": "The data transform may be defined by a query statement"
            }
          ],
          "doc": "Information about a transformation. It may be a query,"
        }
      },
      "name": "transforms",
      "doc": "List of transformations applied"
    }
  ],
  "doc": "Information about a Query against one or more data assets (e.g. Tables or Views)."
}
```





#### documentation
Aspect used for storing all applicable documentations on assets.
This aspect supports multiple documentations from different sources.
There is an implicit assumption that there is only one documentation per
   source.
For example, if there are two documentations from the same source, the
   latest one will overwrite the previous one.
If there are two documentations from different sources, both will be
   stored.
Future evolution considerations:
The first entity that uses this aspect is Schema Field. We will expand this
    aspect to other entities eventually.
The values of the documentation are not currently searchable. This will be
    changed once this aspect develops opinion on which documentation entry is
    the authoritative one.
Ensuring that there is only one documentation per source is a business
    rule that is not enforced by the aspect yet. This will currently be enforced by the
    application that uses this aspect. We will eventually enforce this rule in
    the aspect using AspectMutators.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| documentations | DocumentationAssociation[] | ✓ | Documentations associated with this asset. We could be receiving docs from different sources |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "documentation"
  },
  "name": "Documentation",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "DocumentationAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "string",
              "name": "documentation",
              "doc": "Description of this asset"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "documentationAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "documentationAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "documentationAttributionDates",
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
          "doc": "Properties of applied documentation including the attribution of the doc"
        }
      },
      "name": "documentations",
      "doc": "Documentations associated with this asset. We could be receiving docs from different sources"
    }
  ],
  "doc": "Aspect used for storing all applicable documentations on assets.\nThis aspect supports multiple documentations from different sources.\nThere is an implicit assumption that there is only one documentation per\n   source.\nFor example, if there are two documentations from the same source, the\n   latest one will overwrite the previous one.\nIf there are two documentations from different sources, both will be\n   stored.\nFuture evolution considerations:\nThe first entity that uses this aspect is Schema Field. We will expand this\n    aspect to other entities eventually.\nThe values of the documentation are not currently searchable. This will be\n    changed once this aspect develops opinion on which documentation entry is\n    the authoritative one.\nEnsuring that there is only one documentation per source is a business\n    rule that is not enforced by the aspect yet. This will currently be enforced by the\n    application that uses this aspect. We will eventually enforce this rule in\n    the aspect using AspectMutators."
}
```





#### datahubIngestionRunSummary (Timeseries)
Summary of a datahub ingestion run for a given platform.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| timestampMillis | long | ✓ | The event timestamp field as epoch at UTC in milli seconds. |  |
| eventGranularity | [TimeWindowSize](#timewindowsize) |  | Granularity of the event if applicable |  |
| partitionSpec | [PartitionSpec](#partitionspec) |  | The optional partition specification. |  |
| messageId | string |  | The optional messageId, if provided serves as a custom user-defined unique identifier for an aspe... |  |
| pipelineName | string | ✓ | The name of the pipeline that ran ingestion, a stable unique user provided identifier.  e.g. my_s... |  |
| platformInstanceId | string | ✓ | The id of the instance against which the ingestion pipeline ran. e.g.: Bigquery project ids, MySQ... |  |
| runId | string | ✓ | The runId for this pipeline instance. |  |
| runStatus | JobStatus | ✓ | Run Status - Succeeded/Skipped/Failed etc. |  |
| numWorkUnitsCommitted | long |  | The number of workunits written to sink. |  |
| numWorkUnitsCreated | long |  | The number of workunits that are produced. |  |
| numEvents | long |  | The number of events produced (MCE + MCP). |  |
| numEntities | long |  | The total number of entities produced (unique entity urns). |  |
| numAspects | long |  | The total number of aspects produced across all entities. |  |
| numSourceAPICalls | long |  | Total number of source API calls. |  |
| totalLatencySourceAPICalls | long |  | Total latency across all source API calls. |  |
| numSinkAPICalls | long |  | Total number of sink API calls. |  |
| totalLatencySinkAPICalls | long |  | Total latency across all sink API calls. |  |
| numWarnings | long |  | Number of warnings generated. |  |
| numErrors | long |  | Number of errors generated. |  |
| numEntitiesSkipped | long |  | Number of entities skipped. |  |
| config | string |  | The non-sensitive key-value pairs of the yaml config used as json string. |  |
| custom_summary | string |  | Custom value. |  |
| softwareVersion | string |  | The software version of this ingestion. |  |
| systemHostName | string |  | The hostname the ingestion pipeline ran on. |  |
| operatingSystemName | string |  | The os the ingestion pipeline ran on. |  |
| numProcessors | int |  | The number of processors on the host the ingestion pipeline ran on. |  |
| totalMemory | long |  | The total amount of memory on the host the ingestion pipeline ran on. |  |
| availableMemory | long |  | The available memory on the host the ingestion pipeline ran on. |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "datahubIngestionRunSummary",
    "type": "timeseries"
  },
  "name": "DatahubIngestionRunSummary",
  "namespace": "com.linkedin.datajob.datahub",
  "fields": [
    {
      "type": "long",
      "name": "timestampMillis",
      "doc": "The event timestamp field as epoch at UTC in milli seconds."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "TimeWindowSize",
          "namespace": "com.linkedin.timeseries",
          "fields": [
            {
              "type": {
                "type": "enum",
                "name": "CalendarInterval",
                "namespace": "com.linkedin.timeseries",
                "symbols": [
                  "SECOND",
                  "MINUTE",
                  "HOUR",
                  "DAY",
                  "WEEK",
                  "MONTH",
                  "QUARTER",
                  "YEAR"
                ]
              },
              "name": "unit",
              "doc": "Interval unit such as minute/hour/day etc."
            },
            {
              "type": "int",
              "name": "multiple",
              "default": 1,
              "doc": "How many units. Defaults to 1."
            }
          ],
          "doc": "Defines the size of a time window."
        }
      ],
      "name": "eventGranularity",
      "default": null,
      "doc": "Granularity of the event if applicable"
    },
    {
      "type": [
        {
          "type": "record",
          "name": "PartitionSpec",
          "namespace": "com.linkedin.timeseries",
          "fields": [
            {
              "TimeseriesField": {},
              "type": "string",
              "name": "partition",
              "doc": "A unique id / value for the partition for which statistics were collected,\ngenerated by applying the key definition to a given row."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "TimeWindow",
                  "namespace": "com.linkedin.timeseries",
                  "fields": [
                    {
                      "type": "long",
                      "name": "startTimeMillis",
                      "doc": "Start time as epoch at UTC."
                    },
                    {
                      "type": "com.linkedin.timeseries.TimeWindowSize",
                      "name": "length",
                      "doc": "The length of the window."
                    }
                  ]
                }
              ],
              "name": "timePartition",
              "default": null,
              "doc": "Time window of the partition, if we are able to extract it from the partition key."
            },
            {
              "deprecated": true,
              "type": {
                "type": "enum",
                "name": "PartitionType",
                "namespace": "com.linkedin.timeseries",
                "symbols": [
                  "FULL_TABLE",
                  "QUERY",
                  "PARTITION"
                ]
              },
              "name": "type",
              "default": "PARTITION",
              "doc": "Unused!"
            }
          ],
          "doc": "A reference to a specific partition in a dataset."
        },
        "null"
      ],
      "name": "partitionSpec",
      "default": {
        "partition": "FULL_TABLE_SNAPSHOT",
        "type": "FULL_TABLE",
        "timePartition": null
      },
      "doc": "The optional partition specification."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "messageId",
      "default": null,
      "doc": "The optional messageId, if provided serves as a custom user-defined unique identifier for an aspect value."
    },
    {
      "TimeseriesField": {},
      "type": "string",
      "name": "pipelineName",
      "doc": "The name of the pipeline that ran ingestion, a stable unique user provided identifier.\n e.g. my_snowflake1-to-datahub."
    },
    {
      "TimeseriesField": {},
      "type": "string",
      "name": "platformInstanceId",
      "doc": "The id of the instance against which the ingestion pipeline ran.\ne.g.: Bigquery project ids, MySQL hostnames etc."
    },
    {
      "TimeseriesField": {},
      "type": "string",
      "name": "runId",
      "doc": "The runId for this pipeline instance."
    },
    {
      "TimeseriesField": {},
      "type": {
        "type": "enum",
        "symbolDocs": {
          "COMPLETED": "Jobs with successful completion.",
          "FAILED": "Jobs that have failed.",
          "IN_PROGRESS": "Jobs currently running.",
          "SKIPPED": "Jobs that have been skipped.",
          "STARTING": "Jobs being initialized.",
          "STOPPED": "Jobs that have stopped.",
          "STOPPING": "Jobs being stopped.",
          "UNKNOWN": "Jobs with unknown status (either unmappable or unavailable)"
        },
        "name": "JobStatus",
        "namespace": "com.linkedin.datajob",
        "symbols": [
          "STARTING",
          "IN_PROGRESS",
          "STOPPING",
          "STOPPED",
          "COMPLETED",
          "FAILED",
          "UNKNOWN",
          "SKIPPED"
        ],
        "doc": "Job statuses"
      },
      "name": "runStatus",
      "doc": "Run Status - Succeeded/Skipped/Failed etc."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "numWorkUnitsCommitted",
      "default": null,
      "doc": "The number of workunits written to sink."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "numWorkUnitsCreated",
      "default": null,
      "doc": "The number of workunits that are produced."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "numEvents",
      "default": null,
      "doc": "The number of events produced (MCE + MCP)."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "numEntities",
      "default": null,
      "doc": "The total number of entities produced (unique entity urns)."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "numAspects",
      "default": null,
      "doc": "The total number of aspects produced across all entities."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "numSourceAPICalls",
      "default": null,
      "doc": "Total number of source API calls."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "totalLatencySourceAPICalls",
      "default": null,
      "doc": "Total latency across all source API calls."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "numSinkAPICalls",
      "default": null,
      "doc": "Total number of sink API calls."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "totalLatencySinkAPICalls",
      "default": null,
      "doc": "Total latency across all sink API calls."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "numWarnings",
      "default": null,
      "doc": "Number of warnings generated."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "numErrors",
      "default": null,
      "doc": "Number of errors generated."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "numEntitiesSkipped",
      "default": null,
      "doc": "Number of entities skipped."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "config",
      "default": null,
      "doc": "The non-sensitive key-value pairs of the yaml config used as json string."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "custom_summary",
      "default": null,
      "doc": "Custom value."
    },
    {
      "TimeseriesField": {},
      "type": [
        "null",
        "string"
      ],
      "name": "softwareVersion",
      "default": null,
      "doc": "The software version of this ingestion."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "systemHostName",
      "default": null,
      "doc": "The hostname the ingestion pipeline ran on."
    },
    {
      "TimeseriesField": {},
      "type": [
        "null",
        "string"
      ],
      "name": "operatingSystemName",
      "default": null,
      "doc": "The os the ingestion pipeline ran on."
    },
    {
      "type": [
        "null",
        "int"
      ],
      "name": "numProcessors",
      "default": null,
      "doc": "The number of processors on the host the ingestion pipeline ran on."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "totalMemory",
      "default": null,
      "doc": "The total amount of memory on the host the ingestion pipeline ran on."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "availableMemory",
      "default": null,
      "doc": "The available memory on the host the ingestion pipeline ran on."
    }
  ],
  "doc": "Summary of a datahub ingestion run for a given platform."
}
```





#### datahubIngestionCheckpoint (Timeseries)
Checkpoint of a datahub ingestion run for a given job.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| timestampMillis | long | ✓ | The event timestamp field as epoch at UTC in milli seconds. |  |
| eventGranularity | [TimeWindowSize](#timewindowsize) |  | Granularity of the event if applicable |  |
| partitionSpec | [PartitionSpec](#partitionspec) |  | The optional partition specification. |  |
| messageId | string |  | The optional messageId, if provided serves as a custom user-defined unique identifier for an aspe... |  |
| pipelineName | string | ✓ | The name of the pipeline that ran ingestion, a stable unique user provided identifier.  e.g. my_s... |  |
| platformInstanceId | string | ✓ | The id of the instance against which the ingestion pipeline ran. e.g.: Bigquery project ids, MySQ... |  |
| config | string | ✓ | Json-encoded string representation of the non-secret members of the config . |  |
| state | IngestionCheckpointState | ✓ | Opaque blob of the state representation. |  |
| runId | string | ✓ | The run identifier of this job. |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "datahubIngestionCheckpoint",
    "type": "timeseries"
  },
  "name": "DatahubIngestionCheckpoint",
  "namespace": "com.linkedin.datajob.datahub",
  "fields": [
    {
      "type": "long",
      "name": "timestampMillis",
      "doc": "The event timestamp field as epoch at UTC in milli seconds."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "TimeWindowSize",
          "namespace": "com.linkedin.timeseries",
          "fields": [
            {
              "type": {
                "type": "enum",
                "name": "CalendarInterval",
                "namespace": "com.linkedin.timeseries",
                "symbols": [
                  "SECOND",
                  "MINUTE",
                  "HOUR",
                  "DAY",
                  "WEEK",
                  "MONTH",
                  "QUARTER",
                  "YEAR"
                ]
              },
              "name": "unit",
              "doc": "Interval unit such as minute/hour/day etc."
            },
            {
              "type": "int",
              "name": "multiple",
              "default": 1,
              "doc": "How many units. Defaults to 1."
            }
          ],
          "doc": "Defines the size of a time window."
        }
      ],
      "name": "eventGranularity",
      "default": null,
      "doc": "Granularity of the event if applicable"
    },
    {
      "type": [
        {
          "type": "record",
          "name": "PartitionSpec",
          "namespace": "com.linkedin.timeseries",
          "fields": [
            {
              "TimeseriesField": {},
              "type": "string",
              "name": "partition",
              "doc": "A unique id / value for the partition for which statistics were collected,\ngenerated by applying the key definition to a given row."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "TimeWindow",
                  "namespace": "com.linkedin.timeseries",
                  "fields": [
                    {
                      "type": "long",
                      "name": "startTimeMillis",
                      "doc": "Start time as epoch at UTC."
                    },
                    {
                      "type": "com.linkedin.timeseries.TimeWindowSize",
                      "name": "length",
                      "doc": "The length of the window."
                    }
                  ]
                }
              ],
              "name": "timePartition",
              "default": null,
              "doc": "Time window of the partition, if we are able to extract it from the partition key."
            },
            {
              "deprecated": true,
              "type": {
                "type": "enum",
                "name": "PartitionType",
                "namespace": "com.linkedin.timeseries",
                "symbols": [
                  "FULL_TABLE",
                  "QUERY",
                  "PARTITION"
                ]
              },
              "name": "type",
              "default": "PARTITION",
              "doc": "Unused!"
            }
          ],
          "doc": "A reference to a specific partition in a dataset."
        },
        "null"
      ],
      "name": "partitionSpec",
      "default": {
        "partition": "FULL_TABLE_SNAPSHOT",
        "type": "FULL_TABLE",
        "timePartition": null
      },
      "doc": "The optional partition specification."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "messageId",
      "default": null,
      "doc": "The optional messageId, if provided serves as a custom user-defined unique identifier for an aspect value."
    },
    {
      "TimeseriesField": {},
      "type": "string",
      "name": "pipelineName",
      "doc": "The name of the pipeline that ran ingestion, a stable unique user provided identifier.\n e.g. my_snowflake1-to-datahub."
    },
    {
      "TimeseriesField": {},
      "type": "string",
      "name": "platformInstanceId",
      "doc": "The id of the instance against which the ingestion pipeline ran.\ne.g.: Bigquery project ids, MySQL hostnames etc."
    },
    {
      "type": "string",
      "name": "config",
      "doc": "Json-encoded string representation of the non-secret members of the config ."
    },
    {
      "type": {
        "type": "record",
        "name": "IngestionCheckpointState",
        "namespace": "com.linkedin.datajob.datahub",
        "fields": [
          {
            "type": "string",
            "name": "formatVersion",
            "doc": "The version of the state format."
          },
          {
            "type": "string",
            "name": "serde",
            "doc": "The serialization/deserialization protocol."
          },
          {
            "type": [
              "null",
              "bytes"
            ],
            "name": "payload",
            "default": null,
            "doc": "Opaque blob of the state representation."
          }
        ],
        "doc": "The checkpoint state object of a datahub ingestion run for a given job."
      },
      "name": "state",
      "doc": "Opaque blob of the state representation."
    },
    {
      "TimeseriesField": {},
      "type": "string",
      "name": "runId",
      "doc": "The run identifier of this job."
    }
  ],
  "doc": "Checkpoint of a datahub ingestion run for a given job."
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

#### Edge

A common structure to represent all edges to entities when used inside aspects as collections
This ensures that all edges have common structure around audit-stamps and will support PATCH, time-travel automatically.

**Fields:**

- `sourceUrn` (string?): Urn of the source of this relationship edge. If not specified, assumed to be ...
- `destinationUrn` (string): Urn of the destination of this relationship edge.
- `created` (AuditStamp?): Audit stamp containing who created this relationship edge and when
- `lastModified` (AuditStamp?): Audit stamp containing who last modified this relationship edge and when
- `properties` (map?): A generic properties bag that allows us to store specific information on this...

#### FormAssociation

Properties of an applied form.

**Fields:**

- `urn` (string): Urn of the applied form
- `incompletePrompts` (FormPromptAssociation[]): A list of prompts that are not yet complete for this form.
- `completedPrompts` (FormPromptAssociation[]): A list of prompts that have been completed for this form.

#### IncidentSummaryDetails

Summary statistics about incidents on an entity.

**Fields:**

- `urn` (string): The urn of the incident
- `type` (string): The type of an incident
- `createdAt` (long): The time at which the incident was raised in milliseconds since epoch.
- `resolvedAt` (long?): The time at which the incident was marked as resolved in milliseconds since e...
- `priority` (int?): The priority of the incident

#### PartitionSpec

A reference to a specific partition in a dataset.

**Fields:**

- `partition` (string): A unique id / value for the partition for which statistics were collected, ge...
- `timePartition` (TimeWindow?): Time window of the partition, if we are able to extract it from the partition...
- `type` (PartitionType): Unused!

#### TestResult

Information about a Test Result

**Fields:**

- `test` (string): The urn of the test
- `type` (TestResultType): The type of the result
- `testDefinitionMd5` (string?): The md5 of the test definition that was used to compute this result. See Test...
- `lastComputed` (AuditStamp?): The audit stamp of when the result was computed, including the actor who comp...

#### TimeStamp

A standard event timestamp

**Fields:**

- `time` (long): When did the event occur
- `actor` (string?): Optional: The actor urn involved in the event.

#### TimeWindowSize

Defines the size of a time window.

**Fields:**

- `unit` (CalendarInterval): Interval unit such as minute/hour/day etc.
- `multiple` (int): How many units. Defaults to 1.


### Relationships

#### Self
These are the relationships to itself, stored in this entity's aspects
- DownstreamOf (via `dataJobInputOutput.inputDatajobs`)
- DownstreamOf (via `dataJobInputOutput.inputDatajobEdges`)
#### Outgoing
These are the relationships stored in this entity's aspects
- IsPartOf

   - DataFlow via `dataJobKey.flow`
   - Container via `container.container`
- Consumes

   - Dataset via `dataJobInputOutput.inputDatasets`
   - Dataset via `dataJobInputOutput.inputDatasetEdges`
   - SchemaField via `dataJobInputOutput.inputDatasetFields`
- Produces

   - Dataset via `dataJobInputOutput.outputDatasets`
   - Dataset via `dataJobInputOutput.outputDatasetEdges`
   - SchemaField via `dataJobInputOutput.outputDatasetFields`
- OwnedBy

   - Corpuser via `ownership.owners.owner`
   - CorpGroup via `ownership.owners.owner`
- ownershipType

   - OwnershipType via `ownership.owners.typeUrn`
- TaggedWith

   - Tag via `globalTags.tags`
- TermedWith

   - GlossaryTerm via `glossaryTerms.terms.urn`
- AssociatedWith

   - Domain via `domains.domains`
   - Application via `applications.applications`
- ResolvedIncidents

   - Incident via `incidentsSummary.resolvedIncidentDetails`
- ActiveIncidents

   - Incident via `incidentsSummary.activeIncidentDetails`
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
