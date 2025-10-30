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

<details>
<summary>Python SDK: Create a basic data job</summary>

```python
{{ inline /metadata-ingestion/examples/library/datajob_create_basic.py show_path_as_comment }}
```

</details>

### Adding Tags, Terms, and Ownership

Common metadata can be added to data jobs to enhance discoverability and governance:

<details>
<summary>Python SDK: Add tags, terms, and ownership to a data job</summary>

```python
{{ inline /metadata-ingestion/examples/library/datajob_add_tags_terms_ownership.py show_path_as_comment }}
```

</details>

### Updating Job Properties

You can update job properties like descriptions using the low-level APIs:

<details>
<summary>Python SDK: Update data job description</summary>

```python
{{ inline /metadata-ingestion/examples/library/datajob_update_description.py show_path_as_comment }}
```

</details>

### Querying Data Job Information

Retrieve data job information via the REST API:

<details>
<summary>REST API: Query a data job</summary>

```python
{{ inline /metadata-ingestion/examples/library/datajob_query_rest.py show_path_as_comment }}
```

</details>

### Adding Lineage to Data Jobs

Data jobs are often used to define lineage relationships. See the existing lineage examples:

<details>
<summary>Python SDK: Add lineage using DataJobPatchBuilder</summary>

```python
{{ inline /metadata-ingestion/examples/library/datajob_add_lineage_patch.py show_path_as_comment }}
```

</details>

<details>
<summary>Python SDK: Define fine-grained lineage through a data job</summary>

```python
{{ inline /metadata-ingestion/examples/library/lineage_emitter_datajob_finegrained.py show_path_as_comment }}
```

</details>

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
