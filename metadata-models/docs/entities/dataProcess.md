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

<details>
<summary>Python SDK: Query a dataProcess entity</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataprocess_query_deprecated.py show_path_as_comment }}
```

</details>

### Creating Equivalent DataFlow and DataJob (Recommended)

Instead of using dataProcess, create the modern equivalent:

<details>
<summary>Python SDK: Create DataFlow and DataJob to replace dataProcess</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataprocess_migrate_to_flow_job.py show_path_as_comment }}
```

</details>

### Complete Migration Example

<details>
<summary>Python SDK: Full migration from dataProcess to dataFlow/dataJob</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataprocess_full_migration.py show_path_as_comment }}
```

</details>

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
