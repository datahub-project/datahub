# DataProcessInstance

DataProcessInstance represents an individual execution run of a data pipeline or data processing task. While DataJob and DataFlow entities define the structure and logic of your data pipelines, DataProcessInstance captures the runtime behavior, tracking each specific execution with its inputs, outputs, status, and timing information.

Think of it this way: if a DataJob is a recipe, a DataProcessInstance is one particular time you followed that recipe, recording when you started, what ingredients you used, what you produced, and whether it succeeded or failed.

## Identity

DataProcessInstance entities are uniquely identified by a single field: an id string. This makes the identifier structure simpler than other entities but requires careful consideration when generating IDs.

The URN format is:

```
urn:li:dataProcessInstance:<id>
```

The `<id>` field should be globally unique across all process instances and typically encodes information about the orchestrator, execution context, and run identifier. The Python SDK provides a helper that generates this ID from three components:

- **orchestrator**: The platform executing the process (e.g., `airflow`, `spark`, `dagster`)
- **cluster**: Optional environment identifier (e.g., `prod`, `dev`, `staging`)
- **id**: The execution-specific identifier (e.g., DAG run ID, job execution ID)

Example URNs:

```
urn:li:dataProcessInstance:abc123def456...
```

The actual ID is a deterministic GUID generated from the orchestrator, cluster, and execution id:

```python
from datahub.api.entities.dataprocess.dataprocess_instance import DataProcessInstance

# The ID is automatically generated from these fields
instance = DataProcessInstance(
    id="scheduled__2024-01-15T10:00:00+00:00",
    orchestrator="airflow",
    cluster="prod"
)
# Results in: urn:li:dataProcessInstance:<deterministic-guid>
```

### Relationship to DataFlow and DataJob

DataProcessInstance entities are linked to their template definitions through the `parentTemplate` field in the `dataProcessInstanceRelationships` aspect:

- **DataJob Instance**: When tracking a specific task execution, the `parentTemplate` points to the DataJob URN
- **DataFlow Instance**: When tracking an entire pipeline run (like an Airflow DAG run), the `parentTemplate` points to the DataFlow URN
- **Standalone Process**: For ad-hoc or containerized processes, `parentTemplate` can be null

This relationship enables several important capabilities:

- Navigate from a job definition to all its historical runs
- Aggregate metrics across runs of the same job
- Compare current execution with historical patterns
- Debug failures by examining past successful runs

## Important Capabilities

### Execution Tracking and Monitoring

The `dataProcessInstanceRunEvent` aspect (a timeseries aspect) tracks the lifecycle of each execution with high granularity:

#### Run Status

Process instances move through a simple lifecycle:

- **STARTED**: Process execution has begun
- **COMPLETE**: Process execution has finished (with a result)

#### Run Results

When a process completes, the result type indicates the outcome:

- **SUCCESS**: Process completed successfully
- **FAILURE**: Process failed with errors
- **SKIPPED**: Process was skipped (e.g., due to conditions not being met)
- **UP_FOR_RETRY**: Process failed but will be retried

#### Execution Metadata

Each run event captures:

- **timestampMillis**: When the status change occurred
- **attempt**: The attempt number (for retries)
- **durationMillis**: Total execution time in milliseconds
- **nativeResultType**: The platform-specific result status (e.g., "success" from Airflow)

This enables monitoring dashboards to:

- Track real-time execution status
- Calculate success rates and failure patterns
- Identify performance degradation
- Alert on execution duration anomalies

### Process Properties

The `dataProcessInstanceProperties` aspect captures metadata about the execution:

- **name**: Human-readable name for the execution
- **type**: Process type - BATCH_SCHEDULED, BATCH_AD_HOC, or STREAMING
- **created**: Audit stamp indicating when the instance was created
- **customProperties**: Arbitrary key-value pairs for platform-specific metadata
- **externalUrl**: Link to the execution in the orchestration platform (e.g., Airflow task instance page)

Example use cases for customProperties:

```python
properties={
    "airflow_version": "2.7.0",
    "executor": "CeleryExecutor",
    "pool": "default_pool",
    "queue": "default",
    "operator": "PythonOperator"
}
```

### Instance-Level Lineage

Unlike DataJob, which defines static lineage relationships, DataProcessInstance captures the actual inputs and outputs consumed and produced during a specific execution.

#### Input Tracking

The `dataProcessInstanceInput` aspect records:

- **inputs**: URNs of datasets or ML models consumed during this run
- **inputEdges**: Rich edge information including timestamps and custom properties

#### Output Tracking

The `dataProcessInstanceOutput` aspect records:

- **outputs**: URNs of datasets or ML models produced during this run
- **outputEdges**: Rich edge information including timestamps and custom properties

This enables powerful capabilities:

- **Point-in-time lineage**: See exactly which data versions were used in a specific run
- **Impact analysis**: When a run fails, identify downstream processes that depend on its outputs
- **Data quality tracking**: Correlate data quality issues with specific process executions
- **Reproducibility**: Recreate exact execution conditions by knowing precise inputs

Example: An Airflow task that succeeded might show it read from `dataset_v1` and wrote to `dataset_v2`, while a retry might show different input/output datasets if the data evolved.

### Hierarchical Process Relationships

The `dataProcessInstanceRelationships` aspect supports complex execution hierarchies:

#### Parent Instance

The `parentInstance` field links nested executions:

```
DataFlow Instance (DAG Run)
└── DataJob Instance 1 (Task Run 1)
└── DataJob Instance 2 (Task Run 2)
└── DataJob Instance 3 (Task Run 3)
```

This enables:

- Viewing all task runs within a DAG run
- Aggregating metrics at the DAG run level
- Understanding failure propagation in multi-stage pipelines

#### Upstream Instances

The `upstreamInstances` field creates dependencies between process instances:

```
Process Instance A (completed) → Process Instance B (triggered)
```

This captures dynamic execution dependencies, such as:

- Event-driven triggers
- Cross-DAG dependencies
- Dynamic fan-out/fan-in patterns

### Container-Based Processes

DataProcessInstance can represent processes that run within a Container (like an ML experiment) without being tied to a specific DataJob or DataFlow:

```python
from datahub.emitter.mcp_builder import ContainerKey

container_key = ContainerKey(
    platform="urn:li:dataPlatform:mlflow",
    name="experiment_123",
    env="PROD"
)

instance = DataProcessInstance.from_container(
    container_key=container_key,
    id="training_run_456"
)
```

This is useful for:

- ML training runs in experiment tracking systems
- Notebook executions
- Ad-hoc data processing scripts
- Lambda/serverless function invocations

## Code Examples

### Creating and Tracking a Process Instance

<details>
<summary>Python SDK: Create and track a simple process instance</summary>

```python
# metadata-ingestion/examples/library/data_process_instance_create_simple.py
import time

from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DataProcessTypeClass
from datahub.utilities.urns.data_job_urn import DataJobUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn

# Create REST emitter
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

# Define the parent DataJob that this instance is executing
parent_job_urn = DataJobUrn.create_from_string(
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,sales_pipeline,prod),process_sales_data)"
)

# Create a process instance for a specific execution
# This might represent an Airflow task run on 2024-01-15 at 10:00:00
instance = DataProcessInstance(
    id="scheduled__2024-01-15T10:00:00+00:00",
    orchestrator="airflow",
    cluster="prod",
    template_urn=parent_job_urn,
    type=DataProcessTypeClass.BATCH_SCHEDULED,
    properties={
        "airflow_version": "2.7.0",
        "executor": "CeleryExecutor",
        "pool": "default_pool",
    },
    url="https://airflow.company.com/dags/sales_pipeline/grid?dag_run_id=scheduled__2024-01-15T10:00:00+00:00&task_id=process_sales_data",
    inlets=[
        DatasetUrn.create_from_string(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,sales_db.raw_orders,PROD)"
        )
    ],
    outlets=[
        DatasetUrn.create_from_string(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,sales_db.processed_orders,PROD)"
        )
    ],
)

# Record the start of execution
start_time = int(time.time() * 1000)
instance.emit_process_start(
    emitter=emitter,
    start_timestamp_millis=start_time,
    attempt=1,
    emit_template=True,
    materialize_iolets=True,
)

print(f"Started tracking process instance: {instance.urn}")

# Simulate process execution
print("Process is running...")
time.sleep(2)

# Record the end of execution
end_time = int(time.time() * 1000)
instance.emit_process_end(
    emitter=emitter,
    end_timestamp_millis=end_time,
    result=InstanceRunResult.SUCCESS,
    result_type="airflow",
    attempt=1,
    start_timestamp_millis=start_time,
)

print(f"Completed tracking process instance with result: SUCCESS")
print(f"Duration: {end_time - start_time}ms")
```

</details>

### Tracking a Failed Process with Retry

<details>
<summary>Python SDK: Track a failed process execution and retry</summary>

```python
# metadata-ingestion/examples/library/data_process_instance_create_with_retry.py
import time

from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DataProcessTypeClass
from datahub.utilities.urns.data_job_urn import DataJobUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn

emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

parent_job_urn = DataJobUrn.create_from_string(
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,etl_pipeline,prod),load_customer_data)"
)

instance = DataProcessInstance(
    id="scheduled__2024-01-15T14:30:00+00:00",
    orchestrator="airflow",
    cluster="prod",
    template_urn=parent_job_urn,
    type=DataProcessTypeClass.BATCH_SCHEDULED,
    inlets=[
        DatasetUrn.create_from_string(
            "urn:li:dataset:(urn:li:dataPlatform:s3,customer_exports,PROD)"
        )
    ],
    outlets=[
        DatasetUrn.create_from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customers,PROD)"
        )
    ],
)

# First attempt
start_time_attempt1 = int(time.time() * 1000)
instance.emit_process_start(
    emitter=emitter,
    start_timestamp_millis=start_time_attempt1,
    attempt=1,
    emit_template=True,
    materialize_iolets=True,
)
print("Attempt 1 started...")

time.sleep(1)

# First attempt fails
end_time_attempt1 = int(time.time() * 1000)
instance.emit_process_end(
    emitter=emitter,
    end_timestamp_millis=end_time_attempt1,
    result=InstanceRunResult.UP_FOR_RETRY,
    result_type="airflow",
    attempt=1,
    start_timestamp_millis=start_time_attempt1,
)
print("Attempt 1 failed, will retry...")

time.sleep(2)

# Second attempt (retry)
start_time_attempt2 = int(time.time() * 1000)
instance.emit_process_start(
    emitter=emitter,
    start_timestamp_millis=start_time_attempt2,
    attempt=2,
    emit_template=False,
    materialize_iolets=False,
)
print("Attempt 2 started (retry)...")

time.sleep(1)

# Second attempt succeeds
end_time_attempt2 = int(time.time() * 1000)
instance.emit_process_end(
    emitter=emitter,
    end_timestamp_millis=end_time_attempt2,
    result=InstanceRunResult.SUCCESS,
    result_type="airflow",
    attempt=2,
    start_timestamp_millis=start_time_attempt2,
)
print("Attempt 2 succeeded!")
```

</details>

### Creating a DataFlow Instance (DAG Run)

<details>
<summary>Python SDK: Track an entire workflow execution</summary>

```python
# metadata-ingestion/examples/library/data_process_instance_create_from_dataflow.py
import time

from datahub.api.entities.datajob import DataFlow
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.rest_emitter import DatahubRestEmitter

emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

# Define the DataFlow (Airflow DAG)
dataflow = DataFlow(
    orchestrator="airflow",
    id="daily_reporting_pipeline",
    env="prod",
    description="Daily reporting pipeline that aggregates metrics",
)

# Create a DataProcessInstance for a specific DAG run
dag_run_instance = DataProcessInstance.from_dataflow(
    dataflow=dataflow,
    id="scheduled__2024-01-15T00:00:00+00:00"
)

# Set properties specific to this DAG run
dag_run_instance.properties = {
    "execution_date": "2024-01-15",
    "run_type": "scheduled",
    "external_trigger": "false",
}
dag_run_instance.url = "https://airflow.company.com/dags/daily_reporting_pipeline/grid?dag_run_id=scheduled__2024-01-15T00:00:00+00:00"

# Track DAG run start
start_time = int(time.time() * 1000)
dag_run_instance.emit_process_start(
    emitter=emitter,
    start_timestamp_millis=start_time,
    attempt=1,
    emit_template=True,
    materialize_iolets=True,
)
print(f"DAG run started: {dag_run_instance.urn}")

# Simulate DAG execution
time.sleep(3)

# Track DAG run completion
end_time = int(time.time() * 1000)
dag_run_instance.emit_process_end(
    emitter=emitter,
    end_timestamp_millis=end_time,
    result=InstanceRunResult.SUCCESS,
    result_type="airflow",
    attempt=1,
    start_timestamp_millis=start_time,
)
print(f"DAG run completed successfully in {end_time - start_time}ms")
```

</details>

### Tracking Hierarchical Process Instances

<details>
<summary>Python SDK: Track a DAG run with child task instances</summary>

```python
# metadata-ingestion/examples/library/data_process_instance_create_hierarchical.py
import time

from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.utilities.urns.dataset_urn import DatasetUrn

emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

# Define the DataFlow (Airflow DAG)
dataflow = DataFlow(
    orchestrator="airflow",
    id="etl_pipeline",
    env="prod",
    description="ETL pipeline with multiple tasks",
)

# Define DataJobs (Tasks in the DAG)
extract_job = DataJob(
    id="extract_data",
    flow_urn=dataflow.urn,
    description="Extract data from source",
)

# Create a DAG run instance (parent)
dag_run_id = "scheduled__2024-01-15T12:00:00+00:00"
dag_run_instance = DataProcessInstance.from_dataflow(dataflow=dataflow, id=dag_run_id)

# Track DAG run start
dag_start_time = int(time.time() * 1000)
dag_run_instance.emit_process_start(
    emitter=emitter,
    start_timestamp_millis=dag_start_time,
    attempt=1,
    emit_template=True,
    materialize_iolets=False,
)
print(f"DAG run started: {dag_run_instance.urn}")

# Create task instance for extract_data (child of DAG run)
extract_instance = DataProcessInstance.from_datajob(
    datajob=extract_job,
    id=f"{dag_run_id}__extract_data",
)
extract_instance.parent_instance = dag_run_instance.urn
extract_instance.inlets = [
    DatasetUrn.create_from_string(
        "urn:li:dataset:(urn:li:dataPlatform:postgres,raw_db.orders,PROD)"
    )
]
extract_instance.outlets = [
    DatasetUrn.create_from_string(
        "urn:li:dataset:(urn:li:dataPlatform:s3,staging/orders,PROD)"
    )
]

# Track extract task execution
extract_start_time = int(time.time() * 1000)
extract_instance.emit_process_start(
    emitter=emitter,
    start_timestamp_millis=extract_start_time,
    attempt=1,
    emit_template=True,
    materialize_iolets=True,
)
time.sleep(1)
extract_end_time = int(time.time() * 1000)
extract_instance.emit_process_end(
    emitter=emitter,
    end_timestamp_millis=extract_end_time,
    result=InstanceRunResult.SUCCESS,
    attempt=1,
    start_timestamp_millis=extract_start_time,
)

# Track DAG run completion
dag_end_time = int(time.time() * 1000)
dag_run_instance.emit_process_end(
    emitter=emitter,
    end_timestamp_millis=dag_end_time,
    result=InstanceRunResult.SUCCESS,
    attempt=1,
    start_timestamp_millis=dag_start_time,
)
print(f"DAG run completed successfully")
```

</details>

### Tracking ML Training Run in a Container

ML training runs are a specialized use case of DataProcessInstance entities. In addition to standard process instance aspects, training runs can use ML-specific aspects:

- **Subtype**: `MLAssetSubTypes.MLFLOW_TRAINING_RUN` to identify the process as an ML training run
- **mlTrainingRunProperties**: ML-specific metadata including training metrics, hyperparameters, and output URLs
- **Relationships**: Links to ML models (via `mlModelProperties.trainingJobs`) and experiments (via `container` aspect)

For comprehensive documentation on ML training runs, including the complete workflow with experiments, models, and datasets, see the [ML Model entity documentation](mlModel.md#training-runs-and-experiments).

<details>
<summary>Python SDK: Track a standalone ML training run</summary>

```python
# metadata-ingestion/examples/library/data_process_instance_create_ml_training.py
import time

from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.mcp_builder import ContainerKey
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DataProcessTypeClass
from datahub.utilities.urns.dataset_urn import DatasetUrn

emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

# Define the ML experiment container
experiment_container = ContainerKey(
    platform="urn:li:dataPlatform:mlflow",
    name="customer_churn_experiment",
    env="PROD"
)

# Create a process instance for a training run
training_run = DataProcessInstance.from_container(
    container_key=experiment_container,
    id="run_abc123def456"
)

# Set training-specific properties
training_run.type = DataProcessTypeClass.BATCH_AD_HOC
training_run.properties = {
    "model_type": "RandomForestClassifier",
    "hyperparameters": "n_estimators=100,max_depth=10",
    "framework": "scikit-learn",
    "framework_version": "1.3.0",
}
training_run.url = "https://mlflow.company.com/experiments/5/runs/abc123def456"

# Set training data inputs
training_run.inlets = [
    DatasetUrn.create_from_string(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,ml.training_data,PROD)"
    )
]

# Set model output
training_run.outlets = [
    DatasetUrn.create_from_string(
        "urn:li:mlModel:(urn:li:dataPlatform:mlflow,customer_churn_model_v2,PROD)"
    )
]

# Track training start
start_time = int(time.time() * 1000)
training_run.emit_process_start(
    emitter=emitter,
    start_timestamp_millis=start_time,
    attempt=1,
    emit_template=False,
    materialize_iolets=True,
)
print("ML training run started...")

# Simulate training
time.sleep(5)

# Track training completion
end_time = int(time.time() * 1000)
training_run.emit_process_end(
    emitter=emitter,
    end_timestamp_millis=end_time,
    result=InstanceRunResult.SUCCESS,
    result_type="mlflow",
    attempt=1,
    start_timestamp_millis=start_time,
)
print(f"ML training completed in {(end_time - start_time)/1000:.2f}s")
```

</details>

### Querying Process Instance History via REST API

<details>
<summary>REST API: Query execution history for a DataJob</summary>

```bash
# Get all process instances for a specific DataJob
curl -X GET 'http://localhost:8080/relationships?direction=INCOMING&urn=urn%3Ali%3AdataJob%3A%28urn%3Ali%3AdataFlow%3A%28airflow%2Csales_pipeline%2Cprod%29%2Cprocess_sales_data%29&types=InstanceOf'
```

Response:

```json
{
  "start": 0,
  "count": 10,
  "relationships": [
    {
      "type": "InstanceOf",
      "entity": "urn:li:dataProcessInstance:abc123..."
    },
    {
      "type": "InstanceOf",
      "entity": "urn:li:dataProcessInstance:def456..."
    }
  ],
  "total": 25
}
```

To get full details of each instance, fetch the entities:

```bash
curl 'http://localhost:8080/entities/urn%3Ali%3AdataProcessInstance%3Aabc123...'
```

Response includes all aspects:

```json
{
  "urn": "urn:li:dataProcessInstance:abc123...",
  "aspects": {
    "dataProcessInstanceProperties": {
      "name": "scheduled__2024-01-15T10:00:00+00:00",
      "type": "BATCH_SCHEDULED",
      "created": {
        "time": 1705318800000,
        "actor": "urn:li:corpuser:datahub"
      },
      "customProperties": {
        "airflow_version": "2.7.0"
      }
    },
    "dataProcessInstanceInput": {
      "inputs": [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,sales_db.raw_orders,PROD)"
      ]
    },
    "dataProcessInstanceOutput": {
      "outputs": [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,sales_db.processed_orders,PROD)"
      ]
    },
    "dataProcessInstanceRelationships": {
      "parentTemplate": "urn:li:dataJob:(urn:li:dataFlow:(airflow,sales_pipeline,prod),process_sales_data)"
    }
  }
}
```

</details>

### Querying Process Instance Run Events

<details>
<summary>GraphQL: Query run history with status and timing</summary>

The DataHub GraphQL API provides a `runs` field on DataJob entities to query execution history:

```graphql
query GetJobRuns {
  dataJob(
    urn: "urn:li:dataJob:(urn:li:dataFlow:(airflow,sales_pipeline,prod),process_sales_data)"
  ) {
    runs(start: 0, count: 10) {
      total
      runs {
        urn
        created {
          time
        }
        properties {
          name
          type
          externalUrl
          customProperties {
            key
            value
          }
        }
        relationships {
          parentTemplate
          parentInstance
        }
        inputs {
          urn
          type
        }
        outputs {
          urn
          type
        }
      }
    }
  }
}
```

Note: The timeseries `dataProcessInstanceRunEvent` aspect contains the actual run status, timing, and results. To query this timeseries data, use the timeseries aggregation APIs or directly query the timeseries index.

</details>

## Integration Points

### Orchestration Platforms

DataProcessInstance is the bridge between orchestration platforms and DataHub's metadata layer:

#### Airflow Integration

The DataHub Airflow plugin automatically creates DataProcessInstance entities for:

- **DAG runs**: Each Airflow DagRun becomes a DataProcessInstance with the DataFlow as its parent
- **Task runs**: Each TaskInstance becomes a DataProcessInstance with the DataJob as its parent

The plugin tracks:

- Start and end times from TaskInstance
- Success/failure status
- Retry attempts
- Actual datasets read/written (via lineage backend)

Configuration example:

```python
from datahub_airflow_plugin.datahub_listener import DatahubListener

# In your airflow.cfg or environment:
# AIRFLOW__DATAHUB__ENABLED=true
# AIRFLOW__DATAHUB__DATAHUB_CONN_ID=datahub_rest_default
```

#### Other Orchestrators

Similar patterns apply for:

- **Dagster**: Dagster runs and step executions
- **Prefect**: Flow runs and task runs
- **Spark**: Spark application executions
- **dbt**: dbt run executions
- **Fivetran**: Sync executions

### Relationship to DataJob and DataFlow

DataProcessInstance complements DataJob and DataFlow entities:

| Entity              | Purpose             | Cardinality            | Example                           |
| ------------------- | ------------------- | ---------------------- | --------------------------------- |
| DataFlow            | Pipeline definition | 1 per logical pipeline | Airflow DAG "sales_pipeline"      |
| DataJob             | Task definition     | N per DataFlow         | Airflow Task "process_sales_data" |
| DataProcessInstance | Execution run       | M per DataJob/DataFlow | Task run on 2024-01-15 10:00      |

Key differences:

- **Static vs Dynamic**: DataJob/DataFlow define what should happen; DataProcessInstance records what did happen
- **Lineage**: DataJob defines expected lineage; DataProcessInstance captures actual lineage for that run
- **Metadata**: DataJob describes the task; DataProcessInstance describes the execution

### Relationship to Datasets

DataProcessInstance creates instance-level lineage to datasets:

```
Dataset Version 1 ─┐
                   ├─> DataProcessInstance (Run #1) ─> Dataset Version 2
Dataset Version 1 ─┘

Dataset Version 2 ─┐
                   ├─> DataProcessInstance (Run #2) ─> Dataset Version 3
Dataset Version 2 ─┘
```

This enables:

- **Point-in-time lineage**: Which data version was used in which run
- **Data provenance**: Trace data quality issues to specific executions
- **Reproducibility**: Understand exact conditions of past runs

### GraphQL Resolvers

DataProcessInstance entities are exposed via GraphQL through several resolvers:

1. **DataJobRunsResolver**: Fetches process instances for a DataJob

   - Queries by `parentTemplate` field
   - Sorts by creation time (most recent first)
   - Returns only instances with run events

2. **EntityRunsResolver**: Generic resolver for any entity's runs

   - Used for DataFlow runs
   - Similar filtering and sorting logic

3. **DataProcessInstanceMapper**: Converts internal representation to GraphQL type
   - Maps all aspects to GraphQL fields
   - Handles relationships to parent entities

The GraphQL schema exposes:

```graphql
type DataJob {
  runs(start: Int, count: Int): DataProcessInstanceResult
}

type DataFlow {
  runs(start: Int, count: Int): DataProcessInstanceResult
}

type DataProcessInstance {
  urn: String!
  properties: DataProcessInstanceProperties
  relationships: DataProcessInstanceRelationships
  inputs: [Entity]
  outputs: [Entity]
  # Note: Run events are in timeseries data
}
```

## Notable Exceptions

### Instance vs Definition Distinction

A common pitfall is confusing instance-level metadata with definition-level metadata:

**Wrong**: Attaching tags to a DataProcessInstance

```python
# Don't do this - tags belong on the DataJob
instance = DataProcessInstance(...)
# instance.tags = ["pii", "critical"]  # This doesn't exist
```

**Right**: Attach tags to the DataJob, use properties for run-specific metadata

```python
# Tag the DataJob definition
datajob.tags = ["pii", "critical"]

# Use properties for run-specific metadata
instance = DataProcessInstance(...)
instance.properties = {"data_size_mb": "150", "row_count": "1000000"}
```

### Execution Status Lifecycle

DataProcessInstance status follows a simple state machine:

1. STARTED event emitted when execution begins
2. COMPLETE event emitted when execution finishes (with SUCCESS/FAILURE/SKIPPED/UP_FOR_RETRY result)

Common mistakes:

- **Forgetting to emit COMPLETE**: Always emit both START and END events
- **Multiple COMPLETE events**: Retries should have the same instance but different attempt numbers
- **Wrong result types**: UP_FOR_RETRY is for transient failures that will retry; FAILURE is final

### Historical Data Retention

DataProcessInstance entities can accumulate quickly in high-frequency pipelines:

- A pipeline running every 5 minutes creates 288 instances per day
- A pipeline with 10 tasks creates 2,880 instances per day
- Over a year, this could be over 1 million instances

Considerations:

- **Retention policies**: Consider implementing retention policies to delete old instances
- **Aggregation**: For long-term analytics, aggregate instance data into summary metrics
- **Sampling**: For very high-frequency processes, consider sampling (e.g., track every 10th execution)
- **Storage costs**: Timeseries data (run events) can grow large; monitor index sizes

DataHub does not automatically clean up old DataProcessInstance entities. You should implement cleanup based on your needs:

```python
# Example: Cleanup instances older than 90 days
import datetime
from datahub.emitter.rest_emitter import DatahubRestEmitter

emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

cutoff_time = int((datetime.datetime.now() - datetime.timedelta(days=90)).timestamp() * 1000)

# Query for old instances and soft-delete them
# (Implementation depends on your retention requirements)
```

### Instance URN Stability

The DataProcessInstance URN is generated from the orchestrator, cluster, and id fields. This means:

- **Same id = Same URN**: Reusing the same id overwrites the previous instance
- **Unique ids required**: Each execution must have a unique id
- **Orchestrator changes**: Changing the orchestrator name creates a new entity lineage

Best practices:

- Use execution-specific IDs (timestamps, run IDs, UUIDs)
- Don't reuse IDs across different executions
- Keep orchestrator names consistent
- Include environment (cluster) in the ID if running the same pipeline in multiple environments
