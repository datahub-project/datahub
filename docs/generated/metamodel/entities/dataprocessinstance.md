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


**Python SDK: Create and track a simple process instance**

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



### Tracking a Failed Process with Retry


**Python SDK: Track a failed process execution and retry**

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



### Creating a DataFlow Instance (DAG Run)


**Python SDK: Track an entire workflow execution**

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



### Tracking Hierarchical Process Instances


**Python SDK: Track a DAG run with child task instances**

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



### Tracking ML Training Run in a Container

ML training runs are a specialized use case of DataProcessInstance entities. In addition to standard process instance aspects, training runs can use ML-specific aspects:

- **Subtype**: `MLAssetSubTypes.MLFLOW_TRAINING_RUN` to identify the process as an ML training run
- **mlTrainingRunProperties**: ML-specific metadata including training metrics, hyperparameters, and output URLs
- **Relationships**: Links to ML models (via `mlModelProperties.trainingJobs`) and experiments (via `container` aspect)

For comprehensive documentation on ML training runs, including the complete workflow with experiments, models, and datasets, see the [ML Model entity documentation](mlModel.md#training-runs-and-experiments).


**Python SDK: Track a standalone ML training run**

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



### Querying Process Instance History via REST API


**REST API: Query execution history for a DataJob**

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



### Querying Process Instance Run Events


**GraphQL: Query run history with status and timing**

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

#### dataProcessInstanceInput
Information about the inputs datasets of a Data process



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| inputs | string[] | ✓ | Input assets consumed | Searchable, → Consumes |
| inputEdges | [Edge](#edge)[] |  | Input assets consumed by the data process instance, with additional metadata. Counts as lineage. ... | → DataProcessInstanceConsumes |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataProcessInstanceInput"
  },
  "name": "DataProcessInstanceInput",
  "namespace": "com.linkedin.dataprocess",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataset",
            "mlModel"
          ],
          "name": "Consumes"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "inputs",
          "fieldType": "URN",
          "numValuesFieldName": "numInputs",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "inputs",
      "doc": "Input assets consumed"
    },
    {
      "Relationship": {
        "/*/destinationUrn": {
          "createdActor": "inputEdges/*/created/actor",
          "createdOn": "inputEdges/*/created/time",
          "entityTypes": [
            "dataset",
            "mlModel",
            "dataProcessInstance"
          ],
          "isLineage": true,
          "name": "DataProcessInstanceConsumes",
          "properties": "inputEdges/*/properties",
          "updatedActor": "inputEdges/*/lastModified/actor",
          "updatedOn": "inputEdges/*/lastModified/time"
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
      "name": "inputEdges",
      "default": null,
      "doc": "Input assets consumed by the data process instance, with additional metadata.\nCounts as lineage.\nWill eventually deprecate the inputs field."
    }
  ],
  "doc": "Information about the inputs datasets of a Data process"
}
```





#### dataProcessInstanceOutput
Information about the outputs of a Data process



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| outputs | string[] | ✓ | Output assets produced | Searchable, → Produces |
| outputEdges | [Edge](#edge)[] |  | Output assets produced by the data process instance during processing, with additional metadata. ... | → DataProcessInstanceProduces |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataProcessInstanceOutput"
  },
  "name": "DataProcessInstanceOutput",
  "namespace": "com.linkedin.dataprocess",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataset",
            "mlModel"
          ],
          "name": "Produces"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "outputs",
          "fieldType": "URN",
          "numValuesFieldName": "numOutputs",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "outputs",
      "doc": "Output assets produced"
    },
    {
      "Relationship": {
        "/*/destinationUrn": {
          "createdActor": "outputEdges/*/created/actor",
          "createdOn": "outputEdges/*/created/time",
          "entityTypes": [
            "dataset",
            "mlModel",
            "dataProcessInstance"
          ],
          "isLineage": true,
          "isUpstream": false,
          "name": "DataProcessInstanceProduces",
          "properties": "outputEdges/*/properties",
          "updatedActor": "outputEdges/*/lastModified/actor",
          "updatedOn": "outputEdges/*/lastModified/time"
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
      "name": "outputEdges",
      "default": null,
      "doc": "Output assets produced by the data process instance during processing, with additional metadata.\nCounts as lineage.\nWill eventually deprecate the outputs field."
    }
  ],
  "doc": "Information about the outputs of a Data process"
}
```





#### dataProcessInstanceProperties
The inputs and outputs of this data process



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| customProperties | map | ✓ | Custom property bag. | Searchable |
| externalUrl | string |  | URL where the reference exist | Searchable |
| name | string | ✓ | Process name | Searchable |
| type | DataProcessType |  | Process type | Searchable (processType) |
| created | [AuditStamp](#auditstamp) | ✓ | Audit stamp containing who reported the lineage and when | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataProcessInstanceProperties"
  },
  "name": "DataProcessInstanceProperties",
  "namespace": "com.linkedin.dataprocess",
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
        "fieldType": "WORD_GRAM"
      },
      "type": "string",
      "name": "name",
      "doc": "Process name"
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldName": "processType",
        "fieldType": "KEYWORD",
        "filterNameOverride": "Process Type"
      },
      "type": [
        "null",
        {
          "type": "enum",
          "name": "DataProcessType",
          "namespace": "com.linkedin.dataprocess",
          "symbols": [
            "BATCH_SCHEDULED",
            "BATCH_AD_HOC",
            "STREAMING"
          ]
        }
      ],
      "name": "type",
      "default": null,
      "doc": "Process type"
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "created",
          "fieldType": "COUNT",
          "queryByDefault": false
        }
      },
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
      "doc": "Audit stamp containing who reported the lineage and when"
    }
  ],
  "doc": "The inputs and outputs of this data process"
}
```





#### dataProcessInstanceRelationships
Information about Data process relationships



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| parentTemplate | string |  | The parent entity whose run instance it is | Searchable, → InstanceOf |
| parentInstance | string |  | The parent DataProcessInstance where it belongs to. If it is a Airflow Task then it should belong... | Searchable, → ChildOf |
| upstreamInstances | string[] | ✓ | Input DataProcessInstance which triggered this dataprocess instance | Searchable, → UpstreamOf |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataProcessInstanceRelationships"
  },
  "name": "DataProcessInstanceRelationships",
  "namespace": "com.linkedin.dataprocess",
  "fields": [
    {
      "Relationship": {
        "entityTypes": [
          "dataJob",
          "dataFlow",
          "dataset"
        ],
        "name": "InstanceOf"
      },
      "Searchable": {
        "/*": {
          "fieldName": "parentTemplate",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "parentTemplate",
      "default": null,
      "doc": "The parent entity whose run instance it is"
    },
    {
      "Relationship": {
        "entityTypes": [
          "dataProcessInstance"
        ],
        "name": "ChildOf"
      },
      "Searchable": {
        "/*": {
          "fieldName": "parentInstance",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "parentInstance",
      "default": null,
      "doc": "The parent DataProcessInstance where it belongs to.\nIf it is a Airflow Task then it should belong to an Airflow Dag run as well\nwhich will be another DataProcessInstance"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataProcessInstance"
          ],
          "name": "UpstreamOf"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "upstream",
          "fieldType": "URN",
          "numValuesFieldName": "numUpstreams",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "upstreamInstances",
      "doc": "Input DataProcessInstance which triggered this dataprocess instance"
    }
  ],
  "doc": "Information about Data process relationships"
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





#### mlTrainingRunProperties
The inputs and outputs of this training run



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| customProperties | map | ✓ | Custom property bag. | Searchable |
| externalUrl | string |  | URL where the reference exist | Searchable |
| id | string |  | Run Id of the ML Training Run |  |
| outputUrls | string[] |  | List of URLs for the Outputs of the ML Training Run |  |
| hyperParams | MLHyperParam[] |  | Hyperparameters of the ML Training Run |  |
| trainingMetrics | MLMetric[] |  | Metrics of the ML Training Run |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "mlTrainingRunProperties"
  },
  "name": "MLTrainingRunProperties",
  "namespace": "com.linkedin.ml.metadata",
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
      "type": [
        "null",
        "string"
      ],
      "name": "id",
      "default": null,
      "doc": "Run Id of the ML Training Run"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "outputUrls",
      "default": null,
      "doc": "List of URLs for the Outputs of the ML Training Run"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "Aspect": {
              "name": "mlHyperParam"
            },
            "name": "MLHyperParam",
            "namespace": "com.linkedin.ml.metadata",
            "fields": [
              {
                "type": "string",
                "name": "name",
                "doc": "Name of the MLHyperParam"
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "description",
                "default": null,
                "doc": "Documentation of the MLHyperParam"
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "value",
                "default": null,
                "doc": "The value of the MLHyperParam"
              },
              {
                "type": [
                  "null",
                  "long"
                ],
                "name": "createdAt",
                "default": null,
                "doc": "Date when the MLHyperParam was developed"
              }
            ],
            "doc": "Properties associated with an ML Hyper Param"
          }
        }
      ],
      "name": "hyperParams",
      "default": null,
      "doc": "Hyperparameters of the ML Training Run"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "Aspect": {
              "name": "mlMetric"
            },
            "name": "MLMetric",
            "namespace": "com.linkedin.ml.metadata",
            "fields": [
              {
                "type": "string",
                "name": "name",
                "doc": "Name of the mlMetric"
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "description",
                "default": null,
                "doc": "Documentation of the mlMetric"
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "value",
                "default": null,
                "doc": "The value of the mlMetric"
              },
              {
                "type": [
                  "null",
                  "long"
                ],
                "name": "createdAt",
                "default": null,
                "doc": "Date when the mlMetric was developed"
              }
            ],
            "doc": "Properties associated with an ML Metric"
          }
        }
      ],
      "name": "trainingMetrics",
      "default": null,
      "doc": "Metrics of the ML Training Run"
    }
  ],
  "doc": "The inputs and outputs of this training run"
}
```





#### dataProcessInstanceRunEvent (Timeseries)
An event representing the current status of data process run.
DataProcessRunEvent should be used for reporting the status of a dataProcess' run.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| timestampMillis | long | ✓ | The event timestamp field as epoch at UTC in milli seconds. |  |
| eventGranularity | TimeWindowSize |  | Granularity of the event if applicable |  |
| partitionSpec | PartitionSpec |  | The optional partition specification. |  |
| messageId | string |  | The optional messageId, if provided serves as a custom user-defined unique identifier for an aspe... |  |
| externalUrl | string |  | URL where the reference exist | Searchable |
| status | DataProcessRunStatus | ✓ |  | Searchable |
| attempt | int |  | Return the try number that this Instance Run is in |  |
| result | DataProcessInstanceRunResult |  | The final result of the Data Processing run. |  |
| durationMillis | long |  | The duration of the run in milliseconds. |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataProcessInstanceRunEvent",
    "type": "timeseries"
  },
  "name": "DataProcessInstanceRunEvent",
  "namespace": "com.linkedin.dataprocess",
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
        "hasValuesFieldName": "hasRunEvents"
      },
      "TimeseriesField": {},
      "type": {
        "type": "enum",
        "symbolDocs": {
          "STARTED": "The status where the Data processing run is in."
        },
        "name": "DataProcessRunStatus",
        "namespace": "com.linkedin.dataprocess",
        "symbols": [
          "STARTED",
          "COMPLETE"
        ]
      },
      "name": "status"
    },
    {
      "type": [
        "null",
        "int"
      ],
      "name": "attempt",
      "default": null,
      "doc": "Return the try number that this Instance Run is in"
    },
    {
      "TimeseriesField": {},
      "type": [
        "null",
        {
          "type": "record",
          "name": "DataProcessInstanceRunResult",
          "namespace": "com.linkedin.dataprocess",
          "fields": [
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "FAILURE": " The Run Failed",
                  "SKIPPED": " The Run Skipped",
                  "SUCCESS": " The Run Succeeded",
                  "UP_FOR_RETRY": " The Run Failed and will Retry"
                },
                "name": "RunResultType",
                "namespace": "com.linkedin.dataprocess",
                "symbols": [
                  "SUCCESS",
                  "FAILURE",
                  "SKIPPED",
                  "UP_FOR_RETRY"
                ]
              },
              "name": "type",
              "doc": " The final result, e.g. SUCCESS, FAILURE, SKIPPED, or UP_FOR_RETRY."
            },
            {
              "type": "string",
              "name": "nativeResultType",
              "doc": "It identifies the system where the native result comes from like Airflow, Azkaban, etc.."
            }
          ]
        }
      ],
      "name": "result",
      "default": null,
      "doc": "The final result of the Data Processing run."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "durationMillis",
      "default": null,
      "doc": "The duration of the run in milliseconds."
    }
  ],
  "doc": "An event representing the current status of data process run.\nDataProcessRunEvent should be used for reporting the status of a dataProcess' run."
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

#### TestResult

Information about a Test Result

**Fields:**

- `test` (string): The urn of the test
- `type` (TestResultType): The type of the result
- `testDefinitionMd5` (string?): The md5 of the test definition that was used to compute this result. See Test...
- `lastComputed` (AuditStamp?): The audit stamp of when the result was computed, including the actor who comp...


### Relationships

#### Self
These are the relationships to itself, stored in this entity's aspects
- DataProcessInstanceConsumes (via `dataProcessInstanceInput.inputEdges`)
- DataProcessInstanceProduces (via `dataProcessInstanceOutput.outputEdges`)
- ChildOf (via `dataProcessInstanceRelationships.parentInstance`)
- UpstreamOf (via `dataProcessInstanceRelationships.upstreamInstances`)
#### Outgoing
These are the relationships stored in this entity's aspects
- Consumes

   - Dataset via `dataProcessInstanceInput.inputs`
   - MlModel via `dataProcessInstanceInput.inputs`
- DataProcessInstanceConsumes

   - Dataset via `dataProcessInstanceInput.inputEdges`
   - MlModel via `dataProcessInstanceInput.inputEdges`
- Produces

   - Dataset via `dataProcessInstanceOutput.outputs`
   - MlModel via `dataProcessInstanceOutput.outputs`
- DataProcessInstanceProduces

   - Dataset via `dataProcessInstanceOutput.outputEdges`
   - MlModel via `dataProcessInstanceOutput.outputEdges`
- InstanceOf

   - DataJob via `dataProcessInstanceRelationships.parentTemplate`
   - DataFlow via `dataProcessInstanceRelationships.parentTemplate`
   - Dataset via `dataProcessInstanceRelationships.parentTemplate`
- IsFailing

   - Test via `testResults.failing`
- IsPassing

   - Test via `testResults.passing`
- IsPartOf

   - Container via `container.container`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
