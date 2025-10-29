# Airflow 3.x Migration Guide

This document outlines the changes made to support Apache Airflow 3.x and known limitations.

## Summary of Major Changes

Apache Airflow 3.0 introduced significant breaking changes. The DataHub Airflow plugin has been fully updated to support both Airflow 2.4+ and 3.x with backward compatibility.

### 🎯 Key Architectural Change: Moved Away from Operator-Specific Overrides

**The most significant change** is how lineage extraction works:

| Aspect                  | Airflow 2.x                     | Airflow 3.x                            |
| ----------------------- | ------------------------------- | -------------------------------------- |
| **Lineage Extraction**  | Operator-specific extractors    | Unified SQLParser patch                |
| **Customization Point** | Custom extractor per operator   | Single SQL parser integration          |
| **Column Lineage**      | Extractor-dependent             | ✅ Consistent across all SQL operators |
| **Maintenance**         | Multiple extractors to maintain | Single integration point               |

**In Airflow 2.x**, we used operator-specific extractors:

```python
# Different extractor for each SQL operator
SnowflakeExtractor, PostgresExtractor, MySQLExtractor, etc.
```

**In Airflow 3.x**, we use a **unified SQLParser patch**:

```python
# Single patch point for ALL SQL operators
SQLParser.generate_openlineage_metadata_from_sql = datahub_enhanced_version
```

**Benefits:**

- ✅ **Better consistency** - All SQL operators use the same lineage extraction logic
- ✅ **Easier maintenance** - One integration point instead of many extractors
- ✅ **Column-level lineage for all** - Works across all SQL operators uniformly
- ✅ **Future-proof** - New SQL operators automatically get DataHub lineage

### TaskInstance Attribute Changes

Airflow 3.0 introduced `RuntimeTaskInstance` which has a different structure than Airflow 2.x's `TaskInstance`:

| Attribute              | Airflow 2.x                | Airflow 3.0 RuntimeTaskInstance | Status                                |
| ---------------------- | -------------------------- | ------------------------------- | ------------------------------------- |
| `run_id`               | ✅ Database field          | ✅ Base class                   | Available in both                     |
| `start_date`           | ✅ Database field          | ✅ RuntimeTI field              | Available in both                     |
| `try_number`           | ✅ Database field          | ✅ Base class                   | Available in both                     |
| `state`                | ✅ Database field          | ✅ RuntimeTI field              | Available in both                     |
| `task_id`              | ✅ Database field          | ✅ Base class                   | Available in both                     |
| `dag_id`               | ✅ Database field          | ✅ Base class                   | Available in both                     |
| `max_tries`            | ✅ Database field          | ✅ RuntimeTI field              | Available in both                     |
| `end_date`             | ✅ Database field          | ✅ RuntimeTI field              | Available in both                     |
| `log_url`              | ✅ Property                | ✅ RuntimeTI field              | Available in both                     |
| `execution_date`       | ✅ Database field          | ❌ Renamed                      | **Renamed to `logical_date`**         |
| `duration`             | ✅ Database field          | ❌ Not available                | **Missing - must be calculated**      |
| `external_executor_id` | ✅ Database field          | ❌ Not available                | **Missing in Airflow 3.0**            |
| `operator`             | ✅ Database field (string) | ⚠️ Different                    | **Has `task` (BaseOperator) instead** |
| `priority_weight`      | ✅ Database field          | ❌ Not available                | **Missing in Airflow 3.0**            |

**Key Changes:**

1. **RuntimeTaskInstance is not database-backed** - It's a Pydantic model created at runtime
2. **Minimal base attributes** - Base `TaskInstance` only has: `id`, `task_id`, `dag_id`, `run_id`, `try_number`, `map_index`, `hostname`
3. **execution_date → logical_date** - The familiar `execution_date` was renamed
4. **Missing fields** - Several metadata fields are not available in the runtime context
5. **task vs operator** - Airflow 3.0 provides direct access to the task object, not just its string name

**Plugin Compatibility:**

The plugin uses `hasattr()` checks to gracefully handle missing attributes:

```python
def get_task_instance_attributes(ti: "TaskInstance") -> Dict[str, str]:
    attributes = {}

    # Safe attribute access - works in both versions
    if hasattr(ti, "run_id"):
        attributes["run_id"] = str(ti.run_id)

    # Handle renamed attribute
    if hasattr(ti, "execution_date"):
        attributes["execution_date"] = str(ti.execution_date)
    elif hasattr(ti, "logical_date"):
        attributes["logical_date"] = str(ti.logical_date)

    # Handle missing attributes gracefully
    if hasattr(ti, "duration"):
        attributes["duration"] = str(ti.duration)

    return attributes
```

This approach ensures the plugin works correctly with both Airflow 2.x and 3.x task instances.

**Files Updated:**

- `src/datahub_airflow_plugin/_airflow_version_specific.py:21-74` - Version-compatible attribute extraction

### Other Major Changes

1. **Import Path Updates** - Airflow 3.x reorganized modules under new SDK structure
2. **API Changes** - JWT authentication instead of Basic Auth; v1 API removed
3. **DAG Parameters** - `schedule_interval` → `schedule`; `default_view` removed
4. **Listener Hook Signatures** - Session parameter removed; error parameter added
5. **Database Access Restrictions** - No Variable.get() in hooks (use env vars instead)
6. **Template Rendering** - Skip deepcopy for RuntimeTaskInstance (already rendered)
7. **SubDAG Removal** - SubDAGs completely removed (use TaskGroups)
8. **Log URL Format** - Simplified URL structure and different config key
9. **Threading Support** - ✅ Works in Airflow 3.x (contrary to initial concerns)

### Compatibility Status

| Feature            | Airflow 2.x | Airflow 3.x | Status                 |
| ------------------ | ----------- | ----------- | ---------------------- |
| Task lineage       | ✅          | ✅          | Fully working          |
| Column lineage     | ✅          | ✅          | Fully working          |
| DAG metadata       | ✅          | ✅          | Fully working          |
| Execution tracking | ✅          | ✅          | Fully working          |
| Threading          | ✅          | ✅          | Fully working          |
| SubDAG support     | ✅          | ❌          | Removed in Airflow 3.x |

## Detailed Changes

This section provides in-depth information about each change made for Airflow 3.x compatibility.

### 1. Import Path Updates

Airflow 3.x reorganized many modules under a new SDK structure. The plugin now uses conditional imports with fallbacks.

#### BaseOperator

```python
# Airflow 3.x (preferred)
from airflow.sdk.bases.operator import BaseOperator

# Airflow 2.x (fallback)
from airflow.models.baseoperator import BaseOperator
```

#### Operator Type Alias

```python
# Airflow 3.x (preferred)
from airflow.sdk.types import Operator

# Airflow 2.x (fallback)
from airflow.models.operator import Operator
```

#### EmptyOperator

```python
# Airflow 3.x (preferred)
from airflow.providers.standard.operators.empty import EmptyOperator

# Airflow 2.x (fallback)
from airflow.operators.empty import EmptyOperator

# Airflow <2.2 (fallback)
from airflow.operators.dummy import DummyOperator
```

#### ExternalTaskSensor

```python
# Airflow 3.x (preferred)
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

# Airflow 2.x (fallback)
from airflow.sensors.external_task import ExternalTaskSensor
```

**Files Updated:**

- `src/datahub_airflow_plugin/_airflow_shims.py` - Conditional import logic
- `src/datahub_airflow_plugin/client/airflow_generator.py` - Import from shims

### 1b. OpenLineage Provider Changes

Airflow 2.7+ introduced native OpenLineage support, and Airflow 3.x completely removed support for the old `openlineage-airflow` package. The native provider has a different API and doesn't include SQL extractors.

#### Import Changes

```python
# Airflow 2.7+ and 3.x (native provider)
from airflow.providers.openlineage.extractors import OperatorLineage as TaskMetadata
from airflow.providers.openlineage.extractors.base import BaseExtractor
from airflow.providers.openlineage.extractors.manager import ExtractorManager

# Airflow < 2.7 (old openlineage-airflow package)
from openlineage.airflow.extractors import TaskMetadata, BaseExtractor, ExtractorManager
from openlineage.airflow.extractors.sql_extractor import SqlExtractor
```

#### API Differences

The native provider's `ExtractorManager` has a different API:

| Feature            | Old Package (< 2.7)                 | Native Provider (2.7+)                        |
| ------------------ | ----------------------------------- | --------------------------------------------- |
| Extractor registry | `self.task_to_extractor.extractors` | `self.extractors`                             |
| Add extractor      | Direct dict assignment              | Direct dict assignment or `add_extractor()`   |
| SQL extractor      | ✅ Included                         | ❌ Not included                               |
| Task metadata type | `TaskMetadata`                      | `OperatorLineage` (aliased as `TaskMetadata`) |

#### Compatibility Layer

The plugin implements a compatibility layer that:

1. Detects which OpenLineage implementation is available
2. Uses the appropriate API for registering extractors
3. Implements custom SQL extraction for Airflow 2.7+ (since native provider doesn't include `SqlExtractor`)

```python
# Compatibility check in ExtractorManager.__init__
if hasattr(self, 'task_to_extractor'):
    # Old openlineage-airflow (Airflow < 2.7)
    extractors_dict = self.task_to_extractor.extractors
else:
    # Native provider (Airflow 2.7+)
    extractors_dict = self.extractors
```

**Impact:**

- The plugin automatically selects the correct OpenLineage implementation based on Airflow version
- SQL lineage extraction works seamlessly across all supported Airflow versions
- No user action required - the plugin handles version differences internally

**Files Updated:**

- `src/datahub_airflow_plugin/_extractors.py` - Conditional OpenLineage imports and API compatibility layer

**Known Limitations:**

- The native provider (Airflow 2.7+) doesn't include SQL extractors, so the plugin provides its own implementation
- Some advanced OpenLineage features from the old package may not be available in the native provider

### 2. DAG Parameter Changes

#### 2a. Schedule Parameter

Airflow 3.x removed the `schedule_interval` parameter in favor of `schedule`.

```python
# Airflow 3.x (required)
DAG("my_dag", schedule=None, ...)

# Airflow 2.4+ (deprecated but supported)
DAG("my_dag", schedule_interval=None, ...)

# Airflow <2.4 (only option)
DAG("my_dag", schedule_interval=None, ...)
```

**Note:** The `schedule` parameter was introduced in Airflow 2.4.0, so test DAGs use `schedule=` which works in both Airflow 2.4+ and 3.x.

**Files Updated:**

- All test DAG files in `tests/integration/dags/*.py`

#### 2b. Default View Parameter

Airflow 3.x removed the `default_view` parameter from the DAG constructor.

```python
# Airflow 2.x (supported)
DAG("my_dag", default_view="tree", ...)  # Set default UI view

# Airflow 3.x (removed)
DAG("my_dag", ...)  # default_view parameter no longer accepted
```

**Reason for Removal:**

- **User preferences are now persistent** - The Airflow UI remembers each user's preferred view per DAG
- **Separation of concerns** - DAG definition (pipeline logic) should be separate from UI presentation
- **Cleaner API** - Removes UI-specific parameters from the core DAG class

**Valid `default_view` values in Airflow 2.x:**

- `"tree"` - Tree view (hierarchical task structure)
- `"graph"` - Graph view (visual DAG graph)
- `"duration"` - Duration view
- `"gantt"` - Gantt chart view
- `"landing_times"` - Landing times view

**Migration:** Simply remove the `default_view` parameter from DAG definitions when upgrading to Airflow 3.x.

**Files Updated:**

- `tests/integration/dags/airflow3/datahub_emitter_operator_jinja_template_dag.py` - Removed `default_view="tree"`

### 3. API Changes

#### REST API Version

Airflow 3.x removed the v1 API and only supports v2.

```python
# Airflow 3.x
api_version = "v2"

# Airflow 2.x
api_version = "v1"
```

#### API Authentication

Airflow 3.x uses JWT token-based authentication instead of HTTP Basic Auth.

```python
# Airflow 3.x - Get JWT token
response = requests.post(
    f"{airflow_url}/auth/token",
    data={"username": username, "password": password}
)
token = response.json()["access_token"]
session.headers["Authorization"] = f"Bearer {token}"

# Airflow 2.x - HTTP Basic Auth
session.auth = (username, password)
```

#### Configuration

Airflow 3.x moved some configuration options:

```bash
# Airflow 3.x
AIRFLOW__API__PORT=8080
AIRFLOW__API__BASE_URL=http://airflow.example.com  # Used for log URLs

# Airflow 2.x
AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080
AIRFLOW__WEBSERVER__BASE_URL=http://airflow.example.com  # Used for log URLs
```

**Log URL Format Changes:**

Airflow 3.x changed the log URL format and configuration:

| Aspect     | Airflow 2.x                                                                                      | Airflow 3.x                                                                       |
| ---------- | ------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------- |
| Config key | `webserver.base_url`                                                                             | `api.base_url`                                                                    |
| URL format | `http://host/dags/{dag_id}/grid?dag_run_id={run_id}&task_id={task_id}&base_date={date}&tab=logs` | `http://host/dags/{dag_id}/runs/{run_id}/tasks/{task_id}?try_number={try_number}` |
| Source     | `TaskInstance.log_url` property                                                                  | `TaskInstance.log_url` property                                                   |

**Example Log URLs:**

```python
# Airflow 2.x
"http://airflow.example.com/dags/my_dag/grid?dag_run_id=manual_run&task_id=my_task&base_date=2023-09-27T21%3A34%3A38%2B0000&tab=logs"

# Airflow 3.x
"http://airflow.example.com/dags/my_dag/runs/manual_run/tasks/my_task?try_number=1"
```

**Impact on DataHub:**

- The `log_url` in DataHub's DataProcessInstance will reflect the Airflow version's format
- Both formats link correctly to the Airflow UI task logs
- The plugin automatically uses the correct configuration key for each version

**Files Updated:**

- `tests/integration/test_plugin.py` - Authentication, API version logic, and base URL configuration
- `src/datahub_airflow_plugin/_airflow_version_specific.py` - Task instance attribute extraction including log_url

### 4. CLI Command Changes

#### DAG Trigger

Airflow 3.x renamed the `--exec-date` parameter to `--logical-date`.

```bash
# Airflow 3.x
airflow dags trigger --logical-date "2023-09-27T21:34:38+00:00" my_dag

# Airflow 2.x
airflow dags trigger --exec-date "2023-09-27T21:34:38+00:00" my_dag
```

**Files Updated:**

- `tests/integration/test_plugin.py` - Conditional CLI parameter

### 5. Listener Hook Signature Changes

Airflow 3.x changed the signatures of listener hooks to remove the `session` parameter and add new parameters.

#### Task Instance Hooks

**Airflow 3.x signatures:**

```python
on_task_instance_running(previous_state, task_instance)
on_task_instance_success(previous_state, task_instance)
on_task_instance_failed(previous_state, task_instance, error)
```

**Airflow 2.x signatures:**

```python
on_task_instance_running(previous_state, task_instance, session)
on_task_instance_success(previous_state, task_instance, session)
on_task_instance_failed(previous_state, task_instance, session)
```

**Compatibility Fix:**
The plugin uses `**kwargs` to handle both versions without breaking pluggy's hook matching:

```python
@hookimpl
def on_task_instance_running(self, previous_state, task_instance, **kwargs):
    # Extract session if present (Airflow 2.x)
    session = kwargs.get("session")
    ...

@hookimpl
def on_task_instance_failed(self, previous_state, task_instance, **kwargs):
    # Extract error and session from kwargs (Airflow 3.x passes error, 2.x passes session)
    session = kwargs.get("session")
    error = kwargs.get("error")
    ...
```

**Important:** Using default parameters like `session=None` in Airflow 3.0 causes pluggy to fail to match the hook spec, preventing hooks from being called.

**Files Updated:**

- `src/datahub_airflow_plugin/datahub_listener.py:559-772` - Listener hook signatures

### 6. SubDAG Removal

Airflow 3.x completely removed SubDAGs (deprecated since Airflow 2.0).

#### Affected Attributes

- `dag.is_subdag` - ❌ Removed
- `dag.parent_dag` - ❌ Removed
- `task.subdag` - ❌ Removed
- `SubDagOperator` - ❌ Removed

#### Compatibility Fix

The plugin uses defensive attribute access:

```python
# Safe for both Airflow 2.x and 3.x
if getattr(dag, "is_subdag", False) and dag.parent_dag is not None:
    # Handle subdag (only executes in Airflow 2.x)
    ...
```

**Files Updated:**

- `src/datahub_airflow_plugin/client/airflow_generator.py:76` - SubDAG handling

### 7. Database Commit Restrictions in Listener Hooks

Airflow 3.x introduces strict High Availability (HA) lock management that prohibits database commits within listener hooks.

#### Problem

The kill switch feature used `Variable.get()` to check if the plugin should be disabled:

```python
# This causes: RuntimeError: UNEXPECTED COMMIT - THIS WILL BREAK HA LOCKS!
if Variable.get("datahub_airflow_plugin_disable_listener", "false").lower() == "true":
    return True
```

#### Solution

For Airflow 3.x, use environment variables instead of Airflow Variables:

```python
def check_kill_switch(self):
    if packaging.version.parse(airflow.__version__) >= packaging.version.parse("3.0.0"):
        # Airflow 3.0+: Use environment variable (no database access)
        if os.getenv(f"AIRFLOW_VAR_{KILL_SWITCH_VARIABLE_NAME}".upper(), "false").lower() == "true":
            return True
    else:
        # Airflow 2.x: Use Variable.get()
        if Variable.get(KILL_SWITCH_VARIABLE_NAME, "false").lower() == "true":
            return True
    return False
```

**Migration Note:** To disable the plugin in Airflow 3.x, set the environment variable:

```bash
export AIRFLOW_VAR_DATAHUB_AIRFLOW_PLUGIN_DISABLE_LISTENER=true
```

**Files Updated:**

- `src/datahub_airflow_plugin/datahub_listener.py:540-557` - Kill switch implementation

### 8. Threading Support in Airflow 3.x

#### Status: ✅ Fully Working

Threading is **enabled by default** in both Airflow 2.x and 3.x. Initial concerns about `RuntimeTaskInstance` unpickleable objects were unfounded.

#### Why Threading Works

**Key insight:** `threading.Thread` does **NOT** pickle its arguments - it passes object references directly within the same process. Only `multiprocessing.Process` requires pickling.

```python
# This works fine - no pickling required
thread = threading.Thread(target=f, args=(task_instance,))
thread.start()
```

**Verification:** Tests pass successfully with `DATAHUB_AIRFLOW_PLUGIN_RUN_IN_THREAD=true` in Airflow 3.0.

#### Configuration

Threading is enabled by default for performance benefits:

```python
# Default: threading enabled
_RUN_IN_THREAD = os.getenv(
    "DATAHUB_AIRFLOW_PLUGIN_RUN_IN_THREAD", "true"
).lower() in ("true", "1")
```

**To disable threading** (if needed):

```bash
export DATAHUB_AIRFLOW_PLUGIN_RUN_IN_THREAD=false
```

**Benefits of Threading:**

- Prevents slow lineage extraction from blocking task completion
- Non-blocking metadata emission to DataHub
- Better performance for complex SQL parsing and lineage extraction

**Files Updated:**

- `src/datahub_airflow_plugin/datahub_listener.py:147-152` - Threading configuration

### 9. Template Rendering

Airflow 3.x's `RuntimeTaskInstance` cannot be deep-copied due to unpickleable objects.

#### Problem

The plugin previously deep-copied task instances to render Jinja templates:

```python
# This fails in Airflow 3.x: TypeError: cannot pickle '_thread.lock' object
task_instance_copy = copy.deepcopy(task_instance)
task_instance_copy.render_templates()
```

#### Solution

For Airflow 3.x, skip template rendering since Airflow already renders templates before task execution:

```python
def _render_templates(task_instance: "TaskInstance") -> "TaskInstance":
    if packaging.version.parse(airflow.__version__) >= packaging.version.parse("3.0.0"):
        # Templates are already rendered by Airflow 3.x task execution system
        return task_instance

    # Airflow 2.x: Render templates manually
    try:
        task_instance_copy = copy.deepcopy(task_instance)
        task_instance_copy.render_templates()
        return task_instance_copy
    except Exception as e:
        logger.info(f"Error rendering templates: {e}")
        return task_instance
```

**Impact:** Jinja-templated SQL queries are correctly parsed in both Airflow 2.x and 3.x.

**Files Updated:**

- `src/datahub_airflow_plugin/datahub_listener.py:259-281` - Template rendering logic

### 10. SQL Parser Integration for Airflow 3.x

Airflow 3.x removed the extractor-based SQL parsing mechanism. SQL operators now call `SQLParser.generate_openlineage_metadata_from_sql()` directly.

#### Architecture Change

**Airflow 2.x:**

```
SQL Operator → OpenLineage Extractor → DataHub Extractor → DataHub SQL Parser → Lineage
```

**Airflow 3.x:**

```
SQL Operator → SQLParser.generate_openlineage_metadata_from_sql() → [Patched by DataHub] → DataHub SQL Parser → Lineage
```

#### Implementation

The plugin patches `SQLParser.generate_openlineage_metadata_from_sql()` to use DataHub's SQL parser:

```python
def patch_sqlparser():
    from airflow.providers.openlineage.sqlparser import SQLParser

    # Store original method for fallback
    SQLParser._original_generate_openlineage_metadata_from_sql = (
        SQLParser.generate_openlineage_metadata_from_sql
    )

    # Replace with DataHub-enhanced version
    SQLParser.generate_openlineage_metadata_from_sql = (
        _datahub_generate_openlineage_metadata_from_sql
    )
```

The patched method:

1. Calls DataHub's SQL parser to extract column-level lineage
2. Converts DataHub URNs to OpenLineage Dataset objects
3. Stores the SQL parsing result in `run_facets` for later retrieval
4. Falls back to the original implementation on errors

**Column-Level Lineage:** The SQL parsing result is stored in the `OperatorLineage.run_facets` dictionary:

```python
DATAHUB_SQL_PARSING_RESULT_KEY = "datahub_sql_parsing_result"
run_facets = {
    DATAHUB_SQL_PARSING_RESULT_KEY: sql_parsing_result
}

operator_lineage = OperatorLineage(
    inputs=inputs,
    outputs=outputs,
    job_facets={"sql": SqlJobFacet(query=sql)},
    run_facets=run_facets,
)
```

The listener retrieves it and processes column lineage:

```python
if DATAHUB_SQL_PARSING_RESULT_KEY in operator_lineage.run_facets:
    sql_parsing_result = operator_lineage.run_facets[DATAHUB_SQL_PARSING_RESULT_KEY]

    # Process column lineage
    if sql_parsing_result.column_lineage:
        fine_grained_lineages.extend(
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[...],
                downstreams=[...],
            )
            for column_lineage in sql_parsing_result.column_lineage
        )
```

**Important:** `OperatorLineage` uses `@define` (attrs library) which creates a frozen dataclass. We cannot add arbitrary attributes to it, so we use the `run_facets` dictionary instead.

**Supported Databases:** All databases supported by DataHub's SQL parser:

- Snowflake, BigQuery, Redshift, PostgreSQL, MySQL, Oracle, SQL Server, Athena, Presto, Trino, etc.

**Files Updated:**

- `src/datahub_airflow_plugin/_airflow3_sql_parser_patch.py` - SQLParser patch implementation
- `src/datahub_airflow_plugin/datahub_listener.py:433-439` - Retrieve sql_parsing_result from run_facets
- `src/datahub_airflow_plugin/_config.py` - Enable SQL parser patch for Airflow 3.x

### 11. Column-Level Lineage in Airflow 3.x

**Status:** ✅ Fully Working

Column-level (fine-grained) lineage is now supported in Airflow 3.x through the SQLParser patch mechanism described above.

**Example:** For a SQL query like:

```sql
INSERT INTO processed_costs (id, month, total_cost, area, cost_per_area)
SELECT id, month, total_cost, area, total_cost / area
FROM costs
```

The plugin generates fine-grained lineage:

- `costs.id → processed_costs.id`
- `costs.month → processed_costs.month`
- `costs.total_cost → processed_costs.total_cost`
- `costs.area → processed_costs.area`
- `costs.area + costs.total_cost → processed_costs.cost_per_area` (derived column)

**Verification:** Run the Snowflake operator test:

```bash
tox -e py311-airflow302 -- -k "v2_snowflake_operator_airflow3"
```

Check the golden file for `fineGrainedLineages`:

```bash
grep -A 10 "fineGrainedLineages" tests/integration/goldens/v2_snowflake_operator_airflow3.json
```

## Known Limitations

### 1. SubDAG Lineage Not Supported in Airflow 3.x

**Impact:** If you're upgrading from Airflow 2.x and using SubDAGs, lineage tracking for subdags will no longer work in Airflow 3.x.

**Reason:** SubDAGs were completely removed from Airflow 3.x.

**Migration Path:** Use TaskGroups instead of SubDAGs. TaskGroups provide visual grouping without creating separate DAG runs.

```python
# Old (Airflow 2.x) - SubDAG
from airflow.operators.subdag import SubDagOperator

def subdag(parent_dag_name, child_dag_name, args):
    dag = DAG(f"{parent_dag_name}.{child_dag_name}", **args)
    # Add tasks...
    return dag

with DAG("parent_dag") as dag:
    subdag_task = SubDagOperator(
        task_id="subdag",
        subdag=subdag("parent_dag", "subdag", default_args)
    )

# New (Airflow 3.x) - TaskGroup
from airflow.utils.task_group import TaskGroup

with DAG("parent_dag") as dag:
    with TaskGroup("task_group") as tg:
        # Add tasks to the group...
        task1 = BashOperator(...)
        task2 = BashOperator(...)
```

**Lineage Note:** TaskGroup lineage is tracked at the task level, not as a separate DAG entity.

### 2. Configuration Migration Required

When upgrading to Airflow 3.x, update your configuration:

```bash
# Old Airflow 2.x config
AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080
AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.basic_auth

# New Airflow 3.x config
AIRFLOW__API__PORT=8080
# AUTH_BACKEND no longer needed - uses SimpleAuthManager by default
```

### 3. Test Compatibility Matrix

The DataHub Airflow plugin tests are designed to work with:

| Airflow Version | Test Support | Notes                  |
| --------------- | ------------ | ---------------------- |
| 2.3.x           | ✅ Limited   | Only v1 plugin tested  |
| 2.4.x - 2.9.x   | ✅ Full      | Both v1 and v2 plugins |
| 3.0.x+          | ✅ Full      | v2 plugin only         |

**Note:** Airflow 3.x requires the v2 plugin (listener-based). The v1 plugin is not compatible.

## Testing

### Running Tests Against Airflow 3.x

```bash
# Test with Airflow 3.0.x
tox -e py311-airflow310

# Test with Airflow 3.1.x
tox -e py311-airflow31
```

### Verifying Compatibility

The following checks are performed in tests:

1. ✅ Import compatibility - All modules import without warnings
2. ✅ DAG parsing - DAGs parse successfully without errors
3. ✅ API authentication - JWT token authentication works
4. ✅ Lineage extraction - Inlets/outlets are correctly extracted
5. ✅ Listener functionality - All listener hooks execute properly

## Troubleshooting

### Import Errors

**Error:** `ImportError: cannot import name 'BaseOperator' from 'airflow.models.baseoperator'`

**Solution:** This is expected in Airflow 3.x. The plugin's shims handle this automatically. If you see this error, ensure you're using the latest version of the plugin.

### Authentication Failures

**Error:** `401 Unauthorized` when calling Airflow API

**Solution:**

- Airflow 3.x requires JWT authentication
- Ensure the username/password are correct
- Check that the `/auth/token` endpoint is accessible

### SubDAG Warnings

**Warning:** Code references `is_subdag` attribute

**Solution:** This is expected and safe. The plugin uses `getattr(dag, "is_subdag", False)` which returns `False` in Airflow 3.x without errors.

### Schedule Parameter Issues

**Error:** `TypeError: __init__() got an unexpected keyword argument 'schedule_interval'`

**Solution:** Update DAG definitions to use `schedule=` instead of `schedule_interval=`. The `schedule` parameter is supported in Airflow 2.4+ and required in Airflow 3.x.

### Default View Parameter Issues

**Error:** `TypeError: __init__() got an unexpected keyword argument 'default_view'`

**Solution:** Remove the `default_view` parameter from DAG definitions. This parameter was removed in Airflow 3.x. User view preferences are now persistent in the UI.

## Migration Checklist

When upgrading to Airflow 3.x:

- [ ] Update all DAG definitions to use `schedule=` instead of `schedule_interval=`
- [ ] Remove `default_view=` parameter from all DAG definitions
- [ ] Replace SubDAGs with TaskGroups
- [ ] Update Airflow configuration (port, auth settings)
- [ ] Update any custom operators to use new import paths
- [ ] Test lineage extraction with sample DAGs
- [ ] Verify API authentication works
- [ ] Update CI/CD pipelines to use Airflow 3.x

## References

- [Airflow 3.0 Release Notes](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html#airflow-3-0-0)
- [Airflow 3.0 Migration Guide](https://airflow.apache.org/docs/apache-airflow/stable/migration-guide-to-3.html)
- [TaskGroups Documentation](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#taskgroups)
- [DataHub Airflow Plugin Documentation](https://datahubproject.io/docs/metadata-ingestion/integration_docs/airflow/)

## Support

For issues related to Airflow 3.x compatibility:

1. Check this migration guide
2. Review the [GitHub Issues](https://github.com/datahub-project/datahub/issues)
3. Ask in the [DataHub Slack](https://slack.datahubproject.io/)

## Version History

| Version | Date       | Changes                           |
| ------- | ---------- | --------------------------------- |
| 1.0     | 2025-01-XX | Initial Airflow 3.x support added |
