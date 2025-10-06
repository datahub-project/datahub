# Airflow 3.x Migration Guide

This document outlines the changes made to support Apache Airflow 3.x and known limitations.

## Overview

Apache Airflow 3.0 introduced significant breaking changes to the codebase, including module reorganization, API changes, and removal of deprecated features. The DataHub Airflow plugin has been updated to support both Airflow 2.4+ and Airflow 3.x with full backward compatibility.

## Changes Made for Airflow 3.x Compatibility

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

| Feature | Old Package (< 2.7) | Native Provider (2.7+) |
|---------|---------------------|------------------------|
| Extractor registry | `self.task_to_extractor.extractors` | `self.extractors` |
| Add extractor | Direct dict assignment | Direct dict assignment or `add_extractor()` |
| SQL extractor | ✅ Included | ❌ Not included |
| Task metadata type | `TaskMetadata` | `OperatorLineage` (aliased as `TaskMetadata`) |

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

### 2. DAG Schedule Parameter

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

# Airflow 2.x
AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080
```

**Files Updated:**
- `tests/integration/test_plugin.py` - Authentication and API version logic

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
The plugin makes `session` and `error` parameters optional to support both versions:

```python
@hookimpl
def on_task_instance_running(previous_state, task_instance, session=None):
    # Works with both Airflow 2.x (passes session) and 3.x (doesn't pass session)
    ...

@hookimpl
def on_task_instance_failed(previous_state, task_instance, error=None, session=None):
    # Airflow 3.x passes error, Airflow 2.x passes session
    ...
```

**Files Updated:**
- `src/datahub_airflow_plugin/_datahub_listener_module.py` - Listener hook signatures

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

| Airflow Version | Test Support | Notes |
|----------------|-------------|-------|
| 2.3.x | ✅ Limited | Only v1 plugin tested |
| 2.4.x - 2.9.x | ✅ Full | Both v1 and v2 plugins |
| 3.0.x+ | ✅ Full | v2 plugin only |

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

## Migration Checklist

When upgrading to Airflow 3.x:

- [ ] Update all DAG definitions to use `schedule=` instead of `schedule_interval=`
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

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-01-XX | Initial Airflow 3.x support added |
## macOS SIGSEGV Issue and Fix

### Problem Description

When running Airflow 3.0 tests on macOS, the system experiences SIGSEGV (segmentation fault) crashes. This causes:
- Hundreds of worker processes to spawn and crash repeatedly
- Task execution failures
- Tests timing out or failing

### Root Cause

The issue is caused by the `setproctitle` Python package. According to upstream issues:
- [Gunicorn issue #3021](https://github.com/benoitc/gunicorn/issues/3021)
- [Gunicorn issue #2761](https://github.com/benoitc/gunicorn/issues/2761)
- [Airflow issue #55838](https://github.com/apache/airflow/issues/55838)

On macOS, using `setproctitle` after `fork()` causes segmentation faults due to macOS restrictions on certain library usage after forking.

### Solution

A patch script has been provided to fix this issue:

```bash
./scripts/patch_airflow_macos_sigsegv.sh
```

This script:
1. Removes the `setproctitle` package from the tox environment
2. Patches Airflow's LocalExecutor to make setproctitle import optional
3. Patches Airflow's API server command to make setproctitle import optional

### Important Notes

- **This issue ONLY affects macOS** (both Intel and Apple Silicon)
- **Linux environments are NOT affected** and don't need the patch
- The patch is safe and idempotent (can be run multiple times)
- Process titles will not be set after patching (cosmetic only)

### Documentation

See `AIRFLOW_3_MACOS_SIGSEGV_FIX.md` for:
- Detailed problem description
- Step-by-step manual fix instructions
- Verification procedures
- Alternative solutions considered
- Upstream issue tracking

### Revert Instructions

To revert the patches:

```bash
./scripts/revert_airflow_macos_sigsegv_patch.sh
```

Or recreate the tox environment:

```bash
rm -rf .tox/py311-airflow302
tox -e py311-airflow302 --recreate
```
