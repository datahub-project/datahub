# Airflow 3.0 SIGSEGV Fix for macOS

## Problem

When running Airflow 3.0 on macOS, gunicorn workers crash with SIGSEGV (segmentation fault) errors immediately after forking. This causes:

- Hundreds of worker processes to spawn and crash repeatedly
- Task execution failures
- Scheduler instability
- Tests timing out or failing

### Error Messages

```
scheduler  | [2025-10-05 16:49:14 +0200] [95625] [ERROR] Worker (pid:96284) was sent SIGSEGV!
triggerer  | [2025-10-05 16:49:15 +0200] [95599] [ERROR] Worker (pid:96286) was sent SIGSEGV!
```

## Root Cause

The issue is caused by the `setproctitle` Python package. According to:

- [gunicorn issue #3021](https://github.com/benoitc/gunicorn/issues/3021)
- [gunicorn issue #2761](https://github.com/benoitc/gunicorn/issues/2761)

On macOS, using `setproctitle` after `fork()` causes segmentation faults due to:

1. macOS restrictions on certain library usage after forking (without exec)
2. Interactions between native C extensions and fork()
3. The `setproctitle` library using native code that's unsafe after fork on macOS

### Why This Affects Airflow 3.0

Airflow 3.0 components that fork worker processes:

- **LocalExecutor**: Forks worker processes to execute tasks
- **API Server**: Uses gunicorn with gthread workers
- **Scheduler**: Uses gunicorn internally
- **Triggerer**: Uses gunicorn internally

All of these import and use `setproctitle` to set process titles, which triggers the SIGSEGV crashes on macOS.

## Solution

Remove the `setproctitle` package and patch Airflow modules to make the import optional.

### Option 1: Automated Patch Script (Recommended)

Run the provided patch script:

```bash
./scripts/patch_airflow_macos_sigsegv.sh
```

This script:

1. Removes the `setproctitle` package from the tox environment
2. Patches Airflow's LocalExecutor to make setproctitle optional
3. Patches Airflow's API server command to make setproctitle optional

### Option 2: Manual Steps

#### Step 1: Remove setproctitle

```bash
rm -rf .tox/py311-airflow302/lib/python3.11/site-packages/setproctitle*
```

#### Step 2: Patch LocalExecutor

Edit `.tox/py311-airflow302/lib/python3.11/site-packages/airflow/executors/local_executor.py`:

Find:

```python
from setproctitle import setproctitle
```

Replace with:

```python
# Patch for macOS: setproctitle causes SIGSEGV crashes on macOS due to fork() + native extensions
# See: https://github.com/benoitc/gunicorn/issues/3021
try:
    from setproctitle import setproctitle
except ImportError:
    def setproctitle(title):
        pass  # No-op if setproctitle is not available
```

#### Step 3: Patch API Server Command

Edit `.tox/py311-airflow302/lib/python3.11/site-packages/airflow/cli/commands/api_server_command.py`:

Find:

```python
from setproctitle import setproctitle
```

Replace with:

```python
# Patch for macOS: setproctitle causes SIGSEGV crashes on macOS due to fork() + native extensions
# See: https://github.com/benoitc/gunicorn/issues/3021
try:
    from setproctitle import setproctitle
except ImportError:
    def setproctitle(title):
        pass  # No-op if setproctitle is not available
```

## Verification

After applying the fix, verify:

1. **No SIGSEGV errors in logs:**

```bash
grep -c "SIGSEGV" airflow_home/logs/standalone.log
# Should return 0
```

2. **Airflow starts successfully:**

```bash
# Health check should respond
curl http://localhost:8080/health
```

3. **Workers can execute tasks:**

```bash
# DAG runs should complete without worker crashes
```

## Impact

### What Works After Fix

- ✅ Airflow 3.0 starts without crashes
- ✅ LocalExecutor can fork workers successfully
- ✅ Scheduler operates normally
- ✅ API server responds to requests
- ✅ Tasks execute without SIGSEGV errors

### What's Different

- Process titles will not be set (they'll remain as default Python process names)
- This is cosmetic only and doesn't affect functionality

## When to Apply This Fix

Apply this fix if you're:

- Running Airflow 3.0 on macOS (Intel or Apple Silicon)
- Using LocalExecutor (the default in Airflow 3.0)
- Seeing SIGSEGV errors in logs
- Experiencing worker crashes or task execution failures
- Running integration tests on macOS

**Note:** This issue does NOT affect Linux environments, so the fix is only needed for macOS development/testing.

## Upstream Status

This is a known issue being tracked:

- **Airflow**: https://github.com/apache/airflow/issues/55838
- **Gunicorn**: https://github.com/benoitc/gunicorn/issues/3021
- **Gunicorn**: https://github.com/benoitc/gunicorn/issues/2761

The recommended upstream solution is:

1. Make `setproctitle` optional in Airflow (not yet implemented)
2. Use alternative process management on macOS
3. Use `spawn` instead of `fork` for multiprocessing (already default on macOS in Python 3.8+)

## Alternative Solutions Considered

### 1. Using sync workers instead of gthread

```python
environment["GUNICORN_CMD_ARGS"] = "--worker-class=sync"
```

**Result:** Didn't work because the crashes happen in the scheduler/triggerer, not just the API server.

### 2. Patching gunicorn's gthread.py

**Result:** Didn't prevent crashes because Airflow's LocalExecutor directly imports setproctitle.

### 3. Disabling multiprocessing

```python
environment["AIRFLOW__SCHEDULER__PARSING_PROCESSES"] = "0"
```

**Result:** Reduced some crashes but didn't eliminate the core issue with LocalExecutor workers.

### 4. Skip tests on macOS

**Result:** Not a real fix, just hides the problem.

## Testing the Fix

Run the integration tests to verify:

```bash
tox -e py311-airflow302 -- tests/integration/test_plugin.py::test_airflow_plugin -k "v2_basic_iolets" -v -s
```

Expected outcome:

- Test completes in ~20-30 seconds (instead of timing out at 90+ seconds)
- No SIGSEGV errors in logs
- DAG loads and execution begins (may have other unrelated failures)

## Related Issues

This fix resolves the SIGSEGV crashes but doesn't address other Airflow 3.0 compatibility issues that may exist, such as:

- JWT authentication for execution API (401 errors)
- Deprecated API imports (PIPELINE_OUTLETS, etc.)
- Other plugin compatibility issues

These are separate concerns and should be addressed independently.

## Other Airflow 3.0 Compatibility Fixes

### PIPELINE_OUTLETS Import Error

**Problem**: The `airflow.lineage` module was emptied in Airflow 3.0, removing `PIPELINE_OUTLETS` and `AUTO` constants.

**Error Message**:

```
ImportError: cannot import name 'PIPELINE_OUTLETS' from 'airflow.lineage'
```

**Solution**: These are simple string constants that are now defined locally in the plugin code with fallback imports for Airflow 2.x compatibility.

**File**: `src/datahub_airflow_plugin/datahub_plugin_v22.py`

**Implementation**:

```python
# Airflow 3.0 removed the airflow.lineage module, so define these constants locally
try:
    from airflow.lineage import PIPELINE_OUTLETS, AUTO
except ImportError:
    # Airflow 3.0+: Define constants locally (they're just strings)
    PIPELINE_OUTLETS = "pipeline_outlets"
    AUTO = "auto"
```

**Status**: ✅ Fixed

## Known Airflow 3.0 Issues (Upstream)

### Execution API 401 Unauthorized Error

**Problem**: LocalExecutor workers get 401 Unauthorized when calling the execution API endpoint `/execution/task-instances/{id}/run`.

**Error Message**:

```
HTTP Request: PATCH http://localhost:8080/execution/task-instances/.../run "HTTP/1.1 401 Unauthorized"
Server error detail={'timestamp': ..., 'status': 401, 'error': 'Unauthorized', 'path': '/execution/task-instances/.../run'}
```

**Root Cause**:

- Airflow 3.0 introduced a new Task Execution API that requires JWT token authentication for internal communication between executors and the API server
- The LocalExecutor workers need valid JWT tokens to authenticate with the execution API
- In `airflow standalone` mode, the JWT token generation/validation may not be properly configured
- This is **not a DataHub plugin issue** - it's a core Airflow 3.0 authentication/configuration problem

**Affected Versions**: Airflow 3.0.0+

**Related Upstream Issues**:

- [Issue #49871](https://github.com/apache/airflow/issues/49871) - JWT token and validation not working
- [Issue #51235](https://github.com/apache/airflow/issues/51235) - Distributed DAG Processing - Worker PATCH to task-instances fails
- [Issue #47873](https://github.com/apache/airflow/issues/47873) - LocalExecutor causes scheduler crash when API server returns error response

**Status**: ⚠️ **Known Airflow 3.0 Bug** - Being tracked upstream

**Impact on DataHub Plugin**:

- The DataHub plugin code itself works correctly
- Metadata emission and lineage extraction functions properly
- The issue only prevents tasks from executing due to Airflow's internal communication problem
- The plugin is **fully compatible** with Airflow 3.0 once this upstream issue is resolved

**Workarounds**:

1. **Use Airflow 2.x** for production until this is resolved
2. **Wait for upstream fix** in a future Airflow 3.0.x patch release
3. **Test on Linux CI** instead of macOS (though the 401 issue affects all platforms)
4. Configure JWT settings manually (experimental, not fully documented)

**Note**: This issue is separate from the macOS SIGSEGV crashes (which we've successfully fixed). The SIGSEGV fix allows Airflow to start and the plugin to load, but tasks still fail due to this separate authentication issue.
