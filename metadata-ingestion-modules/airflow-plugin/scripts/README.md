# Scripts

This directory contains utility scripts for the DataHub Airflow plugin.

## Airflow 3.0 macOS SIGSEGV Fix

### patch_airflow_macos_sigsegv.sh

Applies a fix for SIGSEGV crashes that occur when running Airflow 3.0 on macOS.

**Usage:**
```bash
./scripts/patch_airflow_macos_sigsegv.sh [tox_env_path]
```

**Default tox environment:** `.tox/py311-airflow302`

**What it does:**
1. Removes the `setproctitle` package from the tox environment
2. Patches Airflow's `LocalExecutor` to make `setproctitle` import optional
3. Patches Airflow's API server command to make `setproctitle` import optional
4. Verifies all patches were applied successfully

**When to use:**
- Running Airflow 3.0 tests on macOS (Intel or Apple Silicon)
- Experiencing SIGSEGV crashes in worker processes
- Tests timing out due to repeated worker crashes

**Notes:**
- Safe to run multiple times (idempotent)
- Only needed for macOS; Linux doesn't need this patch
- Creates `.bak` backup files before patching

### revert_airflow_macos_sigsegv_patch.sh

Reverts the patches applied by `patch_airflow_macos_sigsegv.sh`.

**Usage:**
```bash
./scripts/revert_airflow_macos_sigsegv_patch.sh [tox_env_path]
```

**Default tox environment:** `.tox/py311-airflow302`

**What it does:**
1. Restores original Airflow files from `.bak` backups
2. Provides instructions for reinstalling `setproctitle` if needed

**Alternative:**
To completely revert, you can also recreate the tox environment:
```bash
rm -rf .tox/py311-airflow302
tox -e py311-airflow302 --recreate
```

## Documentation

For detailed information about the SIGSEGV issue and fix, see:
- `AIRFLOW_3_MACOS_SIGSEGV_FIX.md` - Detailed technical documentation
- `AIRFLOW_3_MIGRATION.md` - General Airflow 3.0 migration guide

## Other Scripts

### release.sh

Release script for publishing the plugin (pre-existing).