# Library Examples Integration Tests

This directory contains integration tests for DataHub library examples in `metadata-ingestion/examples/library/`.

## Overview

These tests validate that library examples work correctly against a running DataHub instance by:

1. Executing example scripts as standalone programs
2. Passing authentication credentials via environment variables
3. Verifying the scripts execute successfully (exit code 0)

This approach ensures:

- **Examples remain simple and easy to understand** - no test-specific code
- **Examples work as documented** - users can copy/paste and run them
- **Testing is comprehensive** - entire script execution is validated, not just functions
- **Self-documenting** - examples show real usage patterns

## Running Tests

**Prerequisites:**

- A running DataHub instance (typically via smoke-test setup)
- The smoke-test virtual environment activated

```bash
cd smoke-test
source venv/bin/activate
pytest tests/library_examples/ -v
```

## Test Structure

```
tests/library_examples/
├── test_library_examples.py   # Main test file that executes scripts
├── example_manifest.py         # Ordered list of examples to test
└── README.md                   # This file
```

### How It Works

1. **example_manifest.py** defines which examples to test and in what order
2. **test_library_examples.py** uses pytest's parametrize to create one test per example
3. Each test:
   - Sets `DATAHUB_GMS_URL` and `DATAHUB_GMS_TOKEN` environment variables
   - Executes the example script as a subprocess
   - Validates it exits with code 0 (success)

## Example Requirements

For examples to work with this testing approach, they must:

### 1. Support Environment Variables

Examples should read connection details from environment variables:

```python
import os

# Get DataHub connection details from environment
gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")

# Use them when creating emitter/client
rest_emitter = DatahubRestEmitter(gms_server=gms_server, token=token)
```

### 2. Exit with Proper Codes

Examples should use proper exit codes:

- **Exit 0**: Success (implicitly done by Python)
- **Exit 1**: Failure (happens automatically on unhandled exceptions)

No special handling needed - Python does this by default!

### 3. Be Self-Contained or Have Dependencies Met

Examples in the manifest should either:

- **Create their own dependencies** (e.g., notebook_create.py creates a notebook)
- **Have dependencies created first** (manifest ordering handles this)
- **Work without dependencies** (e.g., search examples that query empty results)

## Adding Examples to the Test Suite

### Step 1: Update the Example

Ensure the example supports environment variables:

```python
# Before:
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

# After:
import os
gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
rest_emitter = DatahubRestEmitter(gms_server=gms_server, token=token)
```

### Step 2: Add to Manifest

Edit `example_manifest.py`:

```python
EXAMPLE_MANIFEST = [
    # ... existing examples
    "my_new_example.py",  # Add your example
]
```

**Important**: Add examples in dependency order:

- CREATE operations before READ/UPDATE operations
- Entity creation before operations on that entity

### Step 3: Run the Test

```bash
pytest tests/library_examples/test_library_examples.py::test_library_example[my_new_example.py] -v
```

## Test Fixtures

### `datahub_env`

Provides environment dictionary with DataHub credentials from smoke-test's `auth_session`:

```python
def test_library_example(example_script: str, datahub_env):
    result = run_example_script(script_path, datahub_env)
    assert result.returncode == 0
```

## Handling Dependencies

### Self-Contained Examples

Best approach - example creates everything it needs:

```python
# notebook_create.py - self-contained
notebook_urn = "urn:li:notebook:(querybook,customer_analysis_2024)"
# Creates the notebook, doesn't depend on anything existing
```

### Examples with Dependencies

Order them correctly in the manifest:

```python
EXAMPLE_MANIFEST = [
    "glossary_term_create.py",    # Creates term
    "dataset_add_term.py",         # Uses term created above
]
```

### Examples That Can't Be Tested Yet

Document in `example_manifest.py` why they're excluded:

```python
# Note: Some examples are excluded because:
# - create_glossary_term_with_metadata.py: references parent node that doesn't exist
# - dataset_add_term.py: references dataset that doesn't exist
```

These can be tested later once their dependencies are available.

## Debugging Failed Tests

### See Full Output

```bash
pytest tests/library_examples/test_library_examples.py::test_library_example[notebook_create.py] -vv
```

### Run Example Manually

```bash
cd metadata-ingestion/examples/library
export DATAHUB_GMS_URL="http://localhost:8080"
export DATAHUB_GMS_TOKEN="your_token_here"
python notebook_create.py
```

### Check Specific Error

Test output includes both STDOUT and STDERR from the script:

```
AssertionError: Example script 'my_example.py' failed with exit code 1
STDOUT:
Created entity urn:li:...

STDERR:
Traceback (most recent call last):
...
```

## Common Issues

### Issue: 401 Unauthorized

**Cause**: Example doesn't read `DATAHUB_GMS_TOKEN` from environment

**Fix**: Update example to use `os.getenv("DATAHUB_GMS_TOKEN")`

### Issue: Entity Not Found

**Cause**: Example depends on entity that doesn't exist

**Fix**: Either:

1. Reorder manifest to create dependency first
2. Make example create its own dependencies
3. Exclude from manifest (document why)

### Issue: Import Errors

**Cause**: Script can't find datahub modules

**Fix**: Ensure smoke-test venv is activated and includes metadata-ingestion

## Integration with CI

These tests run in the smoke-test CI pipeline:

1. CI spins up full DataHub stack
2. Runs `pytest tests/library_examples/` as part of smoke tests
3. Reports failures if any example script fails

## Benefits of This Approach

✅ **Simple examples** - no test infrastructure mixed into example code
✅ **Real validation** - tests exactly what users will run
✅ **Easy maintenance** - just ensure examples support env vars
✅ **Self-documenting** - examples show real usage patterns
✅ **Scalable** - easy to add new examples to manifest

## Next Steps

1. **Expand manifest** - add more self-contained examples
2. **Create setup examples** - examples that create common dependencies
3. **Document patterns** - create example templates for common operations
4. **Refactor existing examples** - update remaining examples to support env vars
