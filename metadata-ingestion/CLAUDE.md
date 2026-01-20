# DataHub Metadata Ingestion Development Guide

## Build and Test Commands

**Using Gradle (slow but reliable):**

```bash
# Development setup from repository root
../gradlew :metadata-ingestion:installDev   # Setup Python environment
source venv/bin/activate                    # Activate virtual environment

# Linting and formatting
../gradlew :metadata-ingestion:lint         # Run ruff + mypy
../gradlew :metadata-ingestion:lintFix      # Auto-fix linting issues

# Testing
../gradlew :metadata-ingestion:testQuick                           # Fast unit tests
../gradlew :metadata-ingestion:testFull                            # All tests
../gradlew :metadata-ingestion:testSingle -PtestFile=tests/unit/test_file.py  # Single test
```

**Direct Python commands (when venv is activated):**

```bash
# Linting
ruff format src/ tests/
ruff check src/ tests/
mypy src/ tests/

# Testing
pytest -vv                                 # Run all tests
pytest -m 'not integration'                # Unit tests only
pytest -m 'integration'                    # Integration tests
pytest tests/path/to/file.py               # Single test file
pytest tests/path/to/file.py::TestClass    # Single test class
pytest tests/path/to/file.py::TestClass::test_method  # Single test
```

## Environment Variables

**Build configuration:**

- `DATAHUB_VENV_USE_COPIES`: Set to `true` to use `--copies` flag when creating Python virtual environments. This copies the Python binary instead of creating a symlink. Useful for Nix environments, immutable filesystems, Windows, or container environments where symlinks don't work correctly. Increases disk usage and setup time, so only enable if needed.

  ```bash
  export DATAHUB_VENV_USE_COPIES=true
  ../gradlew :metadata-ingestion:installDev
  ```

## Directory Structure

- `src/datahub/`: Source code for the DataHub CLI and ingestion framework
- `tests/`: All tests (NOT in `src/` directory)
- `tests/unit/`: Unit tests
- `tests/integration/`: Integration tests
- `scripts/`: Build and development scripts
- `examples/`: Example ingestion configurations
- `developing.md`: Detailed development environment information

## Code Style Guidelines

- **Formatting**: Uses ruff, 88 character line length
- **Imports**: Sorted with ruff.lint.isort, no relative imports, Imports are always put at the top of the file, just after any module comments and docstrings, and before module globals and constants.
- **Types**: Always use type annotations, prefer Protocol for interfaces
  - Avoid `Any` type - use specific types (`Dict[str, int]`, `TypedDict`, or typevars)
  - Use `isinstance` checks instead of `hasattr`
  - Prefer `assert isinstance(...)` over `cast`
- **Data Structures**: Use dataclasses/pydantic for internal data representation
  - Return dataclasses instead of tuples from methods
  - Centralize utility functions to avoid code duplication
- **Naming**: Descriptive names, match source system terminology in configs
- **Error Handling**: Validators throw only ValueError/TypeError/AssertionError
  - Add robust error handling with layers of protection for known failure points
- **Code Quality**: Avoid global state, use named arguments, don't re-export in `__init__.py`
- **Documentation**: All configs need descriptions
- **Dependencies**: Avoid version pinning, use ranges with comments
- **Architecture**: Avoid tall inheritance hierarchies, prefer mixins

## Security Guidelines - SQL Injection Prevention

**CRITICAL**: Always use parameterized queries for dynamic SQL generation.

### ❌ NEVER: F-string interpolation with user input

```python
# DANGEROUS - SQL injection vulnerability
def bad_query(pattern: str) -> str:
    return f"SELECT * FROM table WHERE column LIKE '%{pattern}%'"
```

### ✅ ALWAYS: Parameterized queries with bind parameters

```python
# SAFE - Uses bind parameters
def safe_query(pattern: str) -> tuple[str, dict]:
    query = "SELECT * FROM table WHERE column LIKE :pattern"
    params = {"pattern": f"%{pattern}%"}
    return query, params

# Usage with SQLAlchemy
result = connection.execute(text(query), params)
```

### Pattern: Query Functions Return (query, params) Tuple

```python
from sqlalchemy import text

def get_data(filter_value: str) -> tuple[str, dict[str, str]]:
    """Return parameterized query and bind parameters."""
    query = "SELECT * FROM users WHERE status = :status"
    params = {"status": filter_value}
    return query, params

# Caller usage
query, params = get_data("active")
result = connection.execute(text(query), params)
```

### Identifier Sanitization

**CRITICAL**: When database identifiers (table names, column names, schema names) must be embedded directly in SQL (not as bind parameters), use strict validation.

#### Pattern: \_sanitize_identifier() Function

```python
import re

def _sanitize_identifier(identifier: str) -> str:
    """
    Validate and sanitize SQL identifiers (table/column/schema names).

    Security Assumptions:
    - Only alphanumeric characters and underscores are allowed
    - Must start with letter or underscore (SQL standard)
    - Rejects quoted identifiers, unicode, special characters
    - Does NOT support schema.table format (use separate validation)

    Limitations:
    - Does not handle quoted identifiers ("My Table")
    - Does not support unicode identifiers (ñ, 中文, etc.)
    - Does not validate schema.table format - split and validate separately
    - Database-specific keywords are NOT checked (e.g., "SELECT" is allowed)

    Why this regex?
    - ^[a-zA-Z_] ensures identifier starts with letter or underscore (SQL standard)
    - [a-zA-Z0-9_]* allows only safe characters in rest of identifier
    - $ ensures no trailing special characters

    Args:
        identifier: Database identifier to validate

    Returns:
        The validated identifier (unchanged if valid)

    Raises:
        ValueError: If identifier contains unsafe characters or invalid format

    Examples:
        >>> _sanitize_identifier("users")  # OK
        'users'
        >>> _sanitize_identifier("my_table_123")  # OK
        'my_table_123'
        >>> _sanitize_identifier("table'; DROP TABLE users; --")  # REJECTED
        ValueError: Invalid identifier
        >>> _sanitize_identifier("123_table")  # REJECTED - starts with digit
        ValueError: Invalid identifier
        >>> _sanitize_identifier("my table")  # REJECTED - contains space
        ValueError: Invalid identifier
    """
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
        raise ValueError(
            f"Invalid identifier '{identifier}'. Must contain only "
            f"alphanumeric characters and underscores, starting with letter or underscore."
        )
    return identifier
```

#### When to Use Identifier Sanitization

**Use \_sanitize_identifier() for**:

- Table names in FROM clauses when table name comes from config/user input
- Column names in WHERE clauses when dynamic column filtering is needed
- Schema names when building schema-qualified table references

**Do NOT use for**:

- Values in WHERE clauses - use bind parameters instead
- LIKE patterns - use bind parameters with pattern escaping
- Any user-provided data values - always use bind parameters

#### Example: Safe Dynamic Table Name

```python
def get_table_query(table_name: str, status_filter: str) -> tuple[str, dict]:
    """
    Build query with dynamic table name (identifier) and parameterized value.

    CRITICAL: table_name is an identifier and must be sanitized.
    CRITICAL: status_filter is a value and must be parameterized.
    """
    # Sanitize identifier (table name)
    safe_table = _sanitize_identifier(table_name)

    # Parameterize values
    query = f"SELECT * FROM {safe_table} WHERE status = :status"
    params = {"status": status_filter}

    return query, params

# Usage
query, params = get_table_query("users", "active")
result = connection.execute(text(query), params)
```

#### Schema.Table Format Handling

```python
def sanitize_qualified_table(qualified_name: str) -> str:
    """
    Sanitize schema-qualified table name.

    Args:
        qualified_name: Format "schema.table" or just "table"

    Returns:
        Sanitized qualified name

    Raises:
        ValueError: If either part is invalid
    """
    parts = qualified_name.split('.')

    if len(parts) == 1:
        # Just table name
        return _sanitize_identifier(parts[0])
    elif len(parts) == 2:
        # schema.table format
        schema = _sanitize_identifier(parts[0])
        table = _sanitize_identifier(parts[1])
        return f"{schema}.{table}"
    else:
        raise ValueError(
            f"Invalid qualified name '{qualified_name}'. "
            f"Must be 'table' or 'schema.table' format."
        )
```

### Security Audit Commands

```bash
# Find dangerous patterns (should return nothing)
grep -r "f\".*SELECT" src/      # F-strings in SQL
grep -r "f'.*INSERT" src/       # F-strings in SQL
grep -r "%.*WHERE" src/         # % formatting in SQL

# Find identifier usage without sanitization
grep -r "f\".*FROM.*{" src/     # Dynamic table names in FROM clause
grep -r "f'.*FROM.*{" src/      # Dynamic table names in FROM clause
```

## Testing Conventions

- **Location**: Tests go in `tests/` directory alongside `src/`, NOT in `src/`
- **Structure**: Test files should mirror the source directory structure
- **Framework**: Use pytest, not unittest
- **Assertions**: Use `assert` statements, not `self.assertEqual()` or `self.assertIsNone()`
  - Boolean: `assert func()` or `assert not func()`, not `assert func() is True/False`
  - None: `assert result is None` (correct), not `assert result == None`
  - Keep tests concise, avoid verbose repetitive patterns
- **Classes**: Use regular classes, not `unittest.TestCase`
- **Imports**: Import `pytest` in test files
- **Naming**: Test files should be named `test_*.py`
- **Categories**:
  - Unit tests: `tests/unit/` - fast, no external dependencies
  - Integration tests: `tests/integration/` - may use Docker/external services

## Configuration Guidelines (Pydantic)

- **Naming**: Match terminology of the source system (e.g., `account_id` for Snowflake, not `host_port`)
- **Descriptions**: All configs must have descriptions
- **Patterns**: Use AllowDenyPatterns for filtering, named `*_pattern`
- **Defaults**: Set reasonable defaults, avoid config-driven filtering that should be automatic
- **Validation**: Single pydantic validator per validation concern
- **Security**: Use `SecretStr` for passwords, auth tokens, etc.
- **Deprecation**: Use `pydantic_removed_field` helper for field deprecations

## Key Files

- `src/datahub/emitter/mcp_builder.py`: Examples of defining various aspect types
- `setup.py`, `pyproject.toml`, `setup.cfg`: Code style and dependency configuration
- `.github/workflows/metadata-ingestion.yml`: CI workflow configuration
