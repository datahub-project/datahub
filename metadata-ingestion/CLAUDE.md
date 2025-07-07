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
- **Imports**: Sorted with ruff.lint.isort, no relative imports
- **Types**: Always use type annotations, prefer Protocol for interfaces
- **Naming**: Descriptive names, match source system terminology in configs
- **Error Handling**: Validators throw only ValueError/TypeError/AssertionError
- **Documentation**: All configs need descriptions
- **Dependencies**: Avoid version pinning, use ranges with comments
- **Architecture**: Avoid tall inheritance hierarchies, prefer mixins

## Testing Conventions

- **Location**: Tests go in `tests/` directory alongside `src/`, NOT in `src/`
- **Structure**: Test files should mirror the source directory structure
- **Framework**: Use pytest, not unittest
- **Assertions**: Use `assert` statements, not `self.assertEqual()` or `self.assertIsNone()`
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
