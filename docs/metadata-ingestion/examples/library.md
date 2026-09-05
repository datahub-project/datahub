# DataHub Library Examples

This directory contains examples demonstrating how to use the DataHub Python SDK and metadata emission APIs.

## Structure

Each example is a standalone Python script that demonstrates a specific use case:

- **Create examples**: Show how to create new metadata entities
- **Update examples**: Show how to modify existing metadata
- **Query examples**: Show how to read and query metadata
- **Delete examples**: Show how to remove metadata

## Writing Testable Examples

To ensure examples are maintainable and correct, follow this pattern when writing new examples:

### Pattern Overview

Examples should have two main components:

1. **Testable functions**: Pure functions that take dependencies as parameters and return values/metadata
2. **Main function**: Entry point that creates dependencies and calls the testable functions

### Example Structure

```python
from typing import Optional
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter


def create_entity_metadata(...) -> MetadataChangeProposalWrapper:
    """
    Create metadata for an entity.

    This function is pure and testable - it doesn't have side effects.

    Args:
        ... (all required parameters)

    Returns:
        MetadataChangeProposalWrapper containing the metadata
    """
    # Build and return the MCP
    return MetadataChangeProposalWrapper(...)


def main(emitter: Optional[DatahubRestEmitter] = None) -> None:
    """
    Main function demonstrating the example use case.

    Args:
        emitter: Optional emitter for testing. If not provided, creates a new one.
    """
    emitter = emitter or DatahubRestEmitter(gms_server="http://localhost:8080")

    # Use the testable function
    mcp = create_entity_metadata(...)

    # Emit the metadata
    emitter.emit(mcp)
    print(f"Successfully created entity")


if __name__ == "__main__":
    main()
```

### For SDK-based Examples

When using the DataHub SDK (`DataHubClient`):

```python
from typing import Optional
from datahub.sdk import DataHubClient


def perform_operation(client: DataHubClient, ...) -> ...:
    """
    Perform an operation using the DataHub client.

    Args:
        client: DataHub client to use
        ...: Other parameters

    Returns:
        Result of the operation
    """
    # Perform the operation
    return result


def main(client: Optional[DataHubClient] = None) -> None:
    """
    Main function demonstrating the example use case.

    Args:
        client: Optional client for testing. If not provided, creates one from env.
    """
    client = client or DataHubClient.from_env()

    result = perform_operation(client, ...)
    print(f"Operation result: {result}")


if __name__ == "__main__":
    main()
```

### Benefits of This Pattern

1. **Testability**: Core logic can be unit tested without needing a running DataHub instance
2. **Reusability**: The testable functions can be imported and used in other code
3. **Clarity**: Separates business logic from infrastructure setup
4. **Flexibility**: Examples can still be run standalone while being testable

### Running Examples

**As standalone scripts:**

```bash
python examples/library/notebook_create.py
```

**In tests:**

```python
from examples.library.create_notebook import create_notebook_metadata

# Unit test
mcp = create_notebook_metadata(...)
assert mcp.entityUrn == "..."

# Integration test
from examples.library.create_notebook import main
main(emitter=test_emitter)  # Inject test emitter
```

## Testing

Examples are tested at two levels:

### Unit Tests

Located in `tests/unit/test_library_examples.py`:

- Test that examples compile and imports resolve
- Test that core functions produce valid metadata structures
- Use mocking to avoid needing a real DataHub instance
- Fast and run on every commit

### Integration Tests

Located in `tests/integration/library_examples/`:

- Test examples against a real DataHub instance
- Verify end-to-end functionality including reads after writes
- Test that metadata is correctly persisted and retrievable
- Slower, may run less frequently

### Running Tests

```bash
# Run all example tests (unit only)
pytest tests/unit/test_library_examples.py

# Run specific unit tests
pytest tests/unit/test_library_examples.py::test_create_notebook_metadata

# Run integration tests (requires running DataHub)
pytest tests/integration/library_examples/ -m integration

# Run all tests
pytest tests/unit/test_library_examples.py tests/integration/library_examples/
```

## Guidelines

1. **Keep examples simple**: Focus on demonstrating one concept clearly
2. **Use realistic data**: URNs, names, and values should look like real-world usage
3. **Add comments**: Explain non-obvious choices or important details
4. **Follow the pattern**: Use the testable function + main() pattern
5. **Document parameters**: Use clear docstrings with type hints
6. **Handle errors gracefully**: Show proper error handling where relevant
7. **Test your examples**: Add unit tests for new examples

## Example Categories

### Entity Creation

- `notebook_create.py` - Create a notebook entity
- `data_platform_create.py` - Create a custom data platform
- `glossary_term_create.py` - Create glossary terms

### Metadata Updates

- `dataset_add_term.py` - Add glossary terms to datasets
- `dataset_add_owner.py` - Add ownership information
- `notebook_add_tags.py` - Add tags to notebooks

### Querying Metadata

- `dataset_query_deprecation.py` - Check if a dataset is deprecated
- `search_with_query.py` - Search for entities
- `lineage_column_get.py` - Query column-level lineage

## Getting Help

- [DataHub Documentation](https://datahubproject.io/docs/)
- [Python SDK Reference](https://datahubproject.io/docs/python-sdk/)
- [Metadata Model](https://datahubproject.io/docs/metadata-model/)
- [GitHub Issues](https://github.com/datahub-project/datahub/issues)
