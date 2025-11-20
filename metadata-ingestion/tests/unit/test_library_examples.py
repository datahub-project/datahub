"""
Test that all library examples can at least be compiled and imported.

This test doesn't execute the examples (they require a running DataHub instance),
but it verifies that:
1. The syntax is valid (no compilation errors)
2. All imports resolve correctly
3. No obvious runtime errors in module-level code

This catches common issues like:
- Missing imports
- Typos in class/function names
- Invalid syntax
"""

import os
import py_compile
import subprocess
from pathlib import Path

import pytest

# Get the examples/library directory
EXAMPLES_DIR = Path(__file__).parent.parent.parent / "examples" / "library"


def get_library_examples():
    """Get all Python files in the examples/library directory."""
    if not EXAMPLES_DIR.exists():
        return []
    return sorted([f for f in os.listdir(EXAMPLES_DIR) if f.endswith(".py")])


@pytest.mark.parametrize("example_file", get_library_examples())
def test_example_compiles(example_file):
    """Test that each example file compiles without syntax errors."""
    filepath = EXAMPLES_DIR / example_file
    try:
        py_compile.compile(filepath, doraise=True)
    except py_compile.PyCompileError as e:
        pytest.fail(f"Compilation failed for {example_file}: {e}")


@pytest.mark.parametrize("example_file", get_library_examples())
def test_example_imports(example_file):
    """
    Test that each example file can be imported without errors.

    This catches import errors and module-level code issues without
    actually executing the main logic (which would require a DataHub instance).
    """
    filepath = EXAMPLES_DIR / example_file

    # Use python -m py_compile to check imports resolve
    result = subprocess.run(
        ["python", "-m", "py_compile", str(filepath)],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        pytest.fail(
            f"Import check failed for {example_file}:\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )


def test_all_examples_accounted_for():
    """Ensure we're testing all example files."""
    examples = get_library_examples()
    assert len(examples) > 0, "No example files found in examples/library"

    # We should have a reasonable number of examples
    assert len(examples) > 50, f"Expected 50+ examples, found {len(examples)}"

    # Verify our new examples are included
    new_examples = [
        "notebook_create.py",
        "notebook_add_content.py",
        "notebook_add_owner.py",
        "notebook_add_tags.py",
        "data_platform_create.py",
    ]

    for example in new_examples:
        assert example in examples, f"New example {example} not found in library"


# ==================== Unit tests for refactored examples ====================
# These tests verify that examples produce valid metadata structures


def test_create_notebook_metadata():
    """Test that create_notebook example produces valid MCP."""
    from examples.library.notebook_create import create_notebook_metadata

    mcp = create_notebook_metadata(
        notebook_urn="urn:li:notebook:(test,test_notebook)",
        title="Test Notebook",
        description="A test notebook",
        external_url="https://example.com/notebook",
        custom_properties={"key": "value"},
        actor="urn:li:corpuser:test",
        timestamp_millis=1234567890000,
    )

    # Validate MCP structure
    assert mcp.entityUrn == "urn:li:notebook:(test,test_notebook)"
    assert mcp.aspectName == "notebookInfo"
    assert mcp.aspect is not None

    # Validate aspect content
    assert mcp.aspect.title == "Test Notebook"
    assert mcp.aspect.description == "A test notebook"
    assert mcp.aspect.externalUrl == "https://example.com/notebook"
    assert mcp.aspect.customProperties == {"key": "value"}
    assert mcp.aspect.changeAuditStamps.created.actor == "urn:li:corpuser:test"
    assert mcp.aspect.changeAuditStamps.created.time == 1234567890000


def test_create_notebook_main_with_mock_emitter():
    """Test that create_notebook main function emits metadata correctly."""
    from unittest import mock

    from examples.library.notebook_create import main

    mock_emitter = mock.Mock()
    main(emitter=mock_emitter)

    # Verify emit was called once
    assert mock_emitter.emit.call_count == 1

    # Verify the emitted MCP
    emitted_mcp = mock_emitter.emit.call_args[0][0]
    assert emitted_mcp.entityUrn == "urn:li:notebook:(querybook,customer_analysis_2024)"
    assert emitted_mcp.aspectName == "notebookInfo"
    assert emitted_mcp.aspect.title == "Customer Segmentation Analysis 2024"


def test_query_dataset_deprecation_not_deprecated():
    """Test querying deprecation for a non-deprecated dataset."""
    from unittest import mock

    from examples.library.dataset_query_deprecation import query_dataset_deprecation

    # Mock client and dataset
    mock_client = mock.Mock()
    mock_dataset = mock.Mock()
    mock_dataset._get_aspect.return_value = None
    mock_client.entities.get.return_value = mock_dataset

    from datahub.sdk import DatasetUrn

    dataset_urn = DatasetUrn(platform="test", name="test_table", env="PROD")

    is_deprecated, note, decommission_time = query_dataset_deprecation(
        mock_client, dataset_urn
    )

    assert not is_deprecated
    assert note is None
    assert decommission_time is None


def test_query_dataset_deprecation_deprecated():
    """Test querying deprecation for a deprecated dataset."""
    from unittest import mock

    from datahub.metadata.schema_classes import DeprecationClass
    from examples.library.dataset_query_deprecation import query_dataset_deprecation

    # Mock client and dataset
    mock_client = mock.Mock()
    mock_dataset = mock.Mock()
    deprecation = DeprecationClass(
        deprecated=True,
        note="This table is deprecated",
        decommissionTime=1234567890000,
        actor="urn:li:corpuser:test",
    )
    mock_dataset._get_aspect.return_value = deprecation
    mock_client.entities.get.return_value = mock_dataset

    from datahub.sdk import DatasetUrn

    dataset_urn = DatasetUrn(platform="test", name="test_table", env="PROD")

    is_deprecated, note, decommission_time = query_dataset_deprecation(
        mock_client, dataset_urn
    )

    assert is_deprecated
    assert note == "This table is deprecated"
    assert decommission_time == 1234567890000


def test_add_terms_to_dataset():
    """Test adding glossary terms to a dataset."""
    from unittest import mock

    from datahub.sdk import DatasetUrn, GlossaryTermUrn
    from examples.library.dataset_add_term import add_terms_to_dataset

    # Mock client and dataset
    mock_client = mock.Mock()
    mock_dataset = mock.Mock()
    mock_client.entities.get.return_value = mock_dataset
    mock_client.resolve.term.return_value = GlossaryTermUrn("ResolvedTerm")

    dataset_urn = DatasetUrn(platform="test", name="test_table", env="PROD")

    add_terms_to_dataset(
        client=mock_client,
        dataset_urn=dataset_urn,
        term_urns=[
            GlossaryTermUrn("Classification.HighlyConfidential"),
            "PII",  # Will be resolved by name
        ],
    )

    # Verify add_term was called twice
    assert mock_dataset.add_term.call_count == 2

    # Verify the terms added
    first_call = mock_dataset.add_term.call_args_list[0][0][0]
    assert str(first_call) == "urn:li:glossaryTerm:Classification.HighlyConfidential"

    # Verify client.entities.update was called
    assert mock_client.entities.update.call_count == 1
