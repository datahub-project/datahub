"""
Utilities for testing library examples.

This module provides helpers for:
- Capturing emitted metadata (MCPs/MCEs)
- Comparing metadata to golden files
- Validating metadata structure
- Setting up test data for integration tests
"""

import json
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from unittest import mock

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import MetadataChangeEventClass
from datahub.testing.compare_metadata_json import assert_metadata_files_equal
from datahub.testing.mce_helpers import clean_nones


@contextmanager
def capture_emitted_mcps():
    """
    Context manager to capture metadata emitted by examples.

    Usage:
        with capture_emitted_mcps() as captured:
            example.main()
        mcps = captured["mcps"]
    """
    captured_mcps: List[
        Union[MetadataChangeProposalWrapper, MetadataChangeEventClass]
    ] = []

    def mock_emit(mcp_or_mce):
        captured_mcps.append(mcp_or_mce)

    with mock.patch.object(DatahubRestEmitter, "emit", side_effect=mock_emit):
        yield {"mcps": captured_mcps}


def validate_mcp_structure(mcp: MetadataChangeProposalWrapper) -> None:
    """
    Validate that an MCP has the required structure.

    Checks:
    - Has entityUrn or entityType + entityKeyAspect
    - Has aspectName
    - Has aspect (the actual metadata)
    """
    assert mcp.entityUrn or (mcp.entityType and mcp.entityKeyAspect), (
        "MCP must have entityUrn or (entityType + entityKeyAspect)"
    )
    assert mcp.aspectName, "MCP must have aspectName"
    assert mcp.aspect, "MCP must have aspect"


def mcp_to_dict(mcp: MetadataChangeProposalWrapper) -> Dict[str, Any]:
    """
    Convert an MCP to a dictionary for comparison.

    Removes None values and normalizes structure.
    """
    return clean_nones(mcp.to_obj())


def compare_mcp_to_golden(
    mcp: MetadataChangeProposalWrapper,
    golden_path: Union[str, Path],
    ignore_paths: Optional[List[str]] = None,
) -> None:
    """
    Compare an MCP to a golden file.

    Args:
        mcp: The MCP to compare
        golden_path: Path to the golden file
        ignore_paths: List of paths to ignore in comparison (e.g., timestamps)
    """
    ignore_paths = ignore_paths or []

    # Add common ignore paths for timestamps
    default_ignore_paths = [
        r"root\['aspect'\]\['value'\]\['created'\]\['time'\]",
        r"root\['aspect'\]\['value'\]\['lastModified'\]\['time'\]",
        r"root\['aspect'\]\['value'\]\['changeAuditStamps'\]\['created'\]\['time'\]",
        r"root\['aspect'\]\['value'\]\['changeAuditStamps'\]\['lastModified'\]\['time'\]",
    ]

    all_ignore_paths = default_ignore_paths + ignore_paths

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(mcp_to_dict(mcp), f, indent=2)
        temp_path = f.name

    try:
        assert_metadata_files_equal(
            output_path=temp_path,
            golden_path=golden_path,
            ignore_paths=all_ignore_paths,
            ignore_order=True,
        )
    finally:
        Path(temp_path).unlink()


def save_mcp_as_golden(
    mcp: MetadataChangeProposalWrapper, golden_path: Union[str, Path]
) -> None:
    """
    Save an MCP as a golden file.

    Use this to create/update golden files for tests.
    """
    golden_path = Path(golden_path)
    golden_path.parent.mkdir(parents=True, exist_ok=True)

    with open(golden_path, "w") as f:
        json.dump(mcp_to_dict(mcp), f, indent=2)


def create_mock_datahub_client(
    responses: Optional[Dict[str, Any]] = None,
) -> mock.Mock:
    """
    Create a mock DataHubClient for testing SDK-based examples.

    Args:
        responses: Dictionary mapping entity URNs to mock response objects

    Returns:
        A mocked DataHubClient with configurable responses
    """
    mock_client = mock.Mock()
    mock_client.entities = mock.Mock()

    if responses:
        mock_client.entities.get.side_effect = lambda urn: responses.get(str(urn))

    return mock_client


def create_test_emitter(capture_list: Optional[List] = None) -> DatahubRestEmitter:
    """
    Create a test emitter that captures emissions without making real HTTP calls.

    Args:
        capture_list: Optional list to append emitted MCPs to

    Returns:
        A mocked DatahubRestEmitter
    """
    mock_emitter = mock.Mock(spec=DatahubRestEmitter)

    if capture_list is not None:

        def capture_emit(mcp):
            capture_list.append(mcp)

        mock_emitter.emit = capture_emit
    else:
        mock_emitter.emit = mock.Mock()

    return mock_emitter
