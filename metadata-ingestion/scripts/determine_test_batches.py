#!/usr/bin/env python3
"""Determine which integration test batches need to run based on changed files.

Used by CI to skip integration batches unaffected by a PR's changes.

Exit behavior:
  - Prints a JSON list of test commands to run (e.g. ["testQuick", "testIntegrationBatch0"])
  - If core code changed, prints all batches.
"""

import json
import re
import subprocess
import sys
from pathlib import Path

# Core paths: changes here can affect any connector, so all batches must run.
INGESTION_CORE_PATHS = [
    "metadata-ingestion/setup.py",
    "metadata-ingestion/pyproject.toml",
    "metadata-ingestion/constraints.txt",
    "metadata-ingestion/setup.cfg",
    "metadata-ingestion/src/datahub/__init__.py",
    "metadata-ingestion/src/datahub/api/",
    "metadata-ingestion/src/datahub/cli/",
    "metadata-ingestion/src/datahub/configuration/",
    "metadata-ingestion/src/datahub/emitter/",
    "metadata-ingestion/src/datahub/ingestion/api/",
    "metadata-ingestion/src/datahub/ingestion/graph/",
    "metadata-ingestion/src/datahub/ingestion/run/",
    "metadata-ingestion/src/datahub/ingestion/sink/",
    "metadata-ingestion/src/datahub/ingestion/source_config/",
    "metadata-ingestion/src/datahub/ingestion/source_report/",
    "metadata-ingestion/src/datahub/ingestion/__init__.py",
    "metadata-ingestion/src/datahub/metadata/",
    "metadata-ingestion/src/datahub/pydantic/",
    "metadata-ingestion/src/datahub/sdk/",
    "metadata-ingestion/src/datahub/secret/",
    "metadata-ingestion/src/datahub/specific/",
    "metadata-ingestion/src/datahub/sql_parsing/",
    "metadata-ingestion/src/datahub/telemetry/",
    "metadata-ingestion/src/datahub/testing/",
    "metadata-ingestion/src/datahub/upgrade/",
    "metadata-ingestion/src/datahub/utilities/",
    "metadata-ingestion/src/datahub/ingestion/extractor/",
    "metadata-ingestion/src/datahub/ingestion/transformer/",
    "metadata-ingestion/src/datahub/ingestion/reporting/",
    "metadata-ingestion/src/datahub/ingestion/fs/",
    "metadata-ingestion/src/datahub/ingestion/glossary/",
    "metadata-ingestion/src/datahub/ingestion/source/common/",
    "metadata-ingestion/tests/conftest.py",
    "metadata-ingestion/tests/test_helpers/",
    "metadata-models/",
]

ALL_BATCHES = [
    "testQuick",
    "testIntegrationBatch0",
    "testIntegrationBatch1",
    "testIntegrationBatch2",
    "testIntegrationBatch3",
    "testIntegrationBatch4",
    "testIntegrationBatch5",
    "testIntegrationBatchRecording",
]


def get_changed_files(base_ref: str | None) -> list[str]:
    """Get list of changed files relative to base branch."""
    if base_ref:
        # PR: compare against base branch
        cmd = ["git", "diff", "--name-only", f"origin/{base_ref}...HEAD"]
    else:
        # Push: compare against previous commit
        cmd = ["git", "diff", "--name-only", "HEAD~1", "HEAD"]

    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return [f for f in result.stdout.strip().split("\n") if f]


def is_core_change(changed_files: list[str]) -> bool:
    """Check if any changed file is in the core paths."""
    for f in changed_files:
        for core_path in INGESTION_CORE_PATHS:
            if f == core_path or f.startswith(core_path):
                return True
    return False


def get_batch_for_test_file(test_file: Path) -> str:
    """Read a test file and extract its batch marker. Default to batch 0."""
    try:
        content = test_file.read_text()
        match = re.search(r"integration_batch_(\w+)", content)
        if match:
            batch_id = match.group(1)
            return (
                f"testIntegrationBatch{batch_id.capitalize()}"
                if batch_id == "recording"
                else f"testIntegrationBatch{batch_id}"
            )
    except OSError:
        pass
    return "testIntegrationBatch0"


def find_test_dir_for_source(source_path: str) -> str | None:
    """Map a source file path to its integration test directory name."""
    # Extract connector name from source path
    # e.g. metadata-ingestion/src/datahub/ingestion/source/snowflake/foo.py -> snowflake
    match = re.match(
        r"metadata-ingestion/src/datahub/ingestion/source/([^/]+)", source_path
    )
    if not match:
        return None

    connector_name = match.group(1)
    # Skip non-connector files
    if connector_name in ("__init__.py", "__pycache__", "common"):
        return None

    # Check if matching test directory exists
    test_dir = Path("metadata-ingestion/tests/integration") / connector_name
    if test_dir.is_dir():
        return connector_name

    # Try common naming variations (e.g. source=csv_enricher -> test=csv-enricher)
    alt_name = connector_name.replace("_", "-")
    test_dir = Path("metadata-ingestion/tests/integration") / alt_name
    if test_dir.is_dir():
        return alt_name

    return None


def determine_batches(changed_files: list[str]) -> list[str]:
    """Determine which test batches to run based on changed files."""
    # Filter to metadata-ingestion files only
    mi_files = [f for f in changed_files if f.startswith("metadata-ingestion/")]
    if not mi_files:
        # No ingestion changes, but might have metadata-models changes
        if any(f.startswith("metadata-models/") for f in changed_files):
            return ALL_BATCHES
        return ["testQuick"]

    # If core code changed, run everything
    if is_core_change(changed_files):
        return ALL_BATCHES

    batches_needed: set[str] = {"testQuick"}  # Always run unit tests

    for f in mi_files:
        # Changed integration test file -> determine its batch
        match = re.match(
            r"metadata-ingestion/tests/integration/([^/]+)/test_.*\.py$", f
        )
        if match:
            test_file = Path(f)
            if test_file.exists():
                batches_needed.add(get_batch_for_test_file(test_file))
            else:
                # File doesn't exist locally (maybe deleted), default to batch 0
                batches_needed.add("testIntegrationBatch0")
            continue

        # Changed source file -> find corresponding test dir
        if f.startswith("metadata-ingestion/src/datahub/ingestion/source/"):
            test_dir = find_test_dir_for_source(f)
            if test_dir:
                test_files = list(
                    Path(f"metadata-ingestion/tests/integration/{test_dir}").glob(
                        "test_*.py"
                    )
                )
                for tf in test_files:
                    batches_needed.add(get_batch_for_test_file(tf))
            else:
                # Source file with no matching test dir - could affect batch 0
                batches_needed.add("testIntegrationBatch0")
            continue

        # Changed unit test file -> only testQuick needed (already added)
        if f.startswith("metadata-ingestion/tests/unit/"):
            continue

        # Any other metadata-ingestion file we don't recognize -> run all to be safe
        # This covers things like scripts/, examples/, docs/ which are low-risk
        # but we err on the side of caution for unknown paths
        if f.startswith("metadata-ingestion/src/"):
            # Unknown source module - run all batches
            return ALL_BATCHES

    return sorted(batches_needed)


def main() -> None:
    base_ref = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] != "" else None
    changed_files = get_changed_files(base_ref)

    batches = determine_batches(changed_files)

    # Output as JSON for GitHub Actions
    print(json.dumps(batches))


if __name__ == "__main__":
    main()
