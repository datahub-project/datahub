import json
import os
import tempfile
from pathlib import Path

import pytest

from tests.utils import run_datahub_cmd, wait_for_writes_to_sync


@pytest.fixture(scope="module")
def golden_files():
    repo_root = Path(__file__).parents[5]
    golden_files = []

    for golden_file in repo_root.rglob("**/*golden*.json"):
        if any(
            excluded in str(golden_file)
            for excluded in ["/out/", "/build/", "/.gradle/"]
        ):
            continue

        if is_valid_mce_mcp_file(golden_file):
            golden_files.append(str(golden_file))

    return sorted(golden_files)


def is_valid_mce_mcp_file(file_path: Path) -> bool:
    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        items = data if isinstance(data, list) else [data]

        for item in items:
            if not isinstance(item, dict):
                continue

            # MCP structure
            if all(
                key in item
                for key in ["entityType", "entityUrn", "changeType", "aspectName"]
            ):
                return True

            # MCE structure
            if "proposedSnapshot" in item:
                return True

    except (json.JSONDecodeError, FileNotFoundError, PermissionError):
        pass

    return False


@pytest.fixture(scope="module", autouse=True)
def cleanup_golden_files(auth_session, golden_files):
    yield

    # after yield => teardown method

    print("Collecting URNs from all golden files for batch deletion...")

    all_urns = set()

    for file_path in golden_files:
        try:
            with open(file_path, "r") as f:
                data = json.load(f)

            items = data if isinstance(data, list) else [data]

            for item in items:
                if not isinstance(item, dict):
                    continue

                urn = None
                # MCP structure
                if "entityUrn" in item:
                    urn = item["entityUrn"]
                # MCE structure
                elif "proposedSnapshot" in item:
                    snapshot_union = item["proposedSnapshot"]
                    snapshot = list(snapshot_union.values())[0]
                    urn = snapshot["urn"]

                if urn:
                    all_urns.add(urn)

        except Exception as e:
            print(f"Warning: Failed to extract URNs from {file_path}: {e}")

    if all_urns:
        print(
            f"Creating temp file with {len(all_urns)} unique URNs for batch deletion..."
        )

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False
        ) as temp_file:
            for urn in all_urns:
                temp_file.write(f"{urn}\n")
            temp_file_path = temp_file.name

        try:
            result = run_datahub_cmd(
                command=[
                    "delete",
                    "by-filter",
                    "--force",
                    "--urn-file",
                    temp_file_path,
                ],
                env={
                    "DATAHUB_GMS_URL": auth_session.gms_url(),
                    "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
                },
            )
            if result.exit_code == 0:
                print("✅ Batch deletion completed successfully")
            else:
                print(f"⚠️ Batch deletion failed: {result.output}")

        finally:
            os.unlink(temp_file_path)

        wait_for_writes_to_sync()


def test_golden_files_discovery(golden_files):
    assert len(golden_files) >= 300, (
        f"Expected at least 300 golden files, found {len(golden_files)}"
    )

    for file_path in golden_files:
        assert os.path.isabs(file_path), f"Path should be absolute: {file_path}"
        assert os.path.exists(file_path), f"File should exist: {file_path}"

    print(f"Successfully discovered {len(golden_files)} golden files")


def test_ingest_golden_files(auth_session, golden_files):
    repo_root = Path(__file__).parents[5]
    failed_files = []

    # TODO: Fix these failing golden files
    known_failing_files = {
        "datahub/metadata-ingestion/tests/unit/sdk_v2/dataset_golden/test_structured_properties_golden.json",
        "datahub/metadata-ingestion/tests/integration/snowflake/snowflake_structured_properties_golden.json",
    }

    for golden_file_path in golden_files:
        relative_path = Path(golden_file_path).relative_to(repo_root)
        print(f"Testing ingestion of golden file: {relative_path}")

        # Skip known failing files temporarily
        if str(relative_path) in known_failing_files:
            print(f"Skipping known failing file: {relative_path} ⚠️")
            continue

        try:
            result = run_datahub_cmd(
                command=["ingest", "mcps", golden_file_path],
                env={
                    "DATAHUB_GMS_URL": auth_session.gms_url(),
                    "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
                },
            )
            print(
                f"Ingestion of golden file {relative_path}: {'✅' if result.exit_code == 0 else '❌'}"
            )

            if result.exit_code != 0:
                failed_files.append(f"{relative_path}: {result.output}")

        except Exception as e:
            failed_files.append(f"{relative_path}: {str(e)}")

    if failed_files:
        pytest.fail(
            f"Failed to ingest {len(failed_files)} golden files:\n"
            + "\n".join(failed_files)
        )
