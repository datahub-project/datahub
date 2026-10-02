"""Notebook path, fabricGitSource decoding, and dataset emission."""

import base64
from unittest.mock import MagicMock

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.fabric.common.models import FabricWorkspace
from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from datahub.ingestion.source.fabric.onelake.notebooks import (
    FabricNotebook,
    decode_notebook_definition,
    notebook_path,
)
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeSourceReport
from datahub.ingestion.source.fabric.onelake.source import FabricOneLakeSource
from datahub.metadata.schema_classes import SubTypesClass


def _definition(path: str, source: str) -> dict:
    payload = base64.b64encode(source.encode("utf-8")).decode("ascii")
    return {
        "definition": {
            "format": "fabricGitSource",
            "parts": [
                {"path": path, "payload": payload, "payloadType": "InlineBase64"},
                {
                    "path": ".platform",
                    "payload": base64.b64encode(b"{}").decode("ascii"),
                    "payloadType": "InlineBase64",
                },
            ],
        }
    }


def test_notebook_path_walks_folder_parents() -> None:
    folders = {
        "folder-shared": ("Shared", None),
        "folder-team": ("team", "folder-shared"),
    }
    assert notebook_path("analysis", "folder-team", folders) == "/Shared/team/analysis"


def test_notebook_path_root_notebook() -> None:
    assert notebook_path("analysis", None, {}) == "/analysis"


def test_decode_fabric_git_source_skips_platform_part() -> None:
    source = "# Fabric notebook source\nprint('hello')\n"
    content, language = decode_notebook_definition(
        _definition("notebook-content.py", source)
    )
    assert content == source
    assert language == "python"


def test_decode_sql_notebook_language() -> None:
    content, language = decode_notebook_definition(
        _definition("notebook-content.sql", "SELECT 1")
    )
    assert content == "SELECT 1"
    assert language == "sql"


def test_get_notebook_definition_polls_long_running_operation() -> None:
    client = OneLakeClient(auth_helper=MagicMock(), timeout=1)
    accepted = MagicMock()
    accepted.status_code = 202
    accepted.headers = {"x-ms-operation-id": "op-1", "Retry-After": "1"}
    running = MagicMock()
    running.json.return_value = {"status": "Running"}
    running.headers = {"Retry-After": "1"}
    succeeded = MagicMock()
    succeeded.json.return_value = {"status": "Succeeded"}
    succeeded.headers = {}
    result = MagicMock()
    result.json.return_value = _definition("notebook-content.py", "print(1)\n")

    client.post = MagicMock(return_value=accepted)  # type: ignore[method-assign]
    client.get = MagicMock(side_effect=[running, succeeded, result])  # type: ignore[method-assign]
    client_module_sleep = MagicMock()

    import datahub.ingestion.source.fabric.onelake.client as client_module

    original_sleep = client_module.time.sleep
    client_module.time.sleep = client_module_sleep
    try:
        definition = client.get_notebook_definition("ws", "nb")
    finally:
        client_module.time.sleep = original_sleep

    content, language = decode_notebook_definition(definition)
    assert content == "print(1)\n"
    assert language == "python"
    client_module_sleep.assert_called_once()
    client.post.assert_called_once_with(
        "workspaces/ws/notebooks/nb/getDefinition",
        params={"format": "fabricGitSource"},
    )


def _notebook_source(
    pattern: AllowDenyPattern,
) -> tuple[MagicMock, FabricWorkspace, FabricNotebook]:
    source = MagicMock()
    source.config.include_notebooks = True
    source.config.notebook_pattern = pattern
    source.config.platform_instance = None
    source.config.env = "PROD"
    source.report = FabricOneLakeSourceReport()
    source._create_notebook_dataset = (
        FabricOneLakeSource._create_notebook_dataset.__get__(
            source, FabricOneLakeSource
        )
    )
    workspace = FabricWorkspace(id="ws-1", name="analytics")
    notebook = FabricNotebook(
        id="nb-1",
        display_name="analysis",
        workspace_id="ws-1",
        description="daily analysis",
        folder_id="folder-shared",
    )
    source.client.list_folders.return_value = {"folder-shared": ("Shared", None)}
    source.client.list_notebooks.return_value = iter([notebook])
    source.client.get_notebook_definition.return_value = _definition(
        "notebook-content.py", "print('hello')\n"
    )
    return source, workspace, notebook


def test_process_notebooks_emits_dataset_with_contents() -> None:
    source, workspace, _notebook = _notebook_source(AllowDenyPattern.allow_all())
    datasets = list(FabricOneLakeSource._process_notebooks(source, workspace))

    assert len(datasets) == 1
    dataset = datasets[0]
    assert dataset.display_name == "analysis"
    assert dataset.external_url == (
        "https://app.fabric.microsoft.com/groups/ws-1/synapsenotebooks/nb-1"
    )
    assert dataset.custom_properties == {
        "path": "/Shared/analysis",
        "language": "python",
        "content": "print('hello')\n",
    }
    subtypes = dataset._get_aspect(SubTypesClass)
    assert subtypes is not None
    assert subtypes.typeNames == ["Notebook"]
    assert source.report.notebooks_scanned == 1
    assert "ws-1.nb-1" in str(dataset.urn)


def test_notebook_pattern_filters_before_definition_fetch() -> None:
    source, workspace, _notebook = _notebook_source(
        AllowDenyPattern(allow=["^/Other/.*"])
    )
    datasets = list(FabricOneLakeSource._process_notebooks(source, workspace))

    assert datasets == []
    assert list(source.report.filtered_notebooks) == ["/Shared/analysis"]
    source.client.get_notebook_definition.assert_not_called()


def test_definition_failure_still_emits_notebook() -> None:
    source, workspace, _notebook = _notebook_source(AllowDenyPattern.allow_all())
    source.client.get_notebook_definition.side_effect = RuntimeError("denied")
    datasets = list(FabricOneLakeSource._process_notebooks(source, workspace))

    assert len(datasets) == 1
    assert datasets[0].custom_properties == {"path": "/Shared/analysis"}
    assert source.report.notebooks_scanned == 1
