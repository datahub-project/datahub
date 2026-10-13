"""Notebook path, fabricGitSource decoding, and dataset emission."""

import base64
from unittest.mock import MagicMock

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.fabric.common.models import FabricWorkspace
from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from datahub.ingestion.source.fabric.onelake.notebooks import (
    FabricNotebook,
    decode_notebook_definition,
    notebook_path,
    notebook_view_logic,
)
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeSourceReport
from datahub.ingestion.source.fabric.onelake.source import FabricOneLakeSource
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DataJobInputOutputClass,
    SubTypesClass,
    ViewPropertiesClass,
)
from datahub.sdk.dataset import Dataset


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


_FABRIC_SOURCE = """# Fabric notebook source

# METADATA ********************

# META {
# META   "language_info": { "name": "python" }
# META }

# CELL ********************

# load the source  # noqa: E501
df = spark.read.table("bronze.events")

# MARKDOWN ********************

# MAGIC %md
# MAGIC ## Write results

# CELL ********************

df.write.saveAsTable("silver.events")
"""


def test_notebook_view_logic_strips_meta_and_splits_cells() -> None:
    logic = notebook_view_logic(_FABRIC_SOURCE, "python")
    assert logic is not None
    # No Fabric scaffolding survives.
    assert "META" not in logic
    assert "********" not in logic
    # Code cells are delimited and the user's in-code comment is kept.
    assert "# \u2500\u2500 Cell 1 \u2500\u2500" in logic
    assert (
        '# load the source  # noqa: E501\ndf = spark.read.table("bronze.events")'
        in logic
    )
    assert "# \u2500\u2500 Cell 2 \u2500\u2500" in logic
    assert 'df.write.saveAsTable("silver.events")' in logic
    # Markdown is preserved as a labelled comment block.
    assert "# \u2500\u2500 Markdown \u2500\u2500" in logic
    assert "# ## Write results" in logic


def test_notebook_view_logic_sql_uses_sql_comment_prefix() -> None:
    source = """# CELL ********************

SELECT 1

# CELL ********************

SELECT 2
"""
    logic = notebook_view_logic(source, "sql")
    assert logic is not None
    assert "-- \u2500\u2500 Cell 1 \u2500\u2500" in logic
    assert "-- \u2500\u2500 Cell 2 \u2500\u2500" in logic


def test_notebook_view_logic_empty_is_none() -> None:
    assert notebook_view_logic(None, "python") is None
    assert notebook_view_logic("   ", "python") is None


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
    assert isinstance(dataset, Dataset)
    assert dataset.display_name == "analysis"
    assert dataset.external_url == (
        "https://app.fabric.microsoft.com/groups/ws-1/synapsenotebooks/nb-1"
    )
    # The decoded source is carried as the view definition, not a custom property.
    assert dataset.custom_properties == {
        "path": "/Shared/analysis",
        "language": "python",
    }
    subtypes = dataset._get_aspect(SubTypesClass)
    assert subtypes is not None
    assert subtypes.typeNames == ["View", "Notebook"]
    view_properties = dataset._get_aspect(ViewPropertiesClass)
    assert view_properties is not None
    # A bare single-cell export has no cell structure to annotate.
    assert view_properties.viewLogic == "print('hello')"
    assert view_properties.viewLanguage == "PYTHON"
    assert view_properties.materialized is False
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
    dataset = datasets[0]
    assert isinstance(dataset, Dataset)
    assert dataset.custom_properties == {"path": "/Shared/analysis"}
    assert source.report.notebooks_scanned == 1


def test_incremental_lineage_converts_datajob_io_to_patch() -> None:
    source = MagicMock()
    source.report = FabricOneLakeSourceReport()
    convert = FabricOneLakeSource._convert_notebook_lineage_to_patch.__get__(
        source, FabricOneLakeSource
    )
    job_urn = "urn:li:dataJob:(urn:li:dataFlow:(fabric-onelake,ws.nb,PROD),nb)"
    ds = "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,ws.lh.s.t,PROD)"
    wu = MetadataWorkUnit(
        id="io",
        mcp=MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInputOutputClass(inputDatasets=[ds], outputDatasets=[]),
        ),
    )
    out = list(convert([wu]))

    assert len(out) == 1
    patched = out[0].metadata
    assert patched.entityUrn == job_urn
    assert patched.changeType == ChangeTypeClass.PATCH


def test_incremental_lineage_drops_empty_io_aspect() -> None:
    # An empty upsert would wipe manually added edges, so it must be dropped
    # rather than passed through (which an upsert would do).
    source = MagicMock()
    source.report = FabricOneLakeSourceReport()
    convert = FabricOneLakeSource._convert_notebook_lineage_to_patch.__get__(
        source, FabricOneLakeSource
    )
    wu = MetadataWorkUnit(
        id="empty",
        mcp=MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataJob:(urn:li:dataFlow:(fabric-onelake,ws.nb,PROD),nb)",
            aspect=DataJobInputOutputClass(inputDatasets=[], outputDatasets=[]),
        ),
    )
    assert list(convert([wu])) == []
