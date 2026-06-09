from typing import Any, Dict, List, Optional
from unittest import mock

from botocore.exceptions import ClientError

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.quicksight.processors.containers import (
    ContainersProcessor,
)
from datahub.ingestion.source.quicksight.processors.enrichment import AssetEnricher
from datahub.ingestion.source.quicksight.quicksight_config import (
    QuickSightSourceConfig,
)
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)
from datahub.metadata.schema_classes import SubTypesClass
from datahub.sdk.container import Container


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "Op")


def _enricher(api: mock.MagicMock, report: QuickSightSourceReport) -> AssetEnricher:
    # Ownership/tags are exercised in test_quicksight_enrichment; disable here so
    # these tests stay focused on container behavior.
    config = QuickSightSourceConfig.model_validate(
        {"aws_region": "us-east-1", "extract_ownership": False, "extract_tags": False}
    )
    return AssetEnricher(config, report, api)


def _processor(
    api: mock.MagicMock, config_dict: Optional[Dict[str, Any]] = None
) -> ContainersProcessor:
    config = QuickSightSourceConfig.model_validate(
        {"aws_region": "us-east-1", **(config_dict or {})}
    )
    report = QuickSightSourceReport()
    return ContainersProcessor(config, report, api, _enricher(api, report))


def _mock_api() -> mock.MagicMock:
    api = mock.MagicMock()
    api.aws_account_id = "064369473231"
    api.list_namespaces.return_value = []
    api.list_folders.return_value = []
    api.list_folder_members.return_value = []
    return api


def _subtypes(workunits: List[MetadataWorkUnit]) -> List[str]:
    names = []
    for wu in workunits:
        aspect = getattr(wu.metadata, "aspect", None)
        if isinstance(aspect, SubTypesClass):
            names.extend(aspect.typeNames)
    return names


def test_no_account_container_emitted():
    # The AWS account is represented via platform_instance, not a container
    # (matching the Glue / Redshift / PowerBI / Informatica convention).
    api = _mock_api()
    api.list_namespaces.return_value = [{"Name": "default"}]

    processor = _processor(api)
    subtypes = _subtypes(list(processor.get_workunits()))

    assert "Account" not in subtypes


def test_namespace_container_off_by_default():
    api = _mock_api()
    api.list_namespaces.return_value = [{"Name": "default"}]

    processor = _processor(api)
    workunits = list(processor.get_workunits())

    # Without add_namespace_container, no namespace container is emitted and
    # account-level assets attach directly to the platform root.
    assert "Namespace" not in _subtypes(workunits)
    assert processor.default_parent is None


def test_namespace_container_emitted_when_opted_in():
    api = _mock_api()
    api.list_namespaces.return_value = [{"Name": "default"}, {"Name": "marketing"}]

    processor = _processor(api, {"add_namespace_container": True})
    workunits = list(processor.get_workunits())

    assert _subtypes(workunits).count("Namespace") == 2
    # Account-level assets resolve to the 'default' namespace container.
    default_parent = processor.default_parent
    assert default_parent is not None
    assert default_parent.urn.urn() == processor._namespace_key("default").as_urn()
    assert processor.namespace_name == "default"


def test_namespace_resolves_to_default_when_present():
    api = _mock_api()
    api.list_namespaces.return_value = [{"Name": "dev"}, {"Name": "default"}]

    processor = _processor(api)
    list(processor.get_workunits())

    assert processor.namespace_name == "default"


def test_namespaces_fall_back_to_default_on_unsupported_edition():
    api = _mock_api()
    api.list_namespaces.side_effect = _client_error("UnsupportedUserEditionException")

    processor = _processor(api, {"add_namespace_container": True})
    # Should not raise — Standard-edition accounts degrade to a single namespace.
    list(processor.get_workunits())

    assert processor.namespace_name == "default"


def test_non_graceful_namespace_error_propagates():
    api = _mock_api()
    api.list_namespaces.side_effect = _client_error("ThrottlingException")

    processor = _processor(api, {"add_namespace_container": True})
    try:
        list(processor.get_workunits())
        raised = False
    except ClientError:
        raised = True
    assert raised


def test_folders_skipped_and_flagged_on_unsupported_edition():
    api = _mock_api()
    api.list_folders.side_effect = _client_error("UnsupportedUserEditionException")

    processor = _processor(api)
    list(processor.get_workunits())

    assert processor.report.folders_unsupported is True


def test_parent_folder_resolved_from_folder_path():
    api = _mock_api()
    api.describe_folder.return_value = {
        "FolderPath": [
            "arn:aws:quicksight:us-east-1:064369473231:folder/f-root",
            "arn:aws:quicksight:us-east-1:064369473231:folder/f-parent",
        ]
    }
    processor = _processor(api)

    # The immediate parent is the last ARN in the root-to-leaf FolderPath.
    assert processor._parent_folder_id("f-child") == "f-parent"


def test_top_level_folder_has_no_parent_folder():
    api = _mock_api()
    api.describe_folder.return_value = {"FolderPath": []}
    processor = _processor(api)

    assert processor._parent_folder_id("f-root") is None


def test_folder_membership_maps_assets_to_folder():
    api = _mock_api()
    api.list_folders.return_value = [{"FolderId": "f-sales", "Name": "Sales"}]
    api.describe_folder.return_value = {"FolderPath": []}
    api.list_folder_members.return_value = [
        {"MemberId": "ds-1"},
        {"MemberId": "dash-1"},
    ]
    processor = _processor(api)
    list(processor.get_workunits())

    parent = processor.parent_for("ds-1")
    assert isinstance(parent, Container)
    assert parent.urn.urn() == processor.folder_key("f-sales").as_urn()
    assert processor.parent_for("dash-1") is parent


def test_first_folder_wins_for_multi_folder_assets():
    api = _mock_api()
    # Folders are processed in sorted FolderId order, so f-a claims the asset.
    api.list_folders.return_value = [
        {"FolderId": "f-b", "Name": "B"},
        {"FolderId": "f-a", "Name": "A"},
    ]
    api.describe_folder.return_value = {"FolderPath": []}
    api.list_folder_members.side_effect = lambda folder_id: (
        [{"MemberId": "ds-shared"}] if folder_id in {"f-a", "f-b"} else []
    )
    processor = _processor(api)
    list(processor.get_workunits())

    parent = processor.parent_for("ds-shared")
    assert isinstance(parent, Container)
    assert parent.urn.urn() == processor.folder_key("f-a").as_urn()


def test_unfoldered_asset_resolves_to_default_parent():
    api = _mock_api()
    processor = _processor(api)
    list(processor.get_workunits())

    # No folders + namespace container off -> platform root (None).
    assert processor.parent_for("ds-orphan") is None


def test_shared_folders_root_off_by_default():
    api = _mock_api()
    api.list_folders.return_value = [{"FolderId": "f-sales", "Name": "Sales"}]
    api.describe_folder.return_value = {"FolderPath": []}

    processor = _processor(api)
    workunits = list(processor.get_workunits())

    # No synthetic root; top-level folder parents to the platform root.
    assert "Shared Folders" not in _subtypes(workunits)
    assert processor.folder_root_parent is None
    assert processor._folder_containers["f-sales"].parent_container is None


def test_shared_folders_root_emitted_when_opted_in():
    api = _mock_api()
    api.list_folders.return_value = [{"FolderId": "f-sales", "Name": "Sales"}]
    api.describe_folder.return_value = {"FolderPath": []}

    processor = _processor(api, {"add_shared_folders_container": True})
    workunits = list(processor.get_workunits())

    assert _subtypes(workunits).count("Shared Folders") == 1
    root = processor.folder_root_parent
    assert isinstance(root, Container)
    assert root.urn.urn() == processor._shared_folders_key().as_urn()
    # Top-level folder nests under the synthetic Shared folders root.
    assert processor._folder_containers["f-sales"].parent_container == root.urn


def test_shared_folders_root_skipped_when_no_folders():
    api = _mock_api()  # list_folders returns []

    processor = _processor(api, {"add_shared_folders_container": True})
    workunits = list(processor.get_workunits())

    # No folders -> don't emit an empty grouping.
    assert "Shared Folders" not in _subtypes(workunits)
    assert processor._shared_folders_container is None


def test_nested_folder_unaffected_by_shared_folders_root():
    api = _mock_api()
    api.list_folders.return_value = [
        {"FolderId": "f-parent", "Name": "Parent"},
        {"FolderId": "f-child", "Name": "Child"},
    ]
    api.describe_folder.side_effect = lambda folder_id: {
        "FolderPath": (
            ["arn:aws:quicksight:us-east-1:064369473231:folder/f-parent"]
            if folder_id == "f-child"
            else []
        )
    }

    processor = _processor(api, {"add_shared_folders_container": True})
    list(processor.get_workunits())

    # Child still parents to its real folder, not the synthetic root.
    child_parent = processor._folder_containers["f-child"].parent_container
    assert child_parent == processor._folder_containers["f-parent"].urn


def test_folder_key_roots_at_platform_no_ghost_namespace():
    # Regression: QuickSightFolderKey must NOT inherit the namespace key, or the
    # SDK auto-derives foldered assets' browse paths through a namespace container
    # we don't emit (add_namespace_container off), leaving a dangling "ghost"
    # container URN above every asset. The folder key must root at the platform.
    api = _mock_api()
    processor = _processor(api)
    folder_key = processor.folder_key("f-sales")

    assert folder_key.parent_key() is None
