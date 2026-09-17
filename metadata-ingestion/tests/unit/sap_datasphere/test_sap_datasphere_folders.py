import re
import urllib.parse
from typing import Dict, List

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sap_datasphere.client import SapDatasphereClient
from datahub.ingestion.source.sap_datasphere.config import (
    FolderContainerKey,
    SapDatasphereConfig,
)
from datahub.ingestion.source.sap_datasphere.folders import parse_folder_assignments
from datahub.ingestion.source.sap_datasphere.models import JsonDict
from datahub.ingestion.source.sap_datasphere.report import SapDatasphereReport
from datahub.ingestion.source.sap_datasphere.source import SapDatasphereSource
from datahub.metadata.schema_classes import (
    ContainerClass,
    ContainerPropertiesClass,
    SubTypesClass,
)
from tests.unit.sap_datasphere.sap_datasphere_test_helpers import aspect_as, aspect_of

BASE_URL = "https://myco.eu10.hcs.cloud.sap"
SEARCH_URL = f"{BASE_URL}/deepsea/repository/DEMO_SPACE/search/$all"
HIERARCHY_KEY = "@com.sap.vocabularies.Search.v1.ParentHierarchies"


def _make_config(**kwargs: object) -> SapDatasphereConfig:
    base: Dict[str, object] = {"base_url": BASE_URL, "token": "tok"}
    base.update(kwargs)
    return SapDatasphereConfig.model_validate(base)


def _hierarchy(*folders: JsonDict) -> List[JsonDict]:
    return [{"scope": "folder", "hierarchy": list(folders)}]


def _folder(folder_id: str, name: str) -> JsonDict:
    return {"folder_id": folder_id, "folder_name": name, "folder_icon": None}


def test_object_inherits_its_nested_folder_path():
    folders = parse_folder_assignments(
        [
            {
                "name": "SALES_VIEW",
                "kind": "entity",
                "folder_id": "Folder_CHILD",
                "folder_name": "Reporting",
                HIERARCHY_KEY: _hierarchy(
                    _folder("Folder_ROOT", "Sales"),
                    _folder("Folder_CHILD", "Reporting"),
                ),
            }
        ]
    )
    assert folders.path_for("SALES_VIEW") == ("Sales", "Reporting")
    # Every ancestor prefix is registered so the parent container gets emitted.
    assert set(folders.paths) == {("Sales",), ("Sales", "Reporting")}


def test_leaf_first_hierarchy_is_reoriented_against_the_immediate_folder():
    """SAP does not document the chain's direction; a chain starting at the object's own folder_id is leaf-first."""
    folders = parse_folder_assignments(
        [
            {
                "name": "SALES_VIEW",
                "kind": "entity",
                "folder_id": "Folder_CHILD",
                "folder_name": "Reporting",
                HIERARCHY_KEY: _hierarchy(
                    _folder("Folder_CHILD", "Reporting"),
                    _folder("Folder_ROOT", "Sales"),
                ),
            }
        ]
    )
    assert folders.path_for("SALES_VIEW") == ("Sales", "Reporting")


def test_falls_back_to_the_immediate_folder_without_a_hierarchy():
    folders = parse_folder_assignments(
        [{"name": "V1", "kind": "entity", "folder_id": "F1", "folder_name": "Staging"}]
    )
    assert folders.path_for("V1") == ("Staging",)


def test_object_at_space_root_has_no_folder():
    folders = parse_folder_assignments(
        [{"name": "V1", "kind": "entity", "folder_id": None, "folder_name": None}]
    )
    assert folders.path_for("V1") is None
    assert folders.paths == []


def test_empty_folder_is_still_registered_under_its_display_name():
    """A folder record contributes its own path, so a folder holding no objects still renders."""
    folders = parse_folder_assignments(
        [
            {
                "name": "Folder_YBEQBWWQ",
                "business_name": "Archive",
                "kind": "sap.repo.folder",
                "folder_id": "Folder_ROOT",
                "folder_name": "Sales",
                HIERARCHY_KEY: _hierarchy(_folder("Folder_ROOT", "Sales")),
            }
        ]
    )
    assert set(folders.paths) == {("Sales",), ("Sales", "Archive")}
    # A folder is a container, never an object that can be parented.
    assert folders.path_for("Folder_YBEQBWWQ") is None


def test_malformed_records_are_skipped():
    folders = parse_folder_assignments(
        [
            "not-a-dict",  # type: ignore[list-item]
            {"kind": "entity", "folder_name": "Sales"},  # no technical name
            {"name": "  ", "kind": "entity", "folder_name": "Sales"},
            {"name": "V1", "kind": "entity", HIERARCHY_KEY: "not-a-list"},
        ]
    )
    assert folders.path_by_object == {}


def test_client_requests_percent_encoded_query_and_paginates(requests_mock):
    page_one = [
        {"name": f"V{i}", "kind": "entity", "folder_name": "Sales"} for i in range(200)
    ]
    requests_mock.get(
        SEARCH_URL,
        [
            {"json": {"value": page_one}},
            {"json": {"value": [{"name": "V200", "kind": "entity"}]}},
        ],
    )
    client = SapDatasphereClient(_make_config(), report=SapDatasphereReport())

    records = client.list_folder_assignments("DEMO_SPACE")

    assert records is not None
    assert len(records) == 201
    first, second = requests_mock.request_history
    # "+" would be rejected by the search endpoint, so spaces must stay %20.
    assert "+" not in first.query
    assert "$skip=0" in first.url and "$skip=200" in second.url
    assert (
        urllib.parse.unquote(first.url).split("$apply=")[1]
        == "filter(Search.search(query='SCOPE:SEARCH_DESIGN *'))"
    )


def test_client_stops_at_the_reported_count_when_the_server_ignores_skip(
    requests_mock,
):
    """A server that ignores $skip would page forever, so @odata.count bounds the walk."""
    full_page = [
        {"name": f"V{i}", "kind": "entity", "folder_name": "Sales"} for i in range(200)
    ]
    requests_mock.get(SEARCH_URL, json={"@odata.count": 200, "value": full_page})
    client = SapDatasphereClient(_make_config(), report=SapDatasphereReport())

    records = client.list_folder_assignments("DEMO_SPACE")

    assert records is not None and len(records) == 200
    assert requests_mock.call_count == 1


def test_client_degrades_when_the_repository_api_is_forbidden(requests_mock):
    requests_mock.get(SEARCH_URL, status_code=403, text="forbidden")
    report = SapDatasphereReport()
    client = SapDatasphereClient(_make_config(), report=report)

    assert client.list_folder_assignments("DEMO_SPACE") is None
    assert len(report.folder_lookup_failed) == 1


def test_client_degrades_on_a_non_json_body(requests_mock):
    """SAP's approuter answers UI routes with an HTTP 200 SSO login page."""
    requests_mock.get(SEARCH_URL, text="<html>login</html>")
    report = SapDatasphereReport()
    client = SapDatasphereClient(_make_config(), report=report)

    assert client.list_folder_assignments("DEMO_SPACE") is None
    assert report.folder_api_unavailable is not None


def test_client_stops_asking_every_space_once_the_api_answers_html(requests_mock):
    """A login page is tenant-wide, so it must cost one warning and one call, not one per space."""
    requests_mock.get(re.compile(r"/deepsea/repository/"), text="<html>login</html>")
    report = SapDatasphereReport()
    client = SapDatasphereClient(_make_config(), report=report)

    for space in ("SPACE_A", "SPACE_B", "SPACE_C"):
        assert client.list_folder_assignments(space) is None

    assert requests_mock.call_count == 1
    assert len(report.warnings) == 1
    assert report.folder_lookup_failed == []


def _folder_source(**config: object) -> SapDatasphereSource:
    return SapDatasphereSource(
        PipelineContext(run_id="test-folders"), _make_config(**config)
    )


def test_assets_parent_to_the_space_when_it_has_no_folders(requests_mock):
    requests_mock.get(SEARCH_URL, json={"value": []})
    source = _folder_source()

    assert list(source._emit_folders("DEMO_SPACE")) == []
    parent = source._parent_container("DEMO_SPACE", "SALES_VIEW")

    assert not isinstance(parent, FolderContainerKey)


def test_folder_containers_nest_and_capture_their_objects(requests_mock):
    requests_mock.get(
        SEARCH_URL,
        json={
            "value": [
                {
                    "name": "SALES_VIEW",
                    "kind": "entity",
                    "folder_id": "Folder_CHILD",
                    "folder_name": "Reporting",
                    HIERARCHY_KEY: _hierarchy(
                        _folder("Folder_ROOT", "Sales"),
                        _folder("Folder_CHILD", "Reporting"),
                    ),
                }
            ]
        },
    )
    source = _folder_source()

    workunits = list(source._emit_folders("DEMO_SPACE"))

    names = [
        aspect_as(wu, ContainerPropertiesClass).name
        for wu in workunits
        if isinstance(aspect_of(wu), ContainerPropertiesClass)
    ]
    assert names == ["Sales", "Reporting"]
    subtypes = {
        subtype
        for wu in workunits
        if isinstance(aspect_of(wu), SubTypesClass)
        for subtype in aspect_as(wu, SubTypesClass).typeNames
    }
    assert subtypes == {"Folder"}

    # The nested folder points at its parent folder, not straight at the space.
    space_key = source._space_key("DEMO_SPACE")
    parents = {
        aspect_as(wu, ContainerClass).container
        for wu in workunits
        if isinstance(aspect_of(wu), ContainerClass)
    }
    assert space_key.as_urn() in parents
    assert source._folder_key("DEMO_SPACE", ("Sales",)).as_urn() in parents

    parent = source._parent_container("DEMO_SPACE", "SALES_VIEW")
    assert parent == source._folder_key("DEMO_SPACE", ("Sales", "Reporting"))
    assert source.report.folders_emitted == 2
    assert source.report.objects_assigned_to_folder == 1


def test_unfoldered_object_still_parents_to_the_space(requests_mock):
    requests_mock.get(
        SEARCH_URL,
        json={"value": [{"name": "OTHER_VIEW", "kind": "entity", "folder_name": "F"}]},
    )
    source = _folder_source()
    list(source._emit_folders("DEMO_SPACE"))

    assert source._parent_container("DEMO_SPACE", "ROOT_VIEW") == source._space_key(
        "DEMO_SPACE"
    )


def test_same_named_folders_under_different_parents_stay_distinct():
    source = _folder_source()
    assert source._folder_key("S", ("Sales", "Archive")) != source._folder_key(
        "S", ("Finance", "Archive")
    )
