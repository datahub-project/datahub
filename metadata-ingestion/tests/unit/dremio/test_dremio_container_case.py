from typing import Dict, List
from unittest.mock import Mock

import pytest

from datahub.ingestion.source.dremio.dremio_api import (
    DremioAPIOperations,
    DremioEdition,
)
from datahub.ingestion.source.dremio.dremio_aspects import DremioAspects
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_entities import DremioCatalog
from datahub.ingestion.source.dremio.dremio_models import (
    DremioContainerPathIndex,
    DremioContainerResponse,
    DremioEntityContainerType,
)
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport
from datahub.ingestion.source.dremio.dremio_source import DremioSource
from datahub.metadata.schema_classes import (
    ContainerClass,
    ContainerPropertiesClass,
)

FOLDER = DremioEntityContainerType.FOLDER.lower()
CONTAINER = DremioEntityContainerType.CONTAINER


@pytest.fixture
def dremio_api(monkeypatch: pytest.MonkeyPatch) -> DremioAPIOperations:
    session = Mock()
    session.post.return_value.json.return_value = {"token": "dummy-token"}
    session.post.return_value.status_code = 200
    monkeypatch.setattr("requests.Session", Mock(return_value=session))
    config = DremioSourceConfig(
        hostname="dummy-host",
        port=9047,
        tls=False,
        authentication_method="password",
        username="dummy-user",
        password="dummy-password",
    )
    return DremioAPIOperations(config, DremioSourceReport())


def test_walk_builds_child_paths_from_the_parent_path(dremio_api):
    responses: Dict[str, dict] = {
        "space-id": {
            "entityType": "space",
            "children": [
                {"id": "no-path", "name": "orphan", "type": CONTAINER, "path": []},
                {"id": "gold", "type": CONTAINER, "path": ["my_space", "Gold"]},
            ],
        },
        "gold": {
            "entityType": FOLDER,
            "children": [
                {
                    "id": "conformed",
                    "type": CONTAINER,
                    "path": ["my_space", "Gold", "Conformed"],
                }
            ],
        },
        "conformed": {
            "entityType": FOLDER,
            "children": [
                {
                    "id": "dims",
                    "type": CONTAINER,
                    "path": ["MY_SPACE", "gold", "conformed", "dims"],
                }
            ],
        },
        "dims": {"entityType": FOLDER, "children": []},
    }
    dremio_api.get = Mock(side_effect=lambda url: responses[url.rsplit("/", 1)[-1]])

    folders = dremio_api.get_containers_for_location(
        resource_id="space-id",
        path=["my_space"],
        root_container_type=DremioEntityContainerType.SPACE,
    )

    assert len(dremio_api.report.warnings) == 1
    assert sorted(".".join([*(f.path or []), f.name]) for f in folders) == [
        "my_space.Gold",
        "my_space.Gold.Conformed",
        "my_space.Gold.Conformed.dims",
    ]


def _container(name: str, path: List[str], container_type: str) -> dict:
    return {"name": name, "path": path, "containerType": container_type}


def _dataset(table_schema: str, table_name: str) -> dict:
    return {
        "RESOURCE_ID": f"{table_name}-id",
        "TABLE_NAME": table_name,
        "TABLE_SCHEMA": table_schema,
        "LOCATION_ID": None,
        "COLUMNS": [
            {
                "name": "col_a",
                "ordinal_position": 1,
                "is_nullable": "YES",
                "data_type": "character varying",
                "column_size": 255,
            }
        ],
    }


def _mock_api() -> Mock:
    api = Mock()
    api.edition = DremioEdition.COMMUNITY
    api.report = DremioSourceReport()
    api.get_description_for_resource.return_value = None
    api.get_tags_for_resource.return_value = None
    api.get_all_containers.return_value = [
        DremioContainerResponse.model_validate(c)
        for c in (
            _container("my_space", [], DremioEntityContainerType.SPACE.value),
            _container("Gold", ["my_space"], DremioEntityContainerType.FOLDER),
            _container(
                "Conformed", ["my_space", "Gold"], DremioEntityContainerType.FOLDER
            ),
            _container("my_source", [], DremioEntityContainerType.SOURCE),
            _container("my_home", [], DremioEntityContainerType.HOME),
        )
    ]
    api.get_all_tables_and_columns.return_value = [
        _dataset("[MY_SPACE, gold, CONFORMED, my_view]", "my_view"),
        _dataset("[MY_SOURCE, my_source_table]", "my_source_table"),
        _dataset("[other_space, my_table]", "my_table"),
        _dataset("[other_space, my_table_2]", "my_table_2"),
        _dataset("[my_home, my_home_table]", "my_home_table"),
    ]
    return api


def _aspects() -> DremioAspects:
    return DremioAspects(
        platform="dremio",
        ui_url="https://dremio.example.com",
        env="PROD",
        ingest_owner=False,
    )


def test_dataset_points_at_emitted_parent_container():
    api = _mock_api()
    # get_datasets() runs first on purpose: it must not rely on get_containers().
    datasets = {d.resource_name: d for d in DremioCatalog(api).get_datasets()}
    aspects = _aspects()

    view_container = aspects._create_container_class(datasets["my_view"])
    assert view_container is not None
    assert view_container.container == aspects.get_container_urn(
        name="Conformed", path=["my_space", "Gold"]
    )
    assert datasets["my_source_table"].container_path == ["my_source"]

    assert datasets["my_table"].container_path == ["other_space"]
    # Not emitted as a container, so not resolvable either.
    assert datasets["my_home_table"].container_path == ["my_home"]
    warned = [context for w in api.report.warnings for context in w.context]
    assert sorted(warned) == ["my_home", "other_space"]


def test_failed_container_is_no_longer_resolved():
    api = _mock_api()
    catalog = DremioCatalog(api)
    conformed = next(
        c for c in catalog.get_containers() if c.container_name == "Conformed"
    )
    catalog.discard_container(conformed)

    datasets = {d.resource_name: d for d in catalog.get_datasets()}
    assert datasets["my_view"].container_path == ["MY_SPACE", "gold", "CONFORMED"]


def test_ambiguous_case_match_gets_no_container():
    api = _mock_api()
    api.get_all_containers.return_value = [
        DremioContainerResponse.model_validate(c)
        for c in (
            _container("src", [], DremioEntityContainerType.SOURCE),
            _container("Data", ["src"], DremioEntityContainerType.FOLDER),
            _container("data", ["src"], DremioEntityContainerType.FOLDER),
        )
    ]
    api.get_all_tables_and_columns.return_value = [
        _dataset("[SRC, DATA, t]", "t"),
        _dataset("[src, data, t2]", "t2"),
    ]
    datasets = {d.resource_name: d for d in DremioCatalog(api).get_datasets()}

    assert _aspects()._create_container_class(datasets["t"]) is None
    assert datasets["t2"].container_path == ["src", "data"]
    assert len(api.report.warnings) == 1


@pytest.mark.parametrize(
    "emitted, lookup, expected",
    [
        ([["src", "Data"]], ["SRC", "data"], ["src", "Data"]),
        ([["src", "Data"], ["src", "data"]], ["src", "data"], ["src", "data"]),
        ([["src", "Data"], ["src", "data"]], ["SRC", "DATA"], None),
        ([["src", "Data"]], ["src", "other"], None),
        ([["a", "b.c"]], ["a.b", "c"], None),
    ],
)
def test_container_path_index_prefers_exact_then_unique_case_insensitive(
    emitted, lookup, expected
):
    index = DremioContainerPathIndex()
    for path in emitted:
        index.add(path)
    assert index.resolve(lookup).path == expected


def test_source_emits_no_orphan_dataset_containers(monkeypatch):
    session = Mock()
    session.post.return_value.json.return_value = {"token": "dummy-token"}
    session.post.return_value.status_code = 200
    monkeypatch.setattr("requests.Session", Mock(return_value=session))
    source = DremioSource(
        DremioSourceConfig(
            hostname="dummy-host",
            port=9047,
            tls=False,
            username="dummy-user",
            password="dummy-password",
        ),
        Mock(run_id="test-run-id"),
    )
    api = _mock_api()
    api.report = source.report
    source.dremio_catalog = DremioCatalog(api)

    workunits = list(source.get_workunits_internal())
    assert api.get_all_containers.call_count == 1

    emitted = {
        wu.get_urn()
        for wu in workunits
        if wu.get_aspect_of_type(ContainerPropertiesClass) is not None
    }
    dataset_parents = {
        aspect.container
        for wu in workunits
        if wu.get_urn().startswith("urn:li:dataset:")
        and (aspect := wu.get_aspect_of_type(ContainerClass)) is not None
    }
    assert dataset_parents
    assert dataset_parents - emitted == {
        _aspects().get_container_urn(path=["other_space"]),
        _aspects().get_container_urn(path=["my_home"]),
    }
