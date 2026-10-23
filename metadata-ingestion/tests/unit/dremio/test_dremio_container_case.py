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
    DremioContainerResponse,
    DremioEntityContainerType,
)
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport

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
                {"id": "gold", "type": CONTAINER, "path": ["my_space", "Gold"]}
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
        "COLUMNS": [],
    }


@pytest.fixture
def catalog() -> DremioCatalog:
    api = Mock()
    api.edition = DremioEdition.COMMUNITY
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
        )
    ]
    api.get_all_tables_and_columns.return_value = [
        _dataset("[MY_SPACE, gold, CONFORMED, my_view]", "my_view"),
        _dataset("[other_space, my_table]", "my_table"),
    ]
    return DremioCatalog(api)


def test_dataset_points_at_emitted_parent_container(catalog):
    aspects = DremioAspects(
        platform="dremio",
        ui_url="https://dremio.example.com",
        env="PROD",
        ingest_owner=False,
    )
    list(catalog.get_containers())
    datasets = {d.resource_name: d for d in catalog.get_datasets()}

    view = datasets["my_view"]
    assert view.path == ["MY_SPACE", "gold", "CONFORMED"]
    container_class = aspects._create_container_class(view)
    assert container_class is not None
    assert container_class.container == aspects.get_container_urn(
        name="Conformed", path=["my_space", "Gold"]
    )
    assert datasets["my_table"].container_path == ["other_space"]
