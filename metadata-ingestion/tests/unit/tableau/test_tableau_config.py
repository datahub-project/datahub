from typing import Dict
from unittest import mock

import pytest
from freezegun import freeze_time
from pydantic import ValidationError
from tableauserverclient import Server
from tableauserverclient.models import SiteItem

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.tableau.tableau import (
    TableauConfig,
    TableauProject,
    TableauSiteSource,
    TableauSourceReport,
)

FROZEN_TIME = "2021-12-07 07:00:00"

GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"

default_config = {
    "username": "username",
    "password": "pass`",
    "connect_uri": "https://do-not-connect",
    "site": "acryl",
    "projects": ["default", "Project 2", "Samples"],
    "extract_project_hierarchy": False,
    "page_size": 1000,
    "workbook_page_size": None,
    "ingest_tags": True,
    "ingest_owner": True,
    "ingest_tables_external": True,
    "default_schema_map": {
        "dvdrental": "public",
        "someotherdb": "schema",
    },
    "platform_instance_map": {"postgres": "demo_postgres_instance"},
    "extract_usage_stats": True,
    "stateful_ingestion": {
        "enabled": True,
        "remove_stale_metadata": True,
        "fail_safe_threshold": 100.0,
        "state_provider": {
            "type": "datahub",
            "config": {"datahub_api": {"server": GMS_SERVER}},
        },
    },
}


def test_value_error_projects_and_project_pattern(
    pytestconfig, tmp_path, mock_datahub_graph
):
    new_config = default_config.copy()
    new_config["projects"] = ["default"]
    new_config["project_pattern"] = {"allow": ["^Samples$"]}

    with pytest.raises(
        ValidationError,
        match=r".*projects is deprecated. Please use project_path_pattern only.*",
    ):
        TableauConfig.parse_obj(new_config)


def test_project_pattern_deprecation(pytestconfig, tmp_path, mock_datahub_graph):
    new_config = default_config.copy()
    del new_config["projects"]
    new_config["project_pattern"] = {"allow": ["^Samples$"]}
    new_config["project_path_pattern"] = {"allow": ["^Samples$"]}

    with pytest.raises(
        ValidationError,
        match=r".*project_pattern is deprecated. Please use project_path_pattern only*",
    ):
        TableauConfig.parse_obj(new_config)


def test_ingest_hidden_assets_bool():
    config_dict = default_config.copy()
    config_dict["ingest_hidden_assets"] = False
    config = TableauConfig.parse_obj(config_dict)
    assert config.ingest_hidden_assets is False


def test_ingest_hidden_assets_list():
    config_dict = default_config.copy()
    config_dict["ingest_hidden_assets"] = ["dashboard"]
    config = TableauConfig.parse_obj(config_dict)
    assert config.ingest_hidden_assets == ["dashboard"]


def test_ingest_hidden_assets_multiple():
    config_dict = default_config.copy()
    config_dict["ingest_hidden_assets"] = ["dashboard", "worksheet"]
    config = TableauConfig.parse_obj(config_dict)
    assert config.ingest_hidden_assets == ["dashboard", "worksheet"]


def test_ingest_hidden_assets_invalid():
    config = default_config.copy()
    config["ingest_hidden_assets"] = ["worksheet", "invalid"]
    with pytest.raises(
        ValidationError,
        match=r".*unexpected value.*given=invalid.*",
    ):
        TableauConfig.parse_obj(config)


@freeze_time(FROZEN_TIME)
@pytest.mark.parametrize(
    "extract_project_hierarchy, allowed_projects",
    [
        (True, ["project1", "project4", "project3"]),
        (False, ["project1", "project4"]),
    ],
)
def test_extract_project_hierarchy(extract_project_hierarchy, allowed_projects):
    context = PipelineContext(run_id="0", pipeline_name="test_tableau")

    config_dict = default_config.copy()

    del config_dict["stateful_ingestion"]
    del config_dict["projects"]

    config_dict["project_pattern"] = {
        "allow": ["project1", "project4"],
        "deny": ["project2"],
    }

    config_dict["extract_project_hierarchy"] = extract_project_hierarchy

    config = TableauConfig.parse_obj(config_dict)

    site_source = TableauSiteSource(
        config=config,
        ctx=context,
        platform="tableau",
        site=mock.MagicMock(spec=SiteItem, id="Site1", content_url="site1"),
        report=TableauSourceReport(),
        server=Server("https://test-tableau-server.com"),
    )

    all_project_map: Dict[str, TableauProject] = {
        "p1": TableauProject(
            id="1",
            name="project1",
            path=[],
            parent_id=None,
            parent_name=None,
            description=None,
        ),
        "p2": TableauProject(
            id="2",
            name="project2",
            path=[],
            parent_id="1",
            parent_name="project1",
            description=None,
        ),
        "p3": TableauProject(
            id="3",
            name="project3",
            path=[],
            parent_id="1",
            parent_name="project1",
            description=None,
        ),
        "p4": TableauProject(
            id="4",
            name="project4",
            path=[],
            parent_id=None,
            parent_name=None,
            description=None,
        ),
    }

    site_source._init_tableau_project_registry(all_project_map)

    assert allowed_projects == [
        project.name for project in site_source.tableau_project_registry.values()
    ]
