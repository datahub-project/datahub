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
from tests.unit.tableau.test_tableau_source import config_source_default

FROZEN_TIME = "2021-12-07 07:00:00"


def test_value_error_projects_and_project_pattern(
    pytestconfig, tmp_path, mock_datahub_graph
):
    new_config = config_source_default.copy()
    new_config["projects"] = ["default"]
    new_config["project_pattern"] = {"allow": ["^Samples$"]}

    with pytest.raises(
        ValidationError,
        match=r".*projects is deprecated. Please use project_path_pattern only.*",
    ):
        TableauConfig.parse_obj(new_config)


def test_project_pattern_deprecation(pytestconfig, tmp_path, mock_datahub_graph):
    new_config = config_source_default.copy()
    del new_config["projects"]
    new_config["project_pattern"] = {"allow": ["^Samples$"]}
    new_config["project_path_pattern"] = {"allow": ["^Samples$"]}

    with pytest.raises(
        ValidationError,
        match=r".*project_pattern is deprecated. Please use project_path_pattern only*",
    ):
        TableauConfig.parse_obj(new_config)


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

    config_dict = config_source_default.copy()

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
