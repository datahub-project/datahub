"""Unit tests for LUID custom property edge cases in Tableau ingestion.

Validates Requirements: 1.3, 2.3, 5.1, 5.2, 5.3
"""

from copy import deepcopy
from unittest import mock

import time_machine
from tableauserverclient import SiteItem

import datahub.ingestion.source.tableau.tableau_constant as c
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.tableau.tableau import (
    TableauConfig,
    TableauProject,
    TableauSiteSource,
    TableauSourceReport,
)
from datahub.metadata.schema_classes import (
    ContainerPropertiesClass,
    DatasetPropertiesClass,
)
from tests.unit.tableau.test_tableau_config import default_config

FROZEN_TIME = "2021-12-07T07:00:00Z"


def _make_site_source() -> TableauSiteSource:
    config_dict = deepcopy(default_config)
    del config_dict["stateful_ingestion"]
    config = TableauConfig.model_validate(config_dict)

    ctx = PipelineContext(run_id="test")
    site = SiteItem(name="test-site", content_url="test")
    site._id = "test-site-id"

    source = TableauSiteSource(
        config=config,
        ctx=ctx,
        site=site,
        report=TableauSourceReport(),
        server=mock.MagicMock(),
        platform="tableau",
    )

    source.tableau_project_registry = {
        "project-luid-1": TableauProject(
            id="project-luid-1",
            name="default",
            description="",
            parent_id=None,
            parent_name=None,
            path=["default"],
        )
    }
    source.datasource_project_map = {"ds-luid-1": "project-luid-1"}
    source.workbook_project_map = {}

    return source


def _extract_dataset_custom_props(work_units: list) -> dict:
    """Extract customProperties from DatasetPropertiesClass in work units."""
    for wu in work_units:
        if isinstance(wu.metadata, MetadataChangeProposalWrapper):
            if isinstance(wu.metadata.aspect, DatasetPropertiesClass):
                return wu.metadata.aspect.customProperties or {}
        elif hasattr(wu.metadata, "proposedSnapshot"):
            for aspect in wu.metadata.proposedSnapshot.aspects:
                if isinstance(aspect, DatasetPropertiesClass):
                    return aspect.customProperties or {}
    return {}


def _extract_container_custom_props(work_units: list) -> dict:
    """Extract customProperties from ContainerPropertiesClass in work units."""
    for wu in work_units:
        if isinstance(wu.metadata, MetadataChangeProposalWrapper):
            if isinstance(wu.metadata.aspect, ContainerPropertiesClass):
                return wu.metadata.aspect.customProperties or {}
    return {}


@time_machine.travel(FROZEN_TIME, tick=False)
def test_emit_datasource_luid_none_omitted():
    """When datasource has luid=None, luid should be omitted from custom properties.

    Validates: Requirements 1.3, 5.1, 5.3
    """
    source = _make_site_source()

    datasource: dict = {
        c.ID: "ds-1",
        c.NAME: "Test DS",
        c.TYPE_NAME: c.PUBLISHED_DATA_SOURCE,
        c.LUID: None,
        c.PROJECT_NAME: "default",
        c.HAS_EXTRACTS: True,
        c.EXTRACT_LAST_REFRESH_TIME: None,
        c.EXTRACT_LAST_INCREMENTAL_UPDATE_TIME: None,
        c.EXTRACT_LAST_UPDATE_TIME: None,
        c.UPSTREAM_TABLES: [],
        c.UPSTREAM_DATA_SOURCES: [],
        c.FIELDS: [],
    }

    with mock.patch.object(
        source, "_get_datasource_project_luid", return_value="project-luid-1"
    ):
        work_units = list(source.emit_datasource(datasource))

    custom_props = _extract_dataset_custom_props(work_units)
    assert c.LUID not in custom_props
    assert "hasExtracts" in custom_props


@time_machine.travel(FROZEN_TIME, tick=False)
def test_emit_datasource_luid_missing_omitted():
    """When datasource dict has no luid key at all, luid should be omitted.

    Validates: Requirements 1.3, 5.2, 5.3
    """
    source = _make_site_source()

    datasource = {
        c.ID: "ds-2",
        c.NAME: "Test DS 2",
        c.TYPE_NAME: c.PUBLISHED_DATA_SOURCE,
        c.PROJECT_NAME: "default",
        c.HAS_EXTRACTS: False,
        c.EXTRACT_LAST_REFRESH_TIME: None,
        c.EXTRACT_LAST_INCREMENTAL_UPDATE_TIME: None,
        c.EXTRACT_LAST_UPDATE_TIME: None,
        c.UPSTREAM_TABLES: [],
        c.UPSTREAM_DATA_SOURCES: [],
        c.FIELDS: [],
    }

    with mock.patch.object(
        source, "_get_datasource_project_luid", return_value="project-luid-1"
    ):
        work_units = list(source.emit_datasource(datasource))

    custom_props = _extract_dataset_custom_props(work_units)
    assert c.LUID not in custom_props


@time_machine.travel(FROZEN_TIME, tick=False)
def test_emit_workbook_luid_none_omitted():
    """When workbook has luid=None, luid should be omitted from extra_properties.

    Validates: Requirements 2.3, 5.1, 5.3
    """
    source = _make_site_source()
    source.tableau_project_registry["project-luid-1"] = TableauProject(
        id="project-luid-1",
        name="default",
        description="",
        parent_id=None,
        parent_name=None,
        path=["default"],
    )

    workbook = {
        c.ID: "wb-1",
        c.NAME: "Test Workbook",
        c.LUID: None,
        c.DESCRIPTION: "A test workbook",
        c.OWNER: {"username": "testuser"},
        c.PROJECT_NAME: "default",
        c.PROJECT_LUID: "project-luid-1",
    }

    work_units = list(source.emit_workbook_as_container(workbook))
    custom_props = _extract_container_custom_props(work_units)
    assert c.LUID not in custom_props


@time_machine.travel(FROZEN_TIME, tick=False)
def test_emit_workbook_luid_missing_omitted():
    """When workbook dict has no luid key, luid should be omitted from extra_properties.

    Validates: Requirements 2.3, 5.2, 5.3
    """
    source = _make_site_source()
    source.tableau_project_registry["project-luid-1"] = TableauProject(
        id="project-luid-1",
        name="default",
        description="",
        parent_id=None,
        parent_name=None,
        path=["default"],
    )

    workbook = {
        c.ID: "wb-2",
        c.NAME: "Test Workbook 2",
        c.DESCRIPTION: "Another test workbook",
        c.OWNER: {"username": "testuser"},
        c.PROJECT_NAME: "default",
        c.PROJECT_LUID: "project-luid-1",
        # No c.LUID key at all
    }

    work_units = list(source.emit_workbook_as_container(workbook))
    custom_props = _extract_container_custom_props(work_units)
    assert c.LUID not in custom_props
