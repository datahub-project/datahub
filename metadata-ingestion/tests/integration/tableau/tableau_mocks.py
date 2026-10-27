"""Tableau Server client mocks and the base recipe shared by the Tableau
integration tests."""

import json
import pathlib
from typing import Any, Callable, List, Sequence, Union
from unittest import mock

from tableauserverclient import PermissionsRule
from tableauserverclient.models import (
    DatasourceItem,
    GroupItem,
    ProjectItem,
    SiteItem,
    ViewItem,
    WorkbookItem,
)
from tableauserverclient.models.reference_item import ResourceReference
from tableauserverclient.server.endpoint.exceptions import TableauError

test_resources_dir = pathlib.Path(__file__).parent


def read_response(file_name):
    response_json_path = f"{test_resources_dir}/setup/{file_name}"
    with open(response_json_path) as file:
        data = json.loads(file.read())
        return data


def side_effect_usage_stat(*arg, **kwargs):
    mock_pagination = mock.MagicMock()
    mock_pagination.total_available = None

    dashboard_stat: ViewItem = ViewItem()

    # Added as luid of Dashboard in workbooksConnection_state_all.json
    dashboard_stat._id = "fc9ea488-f810-4fa8-ac19-aa96018b5d66"
    dashboard_stat._total_views = 3

    # Added as luid of Sheet in workbooksConnection_state_all.json
    sheet_stat: ViewItem = ViewItem()
    sheet_stat._id = "f0779f9d-6765-47a9-a8f6-c740cfd27783"
    sheet_stat._total_views = 5

    return [dashboard_stat, sheet_stat], mock_pagination


def side_effect_project_data(*arg, **kwargs):
    mock_pagination = mock.MagicMock()
    mock_pagination.total_available = None

    project1: ProjectItem = ProjectItem(name="default")
    project1._id = "190a6a5c-63ed-4de1-8045-faeae5df5b01"

    project2: ProjectItem = ProjectItem(name="Project 2")
    project2._id = "c30aafe5-44f4-4f28-80d3-d181010a263c"

    project3: ProjectItem = ProjectItem(name="Samples")
    project3._id = "910733aa-2e95-4ac3-a2e8-71570751099d"

    project4: ProjectItem = ProjectItem(name="DenyProject")
    project4._id = "79d02655-88e5-45a6-9f9b-eeaf5fe54903"
    project4.parent_id = project1._id

    return [project1, project2, project3, project4], mock_pagination


def side_effect_group_data(*arg, **kwargs):
    mock_pagination = mock.MagicMock()
    mock_pagination.total_available = None

    group1: GroupItem = GroupItem(
        name="AB_XY00-Tableau-Access_A_123_PROJECT_XY_Consumer"
    )
    group1._id = "79d02655-88e5-45a6-9f9b-eeaf5fe54903-group1"
    group2: GroupItem = GroupItem(
        name="AB_XY00-Tableau-Access_A_123_PROJECT_XY_Analyst"
    )
    group2._id = "79d02655-88e5-45a6-9f9b-eeaf5fe54903-group2"

    return [group1, group2], mock_pagination


def side_effect_workbook_permissions(*arg, **kwargs):
    project_capabilities1 = {"Read": "Allow", "ViewComments": "Allow"}
    reference: ResourceReference = ResourceReference(
        id_="79d02655-88e5-45a6-9f9b-eeaf5fe54903-group1", tag_name="group"
    )
    rule1 = PermissionsRule(grantee=reference, capabilities=project_capabilities1)

    project_capabilities2 = {
        "Read": "Allow",
        "ViewComments": "Allow",
        "Delete": "Allow",
        "Write": "Allow",
    }
    reference2: ResourceReference = ResourceReference(
        id_="79d02655-88e5-45a6-9f9b-eeaf5fe54903-group2", tag_name="group"
    )
    rule2 = PermissionsRule(grantee=reference2, capabilities=project_capabilities2)

    return [rule1, rule2]


def side_effect_site_data(*arg, **kwargs):
    mock_pagination = mock.MagicMock()
    mock_pagination.total_available = None

    site1: SiteItem = SiteItem(name="Acryl", content_url="acryl")
    site1._id = "190a6a5c-63ed-4de1-8045-site1"
    site1.state = "Active"

    site2: SiteItem = SiteItem(name="Site 2", content_url="site2")
    site2._id = "190a6a5c-63ed-4de1-8045-site2"
    site2.state = "Active"

    site3: SiteItem = SiteItem(name="Site 3", content_url="site3")
    site3._id = "190a6a5c-63ed-4de1-8045-site3"
    site3.state = "Suspended"

    return [site1, site2, site3], mock_pagination


def side_effect_datasource_data(*arg, **kwargs):
    mock_pagination = mock.MagicMock()
    mock_pagination.total_available = None

    datasource1: DatasourceItem = DatasourceItem(
        name="test publish datasource",
        project_id="190a6a5c-63ed-4de1-8045-faeae5df5b01",
    )
    datasource1._id = "ffd72f16-004a-4a7d-8f5b-a8fd18d4317d"

    datasource2: DatasourceItem = DatasourceItem(
        name="Superstore Datasource",
        project_id="910733aa-2e95-4ac3-a2e8-71570751099d",
    )
    datasource2._id = "db86f6cc-9c0e-400f-9fe0-0777f31c6ae2"

    datasource3: DatasourceItem = DatasourceItem(
        name="Customer Payment Query",
        project_id="190a6a5c-63ed-4de1-8045-faeae5df5b01",
    )
    datasource3._id = "1a4e81b9-1107-4b8c-a864-7009b6414858"

    return [
        datasource1,
        datasource2,
        datasource3,
    ], mock_pagination


def side_effect_workbook_data(*arg, **kwargs):
    mock_pagination = mock.MagicMock()
    mock_pagination.total_available = None

    workbook1: WorkbookItem = WorkbookItem(
        project_id="190a6a5c-63ed-4de1-8045-faeae5df5b01",
        name="Email Performance by Campaign",
    )
    workbook1._id = "65a404a8-48a2-4c2a-9eb0-14ee5e78b22b"

    workbook2: WorkbookItem = WorkbookItem(
        project_id="190a6a5c-63ed-4de1-8045-faeae5df5b01", name="Dvdrental Workbook"
    )
    workbook2._id = "b2c84ac6-1e37-4ca0-bf9b-62339be046fc"

    workbook3: WorkbookItem = WorkbookItem(
        project_id="190a6a5c-63ed-4de1-8045-faeae5df5b01", name="Executive Dashboard"
    )
    workbook3._id = "68ebd5b2-ecf6-4fdf-ba1a-95427baef506"

    workbook4: WorkbookItem = WorkbookItem(
        project_id="190a6a5c-63ed-4de1-8045-faeae5df5b01", name="Workbook published ds"
    )
    workbook4._id = "a059a443-7634-4abf-9e46-d147b99168be"

    workbook5: WorkbookItem = WorkbookItem(
        project_id="79d02655-88e5-45a6-9f9b-eeaf5fe54903", name="Deny Pattern WorkBook"
    )
    workbook5._id = "b45eabfe-dc3d-4331-9324-cc1b14b0549b"

    return [
        workbook1,
        workbook2,
        workbook3,
        workbook4,
        workbook5,
    ], mock_pagination


def side_effect_workbook_get_by_id(id, *arg, **kwargs):
    workbooks, _ = side_effect_workbook_data()
    project_id = next((wb.project_id for wb in workbooks if wb._id == id), None)

    workbook_mock = mock.create_autospec(WorkbookItem, instance=True)
    type(workbook_mock).project_id = mock.PropertyMock(return_value=project_id)
    type(workbook_mock).permissions = mock.PropertyMock(
        return_value=side_effect_workbook_permissions()
    )
    return workbook_mock


def side_effect_datasource_get_by_id(id, *arg, **kwargs):
    datasources, _ = side_effect_datasource_data()
    for ds in datasources:
        if ds._id == id:
            return ds


def side_effect_site_get_by_id(id, *arg, **kwargs):
    sites, _ = side_effect_site_data()
    for site in sites:
        if site._id == id:
            return site


def mock_sdk_client(
    side_effect_query_metadata_response: Sequence[Union[dict, TableauError]],
    datasources_side_effect: Union[Callable[..., Any], List[dict]],
    sign_out_side_effect: Union[Callable[..., Any], List[dict]],
) -> mock.MagicMock:
    mock_client = mock.Mock()
    mocked_metadata = mock.Mock()
    mocked_metadata.query.side_effect = side_effect_query_metadata_response
    mock_client.metadata = mocked_metadata

    mock_client.auth = mock.Mock()
    mock_client.site_id = "190a6a5c-63ed-4de1-8045-site1"
    mock_client.views = mock.Mock()
    mock_client.projects = mock.Mock()
    mock_client.sites = mock.Mock()
    mock_client.groups = mock.Mock()

    mock_client.projects.get.side_effect = side_effect_project_data
    mock_client.groups.get.side_effect = side_effect_group_data
    mock_client.sites.get.side_effect = side_effect_site_data
    mock_client.sites.get_by_id.side_effect = side_effect_site_get_by_id

    mock_client.datasources = mock.Mock()
    mock_client.datasources.get.side_effect = datasources_side_effect
    mock_client.datasources.get_by_id.side_effect = side_effect_datasource_get_by_id

    mock_client.workbooks = mock.Mock()
    mock_client.workbooks.get.side_effect = side_effect_workbook_data
    mock_client.workbooks.get_by_id.side_effect = side_effect_workbook_get_by_id

    mock_client.views.get.side_effect = side_effect_usage_stat
    mock_client.auth.sign_in.return_value = None
    mock_client.auth.sign_out.side_effect = sign_out_side_effect

    return mock_client


def mock_data() -> List[dict]:
    return [
        read_response("workbooksConnection_all.json"),
        read_response("sheetsConnection_all.json"),
        read_response("dashboardsConnection_all.json"),
        read_response("embeddedDatasourcesConnection_all.json"),
        read_response("embeddedDatasourcesFieldUpstream_a561c7beccd3_all.json"),
        read_response("embeddedDatasourcesFieldUpstream_04ed1dcc7090_all.json"),
        read_response("embeddedDatasourcesFieldUpstream_6f5f4cc0b6c6_all.json"),
        read_response("embeddedDatasourcesFieldUpstream_69eb47587cc2_all.json"),
        read_response("embeddedDatasourcesFieldUpstream_a0fced25e056_all.json"),
        read_response("embeddedDatasourcesFieldUpstream_1570e7f932f6_all.json"),
        read_response("embeddedDatasourcesFieldUpstream_c651da2f6ad8_all.json"),
        read_response("embeddedDatasourcesFieldUpstream_26675da44a38_all.json"),
        read_response("embeddedDatasourcesFieldUpstream_bda46be068e3_all.json"),
        read_response("publishedDatasourcesConnection_all.json"),
        read_response("publishedDatasourcesFieldUpstream_8e19660bb5dd_all.json"),
        read_response("publishedDatasourcesFieldUpstream_17139d6e97ae_all.json"),
        read_response("customSQLTablesConnection_all.json"),
        read_response("databaseTablesConnection_all.json"),
    ]
