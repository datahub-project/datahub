"""Golden-file integration test for the QuickSight source.

QuickSight is a boto3 client rather than an HTTP API, so instead of mocking
``requests`` we stand up a :class:`FakeQuickSightClient` that answers the exact
``list_*`` / ``describe_*`` calls the connector makes. The fixture is sized to
exercise every entity type and the lineage paths in one run:

- 1 shared folder (``Sales``) with member assets, plus the synthetic
  ``Shared folders`` root (``add_shared_folders_container``).
- 2 data sources: Redshift (RelationalTable upstream) and Athena (CustomSql).
- 3 datasets: a RelationalTable dataset, a CustomSql dataset (column-level
  lineage via sqlglot), and a FILE upload (summary-only, ``describe_data_set``
  raises ``InvalidParameterValueException``).
- 2 analyses + 1 published dashboard linked back to its source analysis.
- 5 charts across the analyses/dashboard definitions.
- Ownership (SSO-federated principal), AWS resource tags, and users/groups.
"""

from typing import Any, Dict, Iterator, List
from unittest import mock

import pytest
from botocore.exceptions import ClientError

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

FROZEN_TIME = "2024-01-01 00:00:00"
ACCOUNT_ID = "123456789012"
REGION = "us-east-1"


def _arn(resource_type: str, resource_id: str) -> str:
    return f"arn:aws:quicksight:{REGION}:{ACCOUNT_ID}:{resource_type}/{resource_id}"


# SSO-federated principal: the owner extractor keeps only the trailing session
# segment (jane@acme.com), matching the CorpUser entity emitted below.
_OWNER_PRINCIPAL = _arn("user", "default/AWSReservedSSO_Admin_abc123/jane@acme.com")
_OWNER_ACTIONS = {
    "dashboard": "quicksight:UpdateDashboardPermissions",
    "analysis": "quicksight:UpdateAnalysisPermissions",
    "data_set": "quicksight:UpdateDataSetPermissions",
    "data_source": "quicksight:UpdateDataSourcePermissions",
    "folder": "quicksight:UpdateFolderPermissions",
}


def _owner_permissions(update_action: str) -> List[Dict[str, Any]]:
    return [
        {"Principal": _OWNER_PRINCIPAL, "Actions": [update_action]},
        # A viewer principal (no Update*Permissions action) is ignored.
        {
            "Principal": _arn("user", "default/reader@acme.com"),
            "Actions": ["quicksight:DescribeDashboard"],
        },
    ]


# --- Summaries returned by the paginated list_* operations ---

_DATA_SOURCES = [
    {
        "Arn": _arn("datasource", "ds-redshift"),
        "DataSourceId": "ds-redshift",
        "Name": "Redshift Warehouse",
        "Type": "REDSHIFT",
    },
    {
        "Arn": _arn("datasource", "ds-athena"),
        "DataSourceId": "ds-athena",
        "Name": "Athena Lake",
        "Type": "ATHENA",
    },
]

_DATA_SETS = [
    {
        "Arn": _arn("dataset", "dataset-orders"),
        "DataSetId": "dataset-orders",
        "Name": "Orders",
    },
    {
        "Arn": _arn("dataset", "dataset-revenue"),
        "DataSetId": "dataset-revenue",
        "Name": "Revenue by Region",
    },
    {
        "Arn": _arn("dataset", "dataset-upload"),
        "DataSetId": "dataset-upload",
        "Name": "Manual Upload",
    },
]

_ANALYSES = [
    {
        "Arn": _arn("analysis", "analysis-sales"),
        "AnalysisId": "analysis-sales",
        "Name": "Sales Analysis",
        "Status": "CREATION_SUCCESSFUL",
    },
    {
        "Arn": _arn("analysis", "analysis-customer"),
        "AnalysisId": "analysis-customer",
        "Name": "Customer Analysis",
        "Status": "CREATION_SUCCESSFUL",
    },
]

_DASHBOARDS = [
    {
        "Arn": _arn("dashboard", "dashboard-sales"),
        "DashboardId": "dashboard-sales",
        "Name": "Sales Dashboard",
    },
]

_FOLDERS = [
    {
        "Arn": _arn("folder", "folder-sales"),
        "FolderId": "folder-sales",
        "Name": "Sales",
    },
]


def _bar_visual(visual_id: str, title: str, identifier: str) -> Dict[str, Any]:
    return {
        "BarChartVisual": {
            "VisualId": visual_id,
            "Title": {"FormatText": {"PlainText": title}},
            "ChartConfiguration": {
                "FieldWells": {
                    "Category": [{"Column": {"DataSetIdentifier": identifier}}]
                }
            },
        }
    }


def _kpi_visual(visual_id: str, title: str, identifier: str) -> Dict[str, Any]:
    return {
        "KPIVisual": {
            "VisualId": visual_id,
            "Title": {"FormatText": {"PlainText": title}},
            "FieldWells": {"Values": [{"Column": {"DataSetIdentifier": identifier}}]},
        }
    }


def _definition(identifier: str, dataset_arn: str, visuals: List[Dict]) -> Dict:
    return {
        "DataSetIdentifierDeclarations": [
            {"Identifier": identifier, "DataSetArn": dataset_arn}
        ],
        "Sheets": [{"SheetId": "sheet-1", "Visuals": visuals}],
    }


class _Paginator:
    def __init__(self, pages: List[Dict[str, Any]]) -> None:
        self._pages = pages

    def paginate(self, **kwargs: Any) -> Iterator[Dict[str, Any]]:
        yield from self._pages


class FakeQuickSightClient:
    """Minimal in-memory stand-in for ``boto3.client("quicksight")``."""

    def get_paginator(self, operation_name: str) -> "_Paginator":
        pages: Dict[str, List[Dict[str, Any]]] = {
            "list_namespaces": [{"Namespaces": [{"Name": "default"}]}],
            "list_data_sources": [{"DataSources": _DATA_SOURCES}],
            "list_data_sets": [{"DataSetSummaries": _DATA_SETS}],
            "list_analyses": [{"AnalysisSummaryList": _ANALYSES}],
            "list_dashboards": [{"DashboardSummaryList": _DASHBOARDS}],
            "list_folders": [{"FolderSummaryList": _FOLDERS}],
            "list_users": [
                {
                    "UserList": [
                        {
                            "UserName": "AWSReservedSSO_Admin_abc123/jane@acme.com",
                            "Email": "jane@acme.com",
                            "Role": "ADMIN",
                        }
                    ]
                }
            ],
            "list_groups": [
                {"GroupList": [{"GroupName": "sales-team", "Description": "Sales org"}]}
            ],
            "list_group_memberships": [
                {
                    "GroupMemberList": [
                        {"MemberName": "AWSReservedSSO_Admin_abc123/jane@acme.com"}
                    ]
                }
            ],
        }
        return _Paginator(pages.get(operation_name, [{}]))

    # --- describe_* ---

    def describe_data_source(self, AwsAccountId: str, DataSourceId: str) -> Dict:
        params = {
            "ds-redshift": {
                "RedshiftParameters": {
                    "Host": "redshift.acme.internal",
                    "Port": 5439,
                    "Database": "dev",
                }
            },
            "ds-athena": {"AthenaParameters": {"WorkGroup": "primary"}},
        }
        return {"DataSource": {"DataSourceParameters": params.get(DataSourceId, {})}}

    def describe_data_set(self, AwsAccountId: str, DataSetId: str) -> Dict:
        if DataSetId == "dataset-orders":
            return {
                "DataSet": {
                    "ImportMode": "SPICE",
                    "OutputColumns": [
                        {"Name": "order_id", "Type": "INTEGER"},
                        {"Name": "amount", "Type": "DECIMAL"},
                        {"Name": "created_at", "Type": "DATETIME"},
                    ],
                    "PhysicalTableMap": {
                        "table-1": {
                            "RelationalTable": {
                                "DataSourceArn": _arn("datasource", "ds-redshift"),
                                "Catalog": "dev",
                                "Schema": "public",
                                "Name": "orders",
                            }
                        }
                    },
                }
            }
        if DataSetId == "dataset-revenue":
            return {
                "DataSet": {
                    "ImportMode": "DIRECT_QUERY",
                    "OutputColumns": [
                        {"Name": "region", "Type": "STRING"},
                        {"Name": "total", "Type": "DECIMAL"},
                    ],
                    "PhysicalTableMap": {
                        "table-1": {
                            "CustomSql": {
                                "DataSourceArn": _arn("datasource", "ds-athena"),
                                "Name": "revenue_query",
                                "SqlQuery": (
                                    "SELECT region, SUM(amount) AS total "
                                    "FROM analytics.orders GROUP BY region"
                                ),
                            }
                        }
                    },
                }
            }
        # dataset-upload is a FILE dataset and cannot be described.
        raise ClientError(
            {
                "Error": {
                    "Code": "InvalidParameterValueException",
                    "Message": "FILE datasets cannot be described.",
                }
            },
            "DescribeDataSet",
        )

    def describe_analysis(self, AwsAccountId: str, AnalysisId: str) -> Dict:
        dataset_arns = {
            "analysis-sales": [
                _arn("dataset", "dataset-orders"),
                _arn("dataset", "dataset-revenue"),
            ],
            "analysis-customer": [_arn("dataset", "dataset-revenue")],
        }
        return {
            "Analysis": {
                "Status": "CREATION_SUCCESSFUL",
                "DataSetArns": dataset_arns.get(AnalysisId, []),
            }
        }

    def describe_analysis_definition(self, AwsAccountId: str, AnalysisId: str) -> Dict:
        if AnalysisId == "analysis-sales":
            return {
                "Definition": _definition(
                    "orders",
                    _arn("dataset", "dataset-orders"),
                    [
                        _bar_visual("v-bar", "Orders by Region", "orders"),
                        _kpi_visual("v-kpi", "Total Orders", "orders"),
                    ],
                )
            }
        return {
            "Definition": _definition(
                "revenue",
                _arn("dataset", "dataset-revenue"),
                [_bar_visual("v-line", "Revenue Trend", "revenue")],
            )
        }

    def describe_dashboard(self, AwsAccountId: str, DashboardId: str) -> Dict:
        return {
            "Dashboard": {
                "Version": {
                    "DataSetArns": [_arn("dataset", "dataset-orders")],
                    "SourceEntityArn": _arn("analysis", "analysis-sales"),
                }
            }
        }

    def describe_dashboard_definition(
        self, AwsAccountId: str, DashboardId: str
    ) -> Dict:
        return {
            "Definition": _definition(
                "orders",
                _arn("dataset", "dataset-orders"),
                [
                    _bar_visual("v-pie", "Revenue Share", "orders"),
                    _kpi_visual("v-table", "Order Details", "orders"),
                ],
            )
        }

    def describe_folder(self, AwsAccountId: str, FolderId: str) -> Dict:
        # Top-level folder: empty FolderPath ancestry.
        return {"Folder": {"FolderPath": []}}

    def list_folder_members(self, **kwargs: Any) -> Dict:
        members = {
            "folder-sales": [
                {"MemberId": "dataset-orders"},
                {"MemberId": "analysis-sales"},
                {"MemberId": "dashboard-sales"},
            ]
        }
        return {"FolderMemberList": members.get(kwargs.get("FolderId", ""), [])}

    # --- permissions (ownership) ---

    def describe_dashboard_permissions(
        self, AwsAccountId: str, DashboardId: str
    ) -> Dict:
        return {"Permissions": _owner_permissions(_OWNER_ACTIONS["dashboard"])}

    def describe_analysis_permissions(self, AwsAccountId: str, AnalysisId: str) -> Dict:
        return {"Permissions": _owner_permissions(_OWNER_ACTIONS["analysis"])}

    def describe_data_set_permissions(self, AwsAccountId: str, DataSetId: str) -> Dict:
        return {"Permissions": _owner_permissions(_OWNER_ACTIONS["data_set"])}

    def describe_data_source_permissions(
        self, AwsAccountId: str, DataSourceId: str
    ) -> Dict:
        return {"Permissions": _owner_permissions(_OWNER_ACTIONS["data_source"])}

    def describe_folder_permissions(self, AwsAccountId: str, FolderId: str) -> Dict:
        return {"Permissions": _owner_permissions(_OWNER_ACTIONS["folder"])}

    # --- tags ---

    def list_tags_for_resource(self, ResourceArn: str) -> Dict:
        tagged = {
            _arn("dataset", "dataset-orders"): [{"Key": "team", "Value": "sales"}],
            _arn("dashboard", "dashboard-sales"): [
                {"Key": "tier", "Value": "gold"},
                {"Key": "pii", "Value": ""},
            ],
        }
        return {"Tags": tagged.get(ResourceArn, [])}


def _fake_sts_client():
    sts = mock.MagicMock()
    sts.get_caller_identity.return_value = {"Account": ACCOUNT_ID}
    return sts


@pytest.mark.integration
def test_quicksight_ingest(pytestconfig, tmp_path):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/quicksight"
    output_path = f"{tmp_path}/quicksight_mces.json"

    with (
        mock.patch(
            "datahub.ingestion.source.quicksight.quicksight_config."
            "QuickSightSourceConfig.get_quicksight_client",
            return_value=FakeQuickSightClient(),
        ),
        mock.patch(
            "datahub.ingestion.source.quicksight.quicksight_config."
            "QuickSightSourceConfig.get_sts_client",
            return_value=_fake_sts_client(),
        ),
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "quicksight-test",
                "source": {
                    "type": "quicksight",
                    "config": {
                        "aws_region": REGION,
                        "aws_account_id": ACCOUNT_ID,
                        "extract_lineage": True,
                        "extract_column_lineage": True,
                        "extract_ownership": True,
                        "extract_tags": True,
                        "extract_users_and_groups": True,
                        "add_shared_folders_container": True,
                        # The CustomSql query is fully qualified (analytics.orders),
                        # so no default_database/schema fallback is needed here.
                        "external_data_sources": {
                            "ds-athena": {"env": "PROD"},
                            "ds-redshift": {"env": "PROD"},
                        },
                    },
                },
                "sink": {"type": "file", "config": {"filename": output_path}},
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=f"{test_resources_dir}/golden_test_quicksight_ingest.json",
    )
