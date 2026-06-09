import logging
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, cast

from datahub.ingestion.source.quicksight.quicksight_config import (
    QuickSightSourceConfig,
)
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)

if TYPE_CHECKING:
    from mypy_boto3_quicksight import QuickSightClient

logger = logging.getLogger(__name__)


class QuickSightAPI:
    """Thin, paginated wrapper around ``boto3.client("quicksight")``.

    Centralizes the boto3 client creation, AWS account-id resolution, and
    pagination so the rest of the connector deals only with plain iterators of
    summary/describe dictionaries. Retry/throttling behaviour is governed by the
    ``aws_retry_num`` / ``aws_retry_mode`` settings on
    :class:`QuickSightSourceConfig` (use ``adaptive`` mode for QuickSight's
    per-API TPS throttling).
    """

    def __init__(
        self, config: QuickSightSourceConfig, report: QuickSightSourceReport
    ) -> None:
        self.config = config
        self.report = report
        self.client: "QuickSightClient" = config.get_quicksight_client()
        self.aws_account_id: str = self._resolve_account_id()
        self.report.aws_account_id = self.aws_account_id

    def _resolve_account_id(self) -> str:
        if self.config.aws_account_id:
            return self.config.aws_account_id
        # Auto-detect via sts:GetCallerIdentity.
        sts_client = self.config.get_sts_client()
        identity = sts_client.get_caller_identity()
        account_id = identity["Account"]
        logger.info(f"Auto-detected AWS account ID {account_id} via STS")
        return account_id

    def _paginate(
        self, operation_name: str, result_key: str, **kwargs: Any
    ) -> Iterator[Dict[str, Any]]:
        """Yield individual items across all pages of a ``list_*`` operation.

        ``AwsAccountId`` is injected into every request.
        """
        paginator = self.client.get_paginator(operation_name)  # type: ignore[call-overload]
        for page in paginator.paginate(AwsAccountId=self.aws_account_id, **kwargs):
            yield from page.get(result_key, [])

    def list_namespaces(self) -> Iterator[Dict[str, Any]]:
        yield from self._paginate("list_namespaces", "Namespaces")

    def list_dashboards(self) -> Iterator[Dict[str, Any]]:
        yield from self._paginate("list_dashboards", "DashboardSummaryList")

    def list_data_sources(self) -> Iterator[Dict[str, Any]]:
        yield from self._paginate("list_data_sources", "DataSources")

    def list_data_sets(self) -> Iterator[Dict[str, Any]]:
        yield from self._paginate("list_data_sets", "DataSetSummaries")

    def list_analyses(self) -> Iterator[Dict[str, Any]]:
        yield from self._paginate("list_analyses", "AnalysisSummaryList")

    def list_folders(self) -> Iterator[Dict[str, Any]]:
        yield from self._paginate("list_folders", "FolderSummaryList")

    def describe_data_source(self, data_source_id: str) -> Dict[str, Any]:
        """Return the full ``DataSource`` object (incl. connection parameters)."""
        response = self.client.describe_data_source(
            AwsAccountId=self.aws_account_id, DataSourceId=data_source_id
        )
        return cast(Dict[str, Any], response.get("DataSource") or {})

    def describe_folder(self, folder_id: str) -> Dict[str, Any]:
        """Return the full ``Folder`` object (incl. ``FolderPath`` ancestry)."""
        response = self.client.describe_folder(
            AwsAccountId=self.aws_account_id, FolderId=folder_id
        )
        return cast(Dict[str, Any], response.get("Folder") or {})

    def list_folder_members(self, folder_id: str) -> Iterator[Dict[str, Any]]:
        """Yield the members (assets) of a folder.

        Each member is ``{"MemberId": <asset_id>, "MemberArn": <arn>}`` where the
        ARN's resource type (dashboard/analysis/dataset) identifies the asset
        kind. ``list_folder_members`` has no boto3 paginator, so we follow
        ``NextToken`` manually.
        """
        next_token: Optional[str] = None
        while True:
            kwargs: Dict[str, Any] = {
                "AwsAccountId": self.aws_account_id,
                "FolderId": folder_id,
            }
            if next_token:
                kwargs["NextToken"] = next_token
            response = self.client.list_folder_members(**kwargs)
            for member in response.get("FolderMemberList", []):
                yield cast(Dict[str, Any], member)
            next_token = response.get("NextToken")
            if not next_token:
                break

    def describe_data_set(self, data_set_id: str) -> Dict[str, Any]:
        """Return the full ``DataSet`` (``PhysicalTableMap``, ``OutputColumns``).

        Raises ``InvalidParameterValueException`` for FILE-type (CSV upload)
        datasets — callers must handle that to emit summary-only metadata.
        """
        response = self.client.describe_data_set(
            AwsAccountId=self.aws_account_id, DataSetId=data_set_id
        )
        return cast(Dict[str, Any], response.get("DataSet") or {})

    def describe_analysis(self, analysis_id: str) -> Dict[str, Any]:
        """Return the full ``Analysis`` object (``DataSetArns``, ``Sheets``)."""
        response = self.client.describe_analysis(
            AwsAccountId=self.aws_account_id, AnalysisId=analysis_id
        )
        return cast(Dict[str, Any], response.get("Analysis") or {})

    def describe_dashboard(self, dashboard_id: str) -> Dict[str, Any]:
        """Return the full ``Dashboard`` (``Version.DataSetArns``/``SourceEntityArn``)."""
        response = self.client.describe_dashboard(
            AwsAccountId=self.aws_account_id, DashboardId=dashboard_id
        )
        return cast(Dict[str, Any], response.get("Dashboard") or {})

    def describe_analysis_definition(self, analysis_id: str) -> Dict[str, Any]:
        """Return the full analysis ``Definition`` (``Sheets[].Visuals[]``).

        This is the largest QuickSight response (thousands of lines per asset),
        so callers should gate it behind ``extract_analysis_definitions``.
        """
        response = self.client.describe_analysis_definition(
            AwsAccountId=self.aws_account_id, AnalysisId=analysis_id
        )
        return cast(Dict[str, Any], response.get("Definition") or {})

    def describe_dashboard_definition(self, dashboard_id: str) -> Dict[str, Any]:
        """Return the full dashboard ``Definition`` (``Sheets[].Visuals[]``).

        This is the largest QuickSight response (thousands of lines per asset),
        so callers should gate it behind ``extract_dashboard_definitions``.
        """
        response = self.client.describe_dashboard_definition(
            AwsAccountId=self.aws_account_id, DashboardId=dashboard_id
        )
        return cast(Dict[str, Any], response.get("Definition") or {})

    # --- Permissions (ownership) ---
    # Each describe_*_permissions returns a list of
    # ``{"Principal": <arn>, "Actions": [...]}`` entries. The principal ARN is a
    # QuickSight user or group; owner-level principals carry an
    # ``Update*Permissions`` action.

    def describe_dashboard_permissions(self, dashboard_id: str) -> List[Dict[str, Any]]:
        response = self.client.describe_dashboard_permissions(
            AwsAccountId=self.aws_account_id, DashboardId=dashboard_id
        )
        return cast(List[Dict[str, Any]], response.get("Permissions") or [])

    def describe_analysis_permissions(self, analysis_id: str) -> List[Dict[str, Any]]:
        response = self.client.describe_analysis_permissions(
            AwsAccountId=self.aws_account_id, AnalysisId=analysis_id
        )
        return cast(List[Dict[str, Any]], response.get("Permissions") or [])

    def describe_data_set_permissions(self, data_set_id: str) -> List[Dict[str, Any]]:
        response = self.client.describe_data_set_permissions(
            AwsAccountId=self.aws_account_id, DataSetId=data_set_id
        )
        return cast(List[Dict[str, Any]], response.get("Permissions") or [])

    def describe_data_source_permissions(
        self, data_source_id: str
    ) -> List[Dict[str, Any]]:
        response = self.client.describe_data_source_permissions(
            AwsAccountId=self.aws_account_id, DataSourceId=data_source_id
        )
        return cast(List[Dict[str, Any]], response.get("Permissions") or [])

    def describe_folder_permissions(self, folder_id: str) -> List[Dict[str, Any]]:
        response = self.client.describe_folder_permissions(
            AwsAccountId=self.aws_account_id, FolderId=folder_id
        )
        return cast(List[Dict[str, Any]], response.get("Permissions") or [])

    # --- Tags ---

    def list_tags_for_resource(self, resource_arn: str) -> List[Dict[str, Any]]:
        """Return ``[{"Key": ..., "Value": ...}]`` AWS resource tags for an asset.

        Unlike the other endpoints this is keyed by ARN (not account id) and is
        not paginated.
        """
        response = self.client.list_tags_for_resource(ResourceArn=resource_arn)
        return cast(List[Dict[str, Any]], response.get("Tags") or [])

    # --- Identity (users / groups) ---
    # All identity endpoints are namespace-scoped and Enterprise-edition only.

    def list_users(self, namespace: str = "default") -> Iterator[Dict[str, Any]]:
        yield from self._paginate("list_users", "UserList", Namespace=namespace)

    def list_groups(self, namespace: str = "default") -> Iterator[Dict[str, Any]]:
        yield from self._paginate("list_groups", "GroupList", Namespace=namespace)

    def list_group_memberships(
        self, group_name: str, namespace: str = "default"
    ) -> Iterator[Dict[str, Any]]:
        yield from self._paginate(
            "list_group_memberships",
            "GroupMemberList",
            GroupName=group_name,
            Namespace=namespace,
        )
