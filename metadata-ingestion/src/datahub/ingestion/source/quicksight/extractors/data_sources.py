from dataclasses import dataclass
from typing import Any, Dict, Optional

from datahub.ingestion.source.quicksight.quicksight_api import QuickSightAPI
from datahub.ingestion.source.quicksight.quicksight_config import (
    QuickSightSourceConfig,
)
from datahub.ingestion.source.quicksight.quicksight_constants import (
    DATA_SOURCE_TYPE_TO_DIALECT,
    DATA_SOURCE_TYPE_TO_PLATFORM,
)
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)


@dataclass(frozen=True)
class ResolvedDataSource:
    """A QuickSight data source resolved to its DataHub upstream platform.

    Built once per ``list_data_sources`` entry and cached in
    :attr:`DataSourcesExtractor.data_source_map` (keyed by ARN) so the lineage
    extractor can resolve ``PhysicalTableMap`` references to upstream URNs and
    pick the correct sqlglot dialect for CustomSql parsing.
    """

    data_source_id: str
    name: str
    # DataHub platform of the upstream system (None for FILE / unsupported SaaS).
    platform: Optional[str]
    # sqlglot dialect for CustomSql parsing (None when not SQL-backed).
    dialect: Optional[str]
    # ``{bucket}/{key}`` of the S3 manifest; used only as context in the
    # "S3 upstream lineage skipped" warning (see quicksight_lineage).
    s3_manifest_uri: Optional[str] = None


class DataSourcesExtractor:
    """Resolves QuickSight data sources to their DataHub upstream platform.

    QuickSight data sources are *not* emitted as DataHub entities — like other BI
    connectors (Tableau/Looker/PowerBI), the raw warehouse connection is treated
    purely as resolution metadata for the upstream table's platform, never its
    own node. This builds the ARN -> :class:`ResolvedDataSource` map consumed by
    the lineage extractor for upstream-URN stitching and SQL-dialect selection.
    """

    def __init__(
        self,
        config: QuickSightSourceConfig,
        report: QuickSightSourceReport,
        api: QuickSightAPI,
    ) -> None:
        self.config = config
        self.report = report
        self.api = api
        self.data_source_map: Dict[str, ResolvedDataSource] = {}

    def build_data_source_map(self) -> None:
        """Populate :attr:`data_source_map`; emits no entities (see class docs)."""
        for summary in self.api.list_data_sources():
            arn = summary.get("Arn")
            data_source_id = summary.get("DataSourceId")
            name = summary.get("Name") or data_source_id or ""
            quicksight_type = summary.get("Type") or ""
            if not arn or not data_source_id:
                continue

            if not self.config.data_source_pattern.allowed(name):
                self.report.data_sources.dropped(f"{data_source_id} ({name})")
                continue

            platform = DATA_SOURCE_TYPE_TO_PLATFORM.get(quicksight_type)
            dialect = DATA_SOURCE_TYPE_TO_DIALECT.get(quicksight_type)
            if platform is None:
                self.report.num_unknown_data_source_types += 1
                self.report.warning(
                    title="Unsupported data source type",
                    message="QuickSight data source type has no upstream platform "
                    "mapping; datasets backed by it will have no upstream lineage.",
                    context=f"{name} (type={quicksight_type})",
                )

            try:
                data_source = self._describe(data_source_id)
                self.data_source_map[arn] = ResolvedDataSource(
                    data_source_id=data_source_id,
                    name=name,
                    platform=platform,
                    dialect=dialect,
                    s3_manifest_uri=self._s3_manifest_uri(data_source),
                )
                self.report.data_sources.processed(f"{data_source_id} ({name})")
            except Exception as e:
                self.report.warning(
                    title="Failed to resolve data source",
                    message="Skipping this data source; datasets backed by it will "
                    "have no upstream lineage. The rest of the run continues.",
                    context=f"{data_source_id} ({name})",
                    exc=e,
                )

    def _describe(self, data_source_id: str) -> Dict[str, Any]:
        """Describe a data source, degrading to ``{}`` on permission errors.

        ``describe_data_source`` requires an extra permission and may fail on
        restricted roles; we degrade rather than dropping the resolution entry.
        """
        try:
            return self.api.describe_data_source(data_source_id)
        except Exception as e:
            self.report.warning(
                title="Failed to describe data source",
                message="Could not fetch the data source's connection details; "
                "upstream lineage for its datasets may be incomplete.",
                context=data_source_id,
                exc=e,
            )
            return {}

    @staticmethod
    def _s3_manifest_uri(data_source: Dict[str, Any]) -> Optional[str]:
        """Extract ``{bucket}/{key}`` of the S3 manifest for S3-type sources."""
        params = data_source.get("DataSourceParameters") or {}
        location = (params.get("S3Parameters") or {}).get("ManifestFileLocation") or {}
        bucket = location.get("Bucket")
        key = location.get("Key")
        return f"{bucket}/{key}" if bucket and key else None
