import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.quicksight.processors.containers import ParentResolver
from datahub.ingestion.source.quicksight.processors.enrichment import AssetEnricher
from datahub.ingestion.source.quicksight.quicksight_api import QuickSightAPI
from datahub.ingestion.source.quicksight.quicksight_config import (
    QuickSightSourceConfig,
)
from datahub.ingestion.source.quicksight.quicksight_constants import (
    DATA_SOURCE_TYPE_TO_DIALECT,
    DATA_SOURCE_TYPE_TO_PLATFORM,
    SUBTYPE_DATA_SOURCE,
)
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)
from datahub.sdk.dataset import Dataset

logger = logging.getLogger(__name__)

PLATFORM = "quicksight"


@dataclass(frozen=True)
class ResolvedDataSource:
    """A QuickSight data source resolved to its DataHub upstream platform.

    Built once per ``list_data_sources`` entry and cached in
    :attr:`DataSourcesProcessor.data_source_map` (keyed by ARN) so the lineage
    extractor can resolve ``PhysicalTableMap`` references to upstream URNs and
    pick the correct sqlglot dialect for CustomSql parsing.
    """

    arn: str
    data_source_id: str
    name: str
    quicksight_type: str
    # DataHub platform of the upstream system (None for FILE / unsupported SaaS).
    platform: Optional[str]
    # sqlglot dialect for CustomSql parsing (None when not SQL-backed).
    dialect: Optional[str]
    # ``{bucket}/{key}`` of the S3 manifest, for S3-type data source lineage.
    s3_manifest_uri: Optional[str] = None


class DataSourcesProcessor:
    """Emits a Dataset per QuickSight data source (``Data Source`` subtype) and,
    in the same pass, builds the ARN -> :class:`ResolvedDataSource` map consumed
    later for lineage stitching and SQL-dialect selection.
    """

    def __init__(
        self,
        config: QuickSightSourceConfig,
        report: QuickSightSourceReport,
        api: QuickSightAPI,
        parent_resolver: ParentResolver,
        enricher: AssetEnricher,
    ) -> None:
        self.config = config
        self.report = report
        self.api = api
        self.parent_resolver = parent_resolver
        self.enricher = enricher
        self.data_source_map: Dict[str, ResolvedDataSource] = {}

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
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
                logger.info(
                    f"QuickSight data source '{name}' has type '{quicksight_type}' "
                    "with no upstream platform mapping; emitting the entity without "
                    "upstream lineage."
                )

            data_source = self._describe(data_source_id)
            self.data_source_map[arn] = ResolvedDataSource(
                arn=arn,
                data_source_id=data_source_id,
                name=name,
                quicksight_type=quicksight_type,
                platform=platform,
                dialect=dialect,
                s3_manifest_uri=self._s3_manifest_uri(data_source),
            )

            yield from self._emit_data_source(
                data_source_id=data_source_id,
                name=name,
                quicksight_type=quicksight_type,
                arn=arn,
                data_source=data_source,
            )

    def _emit_data_source(
        self,
        data_source_id: str,
        name: str,
        quicksight_type: str,
        arn: str,
        data_source: Dict[str, Any],
    ) -> Iterable[MetadataWorkUnit]:
        custom_properties = {
            "type": quicksight_type,
            "arn": arn,
            "dataSourceId": data_source_id,
        }
        custom_properties.update(self._connection_properties(data_source))

        # Data sources are never QuickSight folder members, so this resolves to
        # the namespace container (if enabled) or the platform root. An explicit
        # empty list (rather than None) overwrites any stale browse path when the
        # asset attaches directly to the platform root.
        parent = self.parent_resolver(data_source_id)
        dataset = Dataset(
            platform=PLATFORM,
            name=f"{self.api.aws_account_id}.{data_source_id}",
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=name,
            subtype=SUBTYPE_DATA_SOURCE,
            custom_properties=custom_properties,
            parent_container=parent if parent is not None else [],
            owners=self.enricher.owners("data_source", data_source_id),
            tags=self.enricher.tags(arn),
        )
        self.report.data_sources.processed(f"{data_source_id} ({name})")
        yield from dataset.as_workunits()

    def _describe(self, data_source_id: str) -> Dict[str, Any]:
        """Describe a data source, degrading to ``{}`` on permission errors.

        ``describe_data_source`` requires an extra permission and may fail on
        restricted roles; we degrade rather than dropping the entity.
        """
        try:
            return self.api.describe_data_source(data_source_id)
        except Exception as e:
            logger.info(
                f"Could not describe QuickSight data source {data_source_id}: {e}"
            )
            return {}

    @staticmethod
    def _connection_properties(data_source: Dict[str, Any]) -> Dict[str, str]:
        """Flatten the data source's connection parameters to string properties."""
        params = data_source.get("DataSourceParameters") or {}
        properties: Dict[str, str] = {}
        # DataSourceParameters is a single-key union, e.g. {"AthenaParameters": {...}}.
        for param_group, values in params.items():
            if isinstance(values, dict):
                for key, value in values.items():
                    properties[f"{param_group}.{key}"] = str(value)
            else:
                properties[param_group] = str(values)
        return properties

    @staticmethod
    def _s3_manifest_uri(data_source: Dict[str, Any]) -> Optional[str]:
        """Extract ``{bucket}/{key}`` of the S3 manifest for S3-type sources."""
        params = data_source.get("DataSourceParameters") or {}
        location = (params.get("S3Parameters") or {}).get("ManifestFileLocation") or {}
        bucket = location.get("Bucket")
        key = location.get("Key")
        return f"{bucket}/{key}" if bucket and key else None
