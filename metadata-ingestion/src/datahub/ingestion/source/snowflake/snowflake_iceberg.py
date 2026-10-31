import itertools
import logging
from typing import Dict, List, Optional
from urllib.parse import urlparse

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RawSearchFilterRule
from datahub.ingestion.source.data_lake_common.config import PathMode
from datahub.ingestion.source.snowflake.constants import (
    ICEBERG_BASE_LOCATION_PROPERTY,
    ICEBERG_CATALOG_INTEGRATION_PROPERTY,
    ICEBERG_CATALOG_NAMESPACE_PROPERTY,
    ICEBERG_CATALOG_TABLE_NAME_PROPERTY,
    ICEBERG_EXTERNAL_VOLUME_PROPERTY,
    ICEBERG_STORAGE_LOCATION_PROPERTY,
    ICEBERG_TABLE_TYPE_PROPERTY,
    UNITY_CATALOG_STORAGE_LOCATION_PROPERTY,
)
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDataDictionary,
    SnowflakeIcebergTableInfo,
    SnowflakeTable,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import DatasetLineageTypeClass
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator

logger = logging.getLogger(__name__)

# Search-API vocabulary for the writer lookup (DataHubGraph.get_urns_by_filter).
_CUSTOM_PROPERTIES_SEARCH_FIELD = "customProperties"
_EQUAL_CONDITION = "EQUAL"
# A lookup returning more than one dataset is ambiguous; this is how many hits are
# fetched before deciding so.
_WRITER_LOOKUP_LIMIT = 3

_SNOWFLAKE_GCS_SCHEME = "gcs://"
_HADOOP_GCS_SCHEME = "gs://"
_SNOWFLAKE_AZURE_SCHEME = "azure://"
_AZURE_BLOB_HOST_SUFFIX = ".blob.core.windows.net"
_AZURE_DFS_HOST_SUFFIX = ".dfs.core.windows.net"


def build_storage_location(storage_base_url: str, base_location: Optional[str]) -> str:
    """Join an external volume's STORAGE_BASE_URL with a table's BASE_LOCATION.

    Snowflake keeps a trailing slash on both; other connectors record the same path
    without one (Unity Catalog's `storage_location`), so the result never ends in '/'.
    """
    location = storage_base_url.rstrip("/")
    relative = (base_location or "").strip("/")
    if relative:
        location = f"{location}/{relative}"
    return location


def writer_storage_location_candidates(storage_location: str) -> List[str]:
    """The spellings under which another engine may have recorded this storage path.

    Snowflake writes `gcs://bucket/path` and `azure://<account>.blob.core.windows.net/
    <container>/path`; Spark-based engines such as Databricks record the same places as
    `gs://bucket/path` and `abfss://<container>@<account>.dfs.core.windows.net/path`.
    S3 is spelled the same everywhere. Each spelling is tried with and without a
    trailing slash, since Snowflake strips it and some producers keep it.
    """
    base = storage_location.rstrip("/")
    spellings = [base]
    if base.startswith(_SNOWFLAKE_GCS_SCHEME):
        spellings.append(_HADOOP_GCS_SCHEME + base[len(_SNOWFLAKE_GCS_SCHEME) :])
    elif base.startswith(_SNOWFLAKE_AZURE_SCHEME):
        parsed = urlparse(base)
        host = parsed.netloc
        container, _, path = parsed.path.lstrip("/").partition("/")
        if host.endswith(_AZURE_BLOB_HOST_SUFFIX) and container:
            account = host[: -len(_AZURE_BLOB_HOST_SUFFIX)]
            abfss = f"abfss://{container}@{account}{_AZURE_DFS_HOST_SUFFIX}"
            spellings.append(f"{abfss}/{path}" if path else abfss)
    candidates: List[str] = []
    for spelling in spellings:
        candidates.extend([spelling, spelling + "/"])
    return candidates


class SnowflakeIcebergHandler:
    """Iceberg details for Snowflake tables, and lineage for the externally managed ones.

    An UNMANAGED Iceberg table is written by another engine and only read by Snowflake,
    so its true upstream is whatever produces the files at its storage location. When
    that producer is already in DataHub (the Unity Catalog connector records a table's
    `storage_location`), the edge goes straight to it; otherwise it goes to the storage
    path's own dataset, the same fallback Snowflake external stages use.
    """

    def __init__(
        self,
        config: SnowflakeV2Config,
        report: SnowflakeV2Report,
        data_dictionary: SnowflakeDataDictionary,
        identifiers: SnowflakeIdentifierBuilder,
        graph: Optional[DataHubGraph],
        aggregator: Optional[SqlParsingAggregator],
    ) -> None:
        self.config = config
        self.report = report
        self.data_dictionary = data_dictionary
        self.identifiers = identifiers
        self.graph = graph
        self.aggregator = aggregator

    def enrich_tables(
        self, iceberg_tables: List[SnowflakeTable], db_name: str, schema_name: str
    ) -> None:
        """Attach SnowflakeIcebergTableInfo (with resolved storage location) to each
        table. One SHOW per schema; external volumes are cached across schemas."""
        if not iceberg_tables:
            return
        infos = self.data_dictionary.get_iceberg_tables_for_schema(
            db_name=db_name, schema_name=schema_name
        )
        missing: List[str] = []
        for table in iceberg_tables:
            info = infos.get(table.name)
            if info is None:
                missing.append(table.name)
                continue
            self.report.iceberg_tables_scanned += 1
            if info.external_volume_name:
                volume = self.data_dictionary.get_external_volume_location(
                    info.external_volume_name
                )
                if volume is not None:
                    info.storage_location = build_storage_location(
                        volume.storage_base_url, info.base_location
                    )
            table.iceberg = info
        if missing:
            # Flagged IS_ICEBERG by information_schema but absent from SHOW ICEBERG
            # TABLES: either that SHOW came back short (reported separately) or the role
            # can see the tables without being able to describe them as Iceberg.
            self.report.warning(
                title="Iceberg table details unavailable",
                message="SHOW ICEBERG TABLES did not return these tables, so their "
                "Iceberg properties and external lineage are skipped. Check for an "
                "incomplete SHOW warning on the schema, else the role's privileges.",
                context=f"{db_name}.{schema_name}: {', '.join(missing)}",
            )

    @staticmethod
    def custom_properties(info: SnowflakeIcebergTableInfo) -> Dict[str, str]:
        return {
            key: value
            for key, value in {
                ICEBERG_TABLE_TYPE_PROPERTY: info.iceberg_table_type,
                ICEBERG_EXTERNAL_VOLUME_PROPERTY: info.external_volume_name,
                ICEBERG_BASE_LOCATION_PROPERTY: info.base_location,
                ICEBERG_STORAGE_LOCATION_PROPERTY: info.storage_location,
                ICEBERG_CATALOG_INTEGRATION_PROPERTY: info.catalog_name,
                ICEBERG_CATALOG_NAMESPACE_PROPERTY: info.catalog_namespace,
                ICEBERG_CATALOG_TABLE_NAME_PROPERTY: info.catalog_table_name,
            }.items()
            if value
        }

    def register_lineage(
        self, table: SnowflakeTable, db_name: str, schema_name: str
    ) -> None:
        """Register the upstream of an externally managed Iceberg table with the SQL
        aggregator, which emits it alongside the table's other lineage (and derives
        column lineage by name from the registered Snowflake schema)."""
        info = table.iceberg
        if self.aggregator is None or info is None or not info.is_externally_managed:
            return
        if not info.storage_location:
            # The volume warning already names the cause and mitigation.
            self.report.num_iceberg_lineage_skipped_no_storage_location += 1
            return

        downstream_urn = self.identifiers.gen_dataset_urn(
            self.identifiers.get_dataset_identifier(table.name, schema_name, db_name)
        )
        upstream_urn = self._resolve_writer(info.storage_location, downstream_urn)
        if upstream_urn is None:
            # Same resolver as external stages: folds the path per
            # datalake_lineage_config.path_specs and maps s3:// gcs:// azure:// to a
            # dataset URN.
            upstream_urn = self.config.datalake_lineage_config.get_urn_for_lineage(
                info.storage_location, self.config.env, mode=PathMode.DIRECTORY
            )
            if upstream_urn is None:
                self.report.warning(
                    title="Iceberg storage location not resolved",
                    message="No lineage emitted: the storage location did not map to a "
                    "dataset. Either its scheme is not s3://, gcs:// or azure:// "
                    "(blob.core.windows.net), or a datalake_lineage_config path spec "
                    "excluded it.",
                    context=f"{db_name}.{schema_name}.{table.name} -> {info.storage_location}",
                )
                return
            self.report.num_iceberg_lineage_storage_fallback += 1

        self.aggregator.add_known_lineage_mapping(
            upstream_urn=upstream_urn,
            downstream_urn=downstream_urn,
            lineage_type=DatasetLineageTypeClass.COPY,
        )

    def _resolve_writer(
        self, storage_location: str, downstream_urn: str
    ) -> Optional[str]:
        """The dataset another connector recorded at this storage location, if exactly one."""
        if self.graph is None:
            # Once per run (the report groups entries by title and message).
            self.report.info(
                title="Iceberg writer lookup skipped",
                message="No DataHub graph connection (e.g. file sink), so externally "
                "managed Iceberg tables link to their storage path instead of the "
                "dataset that writes it.",
            )
            return None

        candidates = writer_storage_location_candidates(storage_location)
        try:
            hits = self._search_storage_location(self.graph, candidates)
        except Exception as e:
            self.report.warning(
                title="Iceberg writer lookup failed",
                message="Falling back to storage-path lineage for this table.",
                context=storage_location,
                exc=e,
            )
            return None
        hits = [urn for urn in hits if urn != downstream_urn]
        if len(hits) == 1:
            self.report.num_iceberg_lineage_writer_resolved += 1
            return hits[0]
        if len(hits) > 1:
            self.report.num_iceberg_lineage_writer_ambiguous += 1
            self.report.warning(
                title="Multiple datasets claim the same storage location",
                message="Falling back to storage-path lineage; DataHub cannot tell "
                "which dataset writes this Iceberg table.",
                context=f"{storage_location}: {hits}",
            )
        return None

    @staticmethod
    def _search_storage_location(
        graph: DataHubGraph, locations: List[str]
    ) -> List[str]:
        """Datasets whose storage_location property equals any of `locations`."""
        storage_filter: RawSearchFilterRule = {
            "field": _CUSTOM_PROPERTIES_SEARCH_FIELD,
            "condition": _EQUAL_CONDITION,
            "values": [
                f"{UNITY_CATALOG_STORAGE_LOCATION_PROPERTY}={location}"
                for location in locations
            ],
        }
        return list(
            itertools.islice(
                graph.get_urns_by_filter(
                    entity_types=["dataset"],
                    extraFilters=[storage_filter],
                    batch_size=_WRITER_LOOKUP_LIMIT,
                ),
                _WRITER_LOOKUP_LIMIT,
            )
        )
