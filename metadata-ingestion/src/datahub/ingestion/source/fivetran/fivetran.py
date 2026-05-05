import logging
from typing import ClassVar, Dict, FrozenSet, Iterable, List, Optional, Tuple, Union

import pydantic
import requests

import datahub.emitter.mce_builder as builder
from datahub.api.entities.datajob import DataJob as DataJobV1
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    MetadataWorkUnitProcessor,
    SourceReport,
    StructuredLogCategory,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.fivetran.config import (
    KNOWN_DATA_PLATFORM_MAPPING,
    Constant,
    FivetranSourceConfig,
    FivetranSourceReport,
    PlatformDetail,
)
from datahub.ingestion.source.fivetran.data_classes import Connector, Job
from datahub.ingestion.source.fivetran.fivetran_log_db_reader import FivetranLogDbReader
from datahub.ingestion.source.fivetran.fivetran_log_rest_reader import (
    FivetranLogRestReader,
)
from datahub.ingestion.source.fivetran.fivetran_rest_api import FivetranAPIClient
from datahub.ingestion.source.fivetran.google_sheets_handler import (
    GoogleSheetsConnectorHandler,
)
from datahub.ingestion.source.fivetran.log_reader import FivetranConnectorReader
from datahub.ingestion.source.fivetran.response_models import (
    FivetranDestinationConfig,
    FivetranDestinationDetails,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
)
from datahub.metadata.urns import CorpUserUrn, DataFlowUrn, DatasetUrn
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.entity import Entity

# Logger instance
logger = logging.getLogger(__name__)
CORPUSER_DATAHUB = "urn:li:corpuser:datahub"


@platform_name("Fivetran")
@config_class(FivetranSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default, can be disabled via configuration `include_column_lineage`",
)
class FivetranSource(StatefulIngestionSourceBase):
    """
    This plugin extracts fivetran users, connectors, destinations and sync history.
    """

    config: FivetranSourceConfig
    report: FivetranSourceReport
    platform: str = "fivetran"

    # Maps Fivetran destination `service` strings to:
    #   (urn:li:dataPlatform value, attribute on FivetranDestinationConfig
    #    holding the "database-like" identifier for relational URNs)
    # Source: https://fivetran.com/docs/rest-api/api-reference/destinations
    _RELATIONAL_SERVICE_MAP: ClassVar[Dict[str, Tuple[str, str]]] = {
        # Native warehouse names
        "snowflake": ("snowflake", "database"),
        "big_query": ("bigquery", "project_id"),
        "bigquery": ("bigquery", "project_id"),
        "databricks": ("databricks", "catalog"),
        # AWS warehouse variants
        "aws_redshift": ("redshift", "database"),
        "redshift": ("redshift", "database"),
        "aurora_warehouse": ("postgres", "database"),
        "aurora_postgres_warehouse": ("postgres", "database"),
        "aurora_mysql_warehouse": ("mysql", "database"),
        # Azure warehouse variants
        "azure_postgres_warehouse": ("postgres", "database"),
        "azure_sql_data_warehouse": ("mssql", "database"),
        "azure_sql_managed_db_warehouse": ("mssql", "database"),
        # Standalone warehouse variants
        "postgres_warehouse": ("postgres", "database"),
        "mysql_warehouse": ("mysql", "database"),
        "mysql_rds_warehouse": ("mysql", "database"),
        "postgres_rds_warehouse": ("postgres", "database"),
        "google_cloud_postgresql_warehouse": ("postgres", "database"),
        "google_cloud_mysql_warehouse": ("mysql", "database"),
        "panoply": ("panoply", "database"),
        "periscope_warehouse": ("periscope", "database"),
        "new_s3_datalake": ("s3", "bucket"),
    }

    # URN platforms whose URN shape doesn't carry a `database` segment.
    # Used by `resolve_destination_details` to skip the DB-mode default of
    # `database = fivetran_log_database` (which is meaningful only for
    # three-part `<database>.<schema>.<table>` warehouses).
    _NON_RELATIONAL_DESTINATION_PLATFORMS: ClassVar[FrozenSet[str]] = frozenset(
        {"iceberg", "glue", "s3", "gcs", "abs"}
    )

    # URN platforms whose shape is `<bucket-or-container-path>/<schema>/<table>`
    # (i.e., raw object storage URNs aligned with DataHub's S3 / GCS / ABS
    # sources). For these, `apply_discovered_destination` auto-populates
    # `database` with the bucket-or-container path derived from the
    # discovered Managed Data Lake config, and `build_destination_urn`
    # composes the URN by replacing schema/table dots with slashes and
    # prepending the path. The Fivetran MDL config exposes three cloud
    # backings (AWS / GCS / ADLS) under the same `service: managed_data_lake`
    # — see `_derive_path_prefix` for the field mapping per platform.
    _PATH_STYLE_DESTINATION_PLATFORMS: ClassVar[FrozenSet[str]] = frozenset(
        {"s3", "gcs", "abs"}
    )

    def __init__(self, config: FivetranSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = FivetranSourceReport()
        # Build the API client once if credentials are supplied; share it
        # between the source's destination-discovery path and the REST log
        # reader (when applicable) so both halves see the same
        # `_destination_cache` / session / retry policy. Two separate
        # clients would fragment cache state and double the HTTP setup
        # cost in REST-primary hybrid mode.
        self.api_client: Optional[FivetranAPIClient] = (
            FivetranAPIClient(self.config.api_config)
            if self.config.api_config is not None
            else None
        )
        self.log_reader: FivetranConnectorReader = self._build_log_reader()
        # Google Sheets workaround handler — encapsulates the
        # GSheets-specific dataset emission and lineage URN building so
        # the workaround can be deleted in one place when DataHub adds
        # native Google Sheets support.
        self.gsheets_handler = GoogleSheetsConnectorHandler(
            # Lazy provider — reads `self.api_client` on each call so
            # tests that swap the api_client post-init see the change.
            api_client_provider=lambda: self.api_client,
            config=self.config,
            report=self.report,
        )
        # Negative cache for destination discovery: once a destination_id
        # fails REST lookup, every connector that lives on it would otherwise
        # re-issue the call (the API client only caches successes). Storing
        # the failed IDs prevents retry storms and the duplicate per-call
        # warnings that come with them — one warning per destination, not
        # one per connector.
        self._failed_destination_ids: set[str] = set()
        # Per-destination dedup for "URN could not be constructed" warnings.
        # The most common cause is auto-detected `platform: glue` without
        # a user-supplied `database` — without dedup, every lineage edge
        # for the destination would emit its own warning. With dedup,
        # exactly one warning per destination per ingest, and the
        # individual edges are still skipped silently after the first.
        self._destinations_with_urn_warning: set[str] = set()

    def _build_log_reader(self) -> FivetranConnectorReader:
        # By the time we get here, `validate_log_source_credentials` has
        # resolved `log_source` to a concrete `"log_database" | "rest_api"`.
        # When both credential blocks are supplied, the resolved value
        # determines who owns the log read; the *other* block then plays a
        # secondary role (REST → destination routing + Google Sheets;
        # DB → per-run sync history).
        if self.config.log_source == "rest_api":
            assert self.api_client is not None  # validated upstream
            # Hybrid mode: if `fivetran_log_config` is *also* provided,
            # instantiate a DB log reader to fetch per-run sync history
            # (Fivetran's REST API has no sync-history endpoint) AND to
            # serve as a column-lineage fallback when the Fivetran
            # Metadata API is plan-restricted. REST still owns connector
            # listing, schemas, users; DB plays both jobs+lineage backup
            # roles via the same instance.
            db_log_reader: Optional[FivetranLogDbReader] = (
                self._build_db_log_reader()
                if self.config.fivetran_log_config is not None
                else None
            )
            return FivetranLogRestReader(
                self.api_client,
                self.report,
                max_table_lineage_per_connector=self.config.max_table_lineage_per_connector,
                max_column_lineage_per_connector=self.config.max_column_lineage_per_connector,
                max_workers=self.config.rest_api_max_workers,
                per_connector_timeout_sec=self.config.rest_api_per_connector_timeout_sec,
                db_log_reader=db_log_reader,
                db_lineage_reader=db_log_reader,
            )
        return self._build_db_log_reader()

    def _build_db_log_reader(self) -> FivetranLogDbReader:
        assert self.config.fivetran_log_config is not None  # validated upstream
        return FivetranLogDbReader(
            self.config.fivetran_log_config,
            self.report,
            max_jobs_per_connector=self.config.max_jobs_per_connector,
            max_table_lineage_per_connector=self.config.max_table_lineage_per_connector,
            max_column_lineage_per_connector=self.config.max_column_lineage_per_connector,
        )

    def _extend_lineage(self, connector: Connector, datajob: DataJob) -> Dict[str, str]:
        input_dataset_urn_list: List[Union[str, DatasetUrn]] = []
        output_dataset_urn_list: List[Union[str, DatasetUrn]] = []
        fine_grained_lineage: List[FineGrainedLineage] = []

        # TODO: Once Fivetran exposes the database via the API, we shouldn't ask for it via config.

        # Get platform details for connector source. The dict lookup may
        # return the user's configured PlatformDetail on a hit, so any
        # platform fill-in must go via `model_copy` — mutating in place
        # would poison the shared config object across connectors.
        source_details = self.config.sources_to_platform_instance.get(
            connector.connector_id, PlatformDetail()
        )
        if source_details.platform is None:
            if connector.connector_type in KNOWN_DATA_PLATFORM_MAPPING:
                resolved_platform = KNOWN_DATA_PLATFORM_MAPPING[
                    connector.connector_type
                ]
            else:
                self.report.info(
                    title="Guessing source platform for lineage",
                    message="We encountered a connector type that we don't fully support yet. "
                    "We will attempt to guess the platform based on the connector type. "
                    "Note that we use connector_id as the key not connector_name which you may see in the UI of Fivetran. ",
                    context=f"connector_name: {connector.connector_name} (connector_id: {connector.connector_id}, connector_type: {connector.connector_type})",
                    log_category=StructuredLogCategory.LINEAGE,
                )
                resolved_platform = connector.connector_type
            source_details = source_details.model_copy(
                update={"platform": resolved_platform}
            )
        # The if-block above unconditionally fills in `platform`, but
        # `model_copy` returns `PlatformDetail` whose `platform` field
        # is still typed `Optional[str]` — mypy can't narrow it. Pin the
        # invariant so downstream URN construction sees a `str`.
        assert source_details.platform is not None

        # Get platform details for destination — declarative override + (optional)
        # REST discovery, plus a default fallback.
        destination_details = self.resolve_destination_details(connector.destination_id)

        max_table_lineage = self.config.max_table_lineage_per_connector
        if len(connector.lineage) >= max_table_lineage:
            self.report.warning(
                title="Table lineage truncated",
                message=f"The connector had more than {max_table_lineage} table lineage entries. "
                f"Only the most recent {max_table_lineage} entries were ingested.",
                context=f"{connector.connector_name} (connector_id: {connector.connector_id})",
            )

        for lineage in connector.lineage:
            source_table = (
                lineage.source_table
                if source_details.include_schema_in_urn
                else lineage.source_table.split(".", 1)[1]
            )
            input_dataset_urn: Optional[DatasetUrn] = None
            if self.gsheets_handler.applies_to(connector.connector_type):
                # Google Sheets workaround — see google_sheets_handler.py.
                # Returns None on resolve failure; the structured warning
                # is emitted there.
                input_dataset_urn = self.gsheets_handler.build_input_dataset_urn(
                    connector, source_details.env
                )
            else:
                source_database_for_urn = source_details.database_for_urn
                input_dataset_urn = DatasetUrn.create_from_ids(
                    platform_id=source_details.platform,
                    table_name=(
                        f"{source_database_for_urn}.{source_table}"
                        if source_database_for_urn
                        else source_table
                    ),
                    env=source_details.env,
                    platform_instance=source_details.platform_instance,
                )

            if input_dataset_urn:
                input_dataset_urn_list.append(input_dataset_urn)

            try:
                output_dataset_urn = self.build_destination_urn(
                    lineage.destination_table,
                    destination_details,
                )
            except ValueError as e:
                # Most common causes: (a) no platform discovered for this
                # destination (REST call failed and no declarative
                # override); (b) auto-detected `platform: glue` but no
                # user-supplied `database`. Skip the lineage edge for this
                # connector and keep the ingest going. Dedup the warning
                # per destination — every connector on a misconfigured
                # destination would otherwise emit its own copy.
                if connector.destination_id not in self._destinations_with_urn_warning:
                    self._destinations_with_urn_warning.add(connector.destination_id)
                    self.report.warning(
                        title="Destination URN could not be constructed",
                        message=(
                            "Skipping all lineage edges for this destination. "
                            "Subsequent edges will be skipped silently. Set "
                            "`destination_to_platform_instance.<id>.database` "
                            "(and `platform` if not auto-detected) to fix."
                        ),
                        context=(
                            f"connector_id={connector.connector_id}, "
                            f"destination_id={connector.destination_id}, "
                            f"table={lineage.destination_table}"
                        ),
                        exc=e,
                    )
                continue
            output_dataset_urn_list.append(output_dataset_urn)

            if self.config.include_column_lineage:
                for column_lineage in lineage.column_lineage:
                    fine_grained_lineage.append(
                        FineGrainedLineage(
                            upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                            upstreams=(
                                [
                                    builder.make_schema_field_urn(
                                        str(input_dataset_urn),
                                        column_lineage.source_column,
                                    )
                                ]
                                if input_dataset_urn
                                else []
                            ),
                            downstreamType=FineGrainedLineageDownstreamType.FIELD,
                            downstreams=(
                                [
                                    builder.make_schema_field_urn(
                                        str(output_dataset_urn),
                                        column_lineage.destination_column,
                                    )
                                ]
                                if output_dataset_urn
                                else []
                            ),
                        )
                    )

        datajob.set_inlets(input_dataset_urn_list)
        datajob.set_outlets(output_dataset_urn_list)
        datajob.set_fine_grained_lineages(fine_grained_lineage)

        return dict(
            **{
                f"source.{k}": str(v)
                for k, v in source_details.model_dump().items()
                if v is not None and not isinstance(v, bool)
            },
            **{
                f"destination.{k}": str(v)
                for k, v in destination_details.model_dump().items()
                if v is not None and not isinstance(v, bool)
            },
        )

    @staticmethod
    def build_destination_urn(
        destination_table: str,
        destination_details: PlatformDetail,
    ) -> DatasetUrn:
        """Construct the destination dataset URN for one lineage edge.

        Relational warehouses (snowflake / bigquery / databricks) emit a
        three-part `<database>.<schema>.<table>` URN. Iceberg-style platforms
        (used by REST-discovered Fivetran Managed Data Lake destinations) use
        a two-part `<schema>.<table>` URN with no database prefix — matching
        DataHub's iceberg source convention so URNs align if the same
        catalog is also ingested directly.

        Glue-routed Managed Data Lake destinations are treated as
        relational: the URN is composed as
        `glue.<destination_details.database>.<schema>.<table>`. The user
        is responsible for setting `database` to the actual Glue database
        name observed in their Glue console (Fivetran shares one Glue
        database per region across all destinations and the REST API
        does not expose this name) and for verifying that Fivetran's
        Glue tables are literally named `<schema>.<table>` so the URN
        aligns with DataHub's Glue source. If the Glue catalog uses a
        different table-name convention, the URN won't match and the
        customer must configure their Glue source accordingly.

        For path-style routed Managed Data Lake destinations
        (`platform: s3 | gcs | abs`), the URN follows DataHub's
        corresponding object-storage source convention
        `urn:li:dataset:(<platform>, <prefix>/<schema>/<table>, env)`. The
        prefix path is taken from `destination_details.database`
        (populated by REST discovery — `<bucket>/<prefix>` for AWS/GCS,
        `<storage_account>/<container>/<prefix>` for ADLS) and the
        schema/table path uses `/` instead of `.` so the URN matches the
        on-disk layout.
        """
        if destination_details.platform is None:
            raise ValueError(
                "destination_details.platform must be set before constructing "
                "the destination URN."
            )
        table_name = (
            destination_table
            if destination_details.include_schema_in_urn
            else destination_table.split(".", 1)[1]
        )
        # Iceberg-family platforms have a two-part namespace; no `database`
        # prefix in the URN. REST-discovered MDL destinations land here.
        if destination_details.platform == "iceberg":
            return DatasetUrn.create_from_ids(
                platform_id="iceberg",
                table_name=table_name,
                env=destination_details.env,
                platform_instance=destination_details.platform_instance,
            )
        # Path-style MDL backings: emit a path URN aligned with DataHub's
        # object-storage sources. The bucket-or-container path lives in
        # `database` (set by `apply_discovered_destination` from the REST
        # destination config), and `<schema>.<table>` is converted to
        # `<schema>/<table>` so the URN reflects the on-disk layout.
        if (
            destination_details.platform
            in FivetranSource._PATH_STYLE_DESTINATION_PLATFORMS
        ):
            if not destination_details.database:
                raise ValueError(
                    f"destination_details.database must be set with the "
                    f"{destination_details.platform} path prefix "
                    f"(e.g., `<bucket>/<prefix_path>` for s3/gcs or "
                    f"`<storage_account>/<container>/<prefix_path>` for abs) "
                    f"before constructing a path-style destination URN. "
                    f"Either ensure REST discovery succeeded for this "
                    f"destination or set `database` explicitly via "
                    f"`destination_to_platform_instance`."
                )
            object_table_path = table_name.replace(".", "/")
            return DatasetUrn.create_from_ids(
                platform_id=destination_details.platform,
                table_name=f"{destination_details.database.rstrip('/')}/{object_table_path}",
                env=destination_details.env,
                platform_instance=destination_details.platform_instance,
            )
        # Glue (and any other URN platform that ends up here): fall through
        # to the relational `<platform>.<database>.<schema>.<table>` shape.
        # For Glue specifically, this is the honest minimum: Fivetran's REST
        # API doesn't expose the Glue database name and the Fivetran docs
        # don't document the Glue-table-name convention, so we let the user
        # supply `database` explicitly and trust Fivetran's lineage record's
        # `<schema>.<table>` matches the Glue table name verbatim. If the
        # customer's Glue catalog uses a different table-name convention,
        # the URN won't align — they must override per-destination.
        destination_database_for_urn = destination_details.database_for_urn
        if destination_database_for_urn is None:
            raise ValueError(
                "destination_details.database must be set before constructing "
                "the destination URN."
            )
        return DatasetUrn.create_from_ids(
            platform_id=destination_details.platform,
            table_name=f"{destination_database_for_urn}.{table_name}",
            env=destination_details.env,
            platform_instance=destination_details.platform_instance,
        )

    def resolve_destination_details(self, destination_id: str) -> PlatformDetail:
        """Single source of truth for a destination's PlatformDetail.

        Combines (in order of precedence):
          1. Declarative override from `destination_to_platform_instance`.
          2. REST-discovered details (whenever `api_config` is provided).
          3. The connector-level default `fivetran_log_config.destination_platform`.

        Discovery runs unconditionally when an API client is configured —
        per-field merge inside `apply_discovered_destination` already
        preserves user overrides, and the API client caches successful
        responses, so each unique destination ID issues at most one HTTP
        call per ingest. Discovery failures emit a structured warning on
        `self.report` and fall through to (3); they do not crash the ingest.

        Specifically, an over-ridden `platform` (e.g. `s3`) still triggers
        discovery so that REST can populate `database` (bucket/prefix_path)
        when the user hasn't supplied one.
        """
        base = self.config.destination_to_platform_instance.get(
            destination_id, PlatformDetail()
        )

        # `_failed_destination_ids` short-circuits the call for destinations
        # that already failed once this ingest — `FivetranAPIClient`'s own
        # cache only stores successes, so without this every connector on a
        # broken destination would re-issue the API call.
        needs_discovery = (
            self.api_client is not None
            and destination_id not in self._failed_destination_ids
        )
        if needs_discovery:
            try:
                assert self.api_client is not None
                discovered = self.api_client.get_destination_details_by_id(
                    destination_id
                )
                base = FivetranSource.apply_discovered_destination(base, discovered)
            except (
                requests.RequestException,
                pydantic.ValidationError,
                ValueError,
            ) as e:
                self._failed_destination_ids.add(destination_id)
                self.report.warning(
                    title="Destination discovery failed",
                    message=(
                        "Could not fetch destination details from the Fivetran "
                        "REST API; falling back to the configured default "
                        "destination_platform. Subsequent connectors on this "
                        "destination will skip discovery for the rest of the "
                        "ingest. Set `destination_to_platform_instance` to "
                        "override explicitly for this destination."
                    ),
                    context=f"destination_id={destination_id}",
                    exc=e,
                )

        if base.platform is None and self.config.fivetran_log_config is not None:
            # In rest_api mode the log config (and therefore the default
            # destination_platform) is absent; rely on REST discovery or an
            # explicit `destination_to_platform_instance` override instead.
            base = base.model_copy(
                update={
                    "platform": self.config.fivetran_log_config.destination_platform
                }
            )
        if (
            base.database is None
            and base.platform not in self._NON_RELATIONAL_DESTINATION_PLATFORMS
            and isinstance(self.log_reader, FivetranLogDbReader)
        ):
            # `fivetran_log_database` is only meaningful in DB mode — it's
            # the destination warehouse the Fivetran log lives in (Snowflake
            # database, BigQuery project_id resolved via `SELECT @@project_id`,
            # or Databricks catalog). It's only useful as a fallback for
            # three-part `<database>.<schema>.<table>` URNs; lakehouse-style
            # platforms (iceberg / glue / s3) don't use a `database` segment
            # in their URN shape and would emit a wrong URN with this
            # populated. The isinstance check is the honest expression of
            # this mode-specific semantics — what used to be a Protocol
            # property returning `""` in REST mode.
            base = base.model_copy(
                update={"database": self.log_reader.fivetran_log_database}
            )
        return base

    @staticmethod
    def apply_discovered_destination(
        base: PlatformDetail,
        discovered: FivetranDestinationDetails,
    ) -> PlatformDetail:
        """Enrich `base` with fields derived from a REST-discovered destination.

        Rules:
        - Declarative fields on `base` always win (user override is authoritative).
        - For relational services we know about, set `platform` and `database`
          from the discovered config.
        - For Managed Data Lake destinations, the platform is resolved as:
          (1) user override on `base.platform` (always wins) →
          (2) `glue` if the user supplied `base.database` (a database
          name only makes sense for Glue routing among MDL platforms;
          iceberg/s3/gcs/abs would silently drop it) →
          (3) auto-detect from MDL config toggles (`glue` when
          `should_maintain_tables_in_glue` is set) →
          (4) `iceberg` fallback (the modern Polaris / Iceberg REST
          default). For `glue`, the user must still supply `database`
          themselves — the Fivetran REST API doesn't expose the actual
          Glue database name, so auto-detect picks the platform but the
          customer pins the database. When the resolved platform is
          path-style (`s3` / `gcs` / `abs`), `database` is auto-populated
          from the appropriate fields in the discovered MDL config; the
          same `service: managed_data_lake` covers AWS, GCS, and ADLS
          Gen2, distinguished by which fields are populated. The user's
          `database` override always wins.
        - For services we don't know about, return `base` unchanged. The caller
          should log a structured warning so the user knows discovery didn't
          help for that destination.
        """
        if discovered.service == "managed_data_lake":
            # Precedence: user override → user-database-implies-glue →
            # MDL-toggle auto-detect → iceberg fallback. Treating
            # `base.database` as a glue-intent signal protects users from
            # the silent foot-gun where they set `database` (intending
            # Glue routing) on a destination whose `should_maintain_tables_in_glue`
            # toggle isn't set — without this, we'd fall through to
            # iceberg and quietly drop their `database`.
            resolved_platform = (
                base.platform
                or ("glue" if base.database is not None else None)
                or FivetranSource._detect_default_mdl_platform(discovered.config)
                or "iceberg"
            )
            if resolved_platform in FivetranSource._PATH_STYLE_DESTINATION_PLATFORMS:
                # Stash the bucket-or-container path in `database` so
                # `build_destination_urn` can prepend it to the
                # schema/table path. Reuses `database` rather than adding
                # a new PlatformDetail field — semantically it's the
                # URN-prefix part before schema/table for both relational
                # and path-style platforms.
                path_prefix = FivetranSource._derive_path_prefix(
                    resolved_platform, discovered.config
                )
                return base.model_copy(
                    update={
                        "platform": resolved_platform,
                        "database": base.database or path_prefix,
                    }
                )
            return base.model_copy(update={"platform": resolved_platform})

        mapping = FivetranSource._RELATIONAL_SERVICE_MAP.get(discovered.service)
        if mapping is None:
            # Unknown service: don't synthesize a URN platform from the raw
            # service string — that would emit junk like
            # `urn:li:dataPlatform:aurora_postgres_warehouse_v2`. Leave
            # `base.platform` untouched (None unless the user overrode it);
            # `build_destination_urn` will raise and the caller logs a
            # once-per-destination warning telling the user to set
            # `destination_to_platform_instance.<id>.platform` explicitly.
            # We still mirror `discovered.config.database` because the user
            # may want it once they pin a platform.
            return base.model_copy(
                update={
                    "database": base.database or discovered.config.database,
                }
            )

        platform, database_attr = mapping
        discovered_database = getattr(discovered.config, database_attr, None)

        return base.model_copy(
            update={
                "platform": base.platform or platform,
                "database": base.database or discovered_database,
            }
        )

    @staticmethod
    def _detect_default_mdl_platform(
        config: FivetranDestinationConfig,
    ) -> Optional[str]:
        """Pick a sensible default URN platform from MDL config toggles.

        Used only when the user hasn't pinned `platform` on the
        `destination_to_platform_instance` entry. Returns `None` to fall
        through to the generic `iceberg` default.

        `should_maintain_tables_in_glue` triggers an auto-default to
        `glue`. The auto-detect picks the URN platform; the customer
        still must supply `database` (the actual Glue database name from
        their AWS Glue console — the REST API doesn't expose it). Until
        they do, `build_destination_urn` raises a structured ValueError
        and the caller skips the lineage edge with a once-per-destination
        warning.

        The other catalog toggles (`should_maintain_tables_in_bqms` for
        BigQuery Metastore, `should_maintain_tables_in_one_lake` for
        Microsoft OneLake) intentionally don't auto-default because
        DataHub has no native source for those catalogs.
        """
        if config.should_maintain_tables_in_glue:
            return "glue"
        return None

    @staticmethod
    def _derive_path_prefix(
        platform: str,
        config: FivetranDestinationConfig,
    ) -> Optional[str]:
        """Compose the URN-prefix path for a path-style destination platform.

        Fivetran's `managed_data_lake` config exposes three cloud backings
        under the same `service`. Each has different fields holding the
        bucket-or-container path:

        - `s3`  → `<bucket>/<prefix_path>` (AWS)
        - `gcs` → `<bucket>/<prefix_path>` (same `bucket` field as AWS;
          Fivetran distinguishes via `gcs_project_id` on the request side)
        - `abs` → `<storage_account_name>/<container_name>/<prefix_path>`
          (ADLS Gen2 — uses storage account + container instead of a bucket)

        Returns `None` when none of the relevant fields are populated;
        the caller leaves `database` unset and `build_destination_urn`
        will raise a structured ValueError that the lineage emit-path
        catches and skips with a warning.
        """
        parts: Tuple[Optional[str], ...]
        if platform in ("s3", "gcs"):
            parts = (config.bucket, config.prefix_path)
        elif platform == "abs":
            parts = (
                config.storage_account_name,
                config.container_name,
                config.prefix_path,
            )
        else:
            return None
        non_empty = [p for p in parts if p]
        return "/".join(non_empty) if non_empty else None

    def _generate_dataflow_from_connector(self, connector: Connector) -> DataFlow:
        return DataFlow(
            platform=Constant.ORCHESTRATOR,
            name=connector.connector_id,
            env=self.config.env,
            display_name=connector.connector_name,
            platform_instance=self.config.platform_instance,
        )

    def _generate_datajob_from_connector(self, connector: Connector) -> DataJob:
        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator=Constant.ORCHESTRATOR,
            flow_id=connector.connector_id,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )
        owner_email = self.log_reader.get_user_email(connector.user_id)
        datajob = DataJob(
            name=connector.connector_id,
            flow_urn=dataflow_urn,
            platform_instance=self.config.platform_instance,
            display_name=connector.connector_name,
            owners=[CorpUserUrn(owner_email)] if owner_email else None,
        )

        # Map connector source and destination table with dataset entity
        # Also extend the fine grained lineage of column if include_column_lineage is True
        lineage_properties = self._extend_lineage(connector=connector, datajob=datajob)
        # TODO: Add fine grained lineages of dataset after FineGrainedLineageDownstreamType.DATASET enabled

        connector_properties: Dict[str, str] = {
            "connector_id": connector.connector_id,
            "connector_type": connector.connector_type,
            "paused": str(connector.paused),
            "sync_frequency": str(connector.sync_frequency),
            "destination_id": connector.destination_id,
        }

        datajob.set_custom_properties({**connector_properties, **lineage_properties})

        return datajob

    def _generate_dpi_from_job(self, job: Job, datajob: DataJob) -> DataProcessInstance:
        # hack: convert to old instance for DataProcessInstance.from_datajob compatibility
        datajob_v1 = DataJobV1(
            id=datajob.name,
            flow_urn=datajob.flow_urn,
            platform_instance=self.config.platform_instance,
            name=datajob.name,
            inlets=datajob.inlets,
            outlets=datajob.outlets,
            fine_grained_lineages=datajob.fine_grained_lineages,
        )
        return DataProcessInstance.from_datajob(
            datajob=datajob_v1,
            id=job.job_id,
            clone_inlets=True,
            clone_outlets=True,
        )

    def _get_dpi_workunits(
        self, job: Job, dpi: DataProcessInstance
    ) -> Iterable[MetadataWorkUnit]:
        status_result_map: Dict[str, InstanceRunResult] = {
            Constant.SUCCESSFUL: InstanceRunResult.SUCCESS,
            Constant.FAILURE_WITH_TASK: InstanceRunResult.FAILURE,
            Constant.CANCELED: InstanceRunResult.SKIPPED,
        }
        if job.status not in status_result_map:
            logger.debug(
                f"Status should be either SUCCESSFUL, FAILURE_WITH_TASK or CANCELED and it was "
                f"{job.status}"
            )
            return
        result = status_result_map[job.status]
        start_timestamp_millis = job.start_time * 1000
        for mcp in dpi.generate_mcp(
            created_ts_millis=start_timestamp_millis, materialize_iolets=False
        ):
            yield mcp.as_workunit()
        for mcp in dpi.start_event_mcp(start_timestamp_millis):
            yield mcp.as_workunit()
        for mcp in dpi.end_event_mcp(
            end_timestamp_millis=job.end_time * 1000,
            result=result,
            result_type=Constant.ORCHESTRATOR,
        ):
            yield mcp.as_workunit()

    def _get_connector_workunits(
        self, connector: Connector
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        self.report.report_connectors_scanned()

        # Google Sheets is not yet a native DataHub source platform, so
        # the Fivetran source emits Dataset entities for the sheet and
        # named range as a workaround. The handler encapsulates that
        # logic so it can be deleted in one place once native support
        # lands. See google_sheets_handler.py.
        if self.gsheets_handler.applies_to(connector.connector_type):
            yield from self.gsheets_handler.emit_workunits(connector)

        # Create dataflow entity with same name as connector name
        dataflow = self._generate_dataflow_from_connector(connector)
        yield dataflow

        # Map Fivetran's connector entity with Datahub's datajob entity
        datajob = self._generate_datajob_from_connector(connector)
        yield datajob

        # Map Fivetran's job/sync history entity with Datahub's data process entity
        max_jobs = self.config.max_jobs_per_connector
        if len(connector.jobs) >= max_jobs:
            self.report.warning(
                title="Not all sync history was captured",
                message=f"The connector had more than {max_jobs} sync runs in the past {self.config.history_sync_lookback_period} days. "
                f"Only the most recent {max_jobs} syncs were ingested.",
                context=f"{connector.connector_name} (connector_id: {connector.connector_id})",
            )
        for job in connector.jobs:
            dpi = self._generate_dpi_from_job(job, datajob)
            yield from self._get_dpi_workunits(job, dpi)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        """
        Datahub Ingestion framework invoke this method
        """
        logger.info("Fivetran plugin execution is started")
        connectors = self.log_reader.get_allowed_connectors_list(
            self.config.connector_patterns,
            self.config.destination_patterns,
            self.config.history_sync_lookback_period,
        )
        for connector in connectors:
            logger.info(f"Processing connector id: {connector.connector_id}")
            yield from self._get_connector_workunits(connector)

    def get_report(self) -> SourceReport:
        return self.report
