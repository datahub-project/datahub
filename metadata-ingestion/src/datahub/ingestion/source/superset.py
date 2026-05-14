import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import dateutil.parser as dp
import requests
import sqlglot
from pydantic import BaseModel, ConfigDict, field_validator, model_validator
from pydantic.fields import Field
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern, TransparentSecretStr
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.emitter.mce_builder import (
    make_chart_urn,
    make_dashboard_urn,
    make_data_platform_urn,
    make_dataset_urn,
    make_dataset_urn_with_platform_instance,
    make_domain_urn,
    make_schema_field_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_domain_to_entity_wu
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_types import resolve_sql_type
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    ChangeAuditStamps,
    InputField,
    InputFields,
    Status,
    TimeStamp,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    ChartSnapshot,
    DashboardSnapshot,
    DatasetSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BooleanTypeClass,
    DateTypeClass,
    MySqlDDL,
    NullType,
    NullTypeClass,
    NumberTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChartInfoClass,
    ChartTypeClass,
    DashboardInfoClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    create_lineage_sql_parsed_result,
)
from datahub.utilities import config_clean
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.registries.domain_registry import DomainRegistry
from datahub.utilities.threaded_iterator_executor import ThreadedIteratorExecutor

logger = logging.getLogger(__name__)

PAGE_SIZE = 25

# Retry configuration constants
RETRY_MAX_TIMES = 3
RETRY_STATUS_CODES = [429, 500, 502, 503, 504]
RETRY_BACKOFF_FACTOR = 1
RETRY_ALLOWED_METHODS = ["GET"]
CONNECTION_POOL_BUFFER = (
    10  # Extra connections beyond max_threads for concurrent requests
)


chart_type_from_viz_type = {
    "line": ChartTypeClass.LINE,
    "big_number": ChartTypeClass.LINE,
    "table": ChartTypeClass.TABLE,
    "dist_bar": ChartTypeClass.BAR,
    "area": ChartTypeClass.AREA,
    "bar": ChartTypeClass.BAR,
    "pie": ChartTypeClass.PIE,
    "histogram": ChartTypeClass.HISTOGRAM,
    "big_number_total": ChartTypeClass.LINE,
    "dual_line": ChartTypeClass.LINE,
    "line_multi": ChartTypeClass.LINE,
    "treemap": ChartTypeClass.AREA,
    "box_plot": ChartTypeClass.BAR,
}

platform_without_databases = ["druid"]

FIELD_TYPE_MAPPING = {
    "INT": NumberTypeClass,
    "STRING": StringTypeClass,
    "FLOAT": NumberTypeClass,
    "DATETIME": DateTypeClass,
    "TIMESTAMP": TimeTypeClass,
    "BOOLEAN": BooleanTypeClass,
    "SQL": StringTypeClass,
    "NUMERIC": NumberTypeClass,
    "TEXT": StringTypeClass,
}


def _extract_chart_id_with_status(
    position_value: Any,
) -> Tuple[Optional[Union[int, str]], bool]:
    """Return ``(chart_id, was_uncoercible)`` for a position_json CHART-* entry.

    ``was_uncoercible`` is True only when ``meta.chartId`` was present but
    had a type the caller can't turn into a chart URN. Callers tick
    ``num_chart_ids_uncoercible`` in that case.

    chartId is accepted as int or non-empty str (some deployments emit
    int, others str). bool is rejected explicitly because
    ``isinstance(False, int)`` is True and would otherwise mint
    ``urn:li:chart:(preset,False)``.
    """
    if not isinstance(position_value, dict):
        return None, False
    meta = position_value.get("meta")
    if not isinstance(meta, dict):
        return None, False
    if "chartId" not in meta:
        return None, False
    chart_id = meta["chartId"]
    if chart_id is None:
        return None, False
    if isinstance(chart_id, bool):
        return None, True
    if isinstance(chart_id, int):
        return chart_id, False
    if isinstance(chart_id, str):
        # Reject empty / whitespace-only: ``int("   ")`` raises and would
        # otherwise mint ``urn:li:chart:(preset,   )`` with embedded spaces.
        if not chart_id.strip():
            return None, True
        return chart_id, False
    return None, True


def _extract_chart_id(position_value: Any) -> Optional[Union[int, str]]:
    chart_id, _ = _extract_chart_id_with_status(position_value)
    return chart_id


def _lookup_processed_chart(
    chart_id: Optional[Union[int, str]],
    processed_charts: Dict[int, Tuple[Optional[str], bool]],
) -> Optional[Tuple[Optional[str], bool]]:
    """Look up a chart in ``processed_charts`` tolerating int/str chart IDs.

    ``processed_charts`` is int-keyed but chart IDs reach us as int (Preset
    Cloud), str (older Superset), or str (synthesised by the /charts
    fallback). Coerce to int; anything that won't coerce can't be a real
    Preset chart ID, so treat as missing.
    """
    if chart_id is None:
        return None
    try:
        normalised = int(chart_id)
    except (TypeError, ValueError):
        return None
    return processed_charts.get(normalised)


@dataclass
class SupersetSourceReport(StaleEntityRemovalSourceReport):
    filtered: LossyList[str] = field(default_factory=LossyList)
    num_dashboards_with_no_charts: int = 0
    num_dashboards_invalid_position_json: int = 0
    num_dashboards_missing_position_json: int = 0
    num_dashboards_detail_api_failures: int = 0
    num_dashboards_dropped_unexpected_error: int = 0
    num_dashboards_missing_id: int = 0
    num_dashboards_recovered_via_charts_endpoint: int = 0
    # Fallback returned chart IDs we never ingested as chart entities;
    # the lineage edge dangles.
    num_dashboards_charts_unknown: int = 0
    num_dashboards_charts_api_failures: int = 0
    num_dashboards_charts_malformed_entries: int = 0
    num_datasets_detail_api_failures: int = 0
    num_datasets_missing_id: int = 0
    # CHART-* entries whose meta.chartId was present but not a valid int/
    # non-empty str. Dropped silently because the caller can't recover;
    # non-zero flags upstream API contract drift.
    num_chart_ids_uncoercible: int = 0
    # Charts whose datasource_id was missing/falsy when we tried to fetch
    # column-level lineage. Each tick = one chart with no CLL emitted.
    num_charts_missing_datasource_id_for_cll: int = 0
    # Charts whose dataset detail came back empty/failed when we tried
    # to fetch CLL — distinguishes "API failed" from "dataset has no
    # columns" at the chart level (the underlying dataset failure is
    # already counted on the dataset counters).
    num_charts_missing_dataset_detail_for_cll: int = 0
    # Charts where the URN-construction path had no datasource_id (None
    # or 0). Some Superset chart types (markdown, header) legitimately
    # have no datasource, so this is informational; non-zero combined
    # with non-zero ``num_charts_missing_datasource_id_for_cll`` flags
    # the same charts also losing CLL.
    num_charts_without_datasource_for_urn: int = 0
    # ``params`` field on a chart was not valid JSON. Custom properties
    # derived from params (metrics, filters, dimensions) are skipped for
    # that chart; the chart itself still ingests.
    num_charts_invalid_params_json: int = 0
    # Chart had a non-falsy datasource_id but the dataset detail payload
    # was missing required fields (e.g. ``table_name`` or ``database_id``).
    # The chart still ingests with no input dataset URN; downstream chart
    # → dataset lineage is lost for that chart.
    num_charts_dropped_datasource_urn_failed: int = 0
    # ``params.metrics`` came back as a non-list shape (e.g. dict, str).
    # Custom property derivation skipped; chart itself still ingests.
    num_charts_invalid_params_metrics_shape: int = 0
    # Chart had a datasource_id but the dataset detail did not yield a
    # database_name, so ``database_pattern`` could not be evaluated. The
    # chart is recorded as not-filtered to keep dashboards complete, but
    # operators should know the filter check was effectively skipped.
    num_charts_database_pattern_indeterminate: int = 0
    # Dataset detail payload was structurally valid but ``columns`` or
    # ``metrics`` was not a list; treated as empty for CLL purposes.
    num_datasets_columns_payload_malformed: int = 0
    num_datasets_metrics_payload_malformed: int = 0
    # Columns / metrics dropped from CLL because the entry had no name
    # or no type in the Superset payload.
    num_datasets_columns_dropped_malformed: int = 0
    num_datasets_metrics_dropped_malformed: int = 0
    # SQL expressions in metric definitions that sqlglot failed to parse;
    # the expression is dropped from the metric's column list.
    num_metrics_sql_parse_errors: int = 0

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)


class SupersetDataset(BaseModel):
    id: int
    table_name: str
    changed_on_utc: Optional[str] = None
    explore_url: Optional[str] = ""
    description: Optional[str] = ""

    @property
    def modified_dt(self) -> Optional[datetime]:
        if self.changed_on_utc:
            return dp.parse(self.changed_on_utc)
        return None

    @property
    def modified_ts(self) -> Optional[int]:
        if self.modified_dt:
            return int(self.modified_dt.timestamp() * 1000)
        return None


class SupersetConfig(
    StatefulIngestionConfigBase, EnvConfigMixin, PlatformInstanceConfigMixin
):
    # TODO: Add support for missing dataPlatformInstance/containers
    # See the Superset /security/login endpoint for details
    # https://superset.apache.org/docs/rest-api
    connect_uri: str = Field(
        default="http://localhost:8088", description="Superset host URL."
    )
    display_uri: Optional[str] = Field(
        default=None,
        description="optional URL to use in links (if `connect_uri` is only for ingestion)",
    )
    domain: Dict[str, AllowDenyPattern] = Field(
        default=dict(),
        description="Regex patterns for tables to filter to assign domain_key. ",
    )
    dataset_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for dataset to filter in ingestion.",
    )
    chart_pattern: AllowDenyPattern = Field(
        AllowDenyPattern.allow_all(),
        description="Patterns for selecting chart names that are to be included",
    )
    dashboard_pattern: AllowDenyPattern = Field(
        AllowDenyPattern.allow_all(),
        description="Patterns for selecting dashboard names that are to be included",
    )
    database_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for databases to filter in ingestion.",
    )
    username: Optional[str] = Field(default=None, description="Superset username.")
    password: Optional[TransparentSecretStr] = Field(
        default=None, description="Superset password."
    )
    # Configuration for stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description="Superset Stateful Ingestion Config."
    )
    ingest_dashboards: bool = Field(
        default=True, description="Enable to ingest dashboards."
    )
    ingest_charts: bool = Field(default=True, description="Enable to ingest charts.")
    ingest_datasets: bool = Field(
        default=False, description="Enable to ingest datasets."
    )

    provider: str = Field(default="db", description="Superset provider.")
    options: Dict = Field(default={}, description="")

    timeout: int = Field(
        default=10, description="Timeout of single API call to superset."
    )

    max_threads: int = Field(
        default_factory=lambda: os.cpu_count() or 40,
        description="Max parallelism for API calls. Defaults to cpuCount or 40",
    )

    # TODO: Check and remove this if no longer needed.
    # Config database_alias is removed from sql sources.
    database_alias: Dict[str, str] = Field(
        default={},
        description="Can be used to change mapping for database names in superset to what you have in datahub",
    )

    model_config = ConfigDict(
        extra="allow"  # This is required to allow preset configs to get parsed
    )

    @field_validator("connect_uri", "display_uri", mode="after")
    @classmethod
    def remove_trailing_slash(cls, v: str) -> str:
        return config_clean.remove_trailing_slashes(v)

    @model_validator(mode="after")
    def default_display_uri_to_connect_uri(self) -> "SupersetConfig":
        if self.display_uri is None:
            self.display_uri = self.connect_uri
        return self


def get_metric_name(metric):
    if not metric:
        return ""
    if isinstance(metric, str):
        return metric
    if not isinstance(metric, dict):
        return ""
    label = metric.get("label")
    if not label:
        return ""
    return label


def get_filter_name(filter_obj):
    if not isinstance(filter_obj, dict):
        return ""
    sql_expression = filter_obj.get("sqlExpression")
    if sql_expression:
        return sql_expression

    clause = filter_obj.get("clause")
    column = filter_obj.get("subject")
    operator = filter_obj.get("operator")
    comparator = filter_obj.get("comparator")
    if clause is None or column is None or operator is None:
        return ""
    return f"{clause} {column} {operator} {comparator}"


@platform_name("Superset")
@config_class(SupersetConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(
    SourceCapability.DELETION_DETECTION, "Enabled by default via stateful ingestion"
)
@capability(SourceCapability.DOMAINS, "Enabled by `domain` config to assign domain_key")
@capability(SourceCapability.LINEAGE_COARSE, "Supported by default")
@capability(SourceCapability.TAGS, "Supported by default")
class SupersetSource(StatefulIngestionSourceBase):
    """
    This plugin extracts the following:
    - Charts, dashboards, and associated metadata

    See documentation for superset's /security/login at https://superset.apache.org/docs/rest-api for more details on superset's login api.
    """

    config: SupersetConfig
    report: SupersetSourceReport
    platform = "superset"

    def __hash__(self):
        return id(self)

    def __init__(self, ctx: PipelineContext, config: SupersetConfig):
        super().__init__(config, ctx)
        self.config = config
        self.report = SupersetSourceReport()
        if self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[domain_id for domain_id in self.config.domain],
                graph=self.ctx.graph,
            )
        self.session = self.login()
        self.owner_info = self.parse_owner_info()
        self.filtered_dataset_to_database: Dict[int, str] = {}
        self.filtered_chart_to_database: Dict[int, str] = {}
        self.processed_charts: Dict[int, Tuple[Optional[str], bool]] = {}

        self._dashboard_info_cache: Dict[int, Dict[str, Any]] = {}
        self._dashboard_charts_cache: Dict[int, List[int]] = {}
        self._dataset_info_cache: Dict[int, Dict[str, Any]] = {}

    def login(self) -> requests.Session:
        login_response = requests.post(
            f"{self.config.connect_uri}/api/v1/security/login",
            json={
                "username": self.config.username,
                "password": self.config.password.get_secret_value()
                if self.config.password
                else None,
                "refresh": True,
                "provider": self.config.provider,
            },
        )

        self.access_token = login_response.json()["access_token"]
        logger.debug("Got access token from superset")

        requests_session = requests.Session()

        # Configure retry strategy for transient failures
        retry_strategy = Retry(
            total=RETRY_MAX_TIMES,
            status_forcelist=RETRY_STATUS_CODES,
            backoff_factor=RETRY_BACKOFF_FACTOR,
            allowed_methods=RETRY_ALLOWED_METHODS,
            raise_on_status=False,
        )
        # Set connection pool size to match max_threads to avoid "Connection pool is full" warnings
        pool_size = self.config.max_threads + CONNECTION_POOL_BUFFER
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=pool_size,
            pool_maxsize=pool_size,
        )
        requests_session.mount("http://", adapter)
        requests_session.mount("https://", adapter)

        requests_session.headers.update(
            {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
                "Accept": "*/*",
            }
        )

        test_response = requests_session.get(
            f"{self.config.connect_uri}/api/v1/dashboard/",
            timeout=self.config.timeout,
        )
        if test_response.status_code != 200:
            # throw an error and terminate ingestion,
            # cannot proceed without access token
            logger.error(
                f"Failed to log in to Superset with status: {test_response.status_code}"
            )
        return requests_session

    def paginate_entity_api_results(self, entity_type, page_size=100):
        current_page = 0
        total_items = page_size

        while current_page * page_size < total_items:
            response = self.session.get(
                f"{self.config.connect_uri}/api/v1/{entity_type}",
                params={"q": f"(page:{current_page},page_size:{page_size})"},
                timeout=self.config.timeout,
            )

            if response.status_code != 200:
                self.report.warning(
                    title="Failed to fetch data from Superset API",
                    message="Incomplete metadata extraction due to Superset API failure",
                    context=f"Entity Type: {entity_type}, HTTP Status Code: {response.status_code}, Page: {current_page}. Response: {response.text}",
                )
                # we stop pagination for this entity type and we continue the overall ingestion
                break

            payload = response.json()
            # Update total_items with the actual count from the response
            total_items = payload.get("count", total_items)
            # Yield each item in the result, this gets passed into the construct functions
            for item in payload.get("result", []):
                yield item

            current_page += 1

    def parse_owner_info(self) -> Dict[str, Any]:
        entity_types = ["dataset", "dashboard", "chart"]
        owners_info = {}

        for entity in entity_types:
            for owner in self.paginate_entity_api_results(f"{entity}/related/owners"):
                owner_id = owner.get("value")
                if owner_id:
                    owners_info[owner_id] = owner.get("extra", {}).get("email", "")

        return owners_info

    def build_owner_urn(self, data: Dict[str, Any]) -> List[str]:
        return [
            make_user_urn(self.owner_info.get(owner.get("id"), ""))
            for owner in data.get("owners", [])
            if owner.get("id")
        ]

    # Permissive ``something@something.something`` matcher for redacting
    # email addresses out of log previews. NOT an RFC validator.
    _EMAIL_REDACT_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")

    @staticmethod
    def _warning_context(**fields: Any) -> str:
        return json.dumps(fields, default=str, ensure_ascii=False)

    @staticmethod
    def _safe_body_preview(response: requests.Response) -> str:
        """Return a redacted, length-capped (~200 char) preview of a
        response body for log/warning context.

        Auth-redirect login pages embed cookies in HTML and proxy login
        pages sometimes serve HTML under ``Content-Type: application/json``,
        so we redact when EITHER the content type isn't JSON OR the body
        smells like HTML. Email addresses inside JSON bodies are also
        redacted — Preset error responses can include the offending user's
        email.
        """
        content_type = response.headers.get("Content-Type", "").lower()
        try:
            body = response.text
        except UnicodeDecodeError:
            return f"<undecodable body: content-type={content_type or 'unknown'}>"
        body_smells_html = (
            body.lstrip().startswith("<")
            or "<html" in body.lower()
            or "Set-Cookie" in body
        )
        if "json" not in content_type or body_smells_html:
            return f"<non-json body: content-type={content_type or 'unknown'}, len={len(body)}>"
        return SupersetSource._EMAIL_REDACT_RE.sub("<email>", body[:200])

    def _fetch_api_response(
        self,
        url: str,
        *,
        failure_counter_attr: str,
        base_context: Dict[str, Any],
        endpoint: str,
    ) -> Optional[Any]:
        """GET ``url`` and return the decoded JSON payload, or ``None`` on any
        transport or decode failure.

        On failure, increments ``self.report.<failure_counter_attr>`` and emits
        a structured warning. ``base_context`` fields appear in every warning
        context; the keys ``error``, ``error_type``, ``status_code``, and
        ``body_preview`` are reserved — caller-supplied values for those keys
        are silently overwritten. Warning titles follow ``"{endpoint} {type}"``
        (e.g. ``endpoint="Dashboard detail API"`` →
        ``"Dashboard detail API network error"``).

        Result-shape validation and caching are the caller's responsibility."""
        if not hasattr(self.report, failure_counter_attr):
            raise AttributeError(
                f"SupersetSourceReport has no counter '{failure_counter_attr}'"
            )
        try:
            response = self.session.get(url, timeout=self.config.timeout)
        except requests.RequestException as e:
            setattr(
                self.report,
                failure_counter_attr,
                getattr(self.report, failure_counter_attr) + 1,
            )
            self.report.warning(
                message="Network error.",
                context=self._warning_context(
                    **{**base_context, "error": str(e), "error_type": type(e).__name__}
                ),
                title=f"{endpoint} network error",
            )
            return None
        if response.status_code != 200:
            setattr(
                self.report,
                failure_counter_attr,
                getattr(self.report, failure_counter_attr) + 1,
            )
            self.report.warning(
                message="Unexpected HTTP status.",
                context=self._warning_context(
                    **{
                        **base_context,
                        "status_code": response.status_code,
                        "body_preview": self._safe_body_preview(response),
                    }
                ),
                title=f"{endpoint} failed",
            )
            return None
        try:
            return response.json()
        except ValueError as e:
            setattr(
                self.report,
                failure_counter_attr,
                getattr(self.report, failure_counter_attr) + 1,
            )
            self.report.warning(
                message="Response was not valid JSON.",
                context=self._warning_context(
                    **{
                        **base_context,
                        "error": str(e),
                        "body_preview": self._safe_body_preview(response),
                    }
                ),
                title=f"{endpoint} decode error",
            )
            return None

    def get_dashboard_info(self, dashboard_id: int) -> Dict[str, Any]:
        """Fetch dashboard detail. Returns ``{}`` on failure; failures are
        not cached so transient errors don't poison the rest of the run."""
        cached = self._dashboard_info_cache.get(dashboard_id)
        if cached is not None:
            return cached
        url = f"{self.config.connect_uri}/api/v1/dashboard/{dashboard_id}"
        payload = self._fetch_api_response(
            url,
            failure_counter_attr="num_dashboards_detail_api_failures",
            base_context={"dashboard_id": dashboard_id},
            endpoint="Dashboard detail API",
        )
        if payload is None:
            return {}
        # Reject and don't cache: result must be a dict for downstream
        # iteration; caching a malformed response would break every hit.
        if not isinstance(payload.get("result"), dict):
            self.report.num_dashboards_detail_api_failures += 1
            self.report.warning(
                message="Dashboard detail payload had no result object.",
                context=self._warning_context(
                    dashboard_id=dashboard_id,
                    result_type=type(payload.get("result")).__name__,
                ),
                title="Dashboard detail API malformed payload",
            )
            return {}
        self._dashboard_info_cache[dashboard_id] = payload
        return payload

    def get_dashboard_charts(self, dashboard_id: int) -> List[int]:
        """Fetch chart IDs attached to a dashboard via the /charts endpoint.

        Used as a fallback when ``position_json`` yields no chart references.
        Returns ``[]`` on any failure; failures are not cached.
        """
        cached = self._dashboard_charts_cache.get(dashboard_id)
        if cached is not None:
            return cached
        url = f"{self.config.connect_uri}/api/v1/dashboard/{dashboard_id}/charts"
        payload = self._fetch_api_response(
            url,
            failure_counter_attr="num_dashboards_charts_api_failures",
            base_context={"dashboard_id": dashboard_id},
            endpoint="Dashboard charts API",
        )
        if payload is None:
            return []
        results = payload.get("result")
        if not isinstance(results, list):
            self.report.num_dashboards_charts_api_failures += 1
            self.report.warning(
                message="/charts response had no result list.",
                context=self._warning_context(
                    dashboard_id=dashboard_id,
                    result_type=type(results).__name__,
                ),
                title="Dashboard charts API malformed result",
            )
            return []
        chart_ids: List[int] = []
        malformed = 0
        for chart in results:
            if not isinstance(chart, dict):
                malformed += 1
                continue
            raw_id = chart.get("id")
            # Apply the same rejection rules as _extract_chart_id_with_status:
            # bool must be rejected before int because isinstance(False, int)
            # is True in Python and would mint urn:li:chart:(preset,False).
            if (
                raw_id is None
                or isinstance(raw_id, bool)
                or not isinstance(raw_id, int)
            ):
                malformed += 1
                continue
            chart_ids.append(raw_id)
        if malformed:
            # Aggregate per dashboard so operators can identify which
            # dashboard had bad entries (the global counter alone gives no
            # locator).
            self.report.num_dashboards_charts_malformed_entries += malformed
            self.report.warning(
                message="/charts response contained malformed entries.",
                context=self._warning_context(
                    dashboard_id=dashboard_id,
                    malformed_count=malformed,
                    total_entries=len(results),
                ),
                title="Dashboard charts API malformed entries",
            )
        self._dashboard_charts_cache[dashboard_id] = chart_ids
        return chart_ids

    def _parse_position_json(self, dashboard_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse ``position_json`` defensively. Returns ``{}`` on any
        non-JSON-object input — the dashboard still ingests, the failure
        is visible in the report.

        Accepts an already-parsed dict in case an API client pre-deserialises
        the field. Catches TypeError so a truthy non-string value (list, int,
        …) doesn't escape the try block and crash the dashboard."""
        raw = dashboard_data.get("position_json") or "{}"
        if isinstance(raw, dict):
            return raw
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, TypeError) as e:
            self.report.num_dashboards_invalid_position_json += 1
            self.report.warning(
                message="Could not parse position_json.",
                context=self._warning_context(
                    dashboard_id=dashboard_data.get("id"),
                    dashboard_title=dashboard_data.get("dashboard_title", ""),
                    error=str(e),
                    value_preview=str(raw)[:200],
                ),
                title="Invalid position_json",
            )
            return {}
        if not isinstance(parsed, dict):
            self.report.num_dashboards_invalid_position_json += 1
            self.report.warning(
                message="position_json was valid JSON but not an object.",
                context=self._warning_context(
                    dashboard_id=dashboard_data.get("id"),
                    dashboard_title=dashboard_data.get("dashboard_title", ""),
                    json_type=type(parsed).__name__,
                    value_preview=str(raw)[:200],
                ),
                title="Invalid position_json",
            )
            return {}
        return parsed

    def get_dataset_info(self, dataset_id: Optional[int]) -> Dict[str, Any]:
        """Fetch dataset detail.

        Returns ``{}`` on any failure (missing id, network error, non-200,
        invalid JSON, non-dict ``result``); failures are deliberately not
        cached so a transient 5xx does not poison subsequent calls."""
        if dataset_id is None:
            self.report.num_datasets_missing_id += 1
            self.report.warning(
                message="Dataset entry has no id; cannot fetch detail.",
                title="Dataset missing id",
            )
            return {}
        cached = self._dataset_info_cache.get(dataset_id)
        if cached is not None:
            return cached
        url = f"{self.config.connect_uri}/api/v1/dataset/{dataset_id}"
        payload = self._fetch_api_response(
            url,
            failure_counter_attr="num_datasets_detail_api_failures",
            base_context={"dataset_id": dataset_id},
            endpoint="Dataset detail API",
        )
        if payload is None:
            return {}
        # Reject and don't cache: result must be a dict for downstream
        # iteration; caching a malformed response would break every hit.
        if not isinstance(payload.get("result"), dict):
            self.report.num_datasets_detail_api_failures += 1
            self.report.warning(
                message="Dataset detail payload had no result object.",
                context=self._warning_context(
                    dataset_id=dataset_id,
                    result_type=type(payload.get("result")).__name__,
                ),
                title="Dataset detail API malformed payload",
            )
            return {}
        self._dataset_info_cache[dataset_id] = payload
        return payload

    def get_datasource_urn_from_id(
        self, dataset_response: dict, platform_instance: str
    ) -> str:
        result = dataset_response.get("result", {})
        if not result:
            dataset_id = dataset_response.get("id", "unknown")
            raise ValueError(
                f"Could not construct dataset URN: empty result for dataset_id={dataset_id}"
            )

        schema_name = result.get("schema")
        table_name = result.get("table_name")
        database_id = result.get("database", {}).get("id")
        database_name = result.get("database", {}).get("database_name")
        database_name = self.config.database_alias.get(database_name, database_name)

        # Druid do not have a database concept and has a limited schema concept, but they are nonetheless reported
        # from superset. There is only one database per platform instance, and one schema named druid, so it would be
        # redundant to systemically store them both in the URN.
        if platform_instance in platform_without_databases:
            database_name = None

        if platform_instance == "druid" and schema_name == "druid":
            # Follow DataHub's druid source convention.
            schema_name = None

        # If the information about the datasource is already contained in the dataset response,
        # can just return the urn directly
        if table_name and database_id:
            return make_dataset_urn(
                platform=platform_instance,
                name=".".join(
                    name for name in [database_name, schema_name, table_name] if name
                ),
                env=self.config.env,
            )

        raise ValueError(
            f"Could not construct dataset URN: table_name={table_name}, database_id={database_id}, "
            f"dataset_id={result.get('id', 'unknown')}"
        )

    def construct_dashboard_from_api_data(
        self,
        dashboard_data: dict,
        position_data: Optional[Dict[str, Any]] = None,
    ) -> DashboardSnapshot:
        dashboard_urn = make_dashboard_urn(
            platform=self.platform,
            name=str(dashboard_data["id"]),
            platform_instance=self.config.platform_instance,
        )
        dashboard_snapshot = DashboardSnapshot(
            urn=dashboard_urn,
            aspects=[Status(removed=False)],
        )

        modified_actor = f"urn:li:corpuser:{self.owner_info.get((dashboard_data.get('changed_by') or {}).get('id', -1), 'unknown')}"
        now = datetime.now().strftime("%I:%M%p on %B %d, %Y")
        modified_ts = int(
            dp.parse(dashboard_data.get("changed_on_utc", now)).timestamp() * 1000
        )
        title = dashboard_data.get("dashboard_title", "")
        # note: the API does not currently supply created_by usernames due to a bug
        last_modified = AuditStampClass(time=modified_ts, actor=modified_actor)

        change_audit_stamps = ChangeAuditStamps(
            created=None, lastModified=last_modified
        )
        dashboard_url = f"{self.config.display_uri}{dashboard_data.get('url', '')}"

        if position_data is None:
            position_data = self._parse_position_json(dashboard_data)

        chart_urns = []
        for key, value in position_data.items():
            if not key.startswith("CHART-"):
                continue
            chart_id, was_uncoercible = _extract_chart_id_with_status(value)
            if was_uncoercible:
                self.report.num_chart_ids_uncoercible += 1
                self.report.info(
                    message="Dashboard position_json had a CHART-* entry with an uncoercible chartId; skipping.",
                    context=self._warning_context(
                        dashboard_id=dashboard_data.get("id"),
                        position_key=key,
                    ),
                    title="Uncoercible chart_id in position_json",
                )
            if chart_id is None:
                continue
            chart_urns.append(
                make_chart_urn(
                    platform=self.platform,
                    name=str(chart_id),
                    platform_instance=self.config.platform_instance,
                )
            )

        # Build properties
        custom_properties = {
            "Status": str(dashboard_data.get("status")),
            "IsPublished": str(dashboard_data.get("published", False)).lower(),
            "Owners": ", ".join(
                map(
                    lambda owner: self.owner_info.get(owner.get("id", -1), "unknown"),
                    dashboard_data.get("owners", []),
                )
            ),
            "IsCertified": str(bool(dashboard_data.get("certified_by"))).lower(),
        }

        if dashboard_data.get("certified_by"):
            custom_properties["CertifiedBy"] = dashboard_data.get("certified_by", "")
            custom_properties["CertificationDetails"] = str(
                dashboard_data.get("certification_details")
            )

        # Create DashboardInfo object
        dashboard_info = DashboardInfoClass(
            description="",
            title=title,
            charts=chart_urns,
            dashboardUrl=dashboard_url,
            customProperties=custom_properties,
            lastModified=change_audit_stamps,
        )
        dashboard_snapshot.aspects.append(dashboard_info)

        dashboard_owners_list = self.build_owner_urn(dashboard_data)
        owners_info = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=urn,
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                )
                for urn in (dashboard_owners_list or [])
            ],
            lastModified=last_modified,
        )
        dashboard_snapshot.aspects.append(owners_info)

        superset_tags = self._extract_and_map_tags(dashboard_data.get("tags", []))
        tags = self._merge_tags_with_existing(dashboard_urn, superset_tags)
        if tags:
            dashboard_snapshot.aspects.append(tags)

        return dashboard_snapshot

    def _report_filtered_chart_references(
        self,
        *,
        position_data: Dict[str, Any],
        dashboard_id: int,
        dashboard_title: str,
    ) -> None:
        """Surface charts whose dataset belongs to a database the user
        denied via ``database_pattern`` — recommends adding the dashboard
        to ``dashboard_pattern`` too."""
        for key, value in position_data.items():
            if not key.startswith("CHART-"):
                continue
            raw_chart_id = _extract_chart_id(value)
            if raw_chart_id is None:
                continue
            processed_entry = _lookup_processed_chart(
                raw_chart_id, self.processed_charts
            )
            if processed_entry is None:
                continue
            database_name, is_filtered = processed_entry
            if is_filtered:
                self.report.warning(
                    message="Dashboard contains charts using datasets from a filtered database. Set the dashboard pattern to deny ingestion.",
                    context=self._warning_context(
                        dashboard_id=dashboard_id,
                        dashboard_title=dashboard_title,
                        chart_id=raw_chart_id,
                        database_name=database_name,
                    ),
                    title="Incomplete Ingestion",
                )

    def _warn_dangling_fallback_charts(
        self,
        *,
        fallback_chart_id_strs: Set[str],
        dashboard_id: int,
        dashboard_title: str,
    ) -> None:
        """For every chart the /charts fallback recovered, warn when the
        chart was never ingested as an entity (lineage edge will dangle).
        Runs unconditionally — dangling lineage matters regardless of
        ``database_pattern``."""
        for chart_id_str in fallback_chart_id_strs:
            if _lookup_processed_chart(chart_id_str, self.processed_charts) is None:
                self.report.num_dashboards_charts_unknown += 1
                self.report.warning(
                    message=(
                        "Fallback recovered a chart that was not ingested "
                        "via the chart-listing path; lineage edge will dangle."
                    ),
                    context=self._warning_context(
                        dashboard_id=dashboard_id,
                        dashboard_title=dashboard_title,
                        chart_id=chart_id_str,
                    ),
                    title="Fallback chart not ingested",
                )

    def _process_dashboard(self, dashboard_data: Any) -> Iterable[MetadataWorkUnit]:
        # Initialised before the try so the outer except can build a
        # warning context even if early extraction raises.
        dashboard_id: Optional[int] = None
        dashboard_title: str = ""
        try:
            dashboard_id = dashboard_data.get("id")
            dashboard_title = dashboard_data.get("dashboard_title", "")
            if dashboard_id is None:
                # Without an ID we can't fetch detail, recover charts, or
                # build a URN. Bail with a dedicated counter so a 404 on
                # /dashboard/None doesn't masquerade as a detail-API failure.
                self.report.num_dashboards_missing_id += 1
                self.report.warning(
                    message="Dashboard entry has no id; dropping.",
                    context=self._warning_context(dashboard_title=dashboard_title),
                    title="Dashboard missing id",
                )
                return
            if not self.config.dashboard_pattern.allowed(dashboard_title):
                self.report.report_dropped(
                    f"Dashboard '{dashboard_title}' (id: {dashboard_id}) filtered by dashboard_pattern"
                )
                return

            # The list API doesn't return position_json; the detail API does.
            dashboard_detail = self.get_dashboard_info(dashboard_id)
            if dashboard_detail:
                # ``is not None`` keeps list-API values when detail returns
                # null for a key but lets empty strings overwrite — the
                # empty-string position_json case is handled at parse time.
                detail_result = dashboard_detail.get("result", {})
                for key, value in detail_result.items():
                    if value is not None:
                        dashboard_data[key] = value

                # ``"{}"`` is a structurally valid empty layout and goes
                # through the no-charts path; only None / "" mean the API
                # actually returned nothing.
                if dashboard_data.get("position_json") in (None, ""):
                    self.report.num_dashboards_missing_position_json += 1
                    self.report.warning(
                        message="Dashboard detail returned no position_json.",
                        context=self._warning_context(
                            dashboard_id=dashboard_id,
                            dashboard_title=dashboard_title,
                        ),
                        title="Missing position_json",
                    )

            # We mutate ``position_data`` below to inject synthetic
            # ``CHART-fallback-{id}`` entries; copy defensively so a
            # future caching refactor of ``_parse_position_json`` cannot
            # cross-contaminate dashboards.
            position_data = dict(self._parse_position_json(dashboard_data))

            # We deliberately don't union /charts with position_json by
            # default: it costs an HTTP round trip per dashboard and could
            # add chart IDs that position_json intentionally omitted (soft-
            # deleted slices). Fallback only when position_json yields zero.
            position_yielded_charts = any(
                key.startswith("CHART-") and _extract_chart_id(value) is not None
                for key, value in position_data.items()
            )
            fallback_chart_id_strs: Set[str] = set()
            if not position_yielded_charts:
                fallback_chart_ids = self.get_dashboard_charts(dashboard_id)
                if fallback_chart_ids:
                    self.report.num_dashboards_recovered_via_charts_endpoint += 1
                    for chart_id in fallback_chart_ids:
                        chart_id_str = str(chart_id)
                        fallback_chart_id_strs.add(chart_id_str)
                        position_data[f"CHART-fallback-{chart_id}"] = {
                            "meta": {"chartId": chart_id_str},
                        }

            if fallback_chart_id_strs:
                self._warn_dangling_fallback_charts(
                    fallback_chart_id_strs=fallback_chart_id_strs,
                    dashboard_id=dashboard_id,
                    dashboard_title=dashboard_title,
                )

            if self.config.database_pattern != AllowDenyPattern.allow_all():
                self._report_filtered_chart_references(
                    position_data=position_data,
                    dashboard_id=dashboard_id,
                    dashboard_title=dashboard_title,
                )

            chart_keys = [k for k in position_data if k.startswith("CHART-")]
            if not chart_keys:
                self.report.num_dashboards_with_no_charts += 1
                if position_data:
                    self.report.info(
                        message="Dashboard has position_json but no CHART-* keys",
                        context=self._warning_context(
                            dashboard_id=dashboard_id,
                            dashboard_title=dashboard_title,
                            top_level_keys=list(position_data.keys())[:10],
                        ),
                        title="position_json has no chart entries",
                    )

            dashboard_snapshot = self.construct_dashboard_from_api_data(
                dashboard_data, position_data=position_data
            )

        except Exception as e:
            self.report.num_dashboards_dropped_unexpected_error += 1
            self.report.warning(
                message="Failed to construct dashboard snapshot.",
                context=self._warning_context(
                    dashboard_id=dashboard_id,
                    dashboard_title=dashboard_title,
                    error=str(e),
                ),
                title="Dashboard Construction Failed",
                exc=e,
            )
            return

        mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)
        yield MetadataWorkUnit(id=dashboard_snapshot.urn, mce=mce)
        yield from self._get_domain_wu(
            title=dashboard_title, entity_urn=dashboard_snapshot.urn
        )

    def emit_dashboard_mces(self) -> Iterable[MetadataWorkUnit]:
        dashboard_data_list = [
            (dashboard_data,)
            for dashboard_data in self.paginate_entity_api_results(
                "dashboard/", PAGE_SIZE
            )
        ]

        yield from ThreadedIteratorExecutor.process(
            worker_func=self._process_dashboard,
            args_list=dashboard_data_list,
            max_workers=self.config.max_threads,
        )

    def build_input_fields(
        self,
        chart_columns: List[Tuple[str, str, str]],
        datasource_urn: Union[str, None],
    ) -> List[InputField]:
        input_fields: List[InputField] = []

        for column in chart_columns:
            col_name, col_type, description = column
            if not col_type or not datasource_urn:
                continue

            type_class = FIELD_TYPE_MAPPING.get(
                col_type.upper(), NullTypeClass
            )  # gets the type mapping

            input_fields.append(
                InputField(
                    schemaFieldUrn=builder.make_schema_field_urn(
                        parent_urn=str(datasource_urn),
                        field_path=col_name,
                    ),
                    schemaField=SchemaField(
                        fieldPath=col_name,
                        type=SchemaFieldDataType(type=type_class()),  # type: ignore
                        description=(description if description != "null" else ""),
                        nativeDataType=col_type,
                        globalTags=None,
                        nullable=True,
                    ),
                )
            )

        return input_fields

    def _extract_columns_from_sql(self, sql_expr: Optional[str]) -> List[str]:
        """Return the set of column names referenced in a SQL expression.

        Used for resolving column-level lineage on metrics whose definition
        is a SQL expression (e.g. ``SUM(amount) / COUNT(*)``). Returns an
        empty list for falsy input or unparseable SQL; the latter ticks
        ``num_metrics_sql_parse_errors`` and emits a structured warning."""
        if not sql_expr:
            return []

        try:
            parsed_expr = sqlglot.parse_one(sql_expr)
        except sqlglot.errors.SqlglotError as e:
            # Catch only sqlglot errors so unrelated exceptions surface.
            # SQL goes to context, not the message, to avoid log spam
            # from long expressions.
            self.report.num_metrics_sql_parse_errors += 1
            self.report.warning(
                message="Failed to parse metric SQL expression; column lineage will skip it.",
                context=self._warning_context(
                    error=str(e),
                    error_type=type(e).__name__,
                    sql_preview=sql_expr[:200],
                ),
                title="Metric SQL parse error",
            )
            return []

        column_refs = set()
        for node in parsed_expr.walk():
            if isinstance(node, sqlglot.exp.Column):
                column_refs.add(node.name)
        return list(column_refs)

    def _process_column_item(
        self, item: Union[str, dict], unique_columns: Dict[str, bool]
    ) -> None:
        """Process a single column item and add to unique_columns."""

        def add_column(col_name: str, is_sql: bool) -> None:
            if not col_name:
                return
            # Always set to False if any non-SQL seen, else keep as is_sql
            unique_columns[col_name] = unique_columns.get(col_name, True) and is_sql

        if isinstance(item, str):
            add_column(item, False)
        elif isinstance(item, dict):
            if item.get("expressionType") == "SIMPLE":
                # For metrics with SIMPLE expression type
                add_column(item.get("column", {}).get("column_name", ""), False)
            elif item.get("expressionType") == "SQL":
                sql_expr = item.get("sqlExpression")
                column_refs = self._extract_columns_from_sql(sql_expr)
                for col in column_refs:
                    add_column(col, False)
                if not column_refs:
                    add_column(item.get("label", ""), True)

    def _collect_all_unique_columns(self, form_data: dict) -> Dict[str, bool]:
        """Collect all unique column names from form_data, distinguishing SQL vs non-SQL."""
        unique_columns: Dict[str, bool] = {}

        # Process regular columns
        for column in form_data.get("all_columns", []):
            self._process_column_item(column, unique_columns)

        # Process metrics
        # For charts with a single metric, the metric is stored in the form_data as a string in the 'metric' key
        # For charts with multiple metrics, the metrics are stored in the form_data as a list of strings in the 'metrics' key
        if "metric" in form_data:
            metrics_data = [form_data.get("metric")]
        else:
            metrics_data = form_data.get("metrics", [])

        for metric in metrics_data:
            if metric is not None:
                self._process_column_item(metric, unique_columns)

        # Process group by columns
        for group in form_data.get("groupby", []):
            self._process_column_item(group, unique_columns)

        # Process x-axis columns
        x_axis_data = form_data.get("x_axis")
        if x_axis_data is not None:
            self._process_column_item(x_axis_data, unique_columns)

        return unique_columns

    def _fetch_dataset_columns(
        self,
        datasource_id: Union[Any, int],
        *,
        chart_id: Optional[Union[int, str]] = None,
    ) -> List[Tuple[str, str, str]]:
        """Fetch dataset columns and metrics from Superset API.

        ``chart_id`` is included in warning/info context so operators can
        locate the affected chart when CLL is dropped.
        """
        if datasource_id is None or datasource_id == 0:
            self.report.num_charts_missing_datasource_id_for_cll += 1
            self.report.warning(
                message="Chart has no datasource_id; cannot build column-level lineage.",
                context=self._warning_context(
                    chart_id=chart_id,
                    datasource_id=datasource_id,
                ),
                title="Chart missing datasource_id",
            )
            return []

        dataset_response = self.get_dataset_info(datasource_id)
        dataset_info = dataset_response.get("result", {})
        if not dataset_info:
            # get_dataset_info already emitted a structured warning for the
            # underlying failure; tick a chart-level counter so operators
            # can see CLL impact distinct from dataset-fetch failures.
            self.report.num_charts_missing_dataset_detail_for_cll += 1
            self.report.info(
                message="Chart CLL skipped: dataset detail unavailable.",
                context=self._warning_context(
                    chart_id=chart_id,
                    datasource_id=datasource_id,
                ),
                title="Chart CLL skipped (no dataset detail)",
            )
            return []
        dataset_column_info = dataset_info.get("columns", [])
        if not isinstance(dataset_column_info, list):
            self.report.num_datasets_columns_payload_malformed += 1
            self.report.warning(
                message="Dataset detail returned a non-list ``columns`` payload; CLL columns skipped.",
                context=self._warning_context(
                    chart_id=chart_id,
                    datasource_id=datasource_id,
                    payload_type=type(dataset_column_info).__name__,
                ),
                title="Dataset columns payload malformed",
            )
            dataset_column_info = []
        dataset_metric_info = dataset_info.get("metrics", [])
        if not isinstance(dataset_metric_info, list):
            self.report.num_datasets_metrics_payload_malformed += 1
            self.report.warning(
                message="Dataset detail returned a non-list ``metrics`` payload; CLL metrics skipped.",
                context=self._warning_context(
                    chart_id=chart_id,
                    datasource_id=datasource_id,
                    payload_type=type(dataset_metric_info).__name__,
                ),
                title="Dataset metrics payload malformed",
            )
            dataset_metric_info = []

        dataset_columns: List[Tuple[str, str, str]] = []
        for column in dataset_column_info:
            # Per-item shape guard: a non-dict entry (e.g. a bare string)
            # would crash ``column.get(...)``; treat as malformed and
            # continue so the rest of the chart still ingests.
            if not isinstance(column, dict):
                self.report.num_datasets_columns_dropped_malformed += 1
                continue
            col_name = column.get("column_name", "")
            col_type = column.get("type", "")
            col_description = column.get("description", "")

            if col_name == "" or col_type == "":
                self.report.num_datasets_columns_dropped_malformed += 1
                self.report.info(
                    message="Dataset column dropped from CLL: missing name or type.",
                    context=self._warning_context(
                        chart_id=chart_id,
                        datasource_id=datasource_id,
                        column_keys=list(column.keys())[:10],
                    ),
                    title="Dataset column malformed",
                )
                continue

            dataset_columns.append((col_name, col_type, col_description))

        for metric in dataset_metric_info:
            if not isinstance(metric, dict):
                self.report.num_datasets_metrics_dropped_malformed += 1
                continue
            metric_name = metric.get("metric_name", "")
            metric_type = metric.get("metric_type", "")
            metric_description = metric.get("description", "")

            if metric_name == "" or metric_type == "":
                self.report.num_datasets_metrics_dropped_malformed += 1
                self.report.info(
                    message="Dataset metric dropped from CLL: missing name or type.",
                    context=self._warning_context(
                        chart_id=chart_id,
                        datasource_id=datasource_id,
                        metric_keys=list(metric.keys())[:10],
                    ),
                    title="Dataset metric malformed",
                )
                continue

            dataset_columns.append((metric_name, metric_type, metric_description))

        return dataset_columns

    def _match_chart_columns_with_dataset(
        self,
        unique_chart_columns: Dict[str, bool],
        dataset_columns: List[Tuple[str, str, str]],
    ) -> List[Tuple[str, str, str]]:
        """Match chart columns with dataset columns, preserving SQL/non-SQL status."""
        chart_columns: List[Tuple[str, str, str]] = []

        for chart_col_name, is_sql in unique_chart_columns.items():
            if is_sql:
                chart_columns.append((chart_col_name, "SQL", ""))
                continue

            # find matching upstream column
            for dataset_col in dataset_columns:
                dataset_col_name, dataset_col_type, dataset_col_description = (
                    dataset_col
                )
                if dataset_col_name == chart_col_name:
                    chart_columns.append(
                        (chart_col_name, dataset_col_type, dataset_col_description)
                    )
                    break
            else:
                chart_columns.append((chart_col_name, "", ""))

        return chart_columns

    def construct_chart_cll(
        self,
        chart_data: dict,
        datasource_urn: Union[str, None],
        datasource_id: Union[Any, int],
    ) -> List[InputField]:
        """Construct column-level lineage for a chart."""
        form_data = chart_data.get("form_data", {})

        # Extract and process all columns in one go
        unique_columns = self._collect_all_unique_columns(form_data)

        dataset_columns = self._fetch_dataset_columns(
            datasource_id, chart_id=chart_data.get("id")
        )
        if not dataset_columns:
            return []

        # Match chart columns with dataset columns
        chart_columns = self._match_chart_columns_with_dataset(
            unique_columns, dataset_columns
        )

        # Build input fields
        return self.build_input_fields(chart_columns, datasource_urn)

    def construct_chart_from_chart_data(
        self, chart_data: dict
    ) -> Iterable[MetadataWorkUnit]:
        chart_urn = make_chart_urn(
            platform=self.platform,
            name=str(chart_data["id"]),
            platform_instance=self.config.platform_instance,
        )
        chart_snapshot = ChartSnapshot(
            urn=chart_urn,
            aspects=[Status(removed=False)],
        )

        modified_actor = f"urn:li:corpuser:{self.owner_info.get((chart_data.get('changed_by') or {}).get('id', -1), 'unknown')}"
        now = datetime.now().strftime("%I:%M%p on %B %d, %Y")
        modified_ts = int(
            dp.parse(chart_data.get("changed_on_utc", now)).timestamp() * 1000
        )
        title = chart_data.get("slice_name", "")

        # note: the API does not currently supply created_by usernames due to a bug
        last_modified = AuditStampClass(time=modified_ts, actor=modified_actor)

        change_audit_stamps = ChangeAuditStamps(
            created=None, lastModified=last_modified
        )

        chart_type = chart_type_from_viz_type.get(chart_data.get("viz_type", ""))
        chart_url = f"{self.config.display_uri}{chart_data.get('url', '')}"

        datasource_id = chart_data.get("datasource_id")
        # Treat ``None`` and ``0`` identically: Superset auto-increment
        # never assigns ``0``, so a falsy datasource_id means "unset".
        # Tick a chart-level URN counter with the chart_id locator so
        # operators can reconcile this with the CLL-side counter.
        if datasource_id is None or datasource_id == 0:
            self.report.num_charts_without_datasource_for_urn += 1
            self.report.info(
                message="Chart has no datasource_id; URN inputs will be empty.",
                context=self._warning_context(
                    chart_id=chart_data.get("id"),
                    datasource_id=datasource_id,
                ),
                title="Chart missing datasource_id (URN)",
            )
            datasource_urn = None
        else:
            dataset_response = self.get_dataset_info(datasource_id)
            try:
                datasource_urn = self.get_datasource_urn_from_id(
                    dataset_response, self.platform
                )
            except ValueError as e:
                # Detail payload was structurally fine for caching but
                # missing the fields required to build the URN (rare;
                # surfaced separately so it doesn't get lumped into the
                # generic "dataset detail API failed" counter).
                self.report.num_charts_dropped_datasource_urn_failed += 1
                self.report.warning(
                    message="Could not build datasource URN for chart; chart-to-dataset lineage will be missing.",
                    context=self._warning_context(
                        chart_id=chart_data.get("id"),
                        datasource_id=datasource_id,
                        error=str(e),
                    ),
                    title="Chart datasource URN unavailable",
                )
                datasource_urn = None

        # ``params`` is a JSON string in the Superset API; malformed
        # values have been observed in the wild (manual edits, partial
        # writes). Skip params-derived custom properties on parse error
        # but still emit the chart so dashboard lineage isn't lost.
        # Mirror ``_parse_position_json``'s ``or "{}"`` so ``None`` and
        # ``""`` both collapse to a parseable benign default.
        raw_params = chart_data.get("params") or "{}"
        try:
            parsed_params = json.loads(raw_params)
        except (TypeError, ValueError) as e:
            self.report.num_charts_invalid_params_json += 1
            self.report.warning(
                message="Chart params field was not valid JSON; skipping params-derived properties.",
                context=self._warning_context(
                    chart_id=chart_data.get("id"),
                    error=str(e),
                ),
                title="Chart params invalid JSON",
            )
            parsed_params = {}
        # ``params`` is contractually a JSON object; a JSON scalar/array
        # would crash ``.get(...)`` below. Treat unexpected shapes as
        # empty rather than dropping the chart.
        params = parsed_params if isinstance(parsed_params, dict) else {}
        raw_metrics = params.get("metrics", [])
        if isinstance(raw_metrics, list):
            metrics_iter: Iterable[Any] = raw_metrics or [params.get("metric")]
        else:
            self.report.num_charts_invalid_params_metrics_shape += 1
            self.report.info(
                message="Chart params.metrics had unexpected shape; skipping derived metrics.",
                context=self._warning_context(
                    chart_id=chart_data.get("id"),
                    metrics_type=type(raw_metrics).__name__,
                ),
                title="Chart params.metrics unexpected shape",
            )
            metrics_iter = []
        metrics = [m for m in (get_metric_name(metric) for metric in metrics_iter) if m]
        raw_filters = params.get("adhoc_filters") or []
        if not isinstance(raw_filters, list):
            raw_filters = []
        filters = [f for f in (get_filter_name(fo) for fo in raw_filters) if f]
        group_bys = params.get("groupby", []) or []
        if isinstance(group_bys, str):
            group_bys = [group_bys]
        # handling List[Union[str, dict]] case
        # a dict containing two keys: sqlExpression and label
        elif isinstance(group_bys, list) and len(group_bys) != 0:
            temp_group_bys = []
            for item in group_bys:
                # if the item is a custom label
                if isinstance(item, dict):
                    item_value = item.get("label", "")
                    if item_value != "":
                        temp_group_bys.append(f"{item_value}_custom_label")
                    else:
                        temp_group_bys.append(str(item))

                # if the item is a string
                elif isinstance(item, str):
                    temp_group_bys.append(item)

            group_bys = temp_group_bys

        custom_properties = {
            "Metrics": ", ".join(metrics),
            "Filters": ", ".join(filters),
            "Dimensions": ", ".join(group_bys),
        }

        chart_info = ChartInfoClass(
            type=chart_type,
            description="",
            title=title,
            chartUrl=chart_url,
            inputs=[datasource_urn] if datasource_urn else None,
            customProperties=custom_properties,
            lastModified=change_audit_stamps,
        )
        chart_snapshot.aspects.append(chart_info)

        input_fields = self.construct_chart_cll(
            chart_data, datasource_urn, datasource_id
        )

        if input_fields:
            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=InputFields(
                    fields=sorted(input_fields, key=lambda x: x.schemaFieldUrn)
                ),
            ).as_workunit()

        chart_owners_list = self.build_owner_urn(chart_data)
        owners_info = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=urn,
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                )
                for urn in (chart_owners_list or [])
            ],
            lastModified=last_modified,
        )
        chart_snapshot.aspects.append(owners_info)

        superset_tags = self._extract_and_map_tags(chart_data.get("tags", []))
        tags = self._merge_tags_with_existing(chart_urn, superset_tags)
        if tags:
            chart_snapshot.aspects.append(tags)

        yield MetadataWorkUnit(
            id=chart_urn, mce=MetadataChangeEvent(proposedSnapshot=chart_snapshot)
        )

        yield from self._get_domain_wu(
            title=chart_data.get("slice_name", ""),
            entity_urn=chart_urn,
        )

    def _process_chart(self, chart_data: Any) -> Iterable[MetadataWorkUnit]:
        # Pre-init so the outer ``except`` can build warning context even if
        # the very first ``chart_data.get(...)`` raises (e.g. ``chart_data``
        # is unexpectedly not a dict). Mirrors the pattern in
        # ``_process_dashboard``.
        chart_id: Optional[int] = None
        chart_name = ""
        database_name = None
        try:
            chart_id = chart_data.get("id")
            chart_name = chart_data.get("slice_name", "")
            if not self.config.chart_pattern.allowed(chart_name):
                self.report.report_dropped(
                    f"Chart '{chart_name}' (id: {chart_id}) filtered by chart_pattern"
                )
                return

            # TODO: Make helper methods for database_pattern
            if self.config.database_pattern != AllowDenyPattern.allow_all():
                datasource_id = chart_data.get("datasource_id")

                if datasource_id:
                    if datasource_id in self.filtered_dataset_to_database:
                        database_name = self.filtered_dataset_to_database[datasource_id]
                        self.filtered_chart_to_database[chart_id] = database_name

                        is_filtered = not self.config.database_pattern.allowed(
                            database_name
                        )
                        self.processed_charts[chart_id] = (database_name, is_filtered)

                        if is_filtered:
                            self.report.warning(
                                message="Chart uses a dataset from a filtered database. Set the chart pattern to deny ingestion.",
                                context=self._warning_context(
                                    chart_id=chart_id,
                                    chart_name=chart_name,
                                    database_name=database_name,
                                ),
                                title="Incomplete Ingestion",
                            )

                    else:
                        dataset_response = self.get_dataset_info(datasource_id)
                        database_name = (
                            dataset_response.get("result", {})
                            .get("database", {})
                            .get("database_name")
                        )

                        if not database_name:
                            # Filter check effectively skipped — surface
                            # so operators don't assume database_pattern
                            # was honoured for this chart.
                            self.report.num_charts_database_pattern_indeterminate += 1
                            self.report.info(
                                message="Could not evaluate database_pattern: dataset detail had no database_name.",
                                context=self._warning_context(
                                    chart_id=chart_id,
                                    chart_name=chart_name,
                                    datasource_id=datasource_id,
                                ),
                                title="database_pattern indeterminate",
                            )

                        if database_name:
                            is_filtered = not self.config.database_pattern.allowed(
                                database_name
                            )
                            if is_filtered:
                                self.filtered_chart_to_database[chart_id] = (
                                    database_name
                                )
                                self.filtered_dataset_to_database[datasource_id] = (
                                    database_name
                                )
                            self.processed_charts[chart_id] = (
                                database_name,
                                is_filtered,
                            )

                            if is_filtered:
                                self.report.warning(
                                    message="Chart uses a dataset from a filtered database. Set the chart pattern to deny ingestion.",
                                    context=self._warning_context(
                                        chart_id=chart_id,
                                        chart_name=chart_name,
                                        database_name=database_name,
                                    ),
                                    title="Incomplete Ingestion",
                                )

            if self.config.dataset_pattern != AllowDenyPattern.allow_all():
                datasource_id = chart_data.get("datasource_id")
                if datasource_id:
                    dataset_response = self.get_dataset_info(datasource_id)
                    dataset_name = dataset_response.get("result", {}).get(
                        "table_name", ""
                    )
                    if dataset_name and not self.config.dataset_pattern.allowed(
                        dataset_name
                    ):
                        self.report.warning(
                            message="Chart uses a dataset that was filtered by dataset pattern. Update your dataset pattern to include this dataset.",
                            context=self._warning_context(
                                chart_id=chart_id,
                                chart_name=chart_name,
                                dataset_name=dataset_name,
                            ),
                            title="Incomplete Ingestion",
                        )
            if chart_id not in self.processed_charts:
                self.processed_charts[chart_id] = (database_name, False)

            yield from self.construct_chart_from_chart_data(chart_data)
        except Exception as e:
            self.report.warning(
                message="Failed to construct chart snapshot. This chart will not be ingested.",
                context=self._warning_context(
                    chart_id=chart_id,
                    chart_name=chart_name,
                    error=str(e),
                ),
                title="Chart Construction Failed",
                exc=e,
            )
            return

    def emit_chart_mces(self) -> Iterable[MetadataWorkUnit]:
        chart_data_list = [
            (chart_data,)
            for chart_data in self.paginate_entity_api_results("chart/", PAGE_SIZE)
        ]
        yield from ThreadedIteratorExecutor.process(
            worker_func=self._process_chart,
            args_list=chart_data_list,
            max_workers=self.config.max_threads,
        )

    def gen_schema_fields(self, column_data: List[Dict[str, str]]) -> List[SchemaField]:
        schema_fields: List[SchemaField] = []
        for col in column_data:
            col_type = (col.get("type") or "").lower()
            data_type = resolve_sql_type(col_type)
            if data_type is None:
                data_type = NullType()

            field = SchemaField(
                fieldPath=col.get("column_name", ""),
                type=SchemaFieldDataType(data_type),
                nativeDataType="",
                description=col.get("description") or col.get("column_name", ""),
                nullable=True,
            )
            schema_fields.append(field)
        return schema_fields

    def gen_metric_schema_fields(
        self, metric_data: List[Dict[str, Any]]
    ) -> List[SchemaField]:
        schema_fields: List[SchemaField] = []
        for metric in metric_data:
            metric_type = metric.get("metric_type", "")
            data_type = resolve_sql_type(metric_type)
            if data_type is None:
                data_type = NullType()

            field = SchemaField(
                fieldPath=metric.get("metric_name", ""),
                type=SchemaFieldDataType(data_type),
                nativeDataType=metric_type or "",
                description=metric.get("description", ""),
                nullable=True,
            )
            schema_fields.append(field)
        return schema_fields

    def gen_schema_metadata(
        self,
        dataset_response: dict,
    ) -> SchemaMetadata:
        dataset_response = dataset_response.get("result", {})
        column_data = dataset_response.get("columns", [])
        metric_data = dataset_response.get("metrics", [])

        column_fields = self.gen_schema_fields(column_data)
        metric_fields = self.gen_metric_schema_fields(metric_data)

        schema_metadata = SchemaMetadata(
            schemaName=dataset_response.get("table_name", ""),
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=MySqlDDL(tableSchema=""),
            fields=column_fields + metric_fields,
        )
        return schema_metadata

    def gen_dataset_urn(self, datahub_dataset_name: str) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=datahub_dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _apply_database_alias_to_urn(self, urn: str) -> str:
        """Apply database_alias mapping to transform database names in URNs.

        For example, if database_alias = {"ClickHouse Cloud": "fingerprint"},
        transforms: urn:li:dataset:(...,clickhouse cloud.schema.table,...)
        to: urn:li:dataset:(...,fingerprint.schema.table,...)
        """
        if not self.config.database_alias:
            return urn

        # Parse URN to extract dataset name
        # URN format: urn:li:dataset:(urn:li:dataPlatform:platform,name,env)
        for source_name, alias_name in self.config.database_alias.items():
            # Try both original case and lowercase versions
            source_name_lower = source_name.lower()
            # Replace at start of dataset name (after the comma following platform)
            urn = urn.replace(f",{source_name}.", f",{alias_name}.")
            urn = urn.replace(f",{source_name_lower}.", f",{alias_name}.")
        return urn

    def generate_virtual_dataset_lineage(
        self,
        parsed_query_object: SqlParsingResult,
        datasource_urn: str,
    ) -> UpstreamLineageClass:
        cll = (
            parsed_query_object.column_lineage
            if parsed_query_object.column_lineage is not None
            else []
        )

        fine_grained_lineages: List[FineGrainedLineageClass] = []

        for cll_info in cll:
            downstream = (
                [make_schema_field_urn(datasource_urn, cll_info.downstream.column)]
                if cll_info.downstream and cll_info.downstream.column
                else []
            )
            upstreams = [
                make_schema_field_urn(
                    self._apply_database_alias_to_urn(column_ref.table),
                    column_ref.column,
                )
                for column_ref in cll_info.upstreams
            ]
            fine_grained_lineages.append(
                FineGrainedLineageClass(
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=downstream,
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=upstreams,
                )
            )

        # Apply database_alias to transform database names in URNs
        transformed_in_tables = [
            self._apply_database_alias_to_urn(urn)
            for urn in parsed_query_object.in_tables
        ]

        upstream_lineage = UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    type=DatasetLineageTypeClass.TRANSFORMED,
                    dataset=input_table_urn,
                )
                for input_table_urn in transformed_in_tables
            ],
            fineGrainedLineages=fine_grained_lineages,
        )
        return upstream_lineage

    def generate_physical_dataset_lineage(
        self,
        dataset_response: dict,
        upstream_dataset: str,
        datasource_urn: str,
    ) -> UpstreamLineageClass:
        # To generate column level lineage, we can manually decode the metadata
        # to produce the ColumnLineageInfo
        columns = dataset_response.get("result", {}).get("columns", [])
        metrics = dataset_response.get("result", {}).get("metrics", [])

        fine_grained_lineages: List[FineGrainedLineageClass] = []

        for column in columns:
            column_name = column.get("column_name", "")
            if not column_name:
                continue

            downstream = [make_schema_field_urn(datasource_urn, column_name)]
            upstreams = [make_schema_field_urn(upstream_dataset, column_name)]
            fine_grained_lineages.append(
                FineGrainedLineageClass(
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=downstream,
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=upstreams,
                )
            )

        for metric in metrics:
            metric_name = metric.get("metric_name", "")
            if not metric_name:
                continue

            downstream = [make_schema_field_urn(datasource_urn, metric_name)]
            upstreams = [make_schema_field_urn(upstream_dataset, metric_name)]
            fine_grained_lineages.append(
                FineGrainedLineageClass(
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=downstream,
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=upstreams,
                )
            )

        upstream_lineage = UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    type=DatasetLineageTypeClass.TRANSFORMED,
                    dataset=upstream_dataset,
                )
            ],
            fineGrainedLineages=fine_grained_lineages,
        )
        return upstream_lineage

    def construct_dataset_from_dataset_data(
        self, dataset_data: dict
    ) -> DatasetSnapshot:
        dataset_response = self.get_dataset_info(dataset_data.get("id"))
        result = dataset_response.get("result")
        if not isinstance(result, dict):
            # get_dataset_info already emitted a structured warning; raise
            # a typed error so the outer except doesn't paper over it.
            raise ValueError(
                f"dataset detail unavailable for dataset_id={dataset_data.get('id')}"
            )
        dataset = SupersetDataset(**result)

        datasource_urn = self.get_datasource_urn_from_id(
            dataset_response, self.platform
        )
        dataset_url = f"{self.config.display_uri}/explore/?datasource_type=table&datasource_id={dataset.id}"

        modified_actor = f"urn:li:corpuser:{self.owner_info.get((dataset_data.get('changed_by') or {}).get('id', -1), 'unknown')}"
        now = datetime.now().strftime("%I:%M%p on %B %d, %Y")
        modified_ts = int(
            dp.parse(dataset_data.get("changed_on_utc", now)).timestamp() * 1000
        )
        last_modified = AuditStampClass(time=modified_ts, actor=modified_actor)

        upstream_warehouse_platform = (
            dataset_response.get("result", {}).get("database", {}).get("backend")
        )
        upstream_warehouse_db_name = (
            dataset_response.get("result", {}).get("database", {}).get("database_name")
        )
        # Apply database_alias mapping to match URNs constructed elsewhere
        upstream_warehouse_db_name = self.config.database_alias.get(
            upstream_warehouse_db_name, upstream_warehouse_db_name
        )

        # if we have rendered sql, we always use that and defualt back to regular sql
        sql = dataset_response.get("result", {}).get(
            "rendered_sql"
        ) or dataset_response.get("result", {}).get("sql")

        # Preset has a way of naming their platforms differently than
        # how datahub names them, so map the platform name to the correct naming
        warehouse_naming = {
            "awsathena": "athena",
            "clickhousedb": "clickhouse",
            "postgresql": "postgres",
        }

        if upstream_warehouse_platform in warehouse_naming:
            upstream_warehouse_platform = warehouse_naming[upstream_warehouse_platform]

        upstream_dataset = self.get_datasource_urn_from_id(
            dataset_response, upstream_warehouse_platform
        )

        # Sometimes the field will be null instead of not existing
        if sql == "null" or not sql:
            tag_urn = f"urn:li:tag:{self.platform}:physical"
            upstream_lineage = self.generate_physical_dataset_lineage(
                dataset_response, upstream_dataset, datasource_urn
            )
        else:
            tag_urn = f"urn:li:tag:{self.platform}:virtual"
            parsed_query_object = create_lineage_sql_parsed_result(
                query=sql,
                default_db=upstream_warehouse_db_name,
                platform=upstream_warehouse_platform,
                platform_instance=None,
                env=self.config.env,
            )
            upstream_lineage = self.generate_virtual_dataset_lineage(
                parsed_query_object, datasource_urn
            )

        dataset_info = DatasetPropertiesClass(
            name=dataset.table_name,
            description=dataset.description or "",
            externalUrl=dataset_url,
            lastModified=TimeStamp(time=modified_ts),
        )

        dataset_tags = GlobalTagsClass(tags=[TagAssociationClass(tag=tag_urn)])
        tags = self._merge_tags_with_existing(datasource_urn, dataset_tags)

        aspects_items: List[Any] = [
            self.gen_schema_metadata(dataset_response),
            dataset_info,
            upstream_lineage,
        ]

        if tags:
            aspects_items.append(tags)

        dataset_snapshot = DatasetSnapshot(
            urn=datasource_urn,
            aspects=aspects_items,
        )

        dataset_owners_list = self.build_owner_urn(dataset_data)
        owners_info = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=urn,
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                )
                for urn in (dataset_owners_list or [])
            ],
            lastModified=last_modified,
        )
        aspects_items.append(owners_info)

        return dataset_snapshot

    def _extract_and_map_tags(self, raw_tags: Any) -> Optional[GlobalTagsClass]:
        """Extract and map Superset tags to DataHub GlobalTagsClass.

        Filters out system-generated tags (type != 1) and only processes user-defined tags
        from the Superset API response. Defensive against non-list payloads
        and non-dict items because the Superset API has been observed to
        return ``tags: null`` and per-item shape is not guaranteed.
        """
        if not isinstance(raw_tags, list):
            return None
        user_tags = [
            tag.get("name", "")
            for tag in raw_tags
            if isinstance(tag, dict) and tag.get("type") == 1 and tag.get("name")
        ]

        if not user_tags:
            return None

        tag_urns = [builder.make_tag_urn(tag) for tag in user_tags]
        return GlobalTagsClass(
            tags=[TagAssociationClass(tag=tag_urn) for tag_urn in tag_urns]
        )

    def _merge_tags_with_existing(
        self, entity_urn: str, new_tags: Optional[GlobalTagsClass]
    ) -> Optional[GlobalTagsClass]:
        """Merge new tags with existing ones from DataHub to preserve manually added tags.

        This method ensures that tags manually added via DataHub UI are not overwritten
        during ingestion. It fetches existing tags from the graph and merges them with
        new tags from the source system, avoiding duplicates.

        Args:
            entity_urn: URN of the entity to check for existing tags
            new_tags: New tags to add as GlobalTagsClass object

        Returns:
            GlobalTagsClass with merged tags preserving existing ones, or None if no tags
        """
        if not new_tags or not new_tags.tags:
            return None

        # Fetch existing tags from DataHub
        existing_global_tags = None
        if self.ctx.graph:
            existing_global_tags = self.ctx.graph.get_aspect(
                entity_urn=entity_urn, aspect_type=GlobalTagsClass
            )

        # Merge existing tags with new ones, avoiding duplicates
        all_tags = []
        existing_tag_urns = set()

        if existing_global_tags and existing_global_tags.tags:
            all_tags.extend(existing_global_tags.tags)
            existing_tag_urns = {tag.tag for tag in existing_global_tags.tags}

        # Add new tags that don't already exist
        for new_tag in new_tags.tags:
            if new_tag.tag not in existing_tag_urns:
                all_tags.append(new_tag)

        return GlobalTagsClass(tags=all_tags) if all_tags else None

    def _process_dataset(self, dataset_data: Any) -> Iterable[MetadataWorkUnit]:
        dataset_name = ""
        try:
            dataset_id = dataset_data.get("id")
            dataset_name = dataset_data.get("table_name", "")
            if not self.config.dataset_pattern.allowed(dataset_name):
                self.report.report_dropped(
                    f"Dataset '{dataset_name}' filtered by dataset_pattern"
                )
                return
            if self.config.database_pattern != AllowDenyPattern.allow_all():
                dataset_response = self.get_dataset_info(dataset_id)
                database_name = (
                    dataset_response.get("result", {})
                    .get("database", {})
                    .get("database_name")
                )

                if database_name and not self.config.database_pattern.allowed(
                    database_name
                ):
                    self.filtered_dataset_to_database[dataset_id] = database_name
                    self.report.report_dropped(
                        f"Dataset '{dataset_name}' filtered by database_pattern with database '{database_name}'"
                    )
                    return

            dataset_snapshot = self.construct_dataset_from_dataset_data(dataset_data)
            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        except Exception as e:
            self.report.warning(
                message="Failed to construct dataset snapshot.",
                context=self._warning_context(
                    dataset_id=dataset_data.get("id"),
                    dataset_name=dataset_data.get("table_name"),
                    error=str(e),
                ),
                title="Dataset Construction Failed",
                exc=e,
            )
            return
        yield MetadataWorkUnit(id=dataset_snapshot.urn, mce=mce)
        yield from self._get_domain_wu(
            title=dataset_data.get("table_name", ""),
            entity_urn=dataset_snapshot.urn,
        )

    def emit_dataset_mces(self) -> Iterable[MetadataWorkUnit]:
        dataset_data_list = [
            (dataset_data,)
            for dataset_data in self.paginate_entity_api_results("dataset/", PAGE_SIZE)
        ]
        yield from ThreadedIteratorExecutor.process(
            worker_func=self._process_dataset,
            args_list=dataset_data_list,
            max_workers=self.config.max_threads,
        )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        # TODO: Possibly change ingestion order to minimize API calls
        if self.config.ingest_datasets:
            yield from self.emit_dataset_mces()
        if self.config.ingest_charts:
            yield from self.emit_chart_mces()
        if self.config.ingest_dashboards:
            yield from self.emit_dashboard_mces()

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_report(self) -> StaleEntityRemovalSourceReport:
        return self.report

    def _get_domain_wu(self, title: str, entity_urn: str) -> Iterable[MetadataWorkUnit]:
        domain_urn = None
        for domain, pattern in self.config.domain.items():
            if pattern.allowed(title):
                domain_urn = make_domain_urn(
                    self.domain_registry.get_domain_urn(domain)
                )
                break

        if domain_urn:
            yield from add_domain_to_entity_wu(
                entity_urn=entity_urn,
                domain_urn=domain_urn,
            )
