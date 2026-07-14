import json
import logging
from datetime import datetime, timezone
from functools import lru_cache
from typing import Dict, Iterable, List, Optional, Tuple, Type, TypeVar, Union, cast

import dateutil.parser as dp
import requests
from pydantic import ValidationError

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_entity_to_container, gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.metabase.config import MetabaseConfig
from datahub.ingestion.source.metabase.constants import (
    _API_CARD,
    _API_CARDS,
    _API_COLLECTION_ITEMS,
    _API_COLLECTIONS,
    _API_CURRENT_USER,
    _API_DASHBOARD,
    _API_DATABASE,
    _API_FIELD,
    _API_SESSION,
    _API_TABLE,
    _API_USER,
    _CARD_REF_PREFIX,
    _CARD_TYPE_MODEL,
    _KNOWN_METABASE_ENGINES,
    _MBQL_REF_AGGREGATION,
    _MBQL_REF_EXPRESSION,
    _MBQL_REF_FIELD,
    _OPTIONAL_CLAUSE_PATTERN,
    _QUERY_TYPE_NATIVE,
    _QUERY_TYPE_QUERY,
    _TEMPLATE_VARIABLE_PATTERN,
    DATASOURCE_URN_RECURSION_LIMIT,
    METABASE_CHART_DISPLAY_TYPE_MAP,
    METABASE_ENGINE_TO_DATAHUB_PLATFORM,
    METABASE_TYPE_TO_DATAHUB_TYPE,
)
from datahub.ingestion.source.metabase.mbql import _extract_field_ids_from_mbql
from datahub.ingestion.source.metabase.models import (
    DatasourceInfo,
    MetabaseBaseModel,
    MetabaseCard,
    MetabaseCardListItem,
    MetabaseCollection,
    MetabaseCollectionItemsResponse,
    MetabaseCollectionKey,
    MetabaseDashboard,
    MetabaseDashboardListItem,
    MetabaseDatabase,
    MetabaseField,
    MetabaseLastEditInfo,
    MetabaseLoginResponse,
    MetabaseResultMetadata,
    MetabaseTable,
    MetabaseUser,
    _MBQLContext,
)
from datahub.ingestion.source.metabase.report import MetabaseReport
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BooleanTypeClass,
    BytesTypeClass,
    ChangeAuditStampsClass,
    ChartInfoClass,
    ChartQueryClass,
    ChartQueryTypeClass,
    DashboardInfoClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    EdgeClass,
    EnumTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    InputFieldClass,
    InputFieldsClass,
    MySqlDDLClass,
    NullTypeClass,
    NumberTypeClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    TagAssociationClass,
    TimeTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    create_lineage_sql_parsed_result,
)

logger = logging.getLogger(__name__)

_ModelT = TypeVar("_ModelT", bound=MetabaseBaseModel)


@platform_name("Metabase")
@config_class(MetabaseConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE, "Supported by default for charts and dashboards"
)
class MetabaseSource(StatefulIngestionSourceBase):
    """Extracts dashboards, charts, and models from Metabase via REST API."""

    config: MetabaseConfig
    report: MetabaseReport
    platform = "metabase"

    def __hash__(self) -> int:
        return id(self)

    def __init__(self, ctx: PipelineContext, config: MetabaseConfig):
        super().__init__(config, ctx)
        self.config = config
        self.report = MetabaseReport()
        self.access_token: Optional[str] = None
        # url -> validated model, populated only on success so a transient error
        # for one id does not poison every later reference to it (see _fetch).
        self._fetch_cache: Dict[str, object] = {}
        self._ownership_cache: Dict[int, Optional[OwnershipClass]] = {}
        self._card_items_cache: Optional[List[MetabaseCardListItem]] = None
        self.setup_session()

    def _normalize(self, value: str) -> str:
        return value.lower() if self.config.convert_lineage_urns_to_lowercase else value

    def _url(self, path: str, **kwargs: object) -> str:
        return f"{self.config.connect_uri}{path.format(**kwargs)}"

    def _get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> object:
        # timeout guards against a server that accepts the connection but never
        # responds. Raises ValueError on a non-JSON body (e.g. an SSO/proxy HTML
        # login page returned with a 200), which callers route to the report.
        response = self.session.get(
            url, params=params, timeout=self.config.request_timeout_sec
        )
        response.raise_for_status()
        return response.json()

    def _fetch(
        self,
        model: Type[_ModelT],
        url: str,
        *,
        label: str,
        context: str,
        params: Optional[Dict[str, str]] = None,
    ) -> Optional[_ModelT]:
        # Single fetch+validate+report path shared by all id-keyed getters.
        # RequestException covers Timeout/ConnectionError/HTTPError; ValidationError
        # is a ValueError subclass, so a non-JSON 200 body is caught too.
        cache_key = url if not params else f"{url}?{sorted(params.items())}"
        if cache_key in self._fetch_cache:
            return cast(Optional[_ModelT], self._fetch_cache[cache_key])
        try:
            result = model.model_validate(self._get_json(url, params=params))
        except ValidationError as e:
            self.report.report_warning(
                title=f"Invalid {label} Data",
                message=f"{label} data from Metabase API failed validation.",
                context=f"{context}, Error: {e}",
            )
            return None
        except (requests.exceptions.RequestException, ValueError) as e:
            self.report.report_warning(
                title=f"Failed to Retrieve {label}",
                message=f"Request to retrieve {label.lower()} from Metabase failed.",
                context=f"{context}, Error: {e}",
            )
            return None
        self._fetch_cache[cache_key] = result
        return result

    @staticmethod
    def _fine_grained_field(
        upstreams: List[str], downstream: str
    ) -> FineGrainedLineageClass:
        return FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
            upstreams=upstreams,
            downstreams=[downstream],
        )

    def _emit_ownership_and_tags(
        self,
        entity_urn: str,
        creator_id: Optional[int],
        collection_id: Optional[int],
    ) -> Iterable[MetadataWorkUnit]:
        if creator_id:
            ownership = self._get_ownership(creator_id)
            if ownership is not None:
                yield MetadataChangeProposalWrapper(
                    entityUrn=entity_urn,
                    aspect=ownership,
                ).as_workunit()

        tags = self._get_tags_from_collection(collection_id)
        if tags is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=tags,
            ).as_workunit()

    def _model_urn(self, model_id: Union[int, str]) -> str:
        return builder.make_dataset_urn(
            platform=self.platform,
            name=f"model.{model_id}",
            env=self.config.env,
        )

    def _build_dataset_urn(
        self,
        datasource: DatasourceInfo,
        schema_name: Optional[str],
        table_name: str,
    ) -> str:
        name_components = [
            datasource.database_name,
            schema_name if datasource.has_schema else None,
            table_name,
        ]
        return builder.make_dataset_urn_with_platform_instance(
            platform=datasource.platform,
            name=self._normalize(".".join([v for v in name_components if v])),
            platform_instance=datasource.platform_instance,
            env=self.config.env,
        )

    @staticmethod
    def _upstream_lineage(
        datasets: List[str],
        lineage_type: str,
        fine_grained: Optional[List[FineGrainedLineageClass]] = None,
    ) -> UpstreamLineageClass:
        return UpstreamLineageClass(
            upstreams=[
                UpstreamClass(dataset=urn, type=lineage_type) for urn in datasets
            ],
            fineGrainedLineages=fine_grained if fine_grained else None,
        )

    def _last_modified_stamp(
        self, last_edit_info: Optional[MetabaseLastEditInfo]
    ) -> AuditStampClass:
        email = "unknown"
        timestamp: Optional[str] = None
        if last_edit_info is not None:
            email = last_edit_info.email or "unknown"
            timestamp = last_edit_info.timestamp
        modified_actor = builder.make_user_urn(email)
        modified_ts = (
            self.get_timestamp_millis_from_ts_string(timestamp)
            if timestamp
            else int(datetime.now(timezone.utc).timestamp() * 1000)
        )
        return AuditStampClass(time=modified_ts, actor=modified_actor)

    def setup_session(self) -> None:
        self.session = requests.session()
        if self.config.api_key:
            self.session.headers.update(
                {
                    "x-api-key": self.config.api_key.get_secret_value(),
                    "Content-Type": "application/json",
                    "Accept": "*/*",
                }
            )
        else:
            try:
                login_response = requests.post(
                    self._url(_API_SESSION),
                    None,
                    {
                        "username": self.config.username,
                        "password": (
                            self.config.password.get_secret_value()
                            if self.config.password
                            else None
                        ),
                    },
                    timeout=self.config.request_timeout_sec,
                )
                login_response.raise_for_status()
                login_data = MetabaseLoginResponse.model_validate(login_response.json())
            except (requests.exceptions.RequestException, ValueError) as e:
                self.report.report_failure(
                    title="Unable to Authenticate",
                    message="Failed to log in to Metabase with the provided credentials.",
                    context=str(e),
                )
                return

            self.access_token = login_data.id
            self.session.headers.update(
                {
                    "X-Metabase-Session": f"{self.access_token}",
                    "Content-Type": "application/json",
                    "Accept": "*/*",
                }
            )

        try:
            test_response = self.session.get(
                self._url(_API_CURRENT_USER),
                timeout=self.config.request_timeout_sec,
            )
            test_response.raise_for_status()
        except requests.exceptions.RequestException as e:
            self.report.report_failure(
                title="Unable to Retrieve Current User",
                message="Unable to retrieve current user information from Metabase.",
                context=str(e),
            )

    def close(self) -> None:
        # A teardown network failure must not mask the run's real outcome, so the
        # logout is best-effort and base cleanup always runs in the finally block.
        try:
            # Only username/password auth creates sessions that need cleanup
            if not self.config.api_key and self.access_token:
                response = requests.delete(
                    self._url(_API_SESSION),
                    headers={"X-Metabase-Session": self.access_token},
                    timeout=self.config.request_timeout_sec,
                )
                if response.status_code not in (200, 204):
                    self.report.report_failure(
                        title="Unable to Log User Out",
                        message="Unable to log the ingestion user out of Metabase.",
                        context=f"Status code: {response.status_code}",
                    )
        except requests.exceptions.RequestException as e:
            self.report.report_warning(
                title="Unable to Log User Out",
                message="Failed to log the ingestion user out of Metabase during teardown.",
                context=str(e),
            )
        finally:
            super().close()

    def emit_dashboard_workunits(self) -> Iterable[MetadataWorkUnit]:
        # Reuse the cached (non-root) collections rather than re-fetching and
        # re-validating /api/collection here.
        for collection in self._get_collections_map().values():
            # Isolate per-collection item failures so one bad collection (e.g. a
            # 403/500) does not abort emitting every later collection.
            try:
                items_data = self._get_json(
                    f"{self._url(_API_COLLECTION_ITEMS, collection_id=collection.id)}"
                    f"?models=dashboard"
                )
                collection_dashboards = MetabaseCollectionItemsResponse.model_validate(
                    items_data
                )
            except ValidationError as e:
                self.report.report_warning(
                    title="Invalid Collection Items Response",
                    message="Collection items response failed validation.",
                    context=f"Collection ID: {collection.id}, Error: {str(e)}",
                )
                continue
            except (requests.exceptions.RequestException, ValueError) as error:
                self.report.report_warning(
                    title="Unable to Retrieve Collection Items",
                    message="Request to retrieve collection dashboards failed; skipping this collection.",
                    context=f"Collection ID: {collection.id}, Error: {str(error)}",
                )
                continue

            for dashboard_info in collection_dashboards.data:
                yield from self._emit_dashboard_workunits(dashboard_info)

    @staticmethod
    def get_timestamp_millis_from_ts_string(ts_str: str) -> int:
        """Convert timestamp string to milliseconds, falling back to now on parse failure."""
        try:
            return int(dp.parse(ts_str).timestamp() * 1000)
        except (dp.ParserError, OverflowError) as e:
            logger.warning(
                f"Failed to parse timestamp '{ts_str}': {e}. Using current time instead."
            )
            return int(datetime.now(timezone.utc).timestamp() * 1000)

    def _emit_dashboard_workunits(
        self, dashboard_info: MetabaseDashboardListItem
    ) -> Iterable[MetadataWorkUnit]:
        dashboard_id = dashboard_info.id
        dashboard_url = self._url(_API_DASHBOARD, dashboard_id=dashboard_id)
        try:
            dashboard = MetabaseDashboard.model_validate(self._get_json(dashboard_url))
        except ValidationError as e:
            self.report.dashboards_dropped += 1
            self.report.report_warning(
                title="Invalid Dashboard Data",
                message="Dashboard data from Metabase API failed validation.",
                context=f"Dashboard ID: {dashboard_id}, Error: {str(e)}",
            )
            return
        except (requests.exceptions.RequestException, ValueError) as error:
            self.report.dashboards_dropped += 1
            self.report.report_warning(
                title="Unable to Retrieve Dashboard",
                message="Request to retrieve dashboard from Metabase failed.",
                context=f"Dashboard ID: {dashboard_id}, Error: {str(error)}",
            )
            return

        self.report.dashboards_scanned += 1
        dashboard_urn = builder.make_dashboard_urn(
            platform=self.platform, name=str(dashboard.id)
        )

        modified_stamp = self._last_modified_stamp(dashboard.last_edit_info)
        last_modified = ChangeAuditStampsClass(
            created=modified_stamp,
            lastModified=modified_stamp,
        )

        chart_edges = []
        for dashcard in dashboard.dashcards:
            if not dashcard.card or not dashcard.card.id:
                continue

            chart_urn = builder.make_chart_urn(
                platform=self.platform, name=str(dashcard.card.id)
            )
            chart_edges.append(
                EdgeClass(
                    destinationUrn=chart_urn,
                    lastModified=last_modified.lastModified,
                )
            )

        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=DashboardInfoClass(
                description=dashboard.description or "",
                title=dashboard.name,
                chartEdges=chart_edges,
                lastModified=last_modified,
                dashboardUrl=f"{self.config.display_uri}/dashboard/{dashboard_id}",
                customProperties={},
            ),
        ).as_workunit()

        yield from self._emit_ownership_and_tags(
            dashboard_urn, dashboard.creator_id, dashboard.collection_id
        )

        if dashboard.collection_id is not None:
            yield from add_entity_to_container(
                container_key=self._gen_collection_key(dashboard.collection_id),
                entity_type="dashboard",
                entity_urn=dashboard_urn,
            )

    def _check_recursion_limit(
        self, recursion_depth: int, context: str, card_id: int
    ) -> bool:
        if recursion_depth > DATASOURCE_URN_RECURSION_LIMIT:
            self.report.report_warning(
                title="Card Recursion Limit Exceeded",
                message="Unable to extract lineage. Nested card reference depth exceeded limit.",
                context=f"Context: {context}, Card ID: {card_id}, Recursion Depth: {recursion_depth}, Limit: {DATASOURCE_URN_RECURSION_LIMIT}",
            )
            return True
        return False

    def _get_table_urns_from_card(
        self, card: MetabaseCard, recursion_depth: int = 0
    ) -> List[str]:
        if self._check_recursion_limit(
            recursion_depth=recursion_depth, context="card lineage", card_id=card.id
        ):
            return []

        if not card.dataset_query:
            return []

        query_type = card.dataset_query.type

        if query_type == _QUERY_TYPE_NATIVE:
            return self._get_table_urns_from_native_query(card)
        elif query_type == _QUERY_TYPE_QUERY:
            return self._get_table_urns_from_query_builder(
                card=card, recursion_depth=recursion_depth
            )

        return []

    def _extract_native_query(self, card: MetabaseCard) -> Optional[str]:
        if card.dataset_query and card.dataset_query.native:
            return card.dataset_query.native.query
        return None

    def _parse_native_sql(
        self, card: MetabaseCard
    ) -> Optional[Tuple[SqlParsingResult, DatasourceInfo]]:
        if not card.database_id:
            return None

        datasource = self.get_datasource_from_id(card.database_id)
        if not datasource or not datasource.platform:
            return None

        raw_query = self._extract_native_query(card)
        if not raw_query:
            return None

        raw_query_stripped = self.strip_template_expressions(raw_query)

        result = create_lineage_sql_parsed_result(
            query=raw_query_stripped,
            default_db=self._normalize(datasource.database_name)
            if datasource.database_name
            else None,
            default_schema=datasource.schema_ or self.config.default_schema,
            platform=datasource.platform,
            platform_instance=datasource.platform_instance,
            env=self.config.env,
            graph=self.ctx.graph,
        )
        return result, datasource

    def _warn_native_sql_parse_failure(self, card_id: int, error: str) -> None:
        self.report.native_sql_parse_failures += 1
        self.report.report_warning(
            title="Native SQL Lineage Parse Failure",
            message="Failed to parse lineage from a native SQL query.",
            context=f"Card ID: {card_id}, Error: {error}",
        )

    def _get_table_urns_from_native_query(self, card: MetabaseCard) -> List[str]:
        parsed = self._parse_native_sql(card)
        if not parsed:
            return []

        result, _ = parsed
        if result.debug_info.table_error:
            self._warn_native_sql_parse_failure(
                card.id, str(result.debug_info.table_error)
            )

        return [str(t) for t in result.in_tables]

    def _get_table_urns_from_query_builder(
        self, card: MetabaseCard, recursion_depth: int = 0
    ) -> List[str]:
        # A source-table ref is either an int (direct table), "card__456" (another
        # card/model), or a joins[].source-table; all three are handled below.
        if not card.dataset_query or not card.dataset_query.query:
            return []

        source_table_ids = card.dataset_query.query.source_table_refs
        if not source_table_ids:
            return []

        table_urns: List[str] = []

        for source_table_id in source_table_ids:
            source_table_str = str(source_table_id)
            if source_table_str.startswith(_CARD_REF_PREFIX):
                referenced_card_id = source_table_str.replace(_CARD_REF_PREFIX, "")
                referenced_card = self.get_card_details_by_id(referenced_card_id)
                if referenced_card:
                    if referenced_card.type == _CARD_TYPE_MODEL:
                        table_urns.append(self._model_urn(referenced_card.id))
                    else:
                        table_urns.extend(
                            self._get_table_urns_from_card(
                                card=referenced_card,
                                recursion_depth=recursion_depth + 1,
                            )
                        )
                continue

            if not card.database_id:
                continue

            datasource = self.get_datasource_from_id(card.database_id)
            if not datasource or not datasource.platform:
                continue

            schema_name, table_name = self.get_source_table_from_id(source_table_id)
            if not table_name:
                continue

            table_urns.append(
                self._build_dataset_urn(datasource, schema_name, table_name)
            )

        return table_urns

    def _field_urn(
        self, field: MetabaseField, datasource: DatasourceInfo
    ) -> Optional[str]:
        if not field.table_id:
            return None
        schema_name, table_name = self.get_source_table_from_id(field.table_id)
        if not table_name:
            return None
        dataset_urn = self._build_dataset_urn(datasource, schema_name, table_name)
        return builder.make_schema_field_urn(
            parent_urn=dataset_urn,
            field_path=field.name,
        )

    def _upstream_urns_for_field_ids(
        self,
        field_ids: List[int],
        ctx: _MBQLContext,
    ) -> List[str]:
        urns = []
        for fid in field_ids:
            mbql_field = ctx.resolved.get(fid)
            if mbql_field:
                urn = self._field_urn(mbql_field, ctx.datasource)
                if urn:
                    urns.append(urn)
        return urns

    def _resolve_aggregation_upstream_urns(
        self,
        field_ref: List[object],
        ctx: _MBQLContext,
    ) -> List[str]:
        agg_index = field_ref[1] if len(field_ref) > 1 else None
        if agg_index is None or not ctx.query.aggregation:
            return []
        agg_clauses = ctx.query.aggregation
        # aggregation can be a single clause or a list-of-clauses
        if agg_clauses and isinstance(agg_clauses[0], list):
            agg_clause = (
                agg_clauses[agg_index]
                if isinstance(agg_index, int) and agg_index < len(agg_clauses)
                else None
            )
        else:
            agg_clause = agg_clauses
        if not isinstance(agg_clause, list):
            return []
        agg_field_ids = _extract_field_ids_from_mbql(agg_clause)
        if agg_field_ids:
            return self._upstream_urns_for_field_ids(agg_field_ids, ctx)
        # COUNT(*) — no explicit field; fan in all resolved upstream columns
        return [
            urn
            for mbql_field in ctx.resolved.values()
            for urn in [self._field_urn(mbql_field, ctx.datasource)]
            if urn
        ]

    def _resolve_field_ref_upstream_urns(
        self,
        field_ref: List[object],
        ctx: _MBQLContext,
    ) -> List[str]:
        if not field_ref:
            return []
        ref_type = field_ref[0]

        if ref_type == _MBQL_REF_FIELD:
            return self._upstream_urns_for_field_ids(
                _extract_field_ids_from_mbql(field_ref), ctx
            )

        if ref_type == _MBQL_REF_EXPRESSION:
            expr_name = field_ref[1] if len(field_ref) > 1 else None
            if expr_name and ctx.query.expressions:
                expr_clause = ctx.query.expressions.get(str(expr_name))
                if isinstance(expr_clause, list):
                    return self._upstream_urns_for_field_ids(
                        _extract_field_ids_from_mbql(expr_clause), ctx
                    )
            return []

        if ref_type == _MBQL_REF_AGGREGATION:
            return self._resolve_aggregation_upstream_urns(field_ref, ctx)

        return []

    def _get_mbql_context(self, card: MetabaseCard) -> Optional[_MBQLContext]:
        if not card.dataset_query or not card.dataset_query.query:
            return None
        if not card.database_id:
            return None

        datasource = self.get_datasource_from_id(card.database_id)
        if not datasource or not datasource.platform:
            return None

        query = card.dataset_query.query
        field_refs = query.collect_field_refs()
        resolved = {}
        for fid in set(field_refs.ids):
            f = self.get_field_from_id(fid)
            if f is not None:
                resolved[fid] = f

        if field_refs.named:
            # Name-based MBQL refs cannot be resolved to a concrete upstream
            # column, so their column-level lineage is dropped. Surface it.
            self.report.mbql_field_refs_by_name_dropped += len(field_refs.named)
            self.report.report_warning(
                title="MBQL Column Lineage Dropped",
                message="Query-builder card references columns by name; column-level lineage for those columns was dropped.",
                context=f"Card ID: {card.id}, Named refs: {field_refs.named}",
            )

        return _MBQLContext(query=query, datasource=datasource, resolved=resolved)

    def _get_passthrough_cll(
        self, card: MetabaseCard, entity_urn: str
    ) -> Optional[UpstreamLineageClass]:
        if not card.result_metadata:
            return None

        table_urns = self._get_table_urns_from_query_builder(card)
        if not table_urns or len(table_urns) != 1:
            # A pass-through over zero or several (joined) tables has no clean 1:1
            # mapping, so column-level lineage is dropped despite result metadata.
            self.report.query_builder_cll_dropped += 1
            self.report.report_warning(
                title="Query-Builder Column Lineage Dropped",
                message="Pass-through card does not resolve to a single source table; column-level lineage was not emitted.",
                context=f"Card ID: {card.id}, Source tables: {len(table_urns)}",
            )
            return None

        source_table_urn = table_urns[0]
        fine_grained: List[FineGrainedLineageClass] = []

        for meta in card.result_metadata:
            if not meta.name:
                continue

            fine_grained.append(
                self._fine_grained_field(
                    upstreams=[
                        builder.make_schema_field_urn(
                            parent_urn=source_table_urn,
                            field_path=meta.name,
                        )
                    ],
                    downstream=builder.make_schema_field_urn(
                        parent_urn=entity_urn,
                        field_path=meta.name,
                    ),
                )
            )

        return self._upstream_lineage(
            [source_table_urn],
            DatasetLineageTypeClass.COPY,
            fine_grained,
        )

    def _get_cll_from_query_builder(
        self,
        card: MetabaseCard,
        entity_urn: str,
    ) -> Optional[UpstreamLineageClass]:
        if not card.result_metadata:
            return None

        ctx = self._get_mbql_context(card)
        if not ctx:
            return None

        fine_grained: List[FineGrainedLineageClass] = []
        for meta in card.result_metadata:
            if (
                not meta.name
                or not meta.field_ref
                or not isinstance(meta.field_ref, list)
            ):
                continue
            upstream_urns = self._resolve_field_ref_upstream_urns(meta.field_ref, ctx)
            if upstream_urns:
                fine_grained.append(
                    self._fine_grained_field(
                        upstreams=upstream_urns,
                        downstream=builder.make_schema_field_urn(
                            parent_urn=entity_urn, field_path=meta.name
                        ),
                    )
                )

        table_urns = self._get_table_urns_from_query_builder(card)
        if not table_urns:
            self.report.query_builder_cll_dropped += 1
            self.report.report_warning(
                title="Query-Builder Column Lineage Dropped",
                message="Query-builder card produced no resolvable source tables; column-level lineage was not emitted.",
                context=f"Card ID: {card.id}",
            )
            return None

        if not fine_grained:
            # Table-level lineage still emitted below, but no column mapping could
            # be resolved from the result metadata; surface the gap to operators.
            self.report.query_builder_cll_dropped += 1
            self.report.report_warning(
                title="Query-Builder Column Lineage Dropped",
                message="Query-builder card produced table-level lineage but no column-level lineage could be resolved.",
                context=f"Card ID: {card.id}",
            )

        return self._upstream_lineage(
            table_urns, DatasetLineageTypeClass.TRANSFORMED, fine_grained
        )

    def _get_cll_from_native_sql(
        self, card: MetabaseCard, entity_urn: str
    ) -> Optional[UpstreamLineageClass]:
        parsed = self._parse_native_sql(card)
        if not parsed:
            return None

        result, _ = parsed
        if result.debug_info.table_error:
            self._warn_native_sql_parse_failure(
                card.id, str(result.debug_info.table_error)
            )
            return None

        table_urns = [str(t) for t in result.in_tables]
        if not table_urns:
            return None

        fine_grained: List[FineGrainedLineageClass] = []

        if result.column_lineage:
            for col_lineage in result.column_lineage:
                if not col_lineage.downstream.column:
                    continue

                upstream_urns = [
                    builder.make_schema_field_urn(
                        parent_urn=str(upstream.table), field_path=upstream.column
                    )
                    for upstream in col_lineage.upstreams
                    if upstream.column
                ]

                if upstream_urns:
                    fine_grained.append(
                        self._fine_grained_field(
                            upstreams=upstream_urns,
                            downstream=builder.make_schema_field_urn(
                                parent_urn=entity_urn,
                                field_path=col_lineage.downstream.column,
                            ),
                        )
                    )

        return self._upstream_lineage(
            table_urns, DatasetLineageTypeClass.TRANSFORMED, fine_grained
        )

    def _get_ownership(self, creator_id: int) -> Optional[OwnershipClass]:
        # Cache successes and definitive 404s, but not transient errors: a single
        # timeout for one creator must not suppress ownership for the whole run.
        if creator_id in self._ownership_cache:
            return self._ownership_cache[creator_id]

        user_info_url = self._url(_API_USER, user_id=creator_id)
        try:
            user = MetabaseUser.model_validate(self._get_json(user_info_url))
        except ValidationError as e:
            self.report.report_warning(
                title="Invalid User Data",
                message="User data from Metabase API failed validation.",
                context=f"Creator ID: {creator_id}, Error: {str(e)}",
            )
            return None
        except requests.exceptions.RequestException as req_error:
            response = getattr(req_error, "response", None)
            if response is not None and response.status_code == 404:
                # A 404 is deterministic (user blocked/deleted), so cache it to
                # avoid re-requesting for every entity the same user created.
                self.report.report_warning(
                    title="Cannot find user",
                    message="User is blocked in Metabase or missing",
                    context=f"Creator ID: {creator_id}",
                )
                self._ownership_cache[creator_id] = None
                return None
            self.report.report_warning(
                title="Failed to retrieve user",
                message="Request to Metabase Failed",
                context=f"Creator ID: {creator_id}, Error: {str(req_error)}",
            )
            return None
        except ValueError as e:
            self.report.report_warning(
                title="Failed to retrieve user",
                message="Metabase returned a non-JSON response for a user request.",
                context=f"Creator ID: {creator_id}, Error: {str(e)}",
            )
            return None

        owner_urn = builder.make_user_urn(user.email)
        ownership = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=owner_urn,
                    type=OwnershipTypeClass.DATAOWNER,
                )
            ]
        )
        self._ownership_cache[creator_id] = ownership
        return ownership

    @lru_cache(maxsize=None)
    def _get_collections_map(self) -> Dict[str, MetabaseCollection]:
        """Cached to avoid N+1 API calls when tagging multiple entities."""
        # Embed the query in the URL (rather than passing params=) so the request
        # is byte-for-byte what the API expects; real requests treats both the same.
        collections_url = (
            f"{self._url(_API_COLLECTIONS)}"
            f"?exclude-other-user-collections="
            f"{json.dumps(self.config.exclude_other_user_collections)}"
        )
        try:
            collections_data = self._get_json(collections_url)
        except requests.exceptions.RequestException as req_error:
            response = getattr(req_error, "response", None)
            if response is not None and response.status_code == 404:
                # 404 is expected when collection features are disabled or unavailable
                logger.debug("Collections endpoint not found: %s", str(req_error))
                return {}
            self.report.report_warning(
                title="Failed to retrieve collections",
                message="Unable to fetch collections from Metabase API",
                context=f"Error: {str(req_error)} - Check API credentials and permissions",
            )
            return {}
        except ValueError as e:
            self.report.report_warning(
                title="Failed to retrieve collections",
                message="Metabase returned a non-JSON response for the collections request.",
                context=f"Error: {str(e)} - Check API credentials and permissions",
            )
            return {}

        if not isinstance(collections_data, list):
            self.report.report_failure(
                title="Unexpected Collections Response",
                message="Metabase returned a non-list body for the collections endpoint.",
                context=f"Type: {type(collections_data).__name__}",
            )
            return {}

        collections_dict: Dict[str, MetabaseCollection] = {}
        for coll_data in collections_data:
            try:
                coll = MetabaseCollection.model_validate(coll_data)
            except ValidationError as e:
                self.report.report_warning(
                    title="Invalid Collection Data",
                    message="Collection data from Metabase API failed validation.",
                    context=f"Data: {coll_data}, Error: {str(e)}",
                )
                continue
            if coll.is_root:
                continue
            collections_dict[str(coll.id)] = coll
        return collections_dict

    def _gen_collection_key(self, collection_id: int) -> MetabaseCollectionKey:
        return MetabaseCollectionKey(
            collection_id=collection_id,
            platform=self.platform,
            env=self.config.env,
            backcompat_env_as_instance=True,
        )

    def emit_collection_containers(self) -> Iterable[MetadataWorkUnit]:
        for collection in self._get_collections_map().values():
            assert isinstance(collection.id, int)
            yield from gen_containers(
                container_key=self._gen_collection_key(collection.id),
                name=collection.name,
                description=collection.description,
                sub_types=[BIContainerSubTypes.METABASE_COLLECTION],
            )

    def _get_tags_from_collection(
        self, collection_id: Optional[Union[int, str]]
    ) -> Optional[GlobalTagsClass]:
        if not self.config.extract_collections_as_tags or not collection_id:
            return None

        collections_map = self._get_collections_map()
        collection = collections_map.get(str(collection_id))

        if not collection:
            logger.debug(
                f"Collection {collection_id} not found in available collections"
            )
            return None

        collection_slug = collection.tag_slug
        if not collection_slug:
            logger.debug(
                f"Collection {collection_id} has empty name after sanitization"
            )
            return None

        tag_urn = builder.make_tag_urn(f"metabase_collection_{collection_slug}")

        return GlobalTagsClass(tags=[TagAssociationClass(tag=tag_urn)])

    def _list_card_items(self) -> List[MetabaseCardListItem]:
        # Fetched and validated once, then shared by both the chart and model
        # emitters (which each filter the same list) to avoid a duplicate /api/card
        # round-trip and re-validation.
        if self._card_items_cache is not None:
            return self._card_items_cache

        try:
            cards = self._get_json(self._url(_API_CARDS))
        except (requests.exceptions.RequestException, ValueError) as error:
            self.report.report_failure(
                title="Unable to Retrieve Cards",
                message="Request to retrieve cards from Metabase failed.",
                context=str(error),
            )
            return []

        if not isinstance(cards, list):
            self.report.report_failure(
                title="Unexpected Cards Response",
                message="Metabase returned a non-list body for the cards endpoint.",
                context=f"Type: {type(cards).__name__}",
            )
            return []

        items: List[MetabaseCardListItem] = []
        for card_data in cards:
            try:
                items.append(MetabaseCardListItem.model_validate(card_data))
            except ValidationError as e:
                self.report.report_warning(
                    title="Invalid Card List Item",
                    message="Card list item failed validation.",
                    context=f"Error: {str(e)}",
                )
        self._card_items_cache = items
        return items

    def emit_chart_workunits(self) -> Iterable[MetadataWorkUnit]:
        for card_info in self._list_card_items():
            # Models are emitted as datasets by emit_model_workunits()
            if self.config.extract_models and card_info.is_model:
                continue
            yield from self._emit_chart_workunits(card_info)

    def get_card_details_by_id(
        self, card_id: Union[int, str]
    ) -> Optional[MetabaseCard]:
        # Use legacy-mbql=true to get MBQL 4 format for compatibility.
        # Metabase 0.57+ returns MBQL 5 by default which has a different structure.
        return self._fetch(
            MetabaseCard,
            self._url(_API_CARD, card_id=card_id),
            label="Card",
            context=f"Card ID: {card_id}",
            params={"legacy-mbql": "true"},
        )

    def _create_input_field(
        self, upstream_urn: str, meta: MetabaseResultMetadata
    ) -> InputFieldClass:
        return InputFieldClass(
            schemaFieldUrn=upstream_urn,
            schemaField=SchemaFieldClass(
                fieldPath=meta.name or "",
                type=SchemaFieldDataTypeClass(
                    type=self._map_metabase_type_to_datahub_type(meta.base_type or "")
                ),
                nativeDataType=meta.base_type or "",
                description=meta.display_name or meta.name or "",
                nullable=True,
            ),
        )

    def _get_input_fields_from_card(self, card: MetabaseCard) -> List[InputFieldClass]:
        input_fields: List[InputFieldClass] = []

        if not card.result_metadata:
            return input_fields

        ctx = self._get_mbql_context(card)
        if ctx:
            for meta in card.result_metadata:
                if not meta.name:
                    continue

                upstream_urns: List[str] = []
                if meta.field_ref and isinstance(meta.field_ref, list):
                    upstream_urns = self._resolve_field_ref_upstream_urns(
                        meta.field_ref, ctx
                    )

                if upstream_urns:
                    for upstream_urn in upstream_urns:
                        input_fields.append(
                            self._create_input_field(upstream_urn, meta)
                        )
                else:
                    datasource_urns = self.get_datasource_urn(card)
                    if datasource_urns:
                        input_fields.append(
                            self._create_input_field(
                                builder.make_schema_field_urn(
                                    parent_urn=datasource_urns[0],
                                    field_path=meta.name,
                                ),
                                meta,
                            )
                        )
            return input_fields

        datasource_urns = self.get_datasource_urn(card)
        if not datasource_urns:
            return input_fields

        primary_datasource_urn = datasource_urns[0]

        for meta in card.result_metadata:
            if not meta.name:
                continue

            input_fields.append(
                self._create_input_field(
                    builder.make_schema_field_urn(
                        parent_urn=primary_datasource_urn,
                        field_path=meta.name,
                    ),
                    meta,
                )
            )

        return input_fields

    def _emit_chart_workunits(
        self, card_info: MetabaseCardListItem
    ) -> Iterable[MetadataWorkUnit]:
        card_details = self.get_card_details_by_id(card_info.id)
        if not card_details:
            self.report.charts_dropped += 1
            return

        self.report.charts_scanned += 1
        chart_urn = builder.make_chart_urn(
            platform=self.platform, name=str(card_details.id)
        )

        last_modified = ChangeAuditStampsClass(
            created=None,
            lastModified=self._last_modified_stamp(card_details.last_edit_info),
        )

        chart_type = self._get_chart_type(
            card_id=card_details.id, display_type=card_details.display or ""
        )
        datasource_urn = self.get_datasource_urn(card_details)

        input_edges = (
            [
                EdgeClass(
                    destinationUrn=urn,
                    lastModified=last_modified.lastModified,
                )
                for urn in datasource_urn
            ]
            if datasource_urn
            else None
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=ChartInfoClass(
                type=chart_type,
                description=card_details.description or "",
                title=card_details.name,
                lastModified=last_modified,
                chartUrl=f"{self.config.display_uri}/card/{card_details.id}",
                inputEdges=input_edges,
                customProperties=card_details.custom_properties,
            ),
        ).as_workunit()

        if card_details.query_type == _QUERY_TYPE_NATIVE:
            raw_query = self._extract_native_query(card_details)
            if raw_query:
                yield MetadataChangeProposalWrapper(
                    entityUrn=chart_urn,
                    aspect=ChartQueryClass(
                        rawQuery=raw_query,
                        type=ChartQueryTypeClass.SQL,
                    ),
                ).as_workunit()

        yield from self._emit_ownership_and_tags(
            chart_urn, card_details.creator_id, card_details.collection_id
        )

        input_fields = self._get_input_fields_from_card(card_details)
        if input_fields:
            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=InputFieldsClass(
                    fields=sorted(input_fields, key=lambda x: x.schemaFieldUrn)
                ),
            ).as_workunit()

        if card_details.collection_id is not None:
            yield from add_entity_to_container(
                container_key=self._gen_collection_key(card_details.collection_id),
                entity_type="chart",
                entity_urn=chart_urn,
            )

    def _get_chart_type(self, card_id: int, display_type: str) -> Optional[str]:
        if not display_type:
            self.report.report_warning(
                title="Unrecognized Card Type",
                message="Card has no display type. Setting chart type to None.",
                context=f"Card ID: {card_id}",
            )
            return None
        if display_type not in METABASE_CHART_DISPLAY_TYPE_MAP:
            self.report.report_warning(
                title="Unrecognized Chart Type",
                message="Unrecognized chart type found. Setting to None.",
                context=f"Card ID: {card_id}, Display Type: {display_type}",
            )
        return METABASE_CHART_DISPLAY_TYPE_MAP.get(display_type)

    def get_datasource_urn(
        self, card: MetabaseCard, recursion_depth: int = 0
    ) -> Optional[List[str]]:
        if self._check_recursion_limit(
            recursion_depth=recursion_depth,
            context="datasource URN extraction",
            card_id=card.id,
        ):
            return []

        table_urns = self._get_table_urns_from_card(
            card=card, recursion_depth=recursion_depth
        )
        return table_urns or None

    @staticmethod
    def strip_template_expressions(raw_query: str) -> str:
        # Metabase SQL parameters aren't valid SQL: drop [[optional]] clauses and
        # replace {{variable}} placeholders with "1" before parsing.
        # https://www.metabase.com/docs/latest/questions/native-editor/sql-parameters
        query_patched = _OPTIONAL_CLAUSE_PATTERN.sub(" ", raw_query)
        query_patched = _TEMPLATE_VARIABLE_PATTERN.sub("1", query_patched)
        return query_patched

    def get_source_table_from_id(
        self, table_id: Union[int, str]
    ) -> Tuple[Optional[str], Optional[str]]:
        table = self._fetch(
            MetabaseTable,
            self._url(_API_TABLE, table_id=table_id),
            label="Source Table",
            context=f"Table ID: {table_id}",
        )
        return (table.schema_, table.name) if table else (None, None)

    def get_field_from_id(self, field_id: int) -> Optional[MetabaseField]:
        return self._fetch(
            MetabaseField,
            self._url(_API_FIELD, field_id=field_id),
            label="Field",
            context=f"Field ID: {field_id}",
        )

    @lru_cache(maxsize=None)
    def get_platform_instance(
        self, platform: Optional[str] = None, datasource_id: Optional[int] = None
    ) -> Optional[str]:
        # database_id_to_instance_map (keyed by datasource id) takes precedence
        # over platform_instance_map (keyed by platform).
        platform_instance = None
        if datasource_id is not None and self.config.database_id_to_instance_map:
            platform_instance = self.config.database_id_to_instance_map.get(
                str(datasource_id)
            )

        if platform and self.config.platform_instance_map and platform_instance is None:
            platform_instance = self.config.platform_instance_map.get(platform)

        return platform_instance

    def get_datasource_from_id(
        self, datasource_id: Union[int, str]
    ) -> Optional[DatasourceInfo]:
        database = self._fetch(
            MetabaseDatabase,
            self._url(_API_DATABASE, database_id=datasource_id),
            label="Database",
            context=f"Database ID: {datasource_id}",
        )
        if database is None:
            return None

        engine = database.engine

        engine_mapping = dict(METABASE_ENGINE_TO_DATAHUB_PLATFORM)
        if self.config.engine_platform_map is not None:
            engine_mapping.update(self.config.engine_platform_map)

        if engine in engine_mapping:
            platform = engine_mapping[engine]
        else:
            platform = engine
            if engine not in _KNOWN_METABASE_ENGINES:
                self.report.report_warning(
                    title="Unrecognized Data Platform found",
                    message="Data Platform was not found. Using platform name as is",
                    context=f"Platform: {platform}",
                )

        platform_instance = self.get_platform_instance(
            platform=platform, datasource_id=database.id
        )

        dbname = (
            database.details.get_database_name(engine) if database.details else None
        )
        schema = database.details.schema_ if database.details else None

        if (
            self.config.database_alias_map is not None
            and platform in self.config.database_alias_map
        ):
            dbname = self.config.database_alias_map[platform]

        if dbname is None:
            self.report.report_warning(
                title="Cannot resolve Database Name",
                message="Cannot determine database name for platform",
                context=f"Platform: {platform}",
            )

        return DatasourceInfo(
            platform=platform,
            database_name=dbname,
            schema_=schema,
            platform_instance=platform_instance,
        )

    def emit_model_workunits(self) -> Iterable[MetadataWorkUnit]:
        if not self.config.extract_models:
            return

        for card_info in self._list_card_items():
            if not card_info.is_model:
                continue
            yield from self._emit_model_workunits(card_info)

    def _map_metabase_type_to_datahub_type(
        self, metabase_type: str
    ) -> Union[
        NumberTypeClass,
        StringTypeClass,
        BooleanTypeClass,
        EnumTypeClass,
        BytesTypeClass,
        DateTypeClass,
        TimeTypeClass,
        NullTypeClass,
    ]:
        """Map Metabase base_type (e.g. "type/Integer") to DataHub type."""
        for type_keyword, datahub_class in METABASE_TYPE_TO_DATAHUB_TYPE.items():
            if type_keyword in metabase_type:
                return datahub_class()

        return NullTypeClass()

    def _get_schema_fields_from_result_metadata(
        self,
        result_metadata: List[MetabaseResultMetadata],
        card: Optional[MetabaseCard] = None,
    ) -> List[SchemaFieldClass]:
        schema_fields: List[SchemaFieldClass] = []

        for field_meta in result_metadata:
            field_name = field_meta.name or field_meta.display_name or ""
            base_type = field_meta.base_type or ""

            if not field_name:
                continue

            description = ""
            if (
                field_meta.field_ref
                and isinstance(field_meta.field_ref, list)
                and len(field_meta.field_ref) > 0
            ):
                ref_type = field_meta.field_ref[0]

                if (
                    ref_type == _MBQL_REF_EXPRESSION
                    and card
                    and card.dataset_query
                    and card.dataset_query.query
                    and card.dataset_query.query.expressions
                ):
                    expr_name = (
                        field_meta.field_ref[1]
                        if len(field_meta.field_ref) > 1
                        else None
                    )
                    if expr_name:
                        expr_value = card.dataset_query.query.expressions.get(
                            str(expr_name)
                        )
                        if expr_value:
                            description = f"Expression: {expr_value}"
                elif ref_type == _MBQL_REF_AGGREGATION:
                    description = (
                        f"Aggregation: {field_meta.display_name or field_name}"
                    )

            schema_fields.append(
                SchemaFieldClass(
                    fieldPath=field_name,
                    type=SchemaFieldDataTypeClass(
                        type=self._map_metabase_type_to_datahub_type(base_type)
                    ),
                    nativeDataType=base_type,
                    description=description,
                    nullable=True,
                )
            )

        return schema_fields

    def _get_model_view_properties(
        self, card: MetabaseCard
    ) -> Optional[ViewPropertiesClass]:
        if card.dataset_query and card.query_type == _QUERY_TYPE_NATIVE:
            raw_query = self._extract_native_query(card)
            if raw_query:
                return ViewPropertiesClass(
                    materialized=False,
                    viewLogic=raw_query,
                    viewLanguage="SQL",
                )
        return None

    def _get_model_subtypes(self, card: MetabaseCard) -> List[str]:
        is_passthrough = False
        if card.dataset_query and card.dataset_query.query:
            is_passthrough = card.dataset_query.query.is_passthrough()

        subtypes: List[str] = [DatasetSubTypes.METABASE_MODEL]
        if not is_passthrough:
            subtypes.append(DatasetSubTypes.VIEW)

        return subtypes

    def _get_model_lineage(
        self, card: MetabaseCard, model_urn: str
    ) -> Optional[UpstreamLineageClass]:
        if card.dataset_query and card.dataset_query.type == _QUERY_TYPE_QUERY:
            cll = self._get_cll_from_query_builder(card=card, entity_urn=model_urn)

            if cll and not cll.fineGrainedLineages:
                is_passthrough = (
                    card.dataset_query.query.is_passthrough()
                    if card.dataset_query.query
                    else False
                )
                if is_passthrough:
                    passthrough_cll = self._get_passthrough_cll(
                        card=card, entity_urn=model_urn
                    )
                    if passthrough_cll and passthrough_cll.fineGrainedLineages:
                        cll = passthrough_cll

            return cll
        elif card.query_type == _QUERY_TYPE_NATIVE:
            return self._get_cll_from_native_sql(card=card, entity_urn=model_urn)
        else:
            table_urns = self._get_table_urns_from_card(card)
            if table_urns:
                return self._upstream_lineage(
                    table_urns, DatasetLineageTypeClass.TRANSFORMED
                )
        return None

    def _emit_model_workunits(
        self, card_info: MetabaseCardListItem
    ) -> Iterable[MetadataWorkUnit]:
        card = self.get_card_details_by_id(card_info.id)
        if not card:
            self.report.models_dropped += 1
            return

        self.report.models_scanned += 1
        model_urn = self._model_urn(card.id)

        custom_properties = {
            "model_id": str(card.id),
            "display_type": card.display or "",
            "metabase_url": f"{self.config.display_uri}/model/{card.id}",
            "query_type": card.query_type or "unknown",
        }

        if card.query_type == _QUERY_TYPE_NATIVE:
            raw_query = self._extract_native_query(card)
            if raw_query:
                custom_properties["query"] = raw_query
        elif card.query_type == _QUERY_TYPE_QUERY:
            custom_properties["query_type"] = "query_builder"
            if card.dataset_query and card.dataset_query.query:
                custom_properties["mbql_query"] = json.dumps(
                    card.dataset_query.query.model_dump(
                        exclude_none=True, by_alias=True
                    )
                )

        yield MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            aspect=DatasetPropertiesClass(
                name=card.name,
                description=card.description or "",
                customProperties=custom_properties,
            ),
        ).as_workunit()

        if card.result_metadata:
            schema_fields = self._get_schema_fields_from_result_metadata(
                card.result_metadata, card
            )
            if schema_fields:
                yield MetadataChangeProposalWrapper(
                    entityUrn=model_urn,
                    aspect=SchemaMetadataClass(
                        schemaName=card.name,
                        platform=builder.make_data_platform_urn(self.platform),
                        version=0,
                        hash="",
                        platformSchema=MySqlDDLClass(tableSchema=""),
                        fields=schema_fields,
                    ),
                ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            aspect=SubTypesClass(typeNames=self._get_model_subtypes(card)),
        ).as_workunit()

        view_properties = self._get_model_view_properties(card)
        if view_properties:
            yield MetadataChangeProposalWrapper(
                entityUrn=model_urn,
                aspect=view_properties,
            ).as_workunit()

        lineage = self._get_model_lineage(card=card, model_urn=model_urn)
        if lineage:
            yield MetadataChangeProposalWrapper(
                entityUrn=model_urn,
                aspect=lineage,
            ).as_workunit()

        yield from self._emit_ownership_and_tags(
            model_urn, card.creator_id, card.collection_id
        )

        if card.collection_id is not None:
            yield from add_entity_to_container(
                container_key=self._gen_collection_key(card.collection_id),
                entity_type="dataset",
                entity_urn=model_urn,
            )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from self.emit_collection_containers()
        yield from self.emit_chart_workunits()
        yield from self.emit_dashboard_workunits()
        yield from self.emit_model_workunits()

    def get_report(self) -> SourceReport:
        return self.report
