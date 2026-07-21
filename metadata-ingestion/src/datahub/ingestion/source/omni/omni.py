"""Omni DataHub ingestion source.

Extracts metadata from the Omni BI platform via its REST API and emits
DataHub metadata work-units covering:
  - Semantic models / topics / views (as Datasets)
  - Physical database tables (as Datasets with lineage back to views)
  - Dashboards and tiles/charts (as Dashboard + Chart entities)
  - Folder hierarchy (as Containers)
  - Coarse and fine-grained lineage:  Dashboard → Tile → Topic → View → DB Table
  - Schema metadata (dimensions + measures) for Omni semantic views
  - Ownership from document API
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterator, List, Literal, Optional, Set

from datahub.emitter.mce_builder import (
    make_chart_urn,
    make_dataset_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
    CapabilityReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.omni.omni_api import OmniClient
from datahub.ingestion.source.omni.omni_config import OmniSourceConfig
from datahub.ingestion.source.omni.omni_lineage_parser import (
    FieldRef,
    extract_field_refs,
    parse_field_list,
)
from datahub.ingestion.source.omni.omni_report import OmniSourceReport
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    NumberTypeClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import CorpUserUrn
from datahub.sdk import Chart, Dashboard, Dataset
from datahub.utilities.sentinels import unset

logger = logging.getLogger(__name__)

FieldConfidence = Literal["unresolved", "exact", "derived"]


@dataclass(frozen=True)
class _ModelContext:
    """Context information for an Omni model used for lineage resolution.

    Stored for ALL models (even filtered ones) because dashboards may reference
    any model's topics/views for lineage.
    """

    connection_id: str
    platform: Optional[str]
    database: Optional[str]
    platform_instance: Optional[str]
    model_kind: Optional[str]
    model_layer: str


@dataclass
class _SemanticField:
    model_id: str
    view_name: str
    field_name: str
    expression: str = ""
    confidence: FieldConfidence = "unresolved"
    upstream_physical_urns: Set[str] = field(default_factory=set)


@dataclass
class _TileCollectionResult:
    """Result of collecting tile data from a dashboard document."""

    model_id: Optional[str] = None
    work_units: List[MetadataWorkUnit] = field(default_factory=list)


@platform_name("Omni")
@support_status(SupportStatus.INCUBATING)
@config_class(OmniSourceConfig)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE, "Dashboard → Tile → Topic → View → DB Table"
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Field-level lineage when include_column_lineage=true",
)
@capability(
    SourceCapability.SCHEMA_METADATA,
    "Dimensions and measures extracted as schema columns",
)
@capability(SourceCapability.OWNERSHIP, "Document owner extracted from Omni API")
@capability(
    SourceCapability.PLATFORM_INSTANCE,
    "Supported via connection_to_platform_instance config",
)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class OmniSource(StatefulIngestionSourceBase, TestableSource):
    """Ingestion source for the Omni BI platform.

    Ingestion runs in four stages: connections, semantic models, folders, and
    documents (dashboards/workbooks).  Topics and views are discovered during the
    document stage via the ``get_topic`` API — model processing emits only the
    model dataset itself.

    The ``get_model_yaml`` endpoint was intentionally removed: in production it
    accounted for ~22k API calls (97% of API time) yet yielded zero topics or
    views because the YAML format returned by Omni does not match the
    ``type: topic`` / ``type: view`` structure the parser expected.  All topic
    data is already available through the ``get_topic`` API called during
    dashboard tile processing.
    """

    PLATFORM = "omni"

    def __init__(self, config: OmniSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.config = config
        self.report: OmniSourceReport = OmniSourceReport()
        self.client = OmniClient(
            base_url=config.base_url,
            api_key=config.api_key,
            timeout_seconds=config.timeout_seconds,
            report=self.report.client_report,
        )
        # Internal caches – populated during _ingest_semantic_model and reused
        # later when processing documents/dashboards.
        self._semantic_fields: Dict[str, _SemanticField] = {}
        self._semantic_dataset_urns: Set[str] = set()
        self._topic_dataset_urns: Set[str] = set()
        self._topic_urn_by_key: Dict[str, str] = {}
        self._topic_ingested_keys: Set[str] = set()
        self._physical_dataset_urns: Set[str] = set()
        self._model_dataset_urns: Set[str] = set()
        self._folder_dataset_urns: Set[str] = set()
        self._connection_dataset_urns: Set[str] = set()
        self._connections_by_id: Dict[str, Dict[str, object]] = {}
        self._model_context_by_id: Dict[str, _ModelContext] = {}

        # Lock for source-level collections (URN sets, context dicts, registries)
        # We use a single lock because:
        # 1. Lock contention is minimal - most time is spent in I/O (API calls)
        # 2. Simpler code - no need to reason about lock ordering/deadlocks
        # 3. Critical sections are small - just set.add() or dict.__setitem__()
        # If profiling shows lock contention, we can split into per-collection locks
        self._state_lock = threading.Lock()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "OmniSource":
        config = OmniSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_report(self) -> OmniSourceReport:
        return self.report

    # ------------------------------------------------------------------
    # TestableSource
    # ------------------------------------------------------------------

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        report = TestConnectionReport()
        try:
            config = OmniSourceConfig.model_validate(config_dict)
            client = OmniClient(
                base_url=config.base_url,
                api_key=config.api_key,
                timeout_seconds=config.timeout_seconds,
            )
            if client.test_connection():
                report.basic_connectivity = CapabilityReport(capable=True)
            else:
                report.basic_connectivity = CapabilityReport(
                    capable=False,
                    failure_reason="API returned an unsuccessful response.",
                )
        except Exception as exc:
            report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(exc)
            )
        return report

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    def _emit_upstream_lineage(
        self,
        dataset_urn: str,
        upstreams: Set[str],
        fine_grained_lineages: Optional[List[FineGrainedLineageClass]] = None,
    ) -> Iterator[MetadataWorkUnit]:
        if not upstreams and not fine_grained_lineages:
            return
        lineage = UpstreamLineageClass(
            upstreams=[
                UpstreamClass(dataset=u, type=DatasetLineageTypeClass.TRANSFORMED)
                for u in sorted(upstreams)
            ],
            fineGrainedLineages=fine_grained_lineages or None,
        )
        self.report.increment_counter("dataset_lineage_edges_emitted", len(upstreams))
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=lineage
        ).as_workunit()

    def _build_view_to_physical_lineage(
        self,
        model_id: str,
        view_name: str,
        physical_urn: str,
        view_fields: Set[str],
    ) -> List[FineGrainedLineageClass]:
        """Build field-level lineage edges from a semantic view to its physical table.

        For each dimension/measure in the view, creates an edge mapping:
          physical_table.field_name → semantic_view.field_name
        """
        semantic_urn = self._semantic_dataset_urn(model_id, view_name)
        edges: List[FineGrainedLineageClass] = []
        for field_name in sorted(view_fields):
            upstream_field = make_schema_field_urn(physical_urn, field_name)
            downstream_field = make_schema_field_urn(semantic_urn, field_name)
            edges.append(
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    upstreams=[upstream_field],
                    downstreams=[downstream_field],
                    transformOperation="OMNI_VIEW_FIELD_MAPPING",
                )
            )
        self.report.increment_counter(
            "view_to_physical_column_lineage_edges", len(edges)
        )
        return edges

    def _clear_upstream_lineage(self, dataset_urn: str) -> Iterator[MetadataWorkUnit]:
        """Emit an explicit empty lineage to clear stale edges from prior runs."""
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=UpstreamLineageClass(upstreams=[], fineGrainedLineages=None),
        ).as_workunit()

    # ------------------------------------------------------------------
    # Schema / type helpers
    # ------------------------------------------------------------------

    def _infer_schema_type(self, native_type: str) -> SchemaFieldDataTypeClass:
        lowered = (native_type or "").lower()
        if "bool" in lowered:
            return SchemaFieldDataTypeClass(type=BooleanTypeClass())
        if any(
            t in lowered
            for t in ("int", "number", "numeric", "decimal", "float", "double")
        ):
            return SchemaFieldDataTypeClass(type=NumberTypeClass())
        return SchemaFieldDataTypeClass(type=StringTypeClass())

    # ------------------------------------------------------------------
    # URN helpers
    # ------------------------------------------------------------------

    def _semantic_dataset_urn(self, model_id: str, view_name: str) -> str:
        return make_dataset_urn(
            self.PLATFORM, f"{model_id}.{view_name}", self.config.env
        )

    def _topic_dataset_urn(self, model_id: str, topic_name: str) -> str:
        return make_dataset_urn(
            self.PLATFORM, f"{model_id}.topic.{topic_name}", self.config.env
        )

    def _model_dataset_urn(self, model_id: str) -> str:
        return make_dataset_urn(self.PLATFORM, f"model.{model_id}", self.config.env)

    def _folder_dataset_urn(self, folder_id: str) -> str:
        return make_dataset_urn(self.PLATFORM, f"folder.{folder_id}", self.config.env)

    def _connection_dataset_urn(self, connection_id: str) -> str:
        return make_dataset_urn(
            self.PLATFORM, f"connection.{connection_id}", self.config.env
        )

    def _resolve_platform_from_connection(
        self,
        connection_id: Optional[str],
        conn: Optional[Dict[str, object]],
        warn_on_missing: bool = False,
        model_id: Optional[str] = None,
        model_name: Optional[str] = None,
    ) -> Optional[str]:
        """
        Resolve platform name from connection.

        Priority:
        1. Connection dialect from Omni API (auto-detected)
        2. Manual connection_to_platform config mapping (fallback)

        If warn_on_missing=True and platform cannot be resolved, emits a structured warning.

        Returns None if platform cannot be determined.
        """
        platform: Optional[str] = None

        # Try auto-detection from connection dialect
        if conn and conn.get("dialect"):
            platform = str(conn.get("dialect"))

        # Fall back to manual config mapping
        if not platform and self.config.connection_to_platform and connection_id:
            platform = self.config.connection_to_platform.get(connection_id)

        # Warn if platform resolution failed
        if warn_on_missing and connection_id and not platform:
            self.report.warning(
                title="Missing platform mapping for connection",
                message="Model has a connection but no platform could be resolved. Physical lineage will be skipped for this model.",
                context=f"model_id={model_id}, model_name={model_name}, connection_id={connection_id}",
            )

        return platform

    def _resolve_database_from_connection(
        self,
        connection_id: Optional[str],
        conn: Optional[Dict[str, object]],
    ) -> Optional[str]:
        """
        Resolve database name from connection.

        Priority:
        1. Connection database from Omni API (auto-detected)
        2. Manual connection_to_database config mapping (override)

        Returns None if database cannot be determined.
        Note: Database is optional - some platforms don't use databases.
        """
        database: Optional[str] = None

        # Try auto-detection from connection
        if conn and conn.get("database"):
            database = str(conn.get("database"))

        # Override with manual config if provided
        if self.config.connection_to_database and connection_id:
            database = self.config.connection_to_database.get(connection_id, database)

        return database

    def _resolve_platform_instance_from_connection(
        self,
        connection_id: Optional[str],
    ) -> Optional[str]:
        """
        Resolve platform instance from connection.

        Uses manual connection_to_platform_instance config mapping.
        Platform instance must match the one used when ingesting the warehouse.

        Returns None if not configured (platform instance is optional).
        """
        if self.config.connection_to_platform_instance and connection_id:
            return self.config.connection_to_platform_instance.get(connection_id)
        return None

    def _physical_dataset_urn(
        self,
        platform: str,
        database: str,
        schema: str,
        table: str,
        platform_instance: Optional[str] = None,
    ) -> str:
        db, sc, tb = database or "", schema or "", table or ""
        if self.config.normalize_snowflake_names and platform.lower() == "snowflake":
            db, sc, tb = db.upper(), sc.upper(), tb.upper()
        full_name = ".".join(p for p in [db, sc, tb] if p)
        if platform_instance:
            return make_dataset_urn_with_platform_instance(
                platform=platform,
                name=full_name,
                platform_instance=platform_instance,
                env=self.config.env,
            )
        return make_dataset_urn(platform, full_name, self.config.env)

    def _canonical_semantic_field_key(
        self, model_id: str, view_name: str, field_name: str
    ) -> str:
        return f"{model_id}:{view_name}.{field_name}"

    # ------------------------------------------------------------------
    # SDK V2 entity emitters
    # ------------------------------------------------------------------

    def _emit_dataset(
        self,
        *,
        name: str,
        description: str,
        custom_properties: Dict[str, str],
        subtype: str,
        display_name: Optional[str] = None,
        upstreams: Optional[UpstreamLineageClass] = None,
        schema_fields: Optional[List[SchemaFieldClass]] = None,
        owner_id: str = "",
        owner_name: str = "",
        external_url: Optional[str] = None,
        platform: Optional[str] = None,
        platform_instance: Optional[str] = None,
    ) -> Iterator[MetadataWorkUnit]:
        platform = platform or self.PLATFORM
        dataset = Dataset(
            platform=platform,
            name=name,
            display_name=display_name,
            platform_instance=platform_instance,
            env=self.config.env,
            description=description,
            custom_properties=custom_properties,
            external_url=external_url,
            subtype=subtype,
            schema=schema_fields or None,
            upstreams=upstreams,
        )
        if owner_id or owner_name:
            owner_urn = make_user_urn(owner_id or owner_name)
            dataset.set_owners([(CorpUserUrn(owner_urn), OwnershipTypeClass.DATAOWNER)])
        yield from dataset.as_workunits()

    def _emit_dashboard(
        self,
        *,
        doc_id: str,
        title: str,
        description: str,
        external_url: str,
        chart_urns: List[str],
        custom_properties: Dict[str, str],
        owner_id: str = "",
        owner_name: str = "",
        folder_urn: Optional[str] = None,
        updated_at: Optional[str] = None,
    ) -> Iterator[MetadataWorkUnit]:
        dashboard = Dashboard(
            platform=self.PLATFORM,
            platform_instance=self.config.platform_instance,
            name=doc_id,
            display_name=title,
            description=description,
            external_url=external_url,
            dashboard_url=external_url,
            custom_properties=custom_properties,
            parent_container=[folder_urn] if folder_urn else unset,
        )
        dashboard.set_charts(chart_urns)
        if updated_at:
            try:
                dt = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
                dashboard.set_last_modified(dt)
            except Exception as exc:
                logger.warning(
                    "Skipping dashboard last_modified: failed to parse %r: %s",
                    updated_at,
                    exc,
                )
        if owner_id or owner_name:
            owner_urn = make_user_urn(owner_id or owner_name)
            dashboard.set_owners(
                [(CorpUserUrn(owner_urn), OwnershipTypeClass.DATAOWNER)]
            )
        yield from dashboard.as_workunits()

    def _emit_chart(
        self,
        *,
        qp_id: str,
        title: str,
        description: str,
        external_url: str,
        input_urns: List[str],
        custom_properties: Dict[str, str],
        owner_id: str = "",
        owner_name: str = "",
        updated_at: Optional[str] = None,
    ) -> Iterator[MetadataWorkUnit]:
        chart = Chart(
            platform=self.PLATFORM,
            platform_instance=self.config.platform_instance,
            name=qp_id,
            display_name=title,
            description=description,
            external_url=external_url,
            chart_url=external_url,
            custom_properties=custom_properties,
            input_datasets=input_urns or None,
        )
        if updated_at:
            try:
                dt = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
                chart.set_last_modified(dt)
            except Exception as exc:
                logger.warning(
                    "Skipping chart last_modified: failed to parse %r: %s",
                    updated_at,
                    exc,
                )
        if owner_id or owner_name:
            owner_urn = make_user_urn(owner_id or owner_name)
            chart.set_owners([(CorpUserUrn(owner_urn), OwnershipTypeClass.DATAOWNER)])
        yield from chart.as_workunits()

    # ------------------------------------------------------------------
    # Connection / model-level helpers
    # ------------------------------------------------------------------

    def _normalize_model_layer(self, model_kind: Optional[str]) -> str:
        kind = (model_kind or "").upper()
        return {
            "WORKBOOK": "workbook",
            "SHARED": "shared",
            "SCHEMA": "schema",
            "BRANCH": "branch",
        }.get(kind, (model_kind or "unknown").lower())

    def _ensure_connection_dataset(
        self, connection_id: str, connection: Optional[Dict[str, object]] = None
    ) -> Iterator[MetadataWorkUnit]:
        if not connection_id:
            return
        connection_urn = self._connection_dataset_urn(connection_id)
        if connection_urn in self._connection_dataset_urns:
            return
        payload = connection or {}
        connection_name = str(payload.get("name") or connection_id)
        yield from self._emit_dataset(
            name=f"connection.{connection_id}",
            description="Omni connection entity.",
            custom_properties={
                "entityType": "connection",
                "connectionId": connection_id,
                "connectionName": connection_name,
                "dialect": str(payload.get("dialect") or ""),
                "database": str(payload.get("database") or ""),
                "scope": str(payload.get("scope") or ""),
                "deleted": str(bool(payload.get("deleted"))).lower(),
            },
            subtype="Connection",
        )
        self._connection_dataset_urns.add(connection_urn)
        self.report.increment_counter("connections_emitted")

    # ------------------------------------------------------------------
    # Topic / view ingestion
    # ------------------------------------------------------------------

    def _ingest_topic_payload(
        self,
        model_id: str,
        topic_name: str,
        topic: Dict[str, Any],
        platform: Optional[str],
        database: Optional[str],
        connection_id: Optional[str],
        platform_instance: Optional[str],
        inferred: bool = False,
        model_custom_properties: Optional[Dict[str, str]] = None,
        model_name: Optional[str] = None,
    ) -> Iterator[MetadataWorkUnit]:
        if not topic:
            return
        topic_key = f"{model_id}:{topic_name}"
        if topic_key in self._topic_ingested_keys:
            return
        self._topic_ingested_keys.add(topic_key)
        self.report.increment_counter("topics_scanned")

        topic_urn = self._topic_dataset_urn(model_id, topic_name)
        self._topic_dataset_urns.add(topic_urn)
        self._topic_urn_by_key[topic_key] = topic_urn

        # Clear any stale reverse edges from prior runs.
        yield from self._clear_upstream_lineage(topic_urn)

        topic_props: Dict[str, str] = {
            "modelId": model_id,
            "topicName": topic_name,
            "entityType": "topic",
            "inferred": "true" if inferred else "false",
        }
        if model_custom_properties:
            topic_props.update(model_custom_properties)

        readable_model = model_name or model_id
        yield from self._emit_dataset(
            name=f"{model_id}.topic.{topic_name}",
            display_name=f"{readable_model}.topic.{topic_name}",
            description="Omni topic entity.",
            custom_properties=topic_props,
            subtype=DatasetSubTypes.TOPIC,
        )
        self.report.increment_counter("topics_emitted")

        topic_view_urns: Set[str] = set()
        views_raw = topic.get("views", [])
        views = views_raw if isinstance(views_raw, list) else []

        logger.debug(
            "Full topic object for %s: %s",
            topic_name,
            topic,
        )

        logger.info(
            "Processing topic views: model_id=%s topic_name=%s view_count=%d",
            model_id,
            topic_name,
            len(views),
        )

        for view in views:
            view_name = (view or {}).get("name") if isinstance(view, dict) else None
            if not view_name:
                continue

            logger.debug(
                "Processing view: model_id=%s topic_name=%s view_name=%s",
                model_id,
                topic_name,
                view_name,
            )

            logger.debug(
                "Full view object for %s: %s",
                view_name,
                view,
            )

            semantic_urn = self._semantic_dataset_urn(model_id, view_name)
            self._semantic_dataset_urns.add(semantic_urn)

            schema_fields: List[SchemaFieldClass] = []
            seen_fields: Set[str] = set()
            physical_urn: Optional[str] = None

            schema = view.get("schema") or ""
            catalog = view.get("catalog") or ""
            table = view.get("table_name") or view.get("semantic_view_name") or ""
            table_source = (
                "table_name"
                if view.get("table_name")
                else "semantic_view_name"
                if view.get("semantic_view_name")
                else "none"
            )
            logger.debug(
                "View physical binding for model=%s view=%s: "
                "table=%r (from %s) catalog=%r schema=%r connection_db=%r platform=%r "
                "normalize_snowflake_names=%s convert_urns_to_lowercase=%s",
                model_id,
                view.get("name", ""),
                table,
                table_source,
                catalog,
                schema,
                database,
                platform,
                self.config.normalize_snowflake_names,
                self.config.convert_urns_to_lowercase,
            )
            if table and platform:
                effective_database = catalog or database or ""
                logger.debug(
                    "Constructing physical URN for model=%s view=%s: "
                    "effective_database=%r (catalog=%r overrides connection_db=%r) "
                    "schema=%r table=%r platform_instance=%r",
                    model_id,
                    view.get("name", ""),
                    effective_database,
                    catalog,
                    database,
                    schema,
                    table,
                    platform_instance,
                )
                # Type narrowing: at this point platform is guaranteed to be str (not None)
                assert platform is not None
                physical_urn = self._physical_dataset_urn(
                    platform, effective_database, schema, table, platform_instance
                )
                logger.debug(
                    "Physical URN constructed for lineage: model=%s view=%s physical_urn=%r",
                    model_id,
                    view.get("name", ""),
                    physical_urn,
                )
                self._physical_dataset_urns.add(physical_urn)

            # Collect schema fields and register semantic field metadata
            for dimension in view.get("dimensions", []):
                fn = dimension.get("field_name")
                if not fn:
                    continue
                if fn not in seen_fields:
                    native_type = str(
                        dimension.get("sql_type")
                        or dimension.get("data_type")
                        or dimension.get("type")
                        or "STRING"
                    )
                    schema_fields.append(
                        SchemaFieldClass(
                            fieldPath=fn,
                            type=self._infer_schema_type(native_type),
                            nativeDataType=native_type,
                            description=dimension.get("description"),
                            nullable=True,
                        )
                    )
                    seen_fields.add(fn)
                key = self._canonical_semantic_field_key(model_id, view_name, fn)
                sf = _SemanticField(
                    model_id=model_id,
                    view_name=view_name,
                    field_name=fn,
                    expression=dimension.get("dialect_sql")
                    or dimension.get("display_sql")
                    or "",
                    confidence="unresolved",
                )
                if physical_urn:
                    sf.upstream_physical_urns.add(physical_urn)
                self._semantic_fields[key] = sf

            for measure in view.get("measures", []):
                fn = measure.get("field_name")
                if not fn:
                    continue
                if fn not in seen_fields:
                    native_type = str(
                        measure.get("sql_type")
                        or measure.get("data_type")
                        or measure.get("type")
                        or "NUMBER"
                    )
                    schema_fields.append(
                        SchemaFieldClass(
                            fieldPath=fn,
                            type=self._infer_schema_type(native_type),
                            nativeDataType=native_type,
                            description=measure.get("description"),
                            nullable=True,
                        )
                    )
                    seen_fields.add(fn)
                expr = measure.get("dialect_sql") or measure.get("display_sql") or ""
                refs = extract_field_refs(expr)
                confidence: FieldConfidence = (
                    "exact" if refs else "derived" if expr else "unresolved"
                )
                key = self._canonical_semantic_field_key(model_id, view_name, fn)
                sf = _SemanticField(
                    model_id=model_id,
                    view_name=view_name,
                    field_name=fn,
                    expression=expr,
                    confidence=confidence,
                )
                if physical_urn:
                    sf.upstream_physical_urns.add(physical_urn)
                self._semantic_fields[key] = sf

            view_props = {
                "modelId": model_id,
                "topicName": topic_name,
                "viewName": view_name,
            }
            if model_custom_properties:
                view_props.update(model_custom_properties)

            view_upstreams: Optional[UpstreamLineageClass] = None
            if physical_urn:
                view_fine_grained: Optional[List[FineGrainedLineageClass]] = None
                if self.config.include_column_lineage:
                    view_fine_grained = self._build_view_to_physical_lineage(
                        model_id, view_name, physical_urn, seen_fields
                    )
                view_upstreams = UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=physical_urn,
                            type=DatasetLineageTypeClass.COPY,
                        )
                    ],
                    fineGrainedLineages=view_fine_grained or None,
                )
                logger.debug(
                    "Creating lineage for view: semantic_view=%s upstream_physical=%s fine_grained_edges=%d",
                    f"{model_id}.{view_name}",
                    physical_urn,
                    len(view_fine_grained) if view_fine_grained else 0,
                )
            else:
                logger.debug(
                    "No lineage created for view (no physical URN): semantic_view=%s",
                    f"{model_id}.{view_name}",
                )
            yield from self._emit_dataset(
                name=f"{model_id}.{view_name}",
                display_name=f"{readable_model}.{view_name}",
                description=f"Omni semantic view from topic {topic_name}.",
                custom_properties=view_props,
                subtype="View",
                schema_fields=schema_fields or None,
                upstreams=view_upstreams,
            )
            topic_view_urns.add(semantic_urn)
            self.report.increment_counter("views_emitted")

        # Topic is built from its views — emit topic upstream lineage
        if topic_view_urns:
            yield from self._emit_upstream_lineage(topic_urn, topic_view_urns)

    # ------------------------------------------------------------------
    # Top-level ingestion stages
    # ------------------------------------------------------------------

    def _warn_unmapped_connections(
        self, connections: Dict[str, Dict[str, object]]
    ) -> None:
        self.report.info(
            title="Connection found",
            message="Found Omni connection(s)",
            context=f"connections={list(connections.keys())}",
        )
        unmapped = [
            conn_id
            for conn_id, conn in connections.items()
            if not (
                self.config.connection_to_platform
                and conn_id in self.config.connection_to_platform
            )
            and not (conn or {}).get("dialect")
        ]

        if unmapped:
            self.report.warning(
                title="Connections missing platform mapping",
                message="Found connection(s) without platform mapping. Add them to recipe config: connection_to_platform. Omitting these connections will result in missing lineage and datasets.",
                context=f"unmapped={unmapped}",
            )

    def _setup_connections(self) -> Iterator[MetadataWorkUnit]:
        """Fetch Omni connections and emit connection datasets.

        This runs sequentially before parallel model processing begins,
        populating self._connections_by_id for use by worker threads.
        """
        try:
            self._connections_by_id = {
                str(c["id"]): c
                for c in self.client.list_connections(self.config.include_deleted)
                if c.get("id") is not None
            }

            for connection_id, connection in self._connections_by_id.items():
                if not connection_id:
                    continue
                self.report.increment_counter("connections_scanned")
                yield from self._ensure_connection_dataset(connection_id, connection)

            self._warn_unmapped_connections(self._connections_by_id)
        except Exception as exc:
            self.report.warning(
                title="Connections fetch error",
                message="Failed to fetch Omni connections; proceeding with config overrides only",
                exc=exc,
            )

    def _process_model_worker(self, model: Dict[str, Any]) -> List[MetadataWorkUnit]:
        """Process a single model and return work units (thread-safe).

        Extracts detailed model processing logic into a dedicated worker function
        that can be called in parallel by ThreadPoolExecutor. The model context
        must already be populated in _model_context_by_id.
        """
        work_units: List[MetadataWorkUnit] = []

        try:
            model_id = model.get("id")
            if not model_id:
                return []

            logger.info("Processing model: model_id=%s", model_id)
            logger.debug("Full model object: %s", model)

            # Get pre-populated context
            ctx = self._model_context_by_id.get(model_id)
            if not ctx:
                self.report.warning(
                    title="Missing model context",
                    message="Model context not found in _model_context_by_id",
                    context=f"model_id={model_id}",
                )
                return []

            connection_id = ctx.connection_id
            platform = ctx.platform
            database = ctx.database
            platform_instance = ctx.platform_instance
            model_kind = ctx.model_kind
            model_layer = ctx.model_layer
            model_name = model.get("name") or model_id

            # Build model properties
            model_props: Dict[str, str] = {
                "modelId": model_id,
                "modelName": model_name,
                "modelKind": model_kind or "",
                "modelLayer": model_layer,
                "baseModelId": model.get("baseModelId") or "",
                "connectionId": str(connection_id),
                "createdAt": model.get("createdAt") or "",
                "updatedAt": model.get("updatedAt") or "",
                "deletedAt": model.get("deletedAt") or "",
                "entityType": "model",
            }

            model_urn = self._model_dataset_urn(model_id)
            with self._state_lock:
                self._model_dataset_urns.add(model_urn)

            if connection_id:
                conn = self._connections_by_id.get(str(connection_id))
                work_units.extend(
                    self._ensure_connection_dataset(str(connection_id), conn)
                )

            logger.info(
                "Processing model: model_id=%s model_name=%s model_kind=%s "
                "connection_id=%s platform=%s database=%s platform_instance=%s",
                model_id,
                model_name,
                model_kind,
                connection_id,
                platform,
                database,
                platform_instance,
            )

            # Resolve model upstreams: connection + base model
            model_upstream_urns: Set[str] = set()
            if connection_id:
                model_upstream_urns.add(
                    self._connection_dataset_urn(str(connection_id))
                )
            base_model_id = model.get("baseModelId")
            if base_model_id:
                base_model_urn = self._model_dataset_urn(base_model_id)
                model_upstream_urns.add(base_model_urn)
                with self._state_lock:
                    if base_model_urn not in self._model_dataset_urns:
                        work_units.extend(
                            self._emit_dataset(
                                name=f"model.{base_model_id}",
                                description="Omni base model inferred from model relationship.",
                                custom_properties={
                                    "entityType": "model",
                                    "modelId": base_model_id,
                                    "inferred": "true",
                                },
                                subtype="Model",
                            )
                        )
                        self._model_dataset_urns.add(base_model_urn)
                        self.report.increment_counter("models_emitted")

            model_upstreams_aspect: Optional[UpstreamLineageClass] = None
            if model_upstream_urns:
                model_upstreams_aspect = UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=u, type=DatasetLineageTypeClass.TRANSFORMED
                        )
                        for u in sorted(model_upstream_urns)
                    ]
                )

            work_units.extend(
                self._emit_dataset(
                    name=f"model.{model_id}",
                    description="Omni model entity.",
                    custom_properties=model_props,
                    subtype="Model",
                    upstreams=model_upstreams_aspect,
                )
            )
            self.report.increment_counter("models_emitted")

            return work_units

        except Exception as exc:
            self.report.warning(
                title="Model processing error",
                message="Failed to process model",
                context=f"model_id={model.get('id')}",
                exc=exc,
            )
            return []

    def _ingest_semantic_model(self) -> Iterator[MetadataWorkUnit]:
        """Ingest Omni semantic models in two phases: context collection, then parallel processing.

        Phase 1 (sequential): Collect context for ALL models (even filtered ones)
                              because documents may reference any model.
        Phase 2 (parallel):   Process filtered models in detail using ThreadPoolExecutor.
        """
        # Phase 1: Collect ALL models and their context (sequential - needed for dashboard lineage)
        filtered_models: List[Dict[str, Any]] = []

        try:
            for model in self.client.list_models(page_size=self.config.page_size):
                model_id = model.get("id")
                if not model_id:
                    continue

                # Extract model metadata
                model_name = model.get("name") or model_id
                model_kind = model.get("modelKind")
                model_layer = self._normalize_model_layer(model_kind)
                connection_id = model.get("connectionId") or ""

                # Resolve connection context for ALL models (needed for document/dashboard lineage)
                conn: Optional[Dict[str, object]] = self._connections_by_id.get(
                    connection_id
                )
                platform = self._resolve_platform_from_connection(
                    connection_id=connection_id,
                    conn=conn,
                    warn_on_missing=True,
                    model_id=model_id,
                    model_name=model_name,
                )
                database = self._resolve_database_from_connection(connection_id, conn)
                platform_instance = self._resolve_platform_instance_from_connection(
                    connection_id
                )

                # Store context for ALL models so dashboards can reference them
                self._model_context_by_id[model_id] = _ModelContext(
                    connection_id=connection_id,
                    platform=platform,
                    database=database,
                    platform_instance=platform_instance,
                    model_kind=model_kind,
                    model_layer=model_layer,
                )

                # Now apply filter - if model doesn't match pattern, skip entity emission
                if not self.config.model_pattern.allowed(model_id):
                    self.report.report_model_filtered(model_id)
                    continue

                self.report.increment_counter("models_scanned")
                filtered_models.append(model)

        except Exception as exc:
            self.report.warning(
                title="Models fetch error",
                message="Failed to fetch models from Omni API",
                exc=exc,
            )
            return

        # Phase 2: Process filtered models in parallel
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            future_to_model = {
                executor.submit(self._process_model_worker, model): model
                for model in filtered_models
            }

            for future in as_completed(future_to_model):
                try:
                    work_units = future.result()
                    yield from work_units
                except Exception as exc:
                    model = future_to_model[future]
                    self.report.warning(
                        title="Model processing error",
                        message="Failed to process model",
                        context=f"model_id={model.get('id')}",
                        exc=exc,
                    )

    def _ingest_folders(self) -> Iterator[MetadataWorkUnit]:
        """Ingest Omni folder hierarchy as DataHub containers.

        NOTE: Folders are processed sequentially (not parallelized) because they have
        parent/child relationships that must be resolved in order. Parallelizing would
        require a two-pass approach (collect all, sort by depth, then emit), which adds
        complexity for minimal benefit given typical folder counts (~10-100).
        """
        for folder in self.client.list_folders(page_size=self.config.page_size):
            folder_id = folder.get("id")
            if not folder_id:
                continue
            logger.info(
                "Processing folder: folder_id=%s name=%s", folder_id, folder.get("name")
            )
            owner = folder.get("owner") or {}
            path = str(folder.get("path") or "")
            folder_url = str(folder.get("url") or "")
            parent_path = path.rsplit("/", 1)[0] if "/" in path else ""
            yield from self._emit_dataset(
                name=f"folder.{folder_id}",
                description="Omni folder entity.",
                custom_properties={
                    "entityType": "folder",
                    "folderId": str(folder_id),
                    "folderPath": path,
                    "parentFolderPath": parent_path,
                    "ownerId": str(owner.get("id") or ""),
                    "ownerName": str(owner.get("name") or ""),
                    "scope": str(folder.get("scope") or ""),
                    "url": folder_url,
                },
                subtype="Folder",
                external_url=folder_url or None,
            )
            self._folder_dataset_urns.add(self._folder_dataset_urn(folder_id))

    # ------------------------------------------------------------------
    # _ingest_documents helpers
    # ------------------------------------------------------------------

    def _ensure_inline_folder(
        self, folder_id: str, folder_name: str, folder_path: str
    ) -> Iterator[MetadataWorkUnit]:
        """Emit a folder dataset if it was inlined in the document and not yet seen."""
        folder_urn = self._folder_dataset_urn(folder_id)
        if folder_urn not in self._folder_dataset_urns:
            yield from self._emit_dataset(
                name=f"folder.{folder_id}",
                description="Omni folder entity inferred from document metadata.",
                custom_properties={
                    "entityType": "folder",
                    "folderId": folder_id,
                    "folderName": folder_name,
                    "folderPath": folder_path,
                    "inferred": "true",
                },
                subtype="Folder",
            )
            self._folder_dataset_urns.add(folder_urn)

    def _ingest_topic_payload_for_dashboard_tile(
        self,
        model_id: str,
        topic_name: str,
        topic: Dict[str, Any],
    ) -> Iterator[MetadataWorkUnit]:
        """Pass dashboard tile topic payloads into :meth:`_ingest_topic_payload` with model context."""
        ctx = self._model_context_by_id.get(model_id)

        if not ctx:
            # Model not in filtered set, no context available
            # We'll still emit semantic assets (topic, views) but skip physical lineage
            self.report.warning(
                title="Dashboard topic from unprocessed model",
                message="Model not in filtered set, no platform context available. Semantic assets will be emitted but physical lineage will be skipped.",
                context=f"model_id={model_id}, topic_name={topic_name}",
            )
            # Emit with minimal context
            yield from self._ingest_topic_payload(
                model_id=model_id,
                topic_name=topic_name,
                topic=topic,
                platform=None,
                database=None,
                connection_id=None,
                platform_instance=None,
                inferred=True,
                model_custom_properties={
                    "modelKind": "",
                    "modelLayer": "",
                },
            )
            return

        # Get platform from context, no fallback - None means skip physical lineage
        platform = ctx.platform if ctx.platform else None
        database = ctx.database if ctx.database else None
        connection_id = ctx.connection_id if ctx.connection_id else None

        yield from self._ingest_topic_payload(
            model_id=model_id,
            topic_name=topic_name,
            topic=topic,
            platform=platform,
            database=database,
            connection_id=connection_id,
            platform_instance=ctx.platform_instance,
            inferred=True,
            model_custom_properties={
                "modelKind": ctx.model_kind or "",
                "modelLayer": ctx.model_layer,
            },
        )

    def _ingest_topic_from_dashboard(
        self,
        model_id: str,
        topic_name: str,
    ) -> Iterator[MetadataWorkUnit]:
        """Fetch and ingest a topic referenced by a dashboard tile."""
        try:
            tp = self.client.get_topic(model_id, topic_name)
            if tp:
                yield from self._ingest_topic_payload_for_dashboard_tile(
                    model_id, topic_name, tp
                )
        except Exception as exc:
            self.report.warning(
                title="Topic fetch from dashboard error",
                message="Failed to fetch topic from dashboard model",
                context=f"topic_name={topic_name}, dashboard_model={model_id}",
                exc=exc,
            )

    def _collect_tile_data(
        self,
        doc_id: str,
        dashboard_url: str,
        dashboard_title: str,
        fields_by_dashboard: Set[FieldRef],
        chart_ids: List[str],
        chart_inputs: Dict[str, Set[str]],
        chart_titles: Dict[str, str],
        chart_urls: Dict[str, str],
        dashboard_topics: Set[str],
        dashboard_topic_urns: Set[str],
        view_to_topic_urns: Dict[str, Set[str]],
    ) -> _TileCollectionResult:
        """Fetch dashboard payload, populate tile/topic state dicts, return model_id.

        Returns a _TileCollectionResult with the resolved modelId and work units,
        avoiding shared mutable state between concurrent workers.
        """
        result = _TileCollectionResult()
        try:
            dashboard_payload = self.client.get_dashboard_document(doc_id)
            result.model_id = dashboard_payload.get("modelId")
            model_id = result.model_id
            for idx, qp in enumerate(dashboard_payload.get("queryPresentations", [])):
                qp_id = qp.get("id") or f"{doc_id}:{idx}"
                chart_ids.append(qp_id)
                chart_inputs[qp_id] = set()
                chart_titles[qp_id] = (
                    qp.get("name") or f"{dashboard_title} - tile {idx + 1}"
                )
                query = qp.get("query") or {}
                tile_fields = parse_field_list(query.get("fields", []))
                fields_by_dashboard.update(tile_fields)
                topic_name = qp.get("topicName") or query.get(
                    "join_paths_from_topic_name"
                )
                if topic_name and model_id:
                    dashboard_topics.add(topic_name)
                    topic_key = f"{model_id}:{topic_name}"
                    topic_urn = self._topic_urn_by_key.get(
                        topic_key, self._topic_dataset_urn(model_id, topic_name)
                    )
                    if topic_key not in self._topic_urn_by_key:
                        result.work_units.extend(
                            self._ingest_topic_from_dashboard(model_id, topic_name)
                        )
                        topic_urn = self._topic_urn_by_key.get(topic_key, topic_urn)
                    if topic_urn not in self._topic_dataset_urns:
                        self._topic_dataset_urns.add(topic_urn)
                        result.work_units.extend(
                            self._emit_dataset(
                                name=f"{model_id}.topic.{topic_name}",
                                description="Omni topic inferred from dashboard metadata.",
                                custom_properties={
                                    "modelId": model_id,
                                    "topicName": topic_name,
                                    "inferred": "true",
                                },
                                subtype=DatasetSubTypes.TOPIC,
                            )
                        )
                        self.report.increment_counter("topics_emitted")
                    chart_inputs[qp_id].add(topic_urn)
                    dashboard_topic_urns.add(topic_urn)
                    # Track which topic each view belongs to
                    for field_ref in tile_fields:
                        view_to_topic_urns.setdefault(field_ref.view, set()).add(
                            topic_urn
                        )
                chart_urls[qp_id] = f"{dashboard_url}?queryPresentationId={qp_id}"
        except Exception as exc:
            self.report.warning(
                title="Dashboard document fetch error",
                message="Failed to fetch dashboard payload for dashboard document",
                context=f"doc_id={doc_id}",
                exc=exc,
            )
        return result

    def _emit_inferred_view_datasets(
        self,
        model_id: str,
        fields_by_dashboard: Set[FieldRef],
        view_to_topic_urns: Dict[str, Set[str]],
        dashboard_dataset_urn: str,
        fine_grained_lineages: List[FineGrainedLineageClass],
        fine_grained_dedupe: Set[tuple],
    ) -> Iterator[MetadataWorkUnit]:
        """Emit inferred semantic view datasets and compute fine-grained lineage."""
        inferred_fields_by_view: Dict[str, Set[str]] = {}
        # Track inferred views per topic so we can add them as topic upstreams
        topic_to_inferred_views: Dict[str, Set[str]] = {}

        for field_ref in fields_by_dashboard:
            inferred_fields_by_view.setdefault(field_ref.view, set()).add(
                field_ref.field
            )
            semantic_view_urn = self._semantic_dataset_urn(model_id, field_ref.view)
            view_topic_urns = view_to_topic_urns.get(field_ref.view, set())
            if semantic_view_urn not in self._semantic_dataset_urns:
                self._semantic_dataset_urns.add(semantic_view_urn)
                inferred_schema_fields = [
                    SchemaFieldClass(
                        fieldPath=fn,
                        type=self._infer_schema_type("STRING"),
                        nativeDataType="STRING",
                        nullable=True,
                    )
                    for fn in sorted(inferred_fields_by_view.get(field_ref.view, set()))
                ]
                yield from self._emit_dataset(
                    name=f"{model_id}.{field_ref.view}",
                    description="Omni semantic view inferred from dashboard query fields.",
                    custom_properties={
                        "modelId": model_id,
                        "viewName": field_ref.view,
                        "inferred": "true",
                    },
                    subtype="View",
                    schema_fields=inferred_schema_fields or None,
                )
                self.report.increment_counter("views_emitted")
                # Register this inferred view as upstream of its topic(s)
                for topic_urn in view_topic_urns:
                    topic_to_inferred_views.setdefault(topic_urn, set()).add(
                        semantic_view_urn
                    )

            if not self.config.include_column_lineage:
                continue

            key = self._canonical_semantic_field_key(
                model_id, field_ref.view, field_ref.field
            )
            semantic_field = self._semantic_fields.get(key)
            downstream_field_urn = make_schema_field_urn(
                dashboard_dataset_urn, f"{field_ref.view}.{field_ref.field}"
            )
            if not semantic_field:
                self.report.increment_counter("fine_grained_lineage_edges_unresolved")
                continue
            semantic_urn = self._semantic_dataset_urn(
                semantic_field.model_id, semantic_field.view_name
            )
            semantic_field_urn = make_schema_field_urn(
                semantic_urn, semantic_field.field_name
            )
            edge = (tuple([semantic_field_urn]), tuple([downstream_field_urn]))
            if edge not in fine_grained_dedupe:
                fine_grained_dedupe.add(edge)
                fine_grained_lineages.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        upstreams=[semantic_field_urn],
                        downstreams=[downstream_field_urn],
                        transformOperation="OMNI_QUERY_FIELD_MAPPING",
                    )
                )
            if semantic_field.confidence == "exact":
                self.report.increment_counter("fine_grained_lineage_edges_exact")
            elif semantic_field.confidence == "derived":
                self.report.increment_counter("fine_grained_lineage_edges_derived")
            else:
                self.report.increment_counter("fine_grained_lineage_edges_unresolved")

        # Emit topic upstream lineage for inferred views
        for topic_urn, view_urns in topic_to_inferred_views.items():
            yield from self._emit_upstream_lineage(topic_urn, view_urns)

    def _process_document_worker(
        self, document: Dict[str, Any]
    ) -> List[MetadataWorkUnit]:
        """Process a single document and return work units (thread-safe).

        Extracts all document processing logic into a dedicated worker function
        that can be called in parallel by ThreadPoolExecutor.
        """
        work_units: List[MetadataWorkUnit] = []

        try:
            doc_id = document.get("identifier")
            if not doc_id:
                return []

            logger.info("Processing document: doc_id=%s", doc_id)
            logger.debug("Full document object: %s", document)

            if not self.config.document_pattern.allowed(doc_id):
                with self.report._report_lock:
                    self.report.report_document_filtered(doc_id)
                return []

            self.report.increment_counter("documents_scanned")

            has_dashboard = bool(document.get("hasDashboard"))
            if not has_dashboard and not self.config.include_workbook_only:
                return []

            dashboard_dataset_urn = make_dataset_urn(
                self.PLATFORM, doc_id, self.config.env
            )
            work_units.append(
                MetadataChangeProposalWrapper(
                    entityUrn=dashboard_dataset_urn,
                    aspect=SubTypesClass(typeNames=["Dashboard"]),
                ).as_workunit()
            )

            dashboard_url = (
                document.get("url")
                or f"{self.config.base_url.removesuffix('/api')}/dashboards/{doc_id}"
            )
            dashboard_title = document.get("name") or doc_id
            folder = document.get("folder") or {}
            folder_id = folder.get("id")
            folder_name = str(folder.get("name") or "")
            folder_path = str(folder.get("path") or "")
            owner_info = document.get("owner") or {}
            owner_id = str(owner_info.get("id") or "")
            owner_name = str(owner_info.get("name") or "")
            labels = document.get("labels") or []
            normalized_labels = [
                str(lb.get("name") if isinstance(lb, dict) else lb)
                for lb in (labels if isinstance(labels, list) else [])
            ]
            self.report.increment_counter("dashboards_emitted")

            if folder_id:
                work_units.extend(
                    self._ensure_inline_folder(str(folder_id), folder_name, folder_path)
                )

            document_connection_id = str(document.get("connectionId"))
            if document_connection_id:
                work_units.extend(
                    self._ensure_connection_dataset(
                        document_connection_id,
                        self._connections_by_id.get(document_connection_id),
                    )
                )

            fields_by_dashboard: Set[FieldRef] = set()
            fine_grained_lineages: List[FineGrainedLineageClass] = []
            fine_grained_dedupe: Set[tuple] = set()
            chart_ids: List[str] = []
            dashboard_topics: Set[str] = set()
            model_id_from_dashboard: Optional[str] = None
            chart_inputs: Dict[str, Set[str]] = {}
            chart_titles: Dict[str, str] = {}
            chart_urls: Dict[str, str] = {}
            dashboard_topic_urns: Set[str] = set()
            view_to_topic_urns: Dict[str, Set[str]] = {}

            if has_dashboard:
                tile_result = self._collect_tile_data(
                    doc_id,
                    dashboard_url,
                    dashboard_title,
                    fields_by_dashboard,
                    chart_ids,
                    chart_inputs,
                    chart_titles,
                    chart_urls,
                    dashboard_topics,
                    dashboard_topic_urns,
                    view_to_topic_urns,
                )
                work_units.extend(tile_result.work_units)
                model_id_from_dashboard = tile_result.model_id

            try:
                for query in self.client.get_document_queries(doc_id):
                    if not model_id_from_dashboard:
                        model_id_from_dashboard = (query.get("query") or {}).get(
                            "modelId"
                        )
                    fields_by_dashboard.update(
                        parse_field_list((query.get("query") or {}).get("fields", []))
                    )
            except Exception as exc:
                self.report.warning(
                    title="Document queries fetch error",
                    message="Failed to fetch queries for document queries",
                    context=f"doc_id={doc_id}",
                    exc=exc,
                )

            if model_id_from_dashboard and dashboard_topics:
                for topic_name in sorted(dashboard_topics):
                    topic_key = f"{model_id_from_dashboard}:{topic_name}"
                    topic_urn = self._topic_urn_by_key.get(
                        topic_key,
                        self._topic_dataset_urn(model_id_from_dashboard, topic_name),
                    )
                    dashboard_topic_urns.add(topic_urn)
                    for qp_id in chart_ids:
                        chart_inputs.setdefault(qp_id, set()).add(topic_urn)

            if model_id_from_dashboard and fields_by_dashboard:
                work_units.extend(
                    self._emit_inferred_view_datasets(
                        model_id=model_id_from_dashboard,
                        fields_by_dashboard=fields_by_dashboard,
                        view_to_topic_urns=view_to_topic_urns,
                        dashboard_dataset_urn=dashboard_dataset_urn,
                        fine_grained_lineages=fine_grained_lineages,
                        fine_grained_dedupe=fine_grained_dedupe,
                    )
                )

            folder_urn: Optional[str] = (
                self._folder_dataset_urn(folder_id) if folder_id else None
            )
            structural_upstreams: Set[str] = set()
            if folder_id:
                structural_upstreams.add(self._folder_dataset_urn(folder_id))
            if structural_upstreams or fine_grained_lineages:
                work_units.extend(
                    self._emit_upstream_lineage(
                        dashboard_dataset_urn,
                        structural_upstreams,
                        fine_grained_lineages=fine_grained_lineages,
                    )
                )

            chart_urns: List[str] = []
            for qp_id in chart_ids:
                chart_urn = make_chart_urn(self.PLATFORM, qp_id)
                chart_urns.append(chart_urn)
                work_units.extend(
                    self._emit_chart(
                        qp_id=qp_id,
                        title=chart_titles.get(qp_id, "Omni tile"),
                        description="Omni workbook tab or dashboard tile.",
                        external_url=chart_urls.get(qp_id, dashboard_url),
                        input_urns=sorted(chart_inputs.get(qp_id, set())),
                        custom_properties={"documentId": doc_id},
                        owner_id=owner_id,
                        owner_name=owner_name,
                        updated_at=document.get("updatedAt"),
                    )
                )
                self.report.increment_counter("charts_emitted")

            work_units.extend(
                self._emit_dashboard(
                    doc_id=doc_id,
                    title=dashboard_title,
                    description="Omni dashboard.",
                    external_url=dashboard_url,
                    chart_urns=chart_urns,
                    custom_properties={
                        "documentId": doc_id,
                        "connectionId": str(document.get("connectionId") or ""),
                        "scope": str(document.get("scope") or ""),
                        "folderId": str(folder_id or ""),
                        "folderName": folder_name,
                        "folderPath": folder_path,
                        "labels": ",".join(lb for lb in normalized_labels if lb),
                        "omniEmbedUrl": str(dashboard_url),
                        "omniEmbedIframe": f'<iframe src="{dashboard_url}"></iframe>',
                        "topicNames": ",".join(sorted(dashboard_topics)),
                        "updatedAt": str(document.get("updatedAt") or ""),
                    },
                    owner_id=owner_id,
                    owner_name=owner_name,
                    folder_urn=folder_urn,
                    updated_at=document.get("updatedAt"),
                )
            )

            return work_units

        except Exception as exc:
            self.report.warning(
                title="Document processing error",
                message="Failed to process document",
                context=f"doc_id={document.get('identifier')}",
                exc=exc,
            )
            return []

    def _process_document_batch(
        self, documents: List[Dict[str, Any]]
    ) -> Iterator[MetadataWorkUnit]:
        """Process a batch of documents in parallel."""
        if not documents:
            return
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            future_to_doc = {
                executor.submit(self._process_document_worker, doc): doc
                for doc in documents
            }
            for future in as_completed(future_to_doc):
                try:
                    yield from future.result()
                except Exception as exc:
                    doc = future_to_doc[future]
                    self.report.warning(
                        title="Document processing error",
                        message="Failed to process document",
                        context=f"doc_id={doc.get('identifier')}",
                        exc=exc,
                    )

    def _ingest_documents(self) -> Iterator[MetadataWorkUnit]:
        """Ingest Omni documents (dashboards/workbooks).

        Dashboard documents are processed first because they discover topics and
        populate ``_semantic_fields`` via ``get_topic`` API calls.  Workbook-only
        documents depend on that data for fine-grained lineage resolution, so
        they run in a second batch.
        """
        try:
            documents: List[Dict[str, Any]] = list(
                self.client.list_documents(
                    page_size=self.config.page_size,
                    include_deleted=self.config.include_deleted,
                )
            )
        except Exception as exc:
            self.report.warning(
                title="Documents fetch error",
                message="Failed to fetch documents from Omni API",
                exc=exc,
            )
            return

        dashboard_docs = [d for d in documents if d.get("hasDashboard")]
        workbook_docs = [d for d in documents if not d.get("hasDashboard")]

        yield from self._process_document_batch(dashboard_docs)
        yield from self._process_document_batch(workbook_docs)

    # ------------------------------------------------------------------
    # Main entrypoint
    # ------------------------------------------------------------------

    def get_workunits_internal(self) -> Iterator[MetadataWorkUnit]:
        try:
            with self.report.new_stage("Fetching Omni connections"):
                yield from self._setup_connections()

            with self.report.new_stage("Ingesting Omni semantic models"):
                yield from self._ingest_semantic_model()

            with self.report.new_stage("Ingesting Omni folders"):
                yield from self._ingest_folders()

            with self.report.new_stage("Ingesting Omni documents"):
                yield from self._ingest_documents()
        except Exception as exc:
            self.report.failure(
                title="Omni source error",
                message="Fatal Omni source error",
                exc=exc,
            )
            raise
