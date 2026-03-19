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
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, List, Literal, Optional, Set

import yaml

from datahub.emitter.mce_builder import (
    make_chart_urn,
    make_data_platform_urn,
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
from datahub.ingestion.source.omni.omni_api import OmniClient
from datahub.ingestion.source.omni.omni_config import OmniSourceConfig
from datahub.ingestion.source.omni.omni_lineage_parser import (
    extract_field_refs,
    parse_field_list,
)
from datahub.ingestion.source.omni.omni_report import OmniSourceReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DataPlatformInfoClass,
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    NumberTypeClass,
    OwnershipTypeClass,
    PlatformTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import CorpUserUrn
from datahub.sdk import Chart, Dashboard, Dataset

logger = logging.getLogger(__name__)

FieldConfidence = Literal["unresolved", "exact", "derived"]


@dataclass
class SemanticField:
    model_id: str
    view_name: str
    field_name: str
    expression: str = ""
    confidence: FieldConfidence = "unresolved"
    upstream_physical_urns: Set[str] = field(default_factory=set)


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
    """Ingestion source for the Omni BI platform."""

    DEFAULT_LOGO_URL = "https://avatars.githubusercontent.com/u/100505341?s=200&v=4"
    PLATFORM = "omni"

    def __init__(self, config: OmniSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.config = config
        self.report: OmniSourceReport = OmniSourceReport()
        self.client = OmniClient(
            base_url=config.base_url,
            api_key=config.api_key,
            timeout_seconds=config.timeout_seconds,
            max_requests_per_minute=config.max_requests_per_minute,
        )
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, self.config, self.ctx
        )
        # Internal caches – populated during _ingest_semantic_model and reused
        # later when processing documents/dashboards.
        self._semantic_fields: Dict[str, SemanticField] = {}
        self._semantic_dataset_urns: Set[str] = set()
        self._topic_dataset_urns: Set[str] = set()
        self._topic_urn_by_key: Dict[str, str] = {}
        self._topic_ingested_keys: Set[str] = set()
        self._physical_dataset_urns: Set[str] = set()
        self._model_dataset_urns: Set[str] = set()
        self._folder_dataset_urns: Set[str] = set()
        self._connection_dataset_urns: Set[str] = set()
        self._connections_by_id: Dict[str, Dict[str, object]] = {}
        self._model_context_by_id: Dict[str, Dict[str, Optional[str]]] = {}
        self._topic_specs_by_model_id: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._view_specs_by_model_id: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._current_tile_model_id: Optional[str] = None

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "OmniSource":
        config = OmniSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_report(self) -> OmniSourceReport:
        return self.report

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        return self.get_workunits_internal()

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
                max_requests_per_minute=config.max_requests_per_minute,
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

    def _as_wu(self, mcp: MetadataChangeProposalWrapper) -> MetadataWorkUnit:
        return mcp.as_workunit()

    def _emit_platform_metadata(self) -> Iterator[MetadataWorkUnit]:
        platform_info = DataPlatformInfoClass(
            name=self.PLATFORM,
            type=PlatformTypeClass.OTHERS,
            datasetNameDelimiter=".",
            displayName="Omni",
            logoUrl=self.DEFAULT_LOGO_URL,
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=make_data_platform_urn(self.PLATFORM),
            aspect=platform_info,
        ).as_workunit()

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
        self.report.dataset_lineage_edges_emitted += len(upstreams)
        yield self._as_wu(
            MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=lineage)
        )

    def _clear_upstream_lineage(self, dataset_urn: str) -> Iterator[MetadataWorkUnit]:
        """Emit an explicit empty lineage to clear stale edges from prior runs."""
        yield self._as_wu(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineageClass(upstreams=[], fineGrainedLineages=None),
            )
        )

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
            parent_container=folder_urn,  # type: ignore[arg-type]
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
    # YAML parsing helpers
    # ------------------------------------------------------------------

    def _topic_names_from_yaml(self, model_yaml: Dict[str, str]) -> Set[str]:
        names: Set[str] = set()
        for _, text in model_yaml.items():
            try:
                parsed = yaml.safe_load(text) or {}
            except Exception as exc:
                logger.warning("Skipping model YAML file: failed to parse: %s", exc)
                continue
            if (
                isinstance(parsed, dict)
                and parsed.get("type") == "topic"
                and parsed.get("name")
            ):
                names.add(parsed["name"])
        return names

    def _normalize_semantic_field_entries(
        self, raw_fields: Any
    ) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        if isinstance(raw_fields, dict):
            for field_name, payload in raw_fields.items():
                if isinstance(payload, dict):
                    row = dict(payload)
                    row["field_name"] = (
                        row.get("field_name") or row.get("name") or field_name
                    )
                    normalized.append(row)
                elif isinstance(payload, str):
                    normalized.append({"field_name": field_name, "expression": payload})
                else:
                    normalized.append({"field_name": field_name})
            return normalized
        if isinstance(raw_fields, list):
            for item in raw_fields:
                if isinstance(item, str):
                    normalized.append({"field_name": item})
                    continue
                if not isinstance(item, dict):
                    continue
                row = dict(item)
                row["field_name"] = row.get("field_name") or row.get("name")
                normalized.append(row)
        return normalized

    def _parse_model_yaml_specs(
        self, model_yaml_files: Dict[str, str]
    ) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        topic_specs: Dict[str, Dict[str, Any]] = {}
        view_specs: Dict[str, Dict[str, Any]] = {}
        for _, text in model_yaml_files.items():
            try:
                parsed = yaml.safe_load(text) or {}
            except Exception as exc:
                logger.warning("Skipping model YAML file: failed to parse: %s", exc)
                continue
            if not isinstance(parsed, dict):
                continue
            ptype = str(parsed.get("type") or "").lower()
            pname = parsed.get("name")
            if ptype == "topic" and pname:
                topic_specs[str(pname)] = parsed
            elif (
                ptype == "view"
                and pname
                or pname
                and (
                    parsed.get("dimensions")
                    or parsed.get("measures")
                    or parsed.get("fields")
                    or parsed.get("table_name")
                )
            ):
                view_specs[str(pname)] = parsed
        return topic_specs, view_specs

    def _topic_payload_from_yaml_specs(
        self,
        topic_name: str,
        topic_specs: Dict[str, Dict[str, Any]],
        view_specs: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        topic_spec = topic_specs.get(topic_name) or {}
        candidate_view_names: Set[str] = set()
        base_view_name = topic_spec.get("base_view_name")
        if isinstance(base_view_name, str) and base_view_name:
            candidate_view_names.add(base_view_name)
        for row in topic_spec.get("views") or []:
            if isinstance(row, str) and row:
                candidate_view_names.add(row)
            elif isinstance(row, dict):
                row_name = row.get("name") or row.get("view_name")
                if isinstance(row_name, str) and row_name:
                    candidate_view_names.add(row_name)
        if topic_name in view_specs:
            candidate_view_names.add(topic_name)
        views_payload: List[Dict[str, Any]] = []
        for view_name in sorted(candidate_view_names):
            raw_view = dict(view_specs.get(view_name) or {})
            dimensions = self._normalize_semantic_field_entries(
                raw_view.get("dimensions")
            )
            measures = self._normalize_semantic_field_entries(raw_view.get("measures"))
            for f in self._normalize_semantic_field_entries(raw_view.get("fields")):
                kind = str(f.get("kind") or f.get("field_type") or "").lower()
                if kind == "measure":
                    measures.append(f)
                elif kind == "dimension":
                    dimensions.append(f)
            views_payload.append(
                {
                    "name": view_name,
                    "schema": raw_view.get("schema") or "",
                    "table_name": (
                        raw_view.get("table_name")
                        or raw_view.get("sql_table_name")
                        or raw_view.get("source_table")
                        or ""
                    ),
                    "dimensions": dimensions,
                    "measures": measures,
                }
            )
        return {"views": views_payload}

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
        self.report.semantic_datasets_emitted += 1

    # ------------------------------------------------------------------
    # Topic / view ingestion
    # ------------------------------------------------------------------

    def _ingest_topic_payload(
        self,
        model_id: str,
        topic_name: str,
        topic: Dict[str, object],
        platform: str,
        database: str,
        connection_id: str,
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
        self.report.topics_scanned += 1

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
            subtype="Topic",
        )
        self.report.semantic_datasets_emitted += 1

        for view in topic.get("views", []):  # type: ignore[union-attr,attr-defined]
            view_name = (view or {}).get("name") if isinstance(view, dict) else None
            if not view_name:
                continue

            semantic_urn = self._semantic_dataset_urn(model_id, view_name)
            self._semantic_dataset_urns.add(semantic_urn)

            schema_fields: List[SchemaFieldClass] = []
            seen_fields: Set[str] = set()
            physical_urn: Optional[str] = None

            schema = view.get("schema") or ""
            table = view.get("table_name") or ""
            if table:
                physical_urn = self._physical_dataset_urn(
                    platform, database, schema, table, platform_instance
                )
                self._physical_dataset_urns.add(physical_urn)
                yield from self._emit_dataset(
                    name=".".join(
                        p
                        for p in [
                            (
                                database.upper()
                                if self.config.normalize_snowflake_names
                                and platform.lower() == "snowflake"
                                else database
                            ),
                            (
                                schema.upper()
                                if self.config.normalize_snowflake_names
                                and platform.lower() == "snowflake"
                                else schema
                            ),
                            (
                                table.upper()
                                if self.config.normalize_snowflake_names
                                and platform.lower() == "snowflake"
                                else table
                            ),
                        ]
                        if p
                    ),
                    description="Physical source table referenced by Omni model.",
                    custom_properties={
                        "platform": platform,
                        "database": database,
                        "schema": schema,
                        "table": table,
                        "connectionId": connection_id,
                        "platformInstance": platform_instance or "",
                    },
                    subtype="Table",
                    platform=platform,
                    platform_instance=platform_instance,
                    upstreams=UpstreamLineageClass(
                        upstreams=[
                            UpstreamClass(
                                dataset=semantic_urn,
                                type=DatasetLineageTypeClass.TRANSFORMED,
                            )
                        ]
                    ),
                )
                self.report.physical_datasets_emitted += 1

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
                sf = SemanticField(
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
                confidence = "exact" if refs else "derived" if expr else "unresolved"
                key = self._canonical_semantic_field_key(model_id, view_name, fn)
                sf = SemanticField(
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

            yield from self._emit_dataset(
                name=f"{model_id}.{view_name}",
                display_name=f"{readable_model}.{view_name}",
                description=f"Omni semantic view from topic {topic_name}.",
                custom_properties=view_props,
                subtype="View",
                schema_fields=schema_fields or None,
                upstreams=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=topic_urn, type=DatasetLineageTypeClass.TRANSFORMED
                        )
                    ]
                ),
            )
            self.report.semantic_datasets_emitted += 1

    # ------------------------------------------------------------------
    # Top-level ingestion stages
    # ------------------------------------------------------------------

    def _ingest_semantic_model(self) -> Iterator[MetadataWorkUnit]:
        connections: Dict[str, Dict[str, object]] = {}
        try:
            connections = {
                str(c["id"]): c
                for c in self.client.list_connections(self.config.include_deleted)
                if c.get("id") is not None
            }
            self._connections_by_id = connections
            for connection_id, connection in connections.items():
                if not connection_id:
                    continue
                self.report.connections_scanned += 1
                yield from self._ensure_connection_dataset(connection_id, connection)
        except Exception as exc:
            self.report.warning(
                "connections-fetch",
                f"Failed to fetch Omni connections; proceeding with config overrides only: {exc}",
            )

        for model in self.client.list_models(page_size=self.config.page_size):
            model_id = model.get("id")
            if not model_id:
                continue
            if not self.config.model_pattern.allowed(model_id):
                self.report.report_dropped(model_id)
                continue
            self.report.models_scanned += 1

            model_name = model.get("name") or model_id
            model_kind = model.get("modelKind")
            model_layer = self._normalize_model_layer(model_kind)
            model_props: Dict[str, str] = {
                "modelId": model_id,
                "modelName": model_name,
                "modelKind": model_kind or "",
                "modelLayer": model_layer,
                "baseModelId": model.get("baseModelId") or "",
                "connectionId": model.get("connectionId") or "",
                "createdAt": model.get("createdAt") or "",
                "updatedAt": model.get("updatedAt") or "",
                "deletedAt": model.get("deletedAt") or "",
                "entityType": "model",
            }
            model_urn = self._model_dataset_urn(model_id)
            self._model_dataset_urns.add(model_urn)

            connection_id = model.get("connectionId") or ""
            conn: Optional[Dict[str, object]] = connections.get(connection_id)
            if connection_id:
                yield from self._ensure_connection_dataset(connection_id, conn)
            platform: str = str((conn or {}).get("dialect") or "database")
            if self.config.connection_to_platform:
                platform = self.config.connection_to_platform.get(
                    connection_id, platform
                )
            database: str = str((conn or {}).get("database") or "")
            if self.config.connection_to_database:
                database = self.config.connection_to_database.get(
                    connection_id, database
                )
            platform_instance: Optional[str] = None
            if self.config.connection_to_platform_instance:
                platform_instance = self.config.connection_to_platform_instance.get(
                    connection_id
                )

            self._model_context_by_id[model_id] = {
                "connection_id": connection_id,
                "platform": platform,
                "database": database,
                "platform_instance": platform_instance,
                "model_kind": model_kind,
                "model_layer": model_layer,
            }

            # Resolve model upstreams: connection + base model
            model_upstream_urns: Set[str] = set()
            if connection_id:
                model_upstream_urns.add(self._connection_dataset_urn(connection_id))
            base_model_id = model.get("baseModelId") or ""
            if base_model_id:
                base_model_urn = self._model_dataset_urn(base_model_id)
                model_upstream_urns.add(base_model_urn)
                if base_model_urn not in self._model_dataset_urns:
                    yield from self._emit_dataset(
                        name=f"model.{base_model_id}",
                        description="Omni base model inferred from model relationship.",
                        custom_properties={
                            "entityType": "model",
                            "modelId": base_model_id,
                            "inferred": "true",
                        },
                        subtype="Model",
                    )
                    self._model_dataset_urns.add(base_model_urn)
                    self.report.semantic_datasets_emitted += 1

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

            yield from self._emit_dataset(
                name=f"model.{model_id}",
                description="Omni model entity.",
                custom_properties=model_props,
                subtype="Model",
                upstreams=model_upstreams_aspect,
            )
            self.report.semantic_datasets_emitted += 1

            try:
                model_yaml_payload = self.client.get_model_yaml(model_id)
            except Exception as exc:
                self.report.warning(
                    "model-yaml-fetch",
                    f"Failed to fetch model YAML for {model_id}: {exc}",
                )
                continue

            model_yaml_files = model_yaml_payload.get("files", {})
            topic_names = self._topic_names_from_yaml(model_yaml_files)
            topic_specs, view_specs = self._parse_model_yaml_specs(model_yaml_files)
            self._topic_specs_by_model_id[model_id] = topic_specs
            self._view_specs_by_model_id[model_id] = view_specs
            if not topic_names:
                continue

            for topic_name in sorted(topic_names):
                try:
                    topic = self.client.get_topic(model_id, topic_name)
                except Exception as exc:
                    self.report.warning(
                        "topic-fetch",
                        f"Failed to fetch topic {topic_name} for model {model_id}; using YAML fallback: {exc}",
                    )
                    topic = self._topic_payload_from_yaml_specs(
                        topic_name, topic_specs, view_specs
                    )
                if not topic:
                    continue
                yield from self._ingest_topic_payload(
                    model_id=model_id,
                    topic_name=topic_name,
                    topic=topic,
                    platform=platform,
                    database=database,
                    connection_id=connection_id,
                    platform_instance=platform_instance,
                    inferred=bool(topic.get("views")) and not bool(topic.get("id")),
                    model_custom_properties={
                        "modelKind": model_kind or "",
                        "modelLayer": model_layer,
                    },
                    model_name=model_name,
                )

    def _ingest_folders(self) -> Iterator[MetadataWorkUnit]:
        for folder in self.client.list_folders(page_size=self.config.page_size):
            folder_id = folder.get("id")
            if not folder_id:
                continue
            folder.get("name") or folder_id
            owner = folder.get("owner") or {}
            path = folder.get("path") or ""
            folder_url = folder.get("url") or ""
            parent_path = path.rsplit("/", 1)[0] if "/" in path else ""
            yield from self._emit_dataset(
                name=f"folder.{folder_id}",
                description="Omni folder entity.",
                custom_properties={
                    "entityType": "folder",
                    "folderId": folder_id,
                    "folderPath": path,
                    "parentFolderPath": parent_path,
                    "ownerId": owner.get("id") or "",
                    "ownerName": owner.get("name") or "",
                    "scope": folder.get("scope") or "",
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

    def _ingest_topic_from_dashboard(
        self,
        model_id: str,
        topic_name: str,
    ) -> Iterator[MetadataWorkUnit]:
        """Fetch (or fall back to YAML) and ingest a topic referenced by a dashboard tile."""
        ctx = self._model_context_by_id.get(model_id, {})
        mcp = {
            "modelKind": str(ctx.get("model_kind") or ""),
            "modelLayer": str(ctx.get("model_layer") or ""),
        }
        kwargs = dict(
            model_id=model_id,
            topic_name=topic_name,
            platform=str(ctx.get("platform") or "database"),
            database=str(ctx.get("database") or ""),
            connection_id=str(ctx.get("connection_id") or ""),
            platform_instance=(
                str(ctx.get("platform_instance"))
                if ctx.get("platform_instance")
                else None
            ),
            inferred=True,
            model_custom_properties=mcp,
        )
        try:
            tp = self.client.get_topic(model_id, topic_name)
            if tp:
                yield from self._ingest_topic_payload(topic=tp, **kwargs)  # type: ignore[arg-type]
        except Exception as exc:
            ts = self._topic_specs_by_model_id.get(model_id, {})
            vs = self._view_specs_by_model_id.get(model_id, {})
            yt = self._topic_payload_from_yaml_specs(topic_name, ts, vs)
            if yt.get("views"):
                yield from self._ingest_topic_payload(topic=yt, **kwargs)  # type: ignore[arg-type]
            else:
                self.report.warning(
                    "topic-fetch-from-dashboard",
                    f"Failed to fetch topic {topic_name} for model {model_id}: {exc}",
                )

    def _collect_tile_data(
        self,
        doc_id: str,
        dashboard_url: str,
        dashboard_title: str,
        fields_by_dashboard: Set[Any],
        chart_ids: List[str],
        chart_inputs: Dict[str, Set[str]],
        chart_titles: Dict[str, str],
        chart_urls: Dict[str, str],
        dashboard_topics: Set[str],
        dashboard_topic_urns: Set[str],
    ) -> Iterator[MetadataWorkUnit]:
        """Fetch dashboard payload and populate tile/topic state dicts.

        Yields topic ingestion workunits and stores the resolved modelId in
        ``self._current_tile_model_id`` as a side-effect. Callers should read
        that attribute after exhausting this generator.
        """
        self._current_tile_model_id = None
        try:
            dashboard_payload = self.client.get_dashboard_document(doc_id)
            model_id = dashboard_payload.get("modelId")
            self._current_tile_model_id = model_id
            for idx, qp in enumerate(dashboard_payload.get("queryPresentations", [])):
                qp_id = qp.get("id") or f"{doc_id}:{idx}"
                chart_ids.append(qp_id)
                chart_inputs[qp_id] = set()
                chart_titles[qp_id] = (
                    qp.get("name") or f"{dashboard_title} - tile {idx + 1}"
                )
                query = qp.get("query") or {}
                fields_by_dashboard.update(parse_field_list(query.get("fields", [])))
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
                        yield from self._ingest_topic_from_dashboard(
                            model_id, topic_name
                        )
                        topic_urn = self._topic_urn_by_key.get(topic_key, topic_urn)
                    if topic_urn not in self._topic_dataset_urns:
                        self._topic_dataset_urns.add(topic_urn)
                        yield from self._emit_dataset(
                            name=f"{model_id}.topic.{topic_name}",
                            description="Omni topic inferred from dashboard metadata.",
                            custom_properties={
                                "modelId": model_id,
                                "topicName": topic_name,
                                "inferred": "true",
                            },
                            subtype="Topic",
                        )
                        self.report.semantic_datasets_emitted += 1
                    chart_inputs[qp_id].add(topic_urn)
                    dashboard_topic_urns.add(topic_urn)
                chart_urls[qp_id] = f"{dashboard_url}?queryPresentationId={qp_id}"
        except Exception as exc:
            self.report.warning(
                "dashboard-document-fetch",
                f"Failed to fetch dashboard payload for {doc_id}: {exc}",
            )

    def _emit_inferred_view_datasets(
        self,
        model_id: str,
        fields_by_dashboard: Set[Any],
        dashboard_topic_urns: Set[str],
        dashboard_dataset_urn: str,
        fine_grained_lineages: List[FineGrainedLineageClass],
        fine_grained_dedupe: Set[tuple],
    ) -> Iterator[MetadataWorkUnit]:
        """Emit inferred semantic view datasets and compute fine-grained lineage."""
        inferred_fields_by_view: Dict[str, Set[str]] = {}
        inferred_view_topic_links: Dict[str, Set[str]] = {}

        for field_ref in fields_by_dashboard:
            inferred_fields_by_view.setdefault(field_ref.view, set()).add(
                field_ref.field
            )
            semantic_view_urn = self._semantic_dataset_urn(model_id, field_ref.view)
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
                view_upstreams: Optional[UpstreamLineageClass] = None
                if dashboard_topic_urns:
                    view_upstreams = UpstreamLineageClass(
                        upstreams=[
                            UpstreamClass(
                                dataset=t, type=DatasetLineageTypeClass.TRANSFORMED
                            )
                            for t in sorted(dashboard_topic_urns)
                        ]
                    )
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
                    upstreams=view_upstreams,
                )
                self.report.semantic_datasets_emitted += 1
            elif dashboard_topic_urns:
                inferred_view_topic_links.setdefault(semantic_view_urn, set()).update(
                    dashboard_topic_urns
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
                self.report.fine_grained_lineage_edges_unresolved += 1
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
                self.report.fine_grained_lineage_edges_exact += 1
            elif semantic_field.confidence == "derived":
                self.report.fine_grained_lineage_edges_derived += 1
            else:
                self.report.fine_grained_lineage_edges_unresolved += 1

        for sv_urn, topic_ups in inferred_view_topic_links.items():
            if topic_ups:
                yield from self._emit_upstream_lineage(sv_urn, topic_ups)

    def _ingest_documents(self) -> Iterator[MetadataWorkUnit]:
        for document in self.client.list_documents(
            page_size=self.config.page_size,
            include_deleted=self.config.include_deleted,
        ):
            doc_id = document.get("identifier")
            if not doc_id:
                continue
            if not self.config.document_pattern.allowed(doc_id):
                self.report.report_dropped(doc_id)
                continue
            self.report.documents_scanned += 1

            has_dashboard = bool(document.get("hasDashboard"))
            if not has_dashboard and not self.config.include_workbook_only:
                continue

            dashboard_dataset_urn = make_dataset_urn(
                self.PLATFORM, doc_id, self.config.env
            )
            yield self._as_wu(
                MetadataChangeProposalWrapper(
                    entityUrn=dashboard_dataset_urn,
                    aspect=SubTypesClass(typeNames=["Dashboard"]),
                )
            )

            dashboard_url = (
                document.get("url")
                or f"{self.config.base_url.removesuffix('/api')}/dashboards/{doc_id}"
            )
            dashboard_title = document.get("name") or doc_id
            folder = document.get("folder") or {}
            folder_id = folder.get("id") or ""
            folder_name = folder.get("name") or ""
            folder_path = folder.get("path") or ""
            owner_info = document.get("owner") or {}
            owner_id = str(owner_info.get("id") or "")
            owner_name = str(owner_info.get("name") or "")
            labels = document.get("labels") or []
            normalized_labels = [
                str(lb.get("name") if isinstance(lb, dict) else lb)
                for lb in (labels if isinstance(labels, list) else [])
            ]
            self.report.dashboards_scanned += 1

            if folder_id:
                yield from self._ensure_inline_folder(
                    folder_id, folder_name, folder_path
                )

            document_connection_id = str(document.get("connectionId") or "")
            if document_connection_id:
                yield from self._ensure_connection_dataset(
                    document_connection_id,
                    self._connections_by_id.get(document_connection_id),
                )

            fields_by_dashboard: Set[Any] = set()
            fine_grained_lineages: List[FineGrainedLineageClass] = []
            fine_grained_dedupe: Set[tuple] = set()
            chart_ids: List[str] = []
            dashboard_topics: Set[str] = set()
            model_id_from_dashboard: Optional[str] = None
            chart_inputs: Dict[str, Set[str]] = {}
            chart_titles: Dict[str, str] = {}
            chart_urls: Dict[str, str] = {}
            dashboard_topic_urns: Set[str] = set()

            if has_dashboard:
                yield from self._collect_tile_data(
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
                )
                model_id_from_dashboard = self._current_tile_model_id

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
                    "document-queries-fetch",
                    f"Failed to fetch queries for {doc_id}: {exc}",
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
                yield from self._emit_inferred_view_datasets(
                    model_id=model_id_from_dashboard,
                    fields_by_dashboard=fields_by_dashboard,
                    dashboard_topic_urns=dashboard_topic_urns,
                    dashboard_dataset_urn=dashboard_dataset_urn,
                    fine_grained_lineages=fine_grained_lineages,
                    fine_grained_dedupe=fine_grained_dedupe,
                )

            folder_urn: Optional[str] = (
                self._folder_dataset_urn(folder_id) if folder_id else None
            )
            structural_upstreams: Set[str] = set()
            if folder_id:
                structural_upstreams.add(self._folder_dataset_urn(folder_id))
            if structural_upstreams or fine_grained_lineages:
                yield from self._emit_upstream_lineage(
                    dashboard_dataset_urn,
                    structural_upstreams,
                    fine_grained_lineages=fine_grained_lineages,
                )

            chart_urns: List[str] = []
            for qp_id in chart_ids:
                chart_urn = make_chart_urn(self.PLATFORM, qp_id)
                chart_urns.append(chart_urn)
                yield from self._emit_chart(
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

            yield from self._emit_dashboard(
                doc_id=doc_id,
                title=dashboard_title,
                description="Omni dashboard.",
                external_url=dashboard_url,
                chart_urns=chart_urns,
                custom_properties={
                    "documentId": doc_id,
                    "connectionId": document.get("connectionId") or "",
                    "scope": document.get("scope") or "",
                    "folderId": folder_id,
                    "folderName": folder_name,
                    "folderPath": folder_path,
                    "labels": ",".join(lb for lb in normalized_labels if lb),
                    "omniEmbedUrl": str(dashboard_url),
                    "omniEmbedIframe": f'<iframe src="{dashboard_url}"></iframe>',
                    "topicNames": ",".join(sorted(dashboard_topics)),
                    "updatedAt": document.get("updatedAt") or "",
                },
                owner_id=owner_id,
                owner_name=owner_name,
                folder_urn=folder_urn,
                updated_at=document.get("updatedAt"),
            )

    # ------------------------------------------------------------------
    # Main entrypoint
    # ------------------------------------------------------------------

    def get_workunits_internal(self) -> Iterator[MetadataWorkUnit]:
        try:
            yield from self._emit_platform_metadata()
            yield from self._ingest_semantic_model()
            yield from self._ingest_folders()
            yield from self._ingest_documents()
        except Exception as exc:
            self.report.failure("omni-source", str(exc))
