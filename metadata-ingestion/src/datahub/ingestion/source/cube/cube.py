import json
import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

from requests import RequestException

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey
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
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.cube.config import CubeSourceConfig, CubeSourceReport
from datahub.ingestion.source.cube.constants import (
    CUBE_PLATFORM,
    CUBE_TYPE_TO_SCHEMA_FIELD_TYPE,
    DATA_SOURCE_TYPE_TO_PLATFORM,
    TAG_DIMENSION,
    TAG_MEASURE,
    TAG_TEMPORAL,
)
from datahub.ingestion.source.cube.cube_api import CubeAPIClient
from datahub.ingestion.source.cube.cube_lineage import CubeLineageBuilder
from datahub.ingestion.source.cube.models import (
    CubeEntity,
    CubeMember,
    CubeReport,
    CubeWorkbook,
)
from datahub.ingestion.source.sql.sql_utils import gen_domain_urn
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    DomainsClass,
    GlobalTagsClass,
    GlossaryTermsClass,
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TagAssociationClass,
    ViewPropertiesClass,
)
from datahub.metadata.urns import CorpUserUrn, TagUrn
from datahub.sdk.chart import Chart
from datahub.sdk.container import Container
from datahub.sdk.dashboard import Dashboard
from datahub.sdk.dataset import Dataset
from datahub.utilities.mapping import Constants, OperationProcessor
from datahub.utilities.registries.domain_registry import DomainRegistry

logger = logging.getLogger(__name__)


class CubeDeploymentKey(ContainerKey):
    deployment: str


@platform_name("Cube", id=CUBE_PLATFORM)
@config_class(CubeSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(
    SourceCapability.OWNERSHIP,
    "Enabled via `meta_mapping` against Cube `meta`.",
)
@capability(
    SourceCapability.TAGS,
    "Enabled via `meta_mapping`/`column_meta_mapping`, plus Measure/Dimension/"
    "Temporal field tags.",
)
@capability(
    SourceCapability.GLOSSARY_TERMS,
    "Enabled via `meta_mapping`/`column_meta_mapping`.",
)
@capability(
    SourceCapability.DOMAINS,
    "Enabled via the `domain` config and `meta_mapping`.",
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default. Includes view->cube lineage and, where available, "
    "lineage to upstream warehouse tables.",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default, can be disabled via `include_column_lineage`.",
)
class CubeSource(StatefulIngestionSourceBase, TestableSource):
    platform: str = CUBE_PLATFORM

    def __init__(self, config: CubeSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report: CubeSourceReport = CubeSourceReport()
        self.api_client = CubeAPIClient(config)

        self._deployment = self._deployment_name()
        self._container_key = CubeDeploymentKey(
            platform=self.platform,
            instance=config.platform_instance,
            env=config.env,
            deployment=self._deployment,
        )

        self.meta_processor: Optional[OperationProcessor] = None
        self.column_meta_processor: Optional[OperationProcessor] = None
        if config.enable_meta_mapping:
            self.meta_processor = OperationProcessor(
                config.meta_mapping,
                config.tag_prefix,
                "SOURCE_CONTROL",
                config.strip_user_ids_from_email,
                match_nested_props=True,
            )
            self.column_meta_processor = OperationProcessor(
                config.column_meta_mapping,
                config.tag_prefix,
                "SOURCE_CONTROL",
                config.strip_user_ids_from_email,
                match_nested_props=True,
            )

        self.domain_registry: Optional[DomainRegistry] = None
        if config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=list(config.domain), graph=ctx.graph
            )

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "CubeSource":
        config = CubeSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            config = CubeSourceConfig.model_validate(config_dict)
            CubeAPIClient(config).test_connection()
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except RequestException as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=f"Failed to connect to Cube API: {e}",
            )
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    def get_report(self) -> SourceReport:
        return self.report

    def _deployment_name(self) -> str:
        if self.config.platform_instance:
            return self.config.platform_instance
        host = urlparse(self.config.api_url).hostname or self.config.api_url
        return host

    def _deployment_url(self) -> Optional[str]:
        if self.config.deployment_url:
            return self.config.deployment_url.rstrip("/")
        parsed = urlparse(self.config.api_url)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
        return None

    def _resolve_warehouse_defaults(self) -> None:
        # Auto-detect the warehouse platform/database from Cube Cloud data sources.
        if self.config.warehouse_platform:
            return
        data_sources = self.api_client.get_data_sources()
        self.report.data_sources_scanned = len(data_sources)
        if not data_sources:
            return
        primary = next(
            (ds for ds in data_sources if ds.name == "default"), data_sources[0]
        )
        if primary.type:
            mapped = DATA_SOURCE_TYPE_TO_PLATFORM.get(primary.type.lower())
            if mapped:
                self.config.warehouse_platform = mapped
                logger.info(
                    f"Auto-detected warehouse platform '{mapped}' from Cube data "
                    f"source type '{primary.type}'."
                )
        if not self.config.warehouse_database and primary.database:
            self.config.warehouse_database = primary.database

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self._resolve_warehouse_defaults()

        container = Container(
            self._container_key,
            display_name=self._deployment,
            subtype=DatasetContainerSubTypes.CUBE_DEPLOYMENT,
            external_url=self._deployment_url(),
            extra_properties={"deployment_type": str(self.config.deployment_type)},
        )
        yield from container.as_workunits()

        lineage_builder = CubeLineageBuilder(
            config=self.config,
            ctx=self.ctx,
            warehouse_platform=self.config.warehouse_platform,
            warehouse_database=self.config.warehouse_database,
        )

        self.report.report_api_call()
        entities = self.api_client.get_entities()
        logger.info(f"Fetched {len(entities)} cubes/views from Cube")

        for entity in entities:
            if not self._should_emit(entity):
                continue
            yield from self._emit_entity(entity, container, lineage_builder)

        yield from self._emit_reports_and_workbooks(container)

    def _should_emit(self, entity: CubeEntity) -> bool:
        self.report.report_entity_scanned(entity.name, entity.is_view)

        if entity.is_hidden and not self.config.include_hidden:
            self.report.report_entity_filtered(entity.name, entity.is_view)
            return False
        if entity.is_view and not self.config.include_views:
            return False
        if not entity.is_view and not self.config.include_cubes:
            return False

        pattern = (
            self.config.view_pattern if entity.is_view else self.config.cube_pattern
        )
        if not pattern.allowed(entity.name):
            self.report.report_entity_filtered(entity.name, entity.is_view)
            return False
        return True

    def _emit_entity(
        self,
        entity: CubeEntity,
        container: Container,
        lineage_builder: CubeLineageBuilder,
    ) -> Iterable[MetadataWorkUnit]:
        self.report.measures_scanned += len(entity.measures)
        self.report.dimensions_scanned += len(entity.dimensions)

        upstreams = lineage_builder.build(entity)
        if upstreams is not None:
            self.report.lineage_edges_emitted += len(upstreams.upstreams)
            self.report.column_lineage_edges_emitted += len(
                upstreams.fineGrainedLineages or []
            )

        dataset = Dataset(
            platform=self.platform,
            name=entity.name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=entity.title or entity.name,
            description=entity.description,
            subtype=DatasetSubTypes.VIEW if entity.is_view else DatasetSubTypes.CUBE,
            custom_properties=self._custom_properties(entity),
            schema=self._build_schema_fields(entity),
            parent_container=container,
            upstreams=upstreams,
            view_definition=self._view_definition(entity),
        )
        yield from dataset.as_workunits()
        yield from self._emit_entity_meta(entity, str(dataset.urn))
        self.report.report_entity_emitted(entity.is_view)

    def _dataset_urn(self, entity_name: str) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=entity_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    @staticmethod
    def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None

    def _emit_reports_and_workbooks(
        self, container: Container
    ) -> Iterable[MetadataWorkUnit]:
        # Reports (saved queries) and workbooks (collections of reports) are a
        # Cube Cloud Platform API feature. They map onto DataHub charts and
        # dashboards respectively, extending lineage down from the views/cubes.
        chart_by_report_id: Dict[int, Chart] = {}
        if self.config.include_reports:
            for report in self.api_client.get_reports():
                self.report.reports_scanned += 1
                if not self.config.report_pattern.allowed(report.name):
                    self.report.filtered_reports.append(report.name)
                    continue
                chart = self._build_chart(report)
                chart_by_report_id[report.id] = chart
                yield from chart.as_workunits()
                self.report.reports_emitted += 1

        if self.config.include_workbooks:
            for workbook in self.api_client.get_workbooks():
                self.report.workbooks_scanned += 1
                if not self.config.workbook_pattern.allowed(workbook.name):
                    self.report.filtered_workbooks.append(workbook.name)
                    continue
                dashboard = self._build_dashboard(
                    workbook, chart_by_report_id, container
                )
                yield from dashboard.as_workunits()
                self.report.workbooks_emitted += 1

    def _build_chart(self, report: CubeReport) -> Chart:
        input_datasets = [
            self._dataset_urn(entity) for entity in report.referenced_entities
        ]
        owners = [CorpUserUrn(report.owner_email)] if report.owner_email else None
        return Chart(
            platform=self.platform,
            name=report.public_id or str(report.id),
            platform_instance=self.config.platform_instance,
            display_name=report.title or report.name,
            description=report.description,
            input_datasets=input_datasets or None,
            last_modified=self._parse_timestamp(report.updated_at),
            owners=owners,
            custom_properties={"report_id": str(report.id)},
        )

    def _build_dashboard(
        self,
        workbook: CubeWorkbook,
        chart_by_report_id: Dict[int, Chart],
        container: Container,
    ) -> Dashboard:
        charts = [
            chart_by_report_id[report_id]
            for report_id in workbook.report_ids
            if report_id in chart_by_report_id
        ]
        owners = [CorpUserUrn(workbook.owner_email)] if workbook.owner_email else None
        return Dashboard(
            platform=self.platform,
            name=str(workbook.id),
            platform_instance=self.config.platform_instance,
            display_name=workbook.title or workbook.name,
            description=workbook.description,
            charts=charts or None,
            parent_container=container,
            last_modified=self._parse_timestamp(workbook.updated_at),
            owners=owners,
        )

    def _emit_entity_meta(
        self, entity: CubeEntity, dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        aspects: Dict[str, Any] = {}
        if self.meta_processor is not None and entity.meta:
            aspects = self.meta_processor.process(entity.meta)

        for op in (
            Constants.ADD_OWNER_OPERATION,
            Constants.ADD_TAG_OPERATION,
            Constants.ADD_TERM_OPERATION,
            Constants.ADD_DOC_LINK_OPERATION,
        ):
            aspect = aspects.get(op)
            if aspect is not None:
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn, aspect=aspect
                ).as_workunit()

        domain_urns: List[str] = []
        meta_domain = aspects.get(Constants.ADD_DOMAIN_OPERATION)
        if isinstance(meta_domain, DomainsClass):
            domain_urns.extend(meta_domain.domains)
        if self.domain_registry is not None:
            pattern_domain = gen_domain_urn(
                entity.name, self.config.domain, self.domain_registry
            )
            if pattern_domain:
                domain_urns.append(pattern_domain)
        domain_urns = list(dict.fromkeys(domain_urns))
        if domain_urns:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=DomainsClass(domains=domain_urns)
            ).as_workunit()

    def _view_definition(self, entity: CubeEntity) -> Optional[ViewPropertiesClass]:
        # A cube's defining logic is its SQL (Cube Core / merged `/v1/meta`); a
        # view has no SQL, so its definition is the list of included cube members.
        if entity.sql:
            return ViewPropertiesClass(
                materialized=False,
                viewLanguage="SQL",
                viewLogic=entity.sql,
            )
        if entity.is_view:
            includes = [
                ref for member in entity.members for ref in member.member_references
            ]
            if not includes:
                return None
            view_logic = "includes:\n" + "\n".join(f"  - {ref}" for ref in includes)
            return ViewPropertiesClass(
                materialized=False,
                viewLanguage="YAML",
                viewLogic=view_logic,
            )
        return None

    def _custom_properties(self, entity: CubeEntity) -> Dict[str, str]:
        props: Dict[str, str] = {
            "type": "view" if entity.is_view else "cube",
            "measures": str(len(entity.measures)),
            "dimensions": str(len(entity.dimensions)),
        }
        if entity.is_hidden:
            props["hidden"] = "true"
        if entity.file_name:
            props["file_name"] = entity.file_name
        if entity.segment_names:
            props["segments"] = ", ".join(entity.segment_names)
        if self.config.emit_member_details:
            for join in entity.joins:
                props[f"join.{join.name}"] = join.relationship or "join"
            for hierarchy in entity.hierarchies:
                props[f"hierarchy.{hierarchy.name}"] = ", ".join(hierarchy.levels)
            for folder in entity.folders:
                props[f"folder.{folder.name}"] = ", ".join(folder.members)
            if entity.pre_aggregation_names:
                props["pre_aggregations"] = ", ".join(entity.pre_aggregation_names)
        for key, value in entity.meta.items():
            props[f"meta.{key}"] = str(value)
        return props

    def _build_schema_fields(self, entity: CubeEntity) -> List[SchemaFieldClass]:
        return [
            self._build_schema_field(member)
            for member in entity.visible_members(self.config.include_hidden)
        ]

    def _member_json_props(self, member: CubeMember) -> Optional[str]:
        if not self.config.emit_member_details:
            return None
        props: Dict[str, Any] = {}
        if member.format:
            props["format"] = member.format
        if member.drill_members:
            props["drillMembers"] = member.drill_members
        if member.cumulative:
            props["cumulative"] = True
        return json.dumps(props) if props else None

    def _build_schema_field(self, member: CubeMember) -> SchemaFieldClass:
        type_cls = CUBE_TYPE_TO_SCHEMA_FIELD_TYPE.get(member.data_type or "")
        if type_cls is None:
            type_cls = NumberTypeClass if member.is_measure else StringTypeClass

        if member.is_measure:
            native_type = (
                f"measure ({member.agg_type})" if member.agg_type else "measure"
            )
        else:
            native_type = member.data_type or "string"

        tags: List[TagAssociationClass] = []
        if self.config.tag_measures_and_dimensions:
            tag_name = TAG_MEASURE if member.is_measure else TAG_DIMENSION
            tags.append(TagAssociationClass(tag=TagUrn(tag_name).urn()))
            if member.is_temporal:
                tags.append(TagAssociationClass(tag=TagUrn(TAG_TEMPORAL).urn()))

        glossary_terms: Optional[GlossaryTermsClass] = None
        if self.column_meta_processor is not None and member.meta:
            col_aspects = self.column_meta_processor.process(member.meta)
            tag_aspect = col_aspects.get(Constants.ADD_TAG_OPERATION)
            if isinstance(tag_aspect, GlobalTagsClass):
                tags.extend(tag_aspect.tags)
            term_aspect = col_aspects.get(Constants.ADD_TERM_OPERATION)
            if isinstance(term_aspect, GlossaryTermsClass):
                glossary_terms = term_aspect

        return SchemaFieldClass(
            fieldPath=member.name,
            type=SchemaFieldDataTypeClass(type=type_cls()),
            nativeDataType=native_type,
            description=member.description or member.title,
            isPartOfKey=member.is_primary_key,
            globalTags=GlobalTagsClass(tags=tags) if tags else None,
            glossaryTerms=glossary_terms,
            jsonProps=self._member_json_props(member),
        )
