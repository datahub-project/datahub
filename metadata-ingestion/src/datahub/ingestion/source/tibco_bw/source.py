import re
from pathlib import PurePath
from typing import Dict, Iterable, List, Optional, Set, Type, Union

from requests.exceptions import RequestException

from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DataFlowSubTypes,
    DataJobSubTypes,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.tibco_bw.client import TibcoClient, create_client
from datahub.ingestion.source.tibco_bw.config import (
    TibcoAppLineage,
    TibcoBwSourceConfig,
)
from datahub.ingestion.source.tibco_bw.constants import (
    EMS_NAME_DELIMITER,
    EMS_PLATFORM,
    LINEAGE_KEY_DELIMITER,
    PROPERTY_DOMAIN,
    PROPERTY_SCHEMA_CONTRACT,
    PROPERTY_SCHEMA_DECLARED_BY,
    PROPERTY_SCHEMA_SOURCE,
    SCHEMA_SOURCE_EAR,
    TIBCO_BW_PLATFORM,
)
from datahub.ingestion.source.tibco_bw.ear_parser import parse_archives
from datahub.ingestion.source.tibco_bw.models import (
    JmsMessageSchema,
    TibcoApplication,
    TibcoDeployment,
    TibcoScope,
)
from datahub.ingestion.source.tibco_bw.report import TibcoBwSourceReport
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    DatasetPropertiesClass,
    DateTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    TimeTypeClass,
)
from datahub.metadata.urns import DatasetUrn

_FLOW_SUBTYPE_BY_DEPLOYMENT = {
    TibcoDeployment.ON_PREM: DataFlowSubTypes.TIBCO_BW_APPSPACE,
    TibcoDeployment.CLOUD: DataFlowSubTypes.TIBCO_TCI_SUBSCRIPTION,
}

_XsdFieldType = Union[
    BooleanTypeClass,
    DateTypeClass,
    NumberTypeClass,
    StringTypeClass,
    TimeTypeClass,
]

# XSD built-ins mapped onto DataHub's field types. Anything unlisted - a
# restriction of a built-in, or a type from an unpackaged import - lands as a
# string, which is what the field is carried as on the wire anyway.
_FIELD_TYPE_BY_XSD_TYPE: Dict[str, Type[_XsdFieldType]] = {
    "boolean": BooleanTypeClass,
    "byte": NumberTypeClass,
    "date": DateTypeClass,
    "dateTime": TimeTypeClass,
    "decimal": NumberTypeClass,
    "double": NumberTypeClass,
    "float": NumberTypeClass,
    "int": NumberTypeClass,
    "integer": NumberTypeClass,
    "long": NumberTypeClass,
    "short": NumberTypeClass,
    "time": TimeTypeClass,
    "unsignedInt": NumberTypeClass,
    "unsignedLong": NumberTypeClass,
}

# `bwadmin download` writes "<application>_<version>.ear"; the deployed
# application the bwagent reports carries only the name.
_ARCHIVE_VERSION_SUFFIX = re.compile(r"_\d+(?:[._]\d+)*$")


def _application_name_of(archive_path: str) -> str:
    stem = PurePath(archive_path).stem
    return _ARCHIVE_VERSION_SUFFIX.sub("", stem).casefold()


def _merge(configured: List[str], declared: List[str]) -> List[str]:
    merged = list(configured)
    merged.extend(urn for urn in declared if urn not in configured)
    return merged


def _field_type(xsd_type: str) -> SchemaFieldDataTypeClass:
    type_class = _FIELD_TYPE_BY_XSD_TYPE.get(xsd_type, StringTypeClass)
    return SchemaFieldDataTypeClass(type=type_class())


@platform_name("TIBCO BusinessWorks", id=TIBCO_BW_PLATFORM)
@config_class(TibcoBwSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Emitted from the operator-supplied `application_lineage` map, since the TIBCO "
    "runtime APIs do not expose an application's datasets, and from the JMS "
    "activities in any archive supplied via `application_archives`",
)
@capability(
    SourceCapability.SCHEMA_METADATA,
    "Declared JMS message schemas read from supplied application archives "
    "(`application_archives`) and emitted onto the EMS destination datasets",
    supported=True,
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Best-effort between declared upstream/downstream datasets when both have a "
    "schema in DataHub, matched by name; enable with `emit_column_lineage`",
    supported=True,
)
class TibcoBwSource(StatefulIngestionSourceBase, TestableSource):
    """Ingests TIBCO integration applications from ActiveMatrix BusinessWorks
    (on-prem, via bwagent) or TIBCO Cloud Integration (cloud). Each deployment
    scope (appspace or subscription) becomes a DataFlow and each deployed
    application becomes a DataJob within it."""

    platform: str = TIBCO_BW_PLATFORM

    def __init__(self, config: TibcoBwSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.config = config
        self.report: TibcoBwSourceReport = TibcoBwSourceReport()
        self.client: TibcoClient = create_client(config)
        # Case-folded field path -> real field path per dataset urn, read from
        # DataHub once and reused across applications.
        self._schema_field_cache: Dict[str, Dict[str, str]] = {}
        # Scopes in which each unqualified application_lineage key was applied, so
        # a key that matches the same application name in more than one scope can
        # be flagged rather than silently duplicating lineage.
        self._unqualified_lineage_scopes: Dict[str, Set[str]] = {}
        # Destinations each application's own processes declare, keyed by
        # case-folded application name. Populated from the supplied archives.
        self._archive_lineage: Dict[str, TibcoAppLineage] = {}
        if not config.verify_ssl:
            self.report.warning(
                title="TLS certificate verification is disabled",
                message="Connections to the TIBCO API are open to interception. "
                "Prefer `ca_certificate_path` for a private CA.",
                context=config.base_url,
            )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "TibcoBwSource":
        config = TibcoBwSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            config = TibcoBwSourceConfig.model_validate(config_dict)
            create_client(config).test_connection()
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except RequestException as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=f"Failed to connect to TIBCO API: {e}",
            )
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    def get_report(self) -> SourceReport:
        return self.report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        # Archives are read before the topology so an application can carry the
        # lineage its own processes declare, rather than only what the operator
        # wrote down in `application_lineage`.
        schemas = (
            parse_archives(self.config.application_archives.paths, self.report)
            if self.config.application_archives.paths
            else []
        )
        self._archive_lineage = self._build_archive_lineage(schemas)

        for scope in self.client.fetch_scopes():
            self.report.scopes_scanned += 1
            if not self._scope_allowed(scope):
                self.report.report_scope_filtered(scope.name)
                continue
            yield from self._emit_scope(scope)

        if self.config.application_archives.emit_destination_schemas:
            yield from self._emit_archive_schemas(schemas)

    def _build_archive_lineage(
        self, schemas: List[JmsMessageSchema]
    ) -> Dict[str, TibcoAppLineage]:
        """Map each application to the destinations its processes read and write.

        The archive is named for the application it deploys, which is the only
        link back to the running application the bwagent reports - the process
        definitions inside carry module names, not deployment names. A version
        suffix is tolerated because `bwadmin download` writes one.
        """
        if not self.config.application_archives.emit_destination_lineage:
            return {}
        lineage: Dict[str, TibcoAppLineage] = {}
        for schema in schemas:
            application = _application_name_of(schema.archive)
            entry = lineage.setdefault(application, TibcoAppLineage())
            urn = self._destination_urn(schema)
            side = entry.downstreams if schema.publishes else entry.upstreams
            if urn not in side:
                side.append(urn)
        return lineage

    def _emit_archive_schemas(
        self, schemas: List[JmsMessageSchema]
    ) -> Iterable[MetadataWorkUnit]:
        """Publish the message schemas the supplied archives declare.

        This is the one place a JMS message's shape is actually written down.
        EMS holds no registry and cannot be browsed without consuming production
        messages, so without an archive a destination's fields can only ever be
        inferred from what its consumers happened to keep.
        """
        # One destination can be published to by several processes. They should
        # agree, and where they do not the first is kept and the conflict is
        # reported rather than the last writer silently winning.
        seen: Dict[str, str] = {}
        for schema in schemas:
            if not schema.publishes:
                continue
            urn = self._destination_urn(schema)
            declared_by = seen.get(urn)
            if declared_by is not None:
                if declared_by != schema.declared_by:
                    self.report.report_duplicate_schema_element(
                        f"{schema.destination_name}: declared by {declared_by} "
                        f"and {schema.declared_by}"
                    )
                continue
            seen[urn] = schema.declared_by
            yield from self._destination_schema_workunits(urn, schema)
            self.report.destination_schemas_emitted += 1

    def _destination_schema_workunits(
        self, urn: str, schema: JmsMessageSchema
    ) -> Iterable[MetadataWorkUnit]:
        # Written non-primary: the destination is the EMS source's entity, and
        # this connector only contributes the declaration the publisher holds.
        for aspect in (
            SchemaMetadataClass(
                schemaName=schema.element_name,
                platform=make_data_platform_urn(EMS_PLATFORM),
                version=0,
                hash="",
                platformSchema=OtherSchemaClass(rawSchema=schema.raw_schema),
                fields=[
                    SchemaFieldClass(
                        fieldPath=message_field.path,
                        type=_field_type(message_field.xsd_type),
                        nativeDataType=message_field.xsd_type,
                        nullable=message_field.nullable,
                    )
                    for message_field in schema.fields
                ],
            ),
            DatasetPropertiesClass(
                customProperties={
                    PROPERTY_SCHEMA_SOURCE: SCHEMA_SOURCE_EAR,
                    PROPERTY_SCHEMA_DECLARED_BY: schema.declared_by,
                    PROPERTY_SCHEMA_CONTRACT: schema.element_name,
                }
            ),
        ):
            yield MetadataWorkUnit(
                id=f"{urn}-{aspect.get_aspect_name()}",
                mcp=MetadataChangeProposalWrapper(entityUrn=urn, aspect=aspect),
                is_primary_source=False,
            )

    def _destination_urn(self, schema: JmsMessageSchema) -> str:
        target = self.config.application_archives.ems_target
        # Rebuilt exactly as the TIBCO EMS source names it - server group and
        # destination kind both lead, because both are independent namespaces in
        # EMS. Diverge here and the two connectors describe two different urns
        # for one destination.
        name = EMS_NAME_DELIMITER.join(
            (
                target.server_group,
                schema.destination_type.value,
                schema.destination_name,
            )
        )
        return make_dataset_urn_with_platform_instance(
            platform=EMS_PLATFORM,
            name=name,
            platform_instance=target.platform_instance,
            env=target.env or self.config.env,
        )

    def close(self) -> None:
        self.client.close()
        super().close()

    def _scope_allowed(self, scope: TibcoScope) -> bool:
        if self.config.deployment is TibcoDeployment.ON_PREM:
            return self.config.domain_pattern.allowed(
                scope.properties.get(PROPERTY_DOMAIN, "")
            ) and self.config.appspace_pattern.allowed(scope.name)
        return self.config.subscription_pattern.allowed(scope.name)

    def _emit_scope(self, scope: TibcoScope) -> Iterable[MetadataWorkUnit]:
        flow = DataFlow(
            id=scope.id,
            orchestrator=self.platform,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            name=scope.name,
            description=scope.description,
            properties=scope.properties,
        )
        for mcp in flow.generate_mcp():
            yield mcp.as_workunit()
        yield self._subtype_workunit(
            str(flow.urn), _FLOW_SUBTYPE_BY_DEPLOYMENT[self.config.deployment]
        )
        # DataFlow and DataJob cannot belong to a Container, so the deployment
        # hierarchy is only navigable if the browse path spells it out. Without
        # this the entities land unparented unless a platform_instance happens to
        # be configured, which is what the automatic processor keys off.
        scope_path = self._scope_browse_entries(scope)
        yield self._browse_path_workunit(str(flow.urn), scope_path)
        self.report.flows_emitted += 1

        for application in scope.applications:
            self.report.applications_scanned += 1
            if not self.config.application_pattern.allowed(application.name):
                self.report.report_application_filtered(application.name)
                continue
            yield from self._emit_application(scope, flow, application)

    def _scope_browse_entries(self, scope: TibcoScope) -> List[BrowsePathEntryClass]:
        # On-prem an appspace lives inside a domain; in the cloud a subscription is
        # already the top of the hierarchy.
        domain = scope.properties.get(PROPERTY_DOMAIN)
        return [BrowsePathEntryClass(id=domain)] if domain else []

    def _browse_path_workunit(
        self, entity_urn: str, path: List[BrowsePathEntryClass]
    ) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn, aspect=BrowsePathsV2Class(path=path)
        ).as_workunit()

    def _emit_application(
        self, scope: TibcoScope, flow: DataFlow, application: TibcoApplication
    ) -> Iterable[MetadataWorkUnit]:
        job = DataJob(
            id=application.name,
            flow_urn=flow.urn,
            name=application.name,
            description=application.description,
            properties=application.properties,
            platform_instance=self.config.platform_instance,
        )
        # The runtime APIs expose deployment topology but not the datasets an
        # application reads or writes, so lineage is taken from the operator-supplied
        # application_lineage map rather than discovered.
        lineage = self._merged_lineage(scope, application)
        if lineage is not None:
            job.inlets = [DatasetUrn.from_string(urn) for urn in lineage.upstreams]
            job.outlets = [DatasetUrn.from_string(urn) for urn in lineage.downstreams]
            if self.config.emit_column_lineage:
                job.fine_grained_lineages = self._build_column_lineage(
                    lineage.upstreams, lineage.downstreams
                )

        has_lineage = bool(job.inlets or job.outlets)
        # materialize_iolets stays False: the iolets are datasets owned by other
        # connectors, so we link to them without emitting empty stub entities.
        for mcp in job.generate_mcp(
            generate_lineage=has_lineage, materialize_iolets=False
        ):
            yield mcp.as_workunit()
        yield self._subtype_workunit(str(job.urn), DataJobSubTypes.TIBCO_APPLICATION)
        flow_urn = str(flow.urn)
        yield self._browse_path_workunit(
            str(job.urn),
            [
                *self._scope_browse_entries(scope),
                BrowsePathEntryClass(id=flow_urn, urn=flow_urn),
            ],
        )
        self.report.jobs_emitted += 1
        if has_lineage:
            self.report.jobs_with_lineage += 1
            self.report.lineage_iolets_emitted += len(job.inlets) + len(job.outlets)

    def _merged_lineage(
        self, scope: TibcoScope, application: TibcoApplication
    ) -> Optional[TibcoAppLineage]:
        """Combine what the archive declares with what the operator configured.

        The two are additive rather than one overriding the other: an archive
        only knows about the JMS destinations its own activities name, while the
        configured map is how a database or file endpoint gets declared at all.
        Dropping either would lose real edges.
        """
        configured = self._lineage_for(scope, application)
        declared = self._archive_lineage.get(application.name.casefold())
        if declared is None:
            return configured
        if configured is None:
            return declared
        return TibcoAppLineage(
            upstreams=_merge(configured.upstreams, declared.upstreams),
            downstreams=_merge(configured.downstreams, declared.downstreams),
        )

    def _lineage_for(
        self, scope: TibcoScope, application: TibcoApplication
    ) -> Optional[TibcoAppLineage]:
        # Application names are only unique within their deployment scope, so the
        # scope-qualified key wins. The bare name stays supported because a
        # single-scope estate has nothing to disambiguate, but reusing it across
        # scopes silently copies one application's lineage onto its namesakes.
        qualified_key = f"{scope.id}{LINEAGE_KEY_DELIMITER}{application.name}"
        lineage = self.config.application_lineage.get(qualified_key)
        if lineage is not None:
            return lineage
        lineage = self.config.application_lineage.get(application.name)
        if lineage is None:
            return None
        scopes = self._unqualified_lineage_scopes.setdefault(application.name, set())
        scopes.add(scope.id)
        if len(scopes) > 1:
            self.report.warning(
                title="Unqualified application_lineage key matches several scopes",
                message="The same lineage is being applied to every application "
                f"with this name. Qualify the key as `<scope>{LINEAGE_KEY_DELIMITER}"
                "<application>` to target one of them.",
                context=f"{application.name}: {sorted(scopes)}",
            )
        return lineage

    def _build_column_lineage(
        self, upstreams: List[str], downstreams: List[str]
    ) -> List[FineGrainedLineageClass]:
        # Best-effort field-level lineage for a passthrough application: a field
        # present on both a declared upstream and downstream is treated as the same
        # field. Only fields described in DataHub can be matched, so absent schemas
        # simply yield no column lineage. Matching is case-insensitive because the
        # two platforms often case fields differently, but the schemaField urns use
        # each side's real field path.
        fine_grained: List[FineGrainedLineageClass] = []
        for downstream_urn in downstreams:
            downstream_fields = self._schema_field_names(downstream_urn)
            if not downstream_fields:
                continue
            for upstream_urn in upstreams:
                upstream_fields = self._schema_field_names(upstream_urn)
                for key in sorted(upstream_fields.keys() & downstream_fields.keys()):
                    fine_grained.append(
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                            upstreams=[
                                make_schema_field_urn(
                                    upstream_urn, upstream_fields[key]
                                )
                            ],
                            downstreams=[
                                make_schema_field_urn(
                                    downstream_urn, downstream_fields[key]
                                )
                            ],
                        )
                    )
                    self.report.column_lineage_edges_emitted += 1
        return fine_grained

    def _schema_field_names(self, urn: str) -> Dict[str, str]:
        # Maps a case-folded field path to the real field path so callers can match
        # fields case-insensitively while still emitting correctly-cased urns.
        # ponytail: a schema with two fields differing only in case collapses to one
        # entry (last wins); acceptable for this best-effort name match.
        if urn in self._schema_field_cache:
            return self._schema_field_cache[urn]
        fields: Dict[str, str] = {}
        graph = self.ctx.graph
        if graph is not None:
            schema = graph.get_schema_metadata(urn)
            if schema is not None:
                fields = {
                    field.fieldPath.casefold(): field.fieldPath
                    for field in schema.fields
                }
        self._schema_field_cache[urn] = fields
        return fields

    @staticmethod
    def _subtype_workunit(urn: str, subtype: str) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=SubTypesClass(typeNames=[subtype]),
        ).as_workunit()
