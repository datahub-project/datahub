from typing import Dict, Iterable, List, Optional, Set

from requests.exceptions import RequestException

from datahub.emitter.mce_builder import (
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
from datahub.ingestion.graph.openapi import RelationshipDirection
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.tibco_ems.config import TibcoEmsSourceConfig
from datahub.ingestion.source.tibco_ems.constants import (
    BRIDGES_PATH,
    CONSUMER_RELATIONSHIP,
    NAME_DELIMITER,
    PROPERTY_CONSUMER_COUNT,
    PROPERTY_DESTINATION_TYPE,
    PROPERTY_EXPIRATION,
    PROPERTY_GLOBAL,
    PROPERTY_MAX_BYTES,
    PROPERTY_MAX_MSGS,
    PROPERTY_PENDING_MESSAGES,
    PROPERTY_PREFETCH,
    PROPERTY_SCHEMA_SOURCE,
    PROPERTY_SECURE,
    QUEUES_PATH,
    SCHEMA_SOURCE_DERIVED,
    SYSTEM_DESTINATION_PATTERN,
    TIBCO_EMS_PLATFORM,
    TOPICS_PATH,
    WILDCARD_DESTINATION_PATTERN,
)
from datahub.ingestion.source.tibco_ems.models import (
    DestinationType,
    TibcoBridge,
    TibcoDestination,
    TibcoEmsServerGroupKey,
)
from datahub.ingestion.source.tibco_ems.report import TibcoEmsSourceReport
from datahub.ingestion.source.tibco_ems.rest_client import TibcoEmsRestClient
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    SchemaFieldClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset

_SUBTYPE_BY_DESTINATION_TYPE: Dict[DestinationType, str] = {
    DestinationType.QUEUE: DatasetSubTypes.QUEUE,
    DestinationType.TOPIC: DatasetSubTypes.TOPIC,
}


@platform_name("TIBCO EMS", id=TIBCO_EMS_PLATFORM)
@config_class(TibcoEmsSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
@capability(
    SourceCapability.CONTAINERS,
    "Each EMS server group is emitted as a container holding its destinations",
)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Emitted from EMS bridges via the `include_bridges` config field",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Best-effort for bridges when both endpoints have a schema in DataHub, matched by "
    "name (a bridge copies whole messages); enable with `emit_column_lineage`",
    supported=True,
)
@capability(
    SourceCapability.SCHEMA_METADATA,
    "EMS has no message schema registry. Schemas can be estimated from the schemas of "
    "downstream consumers with `derive_schemas_from_lineage`",
    supported=True,
)
class TibcoEmsSource(StatefulIngestionSourceBase, TestableSource):
    platform: str = TIBCO_EMS_PLATFORM

    def __init__(self, config: TibcoEmsSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.config = config
        self.report: TibcoEmsSourceReport = TibcoEmsSourceReport()
        self.client = TibcoEmsRestClient(config)
        # Field names per dataset urn, read from DataHub once and reused across bridges.
        self._schema_field_cache: Dict[str, Dict[str, str]] = {}
        self._emitted_server_groups: Set[str] = set()
        if not config.verify_ssl:
            self.report.warning(
                title="TLS certificate verification is disabled",
                message="Connections to the EMS REST Proxy are open to interception. "
                "Prefer `ca_certificate_path` for a private CA.",
                context=config.base_url,
            )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "TibcoEmsSource":
        config = TibcoEmsSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            config = TibcoEmsSourceConfig.model_validate(config_dict)
            TibcoEmsRestClient(config).test_connection()
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except RequestException as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=f"Failed to connect to TIBCO EMS REST Proxy: {e}",
            )
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    def get_report(self) -> SourceReport:
        return self.report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        for destination in self._fetch_destinations():
            if not self._allowed(destination):
                self.report.report_destination_filtered(destination.name)
                continue
            yield from self._emit_destination(destination)

        if self.config.include_bridges:
            yield from self._emit_bridge_lineage()

    def close(self) -> None:
        self.client.close()
        super().close()

    def _fetch_destinations(self) -> List[TibcoDestination]:
        queues = self.client.fetch_queues()
        self.report.queues_scanned = len(queues.records)
        topics = self.client.fetch_topics()
        self.report.topics_scanned = len(topics.records)
        self._report_proxy_errors(QUEUES_PATH, queues.errors)
        self._report_proxy_errors(TOPICS_PATH, topics.errors)
        return [*queues.records, *topics.records]

    def _report_proxy_errors(self, path: str, errors: List[str]) -> None:
        # Reported as a failure rather than a warning so the run is marked failed
        # and stateful ingestion does not commit its checkpoint: an unreachable
        # server group would otherwise look like a group whose destinations were
        # all deleted, and stale-entity removal would soft-delete them.
        for error in errors:
            self.report.failure(
                title="EMS REST Proxy returned a partial result",
                message="One or more server groups could not be read, so their "
                "destinations are missing from this run.",
                context=f"{path}: {error}",
            )

    def _allowed(self, destination: TibcoDestination) -> bool:
        if not self.config.include_system_destinations and (
            SYSTEM_DESTINATION_PATTERN.match(destination.name)
        ):
            return False
        pattern = (
            self.config.queue_pattern
            if destination.destination_type is DestinationType.QUEUE
            else self.config.topic_pattern
        )
        return pattern.allowed(destination.name)

    def _emit_destination(
        self, destination: TibcoDestination
    ) -> Iterable[MetadataWorkUnit]:
        yield from self._emit_server_group(destination.server_group)
        name = self._dataset_name(
            destination.server_group,
            destination.destination_type,
            destination.name,
        )
        properties = self._custom_properties(destination)
        schema = (
            self._derive_schema(self._dataset_urn(name), destination.name)
            if self.config.derive_schemas_from_lineage
            else None
        )
        if schema:
            properties[PROPERTY_SCHEMA_SOURCE] = SCHEMA_SOURCE_DERIVED
        dataset = Dataset(
            platform=self.platform,
            name=name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=destination.name,
            subtype=_SUBTYPE_BY_DESTINATION_TYPE[destination.destination_type],
            custom_properties=properties,
            parent_container=self._server_group_key(destination.server_group),
            schema=schema,
        )
        yield from dataset.as_workunits()
        self.report.datasets_emitted += 1

    def _derive_schema(
        self, dataset_urn: str, display_name: str
    ) -> Optional[List[SchemaFieldClass]]:
        """Estimate a destination's message fields from what its consumers landed.

        EMS has no schema registry, and unlike a Kafka log a JMS destination cannot
        be sampled without consuming the customer's production messages - so the
        only surviving evidence of a message's shape is what came out of it.

        The union across consumers, not the intersection: consumers keep different
        subsets of a message, and any field one of them landed must have been on the
        wire. That makes the result downstream-shaped, which is why it is labelled
        rather than presented as a contract.
        """
        graph = self.ctx.graph
        if graph is None:
            self.report.warning(
                title="Cannot derive schemas without a DataHub graph",
                message="`derive_schemas_from_lineage` reads consumer schemas from "
                "DataHub. Configure a datahub-rest sink or a `datahub_api` block.",
                context=display_name,
            )
            return None
        if self._has_declared_schema(dataset_urn):
            return None

        consumers = self._consumers(dataset_urn)
        if not consumers:
            self.report.report_destination_without_consumers(display_name)
            return None

        fields: Dict[str, SchemaFieldClass] = {}
        for consumer_urn in consumers:
            consumer_schema = graph.get_schema_metadata(consumer_urn)
            if consumer_schema is None:
                continue
            for field in consumer_schema.fields:
                self._merge_derived_field(fields, field, display_name)
        if not fields:
            self.report.report_destination_without_consumers(display_name)
            return None

        self.report.derived_schemas_emitted += 1
        self.report.derived_schema_fields_emitted += len(fields)
        return [fields[key] for key in sorted(fields)]

    def _merge_derived_field(
        self,
        fields: Dict[str, SchemaFieldClass],
        field: SchemaFieldClass,
        display_name: str,
    ) -> None:
        if not self.config.generated_field_pattern.allowed(field.fieldPath):
            self.report.derived_fields_excluded += 1
            return
        # Keyed case-folded because the same field is routinely cased differently on
        # each consuming platform; the first casing seen becomes the message's.
        key = field.fieldPath.casefold()
        existing = fields.get(key)
        if existing is None:
            # Only the shape is carried over. Copying the whole field would drag the
            # consumer's descriptions, tags and terms onto the bus, presenting one
            # consumer's documentation as the message's own.
            fields[key] = SchemaFieldClass(
                fieldPath=field.fieldPath,
                type=field.type,
                nativeDataType=field.nativeDataType,
                # Nullability is a property of the message, and nothing downstream
                # can prove a field was always present.
                nullable=True,
            )
        elif existing.nativeDataType != field.nativeDataType:
            self.report.report_derived_field_type_conflict(
                display_name,
                field.fieldPath,
                existing.nativeDataType,
                field.nativeDataType,
            )

    def _has_declared_schema(self, dataset_urn: str) -> bool:
        # A schema the publisher declared outranks anything estimated here, so a
        # destination already carrying one is left alone. A previously derived
        # schema is refreshed, since its inputs may have changed.
        graph = self.ctx.graph
        if graph is None or graph.get_schema_metadata(dataset_urn) is None:
            return False
        properties = graph.get_aspect(dataset_urn, DatasetPropertiesClass)
        # No properties at all means no provenance to read, and a schema of unknown
        # origin is treated as declared - overwriting someone else's work silently is
        # the worse failure.
        if properties is None:
            return True
        return (properties.customProperties or {}).get(
            PROPERTY_SCHEMA_SOURCE
        ) != SCHEMA_SOURCE_DERIVED

    def _consumers(self, dataset_urn: str) -> List[str]:
        graph = self.ctx.graph
        if graph is None:
            return []
        try:
            related = graph.get_related_entities(
                dataset_urn,
                [CONSUMER_RELATIONSHIP],
                RelationshipDirection.INCOMING,
            )
            return sorted(
                {entity.urn for entity in related if entity.urn != dataset_urn}
            )
        except Exception as exc:
            self.report.warning(
                title="Could not read a destination's consumers",
                message="Schema derivation needs the destination's downstream "
                "lineage, which could not be read from DataHub.",
                context=dataset_urn,
                exc=exc,
            )
            return []

    def _emit_server_group(self, server_group: str) -> Iterable[MetadataWorkUnit]:
        if server_group in self._emitted_server_groups:
            return
        self._emitted_server_groups.add(server_group)
        container = Container(
            self._server_group_key(server_group),
            display_name=server_group,
            subtype=DatasetContainerSubTypes.TIBCO_EMS_SERVER_GROUP,
        )
        yield from container.as_workunits()
        self.report.server_groups_emitted += 1

    def _server_group_key(self, server_group: str) -> TibcoEmsServerGroupKey:
        return TibcoEmsServerGroupKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            server_group=server_group,
        )

    def _emit_bridge_lineage(self) -> Iterable[MetadataWorkUnit]:
        bridges = self.client.fetch_bridges()
        self.report.bridges_scanned = len(bridges.records)
        self._report_proxy_errors(BRIDGES_PATH, bridges.errors)

        upstreams_by_target: Dict[str, Set[str]] = {}
        for bridge in bridges.records:
            self._collect_bridge_upstreams(bridge, upstreams_by_target)

        for target_urn, source_urns in upstreams_by_target.items():
            fine_grained = (
                self._build_column_lineage(target_urn, source_urns)
                if self.config.emit_column_lineage
                else []
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=target_urn,
                aspect=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=source_urn,
                            type=DatasetLineageTypeClass.COPY,
                        )
                        for source_urn in sorted(source_urns)
                    ],
                    fineGrainedLineages=fine_grained or None,
                ),
            ).as_workunit()
            self.report.lineage_edges_emitted += len(source_urns)

    def _build_column_lineage(
        self, target_urn: str, source_urns: Set[str]
    ) -> List[FineGrainedLineageClass]:
        # A bridge copies whole messages unchanged, so a field present on both the
        # source and target destination is the same field. We can only match fields
        # that are actually described in DataHub, so this is best-effort: absent
        # schemas simply yield no column lineage and the coarse edge still stands.
        # Matching is case-insensitive because the same field is often cased
        # differently across platforms (e.g. ID vs id), but the schemaField urns use
        # each side's real field path.
        target_fields = self._schema_field_names(target_urn)
        if not target_fields:
            return []
        fine_grained: List[FineGrainedLineageClass] = []
        for source_urn in sorted(source_urns):
            source_fields = self._schema_field_names(source_urn)
            for key in sorted(source_fields.keys() & target_fields.keys()):
                fine_grained.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        upstreams=[
                            make_schema_field_urn(source_urn, source_fields[key])
                        ],
                        downstreams=[
                            make_schema_field_urn(target_urn, target_fields[key])
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

    def _collect_bridge_upstreams(
        self,
        bridge: TibcoBridge,
        upstreams_by_target: Dict[str, Set[str]],
    ) -> None:
        source_urn = self._resolve_destination_urn(
            bridge.server_group, bridge.source_type, bridge.source_name
        )
        for target in bridge.targets:
            # A bridge is configured on one EMS server, so its targets are in the
            # same server group as the bridge itself.
            target_urn = self._resolve_destination_urn(
                bridge.server_group, target.destination_type, target.name
            )
            if source_urn is None or target_urn is None:
                self.report.lineage_edges_unresolved += 1
                continue
            upstreams_by_target.setdefault(target_urn, set()).add(source_urn)

    def _resolve_destination_urn(
        self,
        server_group: str,
        destination_type: Optional[DestinationType],
        name: str,
    ) -> Optional[str]:
        # A bridge endpoint on the same EMS server shares this source's platform,
        # platform_instance and env, so its urn is deterministic even when the
        # destination was excluded from ingestion by a filter. Only wildcard
        # subscriptions and endpoints of unknown type cannot be mapped.
        if destination_type is None or WILDCARD_DESTINATION_PATTERN.search(name):
            self.report.report_bridge_endpoint_unresolved(name)
            return None
        return self._dataset_urn(
            self._dataset_name(server_group, destination_type, name)
        )

    def _custom_properties(self, destination: TibcoDestination) -> Dict[str, str]:
        properties: Dict[str, str] = {
            PROPERTY_DESTINATION_TYPE: destination.destination_type.value
        }
        optional: Dict[str, Optional[object]] = {
            PROPERTY_GLOBAL: destination.is_global,
            PROPERTY_SECURE: destination.secure,
            PROPERTY_MAX_MSGS: destination.max_msgs,
            PROPERTY_MAX_BYTES: destination.max_bytes,
            PROPERTY_PREFETCH: destination.prefetch,
            PROPERTY_EXPIRATION: destination.expiration,
            PROPERTY_PENDING_MESSAGES: destination.pending_message_count,
            PROPERTY_CONSUMER_COUNT: destination.consumer_count,
        }
        for key, value in optional.items():
            if value is not None:
                properties[key] = (
                    str(value).lower() if isinstance(value, bool) else str(value)
                )
        return properties

    def _dataset_name(
        self, server_group: str, destination_type: DestinationType, name: str
    ) -> str:
        # Server groups are independent namespaces and queues and topics are
        # independent of each other, so both lead the name to keep urns unique.
        return NAME_DELIMITER.join((server_group, destination_type.value, name))

    def _dataset_urn(self, name: str) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )
