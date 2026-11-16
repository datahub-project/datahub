from typing import Dict, Iterable, List, Optional, Set, Union

from datahub.api.entities.datacontract.datacontract import DataContract
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.extractor.json_schema_util import JsonSchemaTranslator
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.hightouch.config import (
    HightouchSourceConfig,
    HightouchSourceReport,
)
from datahub.ingestion.source.hightouch.constants import HIGHTOUCH_PLATFORM
from datahub.ingestion.source.hightouch.models import (
    HightouchContract,
    HightouchContractEvent,
    HightouchEventSource,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    SchemaFieldClass,
    UpstreamClass,
)
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity


class HightouchContractEmitter:
    """Emits each event of a Hightouch Event Contract as a dataset whose schema is
    the event's JSON Schema, with lineage from the contract's event sources and a
    DataContract holding a schema assertion.

    The API exposes no contract validation history, so the assertions are
    definitions without run results.
    """

    def __init__(self, config: HightouchSourceConfig, report: HightouchSourceReport):
        self.config = config
        self.report = report
        self._emitted_source_urns: Set[str] = set()

    def emit(
        self, contract: HightouchContract, workspace: ContainerKey
    ) -> Iterable[Union[Entity, MetadataWorkUnit]]:
        source_urns: List[str] = []
        for source in contract.event_sources:
            source_dataset = self._event_source_dataset(source, workspace)
            urn = str(source_dataset.urn)
            source_urns.append(urn)
            # Sources are shared across contracts; emit each once, before it is
            # referenced as an upstream.
            if urn not in self._emitted_source_urns:
                self._emitted_source_urns.add(urn)
                yield source_dataset
                self.report.event_sources_emitted += 1

        for event in contract.events:
            fields = self._schema_fields(event)
            dataset = Dataset(
                platform=HIGHTOUCH_PLATFORM,
                name=_event_dataset_name(contract, event),
                env=self.config.env,
                platform_instance=self.config.platform_instance,
                display_name=event.name or event.type,
                description=contract.description,
                subtype=DatasetSubTypes.HIGHTOUCH_EVENT_CONTRACT,
                parent_container=workspace,
                custom_properties=_custom_properties(contract, event),
                schema=fields,
                upstreams=[
                    UpstreamClass(dataset=urn, type=DatasetLineageTypeClass.COPY)
                    for urn in source_urns
                ]
                or None,
            )
            yield dataset
            self.report.contract_events_emitted += 1
            if fields:
                yield from self._data_contract(
                    contract, event, str(dataset.urn), fields
                )

        self.report.contracts_emitted += 1

    def _event_source_dataset(
        self, source: HightouchEventSource, workspace: ContainerKey
    ) -> Dataset:
        return Dataset(
            platform=HIGHTOUCH_PLATFORM,
            name=f"event_source.{source.id}",
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            display_name=source.name,
            subtype=DatasetSubTypes.HIGHTOUCH_EVENT_SOURCE,
            parent_container=workspace,
        )

    def _data_contract(
        self,
        contract: HightouchContract,
        event: HightouchContractEvent,
        dataset_urn: str,
        fields: List[SchemaFieldClass],
    ) -> Iterable[MetadataWorkUnit]:
        # A field-list schema rather than the json-schema contract type: the latter
        # double-prefixes the platform in the generated SchemaMetadata.
        try:
            data_contract = DataContract.model_validate(
                {
                    "version": 1,
                    "entity": dataset_urn,
                    "schema": {
                        "type": "field-list",
                        "fields": fields,
                        "description": _contract_description(contract, event),
                    },
                }
            )
            for mcp in data_contract.generate_mcp():
                yield mcp.as_workunit()
            self.report.contract_assertions_emitted += 1
        except Exception as e:
            self.report.warning(
                title="Failed to build event data contract",
                message="Could not build a DataContract from the event's JSON Schema.",
                context=f"contract: {contract.name}, event: {event.name or event.type}",
                exc=e,
            )

    def _schema_fields(
        self, event: HightouchContractEvent
    ) -> Optional[List[SchemaFieldClass]]:
        if not event.json_schema:
            return None
        try:
            return list(JsonSchemaTranslator.get_fields_from_schema(event.json_schema))
        except Exception as e:
            self.report.warning(
                title="Failed to parse event schema",
                message="Could not translate the event's JSON Schema into a DataHub "
                "schema.",
                context=f"event: {event.name or event.type}",
                exc=e,
            )
            return None


def _event_dataset_name(
    contract: HightouchContract, event: HightouchContractEvent
) -> str:
    event_key = event.slug or event.name or event.type
    if event.version and event.version != "default":
        event_key = f"{event_key}.{event.version}"
    return f"{contract.slug or contract.id}.{event_key}"


def _custom_properties(
    contract: HightouchContract, event: HightouchContractEvent
) -> Dict[str, str]:
    optional = {
        "contract_slug": contract.slug,
        "on_undeclared_schema": contract.on_undeclared_schema,
        "event_name": event.name,
        "event_version": event.version,
        "on_schema_violation": event.on_schema_violation,
        "on_undeclared_fields": event.on_undeclared_fields,
    }
    return {
        "contract_id": contract.id,
        "contract_name": contract.name,
        "event_type": event.type,
        **{key: value for key, value in optional.items() if value},
    }


def _contract_description(
    contract: HightouchContract, event: HightouchContractEvent
) -> str:
    rules = {
        "onSchemaViolation": event.on_schema_violation,
        "onUndeclaredFields": event.on_undeclared_fields,
        "onUndeclaredSchema": contract.on_undeclared_schema,
    }
    enforcement = ", ".join(f"{k}={v}" for k, v in rules.items() if v)
    base = f"Hightouch event contract '{contract.name}'"
    return f"{base} ({enforcement})" if enforcement else base
