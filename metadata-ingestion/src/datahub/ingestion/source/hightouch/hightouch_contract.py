from typing import Dict, Iterable, List, Optional, Set, Union

from datahub.api.entities.datacontract.datacontract import DataContract
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
    UpstreamLineageClass,
)
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity


class HightouchContractHandler:
    """Emits Hightouch Event Contracts as DataHub entities.

    Each contract is linked to one or more Event Sources and defines a set of
    events, each carrying a JSON Schema that validates incoming events. For each
    event we emit:

    - a dataset whose schema is the event's JSON Schema, with the enforcement
      rules captured as custom properties,
    - upstream lineage from the contract's Event Sources to that dataset,
    - a DataHub DataContract on that dataset, housing a schema assertion derived
      from the event's JSON Schema.

    Note: the Hightouch API exposes no contract validation history, so the
    assertions are definitions only (no run results).
    """

    def __init__(
        self,
        config: HightouchSourceConfig,
        report: HightouchSourceReport,
    ) -> None:
        self.config = config
        self.report = report
        self._emitted_source_urns: Set[str] = set()

    def get_contract_workunits(
        self, contracts: List[HightouchContract]
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        for contract in contracts:
            if not self.config.contract_patterns.allowed(contract.name):
                self.report.report_contracts_dropped(contract.name)
                continue

            try:
                yield from self._emit_contract(contract)
            except Exception as e:
                self.report.warning(
                    title="Failed to process contract",
                    message="An error occurred while processing an event contract.",
                    context=f"contract: {contract.name} (contract_id: {contract.id})",
                    exc=e,
                )

    def _emit_contract(
        self, contract: HightouchContract
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        self.report.report_contracts_scanned()

        if not contract.events:
            return

        source_urns: List[str] = []
        for source in contract.event_sources:
            source_dataset = self._build_event_source_dataset(source)
            source_urns.append(str(source_dataset.urn))
            if str(source_dataset.urn) not in self._emitted_source_urns:
                self._emitted_source_urns.add(str(source_dataset.urn))
                yield source_dataset
                self.report.report_event_sources_emitted()

        for event in contract.events:
            event_dataset = self._build_event_dataset(contract, event, source_urns)
            yield event_dataset
            self.report.report_contract_events_emitted()

            yield from self._emit_event_contract(contract, event, event_dataset)

        self.report.report_contracts_emitted()

    def _build_event_source_dataset(self, source: HightouchEventSource) -> Dataset:
        return Dataset(
            name=f"event_source.{source.id}",
            platform=HIGHTOUCH_PLATFORM,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            display_name=source.name,
            subtype=DatasetSubTypes.HIGHTOUCH_EVENT_SOURCE,
        )

    def _build_event_dataset(
        self,
        contract: HightouchContract,
        event: HightouchContractEvent,
        source_urns: List[str],
    ) -> Dataset:
        upstreams: Optional[UpstreamLineageClass] = None
        if source_urns:
            upstreams = UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=source_urn,
                        type=DatasetLineageTypeClass.COPY,
                    )
                    for source_urn in source_urns
                ]
            )

        dataset = Dataset(
            name=self._event_dataset_name(contract, event),
            platform=HIGHTOUCH_PLATFORM,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            display_name=event.name or event.type,
            description=contract.description,
            subtype=DatasetSubTypes.HIGHTOUCH_EVENT_CONTRACT,
            custom_properties=self._build_custom_properties(contract, event),
            upstreams=upstreams,
        )

        schema_fields = self._schema_fields(event)
        if schema_fields:
            dataset._set_schema(schema_fields)

        return dataset

    def _emit_event_contract(
        self,
        contract: HightouchContract,
        event: HightouchContractEvent,
        event_dataset: Dataset,
    ) -> Iterable[MetadataWorkUnit]:
        if not event.json_schema:
            return

        try:
            data_contract = DataContract.model_validate(
                {
                    "version": 1,
                    "entity": str(event_dataset.urn),
                    "schema": {
                        "type": "json-schema",
                        "json-schema": event.json_schema,
                        "description": self._contract_description(contract, event),
                    },
                }
            )
            for mcp in data_contract.generate_mcp():
                yield mcp.as_workunit()
            self.report.report_contract_assertions_emitted()
        except Exception as e:
            self.report.warning(
                title="Failed to build event data contract",
                message="Could not build a DataContract from the event's JSON Schema.",
                context=f"event: {event.name or event.type}",
                exc=e,
            )

    def _contract_description(
        self, contract: HightouchContract, event: HightouchContractEvent
    ) -> str:
        rules = {
            "onSchemaViolation": event.on_schema_violation,
            "onUndeclaredFields": event.on_undeclared_fields,
            "onUndeclaredSchema": contract.on_undeclared_schema,
        }
        enforcement = ", ".join(f"{k}={v}" for k, v in rules.items() if v)
        base = f"Hightouch event contract '{contract.name}'"
        return f"{base} ({enforcement})" if enforcement else base

    def _event_dataset_name(
        self, contract: HightouchContract, event: HightouchContractEvent
    ) -> str:
        contract_key = contract.slug or contract.id
        event_key = event.slug or event.name or event.type
        if event.version and event.version != "default":
            event_key = f"{event_key}.{event.version}"
        return f"{contract_key}.{event_key}"

    def _build_custom_properties(
        self, contract: HightouchContract, event: HightouchContractEvent
    ) -> Dict[str, str]:
        properties = {
            "contract_id": contract.id,
            "contract_name": contract.name,
            "event_type": event.type,
        }
        optional = {
            "contract_slug": contract.slug,
            "on_undeclared_schema": contract.on_undeclared_schema,
            "event_name": event.name,
            "event_version": event.version,
            "on_schema_violation": event.on_schema_violation,
            "on_undeclared_fields": event.on_undeclared_fields,
        }
        properties.update({k: v for k, v in optional.items() if v})
        return properties

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
                message="Could not translate the event's JSON Schema into a DataHub schema.",
                context=f"event: {event.name or event.type}",
                exc=e,
            )
            return None
