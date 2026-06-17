from typing import Dict, Iterable, List, Optional, Union

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
)
from datahub.metadata.schema_classes import SchemaFieldClass
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity


class HightouchContractHandler:
    """Emits Hightouch Event Contracts as datasets.

    Each event in a contract carries a JSON Schema that validates incoming events.
    We emit one dataset per event, with the JSON Schema translated to a DataHub
    schema and the contract's enforcement rules captured as custom properties.
    """

    def __init__(
        self,
        config: HightouchSourceConfig,
        report: HightouchSourceReport,
    ) -> None:
        self.config = config
        self.report = report

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

        for event in contract.events:
            yield self._build_event_dataset(contract, event)
            self.report.report_contract_events_emitted()

        self.report.report_contracts_emitted()

    def _build_event_dataset(
        self, contract: HightouchContract, event: HightouchContractEvent
    ) -> Dataset:
        dataset = Dataset(
            name=self._event_dataset_name(contract, event),
            platform=HIGHTOUCH_PLATFORM,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            display_name=event.name or event.type,
            description=contract.description,
            subtype=DatasetSubTypes.HIGHTOUCH_EVENT_CONTRACT,
            custom_properties=self._build_custom_properties(contract, event),
        )

        schema_fields = self._schema_fields(event)
        if schema_fields:
            dataset._set_schema(schema_fields)

        return dataset

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
