from typing import Dict, Iterable, List, Union

import requests

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
from datahub.ingestion.source.common.subtypes import GenericContainerSubTypes
from datahub.ingestion.source.hightouch.config import (
    HightouchSourceConfig,
    HightouchSourceReport,
)
from datahub.ingestion.source.hightouch.constants import HIGHTOUCH_PLATFORM
from datahub.ingestion.source.hightouch.hightouch_api import HightouchAPIClient
from datahub.ingestion.source.hightouch.hightouch_contract import (
    HightouchContractEmitter,
)
from datahub.ingestion.source.hightouch.hightouch_model import HightouchModelEmitter
from datahub.ingestion.source.hightouch.hightouch_sync import HightouchSyncEmitter
from datahub.ingestion.source.hightouch.models import HightouchSync
from datahub.ingestion.source.hightouch.resolver import HightouchResolver
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.sdk.container import Container
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.entity import Entity


class HightouchWorkspaceKey(ContainerKey):
    workspace_id: str


@platform_name("Hightouch")
@config_class(HightouchSourceConfig)
@support_status(SupportStatus.ALPHA)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.CONTAINERS, "Enabled by default, one per workspace")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default, from warehouse tables through models and syncs to "
    "destinations",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default, from parsed model SQL and sync field mappings",
)
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default for models")
@capability(SourceCapability.OWNERSHIP, "Enabled by default via `extract_owners`")
@capability(SourceCapability.TAGS, "Enabled by default")
@capability(
    SourceCapability.DELETION_DETECTION, "Enabled by default via stateful ingestion"
)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class HightouchSource(StatefulIngestionSourceBase, TestableSource):
    """Ingests Hightouch models, syncs, sync runs and event contracts.

    Each workspace becomes a container holding its models as datasets and a
    DataFlow whose DataJobs are the workspace's syncs.
    """

    config: HightouchSourceConfig
    report: HightouchSourceReport
    platform: str = HIGHTOUCH_PLATFORM

    def __init__(self, config: HightouchSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = HightouchSourceReport()
        self.api_client = HightouchAPIClient(config.api_config, self.report)
        self.resolver = HightouchResolver(config, self.report, ctx.graph)
        self.model_emitter = HightouchModelEmitter(config, self.report, self.resolver)
        self.sync_emitter = HightouchSyncEmitter(
            config, self.report, self.resolver, self.api_client
        )
        self.contract_emitter = HightouchContractEmitter(config, self.report)
        self._workspaces: Dict[str, HightouchWorkspaceKey] = {}
        self._flows: Dict[str, DataFlow] = {}

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        report = TestConnectionReport()
        try:
            config = HightouchSourceConfig.parse_obj_allow_extras(config_dict)
            HightouchAPIClient(
                config.api_config, HightouchSourceReport()
            ).check_access()
            report.basic_connectivity = CapabilityReport(capable=True)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            reason = (
                f"Hightouch rejected the API key (HTTP {status}). Check that the key "
                "is valid and belongs to the workspace to ingest."
                if status in (401, 403)
                else f"The Hightouch API returned an error: {e}"
            )
            report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=reason
            )
        except requests.exceptions.RequestException as e:
            report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=f"Could not reach the Hightouch API at base_url: {e}",
            )
        except Exception as e:
            report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=f"Invalid configuration: {e}"
            )
        return report

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        all_syncs = self.api_client.get_syncs()
        self.resolver.sources = {s.id: s for s in self.api_client.get_sources()}
        self.resolver.models = {m.id: m for m in self.api_client.get_models()}
        self.resolver.destinations = {
            d.id: d for d in self.api_client.get_destinations()
        }

        if self.config.emit_models_as_datasets:
            yield from self._models(all_syncs)

        for sync in all_syncs:
            if not self.config.sync_patterns.allowed(sync.slug):
                self.report.filtered_syncs.append(sync.slug)
                continue
            try:
                yield from self._flow(sync.workspace_id)
                yield from self.sync_emitter.emit(sync, self._flows[sync.workspace_id])
            except Exception as e:
                # A failure, not a warning: it keeps stale-entity removal from
                # soft-deleting a sync that still exists in Hightouch.
                self.report.failure(
                    title="Failed to process sync",
                    message="The sync could not be ingested.",
                    context=f"sync_slug: {sync.slug}",
                    exc=e,
                )

        if self.config.include_contracts:
            yield from self._contracts()

    def _models(
        self, all_syncs: List[HightouchSync]
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        # A model's columns may only be known from the syncs that read it, so its
        # schema is the union of every sync's referenced columns.
        columns_by_model: Dict[str, List[str]] = {}
        for sync in all_syncs:
            columns = columns_by_model.setdefault(sync.model_id, [])
            columns.extend(c for c in sync.referenced_columns or [] if c not in columns)

        for model in self.resolver.models.values():
            if not self.config.model_patterns.allowed(model.name):
                self.report.filtered_models.append(model.name)
                continue
            try:
                yield from self._workspace(model.workspace_id)
                yield from self.model_emitter.emit(
                    model,
                    columns_by_model.get(model.id, []),
                    self._workspaces[model.workspace_id],
                )
                self.resolver.emitted_models.add(model.id)
            except Exception as e:
                # A failure for the same reason as a failed sync.
                self.report.failure(
                    title="Failed to process model",
                    message="The model could not be ingested.",
                    context=f"model_slug: {model.slug}",
                    exc=e,
                )

    def _contracts(self) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        for contract in self.api_client.get_contracts() or []:
            if not self.config.contract_patterns.allowed(contract.name):
                self.report.filtered_contracts.append(contract.name)
                continue
            try:
                yield from self._workspace(contract.workspace_id)
                yield from self.contract_emitter.emit(
                    contract, self._workspaces[contract.workspace_id]
                )
            except Exception as e:
                self.report.failure(
                    title="Failed to process contract",
                    message="The event contract could not be ingested.",
                    context=f"contract: {contract.name}",
                    exc=e,
                )

    def _workspace(self, workspace_id: str) -> Iterable[Entity]:
        if workspace_id in self._workspaces:
            return
        key = HightouchWorkspaceKey(
            platform=HIGHTOUCH_PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
            workspace_id=workspace_id,
        )
        self._workspaces[workspace_id] = key
        yield Container(
            key,
            display_name=f"Workspace {workspace_id}",
            subtype=GenericContainerSubTypes.HIGHTOUCH_WORKSPACE,
            extra_properties={"workspace_id": workspace_id},
        )

    def _flow(self, workspace_id: str) -> Iterable[Entity]:
        """One DataFlow per workspace, holding a DataJob per sync."""
        if workspace_id in self._flows:
            return
        yield from self._workspace(workspace_id)
        flow = DataFlow(
            platform=HIGHTOUCH_PLATFORM,
            name=workspace_id,
            display_name="Syncs",
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            parent_container=self._workspaces[workspace_id],
        )
        self._flows[workspace_id] = flow
        yield flow

    def get_report(self) -> SourceReport:
        return self.report
