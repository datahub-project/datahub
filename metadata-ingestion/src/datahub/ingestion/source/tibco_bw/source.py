from typing import Iterable

from requests.exceptions import RequestException

from datahub.api.entities.datajob import DataFlow, DataJob
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
from datahub.ingestion.source.tibco_bw.config import TibcoBwSourceConfig
from datahub.ingestion.source.tibco_bw.constants import (
    PROPERTY_DOMAIN,
    TIBCO_BW_PLATFORM,
)
from datahub.ingestion.source.tibco_bw.models import (
    TibcoApplication,
    TibcoDeployment,
    TibcoScope,
)
from datahub.ingestion.source.tibco_bw.report import TibcoBwSourceReport
from datahub.metadata.schema_classes import SubTypesClass
from datahub.metadata.urns import DatasetUrn

_FLOW_SUBTYPE_BY_DEPLOYMENT = {
    TibcoDeployment.ON_PREM: DataFlowSubTypes.TIBCO_BW_APPSPACE,
    TibcoDeployment.CLOUD: DataFlowSubTypes.TIBCO_TCI_SUBSCRIPTION,
}


@platform_name("TIBCO BusinessWorks", id=TIBCO_BW_PLATFORM)
@config_class(TibcoBwSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Emitted from the operator-supplied `application_lineage` map, since the TIBCO "
    "runtime APIs do not expose an application's datasets",
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
        for scope in self.client.fetch_scopes():
            self.report.scopes_scanned += 1
            if not self._scope_allowed(scope):
                self.report.report_scope_filtered(scope.name)
                continue
            yield from self._emit_scope(scope)

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
        self.report.flows_emitted += 1

        for application in scope.applications:
            self.report.applications_scanned += 1
            if not self.config.application_pattern.allowed(application.name):
                self.report.report_application_filtered(application.name)
                continue
            yield from self._emit_application(flow, application)

    def _emit_application(
        self, flow: DataFlow, application: TibcoApplication
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
        lineage = self.config.application_lineage.get(application.name)
        if lineage is not None:
            job.inlets = [DatasetUrn.from_string(urn) for urn in lineage.upstreams]
            job.outlets = [DatasetUrn.from_string(urn) for urn in lineage.downstreams]

        has_lineage = bool(job.inlets or job.outlets)
        # materialize_iolets stays False: the iolets are datasets owned by other
        # connectors, so we link to them without emitting empty stub entities.
        for mcp in job.generate_mcp(
            generate_lineage=has_lineage, materialize_iolets=False
        ):
            yield mcp.as_workunit()
        yield self._subtype_workunit(str(job.urn), DataJobSubTypes.TIBCO_APPLICATION)
        self.report.jobs_emitted += 1
        if has_lineage:
            self.report.jobs_with_lineage += 1
            self.report.lineage_iolets_emitted += len(job.inlets) + len(job.outlets)

    @staticmethod
    def _subtype_workunit(urn: str, subtype: str) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=SubTypesClass(typeNames=[subtype]),
        ).as_workunit()
