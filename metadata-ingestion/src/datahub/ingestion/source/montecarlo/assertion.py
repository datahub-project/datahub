import logging
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Type

from datahub.emitter.mce_builder import (
    make_assertion_source,
    make_assertion_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DatahubKey
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.montecarlo.client import (
    MonteCarloAlert,
    MonteCarloAssertionDef,
)
from datahub.ingestion.source.montecarlo.config import MonteCarloSourceConfig
from datahub.ingestion.source.montecarlo.mcon_resolver import MconResolver
from datahub.ingestion.source.montecarlo.report import MonteCarloSourceReport
from datahub.metadata.com.linkedin.pegasus2avro.assertion import (
    AssertionResult,
    AssertionResultType,
    AssertionRunEvent,
    AssertionRunStatus,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import DataPlatformInstance
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionTypeClass,
    CustomAssertionInfoClass,
)
from datahub.utilities.time import datetime_to_ts_millis

if TYPE_CHECKING:
    from acryl_datahub_cloud.sdk.entities.assertion import (  # type: ignore[import-not-found]
        Assertion,
    )

logger = logging.getLogger(__name__)

PLATFORM = "montecarlo"


_CLOUD_ASSERTION_CLASS: "Optional[Type[Assertion]]" = None
_CLOUD_ASSERTION_CLASS_LOADED: bool = False


def _load_cloud_assertion_class() -> "Optional[Type[Assertion]]":
    """Return the DataHub Cloud ``Assertion`` entity class if available, cached.

    The connector prefers the Cloud SDK's assertion entity (it manages the
    assertionInfo + dataPlatformInstance aspects). When ``acryl-datahub-cloud`` is
    not installed we fall back to emitting equivalent OSS aspects directly, mirroring
    the optional-import pattern in ``datahub.sdk.main_client``.
    """
    global _CLOUD_ASSERTION_CLASS, _CLOUD_ASSERTION_CLASS_LOADED
    if _CLOUD_ASSERTION_CLASS_LOADED:
        return _CLOUD_ASSERTION_CLASS
    _CLOUD_ASSERTION_CLASS_LOADED = True
    try:
        from acryl_datahub_cloud.sdk.entities.assertion import (  # type: ignore[import-not-found]
            Assertion,
        )

        _CLOUD_ASSERTION_CLASS = Assertion
    except ImportError:
        pass
    except Exception:
        logger.warning(
            "Failed to load cloud Assertion class; using OSS fallback.", exc_info=True
        )
    return _CLOUD_ASSERTION_CLASS


class MonteCarloAssertionKey(DatahubKey):
    """Key for deterministic, stable assertion GUIDs across ingestion runs."""

    platform: str = PLATFORM
    monitor_uuid: str
    instance: Optional[str] = None


class MonteCarloAssertionBuilder:
    """Builds DataHub assertion workunits from Monte Carlo monitors/rules and alerts."""

    def __init__(
        self,
        config: MonteCarloSourceConfig,
        report: MonteCarloSourceReport,
        resolver: MconResolver,
    ) -> None:
        self.config = config
        self.report = report
        self.resolver = resolver
        # Maps a monitor/rule uuid to its assertion urn so alerts can attach run
        # events, and tracks the dataset each assertion targets.
        self._assertion_urn_by_monitor: Dict[str, str] = {}
        self._dataset_by_monitor: Dict[str, str] = {}

    def _assertion_urn(self, monitor_uuid: str) -> str:
        key = MonteCarloAssertionKey(
            monitor_uuid=monitor_uuid,
            instance=self.config.platform_instance,
        )
        return make_assertion_urn(key.guid())

    def build_assertion(
        self, definition: MonteCarloAssertionDef
    ) -> Iterable[MetadataWorkUnit]:
        if not self.config.monitor_pattern.allowed(definition.name or definition.uuid):
            self.report.report_dropped(definition.name or definition.uuid)
            return

        # Resolve the monitored asset. We use the first MCON that resolves to a URN;
        # a monitor without a resolvable asset is skipped with a warning.
        if not definition.entity_mcons:
            self.report.warning(
                title="Monitor has no monitored entities",
                message="Skipping monitor with no entity_mcons; cannot build a dataset URN.",
                context=f"monitor_uuid={definition.uuid}",
            )
            return

        dataset_urn: Optional[str] = None
        for mcon in definition.entity_mcons:
            dataset_urn = self.resolver.dataset_urn_for_mcon(mcon)
            if dataset_urn:
                break
        if dataset_urn is None:
            return

        assertion_urn = self._assertion_urn(definition.uuid)
        self._assertion_urn_by_monitor[definition.uuid] = assertion_urn
        self._dataset_by_monitor[definition.uuid] = dataset_urn

        custom_properties: Dict[str, str] = {
            "mc_monitor_uuid": definition.uuid,
            "mc_monitor_type": definition.native_type,
        }
        if definition.resource_id:
            custom_properties["mc_resource_id"] = definition.resource_id
        if definition.data_quality_dimension:
            custom_properties["mc_dq_dimension"] = definition.data_quality_dimension
        if definition.severity:
            custom_properties["mc_severity"] = definition.severity

        custom_assertion = CustomAssertionInfoClass(
            type=definition.native_type,
            entity=dataset_urn,
            field=None,
            logic=definition.custom_sql,
        )
        yield from self._emit_assertion(
            assertion_urn=assertion_urn,
            custom_assertion=custom_assertion,
            description=definition.description,
            custom_properties=custom_properties,
        )
        self.report.report_assertion_emitted()

    def _emit_assertion(
        self,
        assertion_urn: str,
        custom_assertion: CustomAssertionInfoClass,
        description: Optional[str],
        custom_properties: Dict[str, str],
    ) -> Iterable[MetadataWorkUnit]:
        assertion_cls = _load_cloud_assertion_class()
        if assertion_cls is not None:
            assertion = assertion_cls(
                id=assertion_urn,
                info=custom_assertion,
                description=description,
                custom_properties=custom_properties,
                source=make_assertion_source(),
                platform=PLATFORM,
                platform_instance=self.config.platform_instance,
            )
            yield from assertion.as_workunits()
        else:
            yield from self._emit_assertion_oss(
                assertion_urn, custom_assertion, description, custom_properties
            )

    def _emit_assertion_oss(
        self,
        assertion_urn: str,
        custom_assertion: CustomAssertionInfoClass,
        description: Optional[str],
        custom_properties: Dict[str, str],
    ) -> Iterable[MetadataWorkUnit]:
        """Fallback used when acryl-datahub-cloud is not installed.

        Emits the same assertionInfo + dataPlatformInstance aspects (as primary
        workunits) that the Cloud ``Assertion`` entity emits, so the connector
        output is identical whether or not the Cloud SDK is present. Emitting as
        primary also lets ``auto_status_aspect`` add the status aspect.
        """
        assertion_info = AssertionInfoClass(
            type=AssertionTypeClass.CUSTOM,
            customAssertion=custom_assertion,
            source=make_assertion_source(),
            description=description,
            customProperties=custom_properties,
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=assertion_info,
        ).as_workunit()

        platform_instance = DataPlatformInstance(
            platform=make_data_platform_urn(PLATFORM),
            instance=(
                make_dataplatform_instance_urn(PLATFORM, self.config.platform_instance)
                if self.config.platform_instance
                else None
            ),
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=platform_instance,
        ).as_workunit()

    def build_run_event(self, alert: MonteCarloAlert) -> Iterable[MetadataWorkUnit]:
        if alert.monitor_uuid is None:
            return
        assertion_urn = self._assertion_urn_by_monitor.get(alert.monitor_uuid)
        dataset_urn = self._dataset_by_monitor.get(alert.monitor_uuid)
        if assertion_urn is None or dataset_urn is None:
            # Alert for a monitor we didn't ingest (filtered out, unresolved asset).
            return
        if alert.created_time is None:
            self.report.warning(
                title="Alert skipped: missing timestamp",
                message="Alert has no createdTime and cannot be emitted as a run event.",
                context=f"alert_uuid={alert.uuid}",
            )
            return

        native_results: Dict[str, str] = {}
        if alert.severity:
            native_results["severity"] = alert.severity
        if alert.priority:
            native_results["priority"] = alert.priority
        if alert.sub_types:
            native_results["subType"] = ",".join(alert.sub_types)

        run_event = AssertionRunEvent(
            timestampMillis=datetime_to_ts_millis(alert.created_time),
            runId=alert.uuid,
            asserteeUrn=dataset_urn,
            status=AssertionRunStatus.COMPLETE,
            assertionUrn=assertion_urn,
            result=AssertionResult(
                type=AssertionResultType.FAILURE,
                nativeResults=native_results or None,
            ),
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=run_event,
        ).as_workunit(is_primary_source=False)
        self.report.report_run_event_emitted()

    @property
    def ingested_monitor_uuids(self) -> List[str]:
        return list(self._assertion_urn_by_monitor.keys())
