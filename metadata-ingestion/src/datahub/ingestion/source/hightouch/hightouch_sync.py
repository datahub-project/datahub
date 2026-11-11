import dataclasses
import json
from typing import Dict, Iterable, List, Optional, Union

from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.api.source import StructuredLogCategory
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DataJobSubTypes
from datahub.ingestion.source.hightouch.config import (
    HightouchSourceConfig,
    HightouchSourceReport,
)
from datahub.ingestion.source.hightouch.constants import HIGHTOUCH_PLATFORM
from datahub.ingestion.source.hightouch.hightouch_api import HightouchAPIClient
from datahub.ingestion.source.hightouch.hightouch_model import owners_of, tags_of
from datahub.ingestion.source.hightouch.models import (
    HightouchDestination,
    HightouchModel,
    HightouchSync,
    HightouchSyncRun,
)
from datahub.ingestion.source.hightouch.resolver import HightouchResolver, match_field
from datahub.ingestion.source.hightouch.sync_config import (
    destination_object,
    field_mappings,
)
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.entity import Entity

# Unknown statuses are deliberately absent so they are never recorded as SUCCESS.
_FINISHED_RUN_RESULTS = {
    "success": InstanceRunResult.SUCCESS,
    "warning": InstanceRunResult.SUCCESS,
    "failed": InstanceRunResult.FAILURE,
    "cancelled": InstanceRunResult.SKIPPED,
    "interrupted": InstanceRunResult.SKIPPED,
}
# A run with one of these statuses has not finished: only its start is recorded.
_IN_PROGRESS_RUN_STATUSES = {"queued", "querying", "processing", "reporting"}


@dataclasses.dataclass
class SyncUpstreams:
    inlets: List[str]
    # Columns of the single inlet the field mappings read from, when known.
    field_paths: Optional[List[str]] = None


class HightouchSyncEmitter:
    def __init__(
        self,
        config: HightouchSourceConfig,
        report: HightouchSourceReport,
        resolver: HightouchResolver,
        api_client: HightouchAPIClient,
    ):
        self.config = config
        self.report = report
        self.resolver = resolver
        self.api_client = api_client

    def emit(
        self, sync: HightouchSync, flow: DataFlow
    ) -> Iterable[Union[Entity, MetadataWorkUnit]]:
        self.report.syncs_scanned += 1
        model = self.resolver.models.get(sync.model_id)
        destination = self.resolver.destinations.get(sync.destination_id)
        object_name = destination_object(sync)

        upstreams = SyncUpstreams(inlets=[])
        if model:
            upstreams = self._upstreams(sync, model)
        else:
            self.report.warning(
                title="Sync model not found",
                message="The sync's model was not returned by the Hightouch API, so "
                "the sync has no upstream lineage.",
                context=f"sync_slug: {sync.slug}, model_id: {sync.model_id}",
                log_category=StructuredLogCategory.LINEAGE,
            )

        outlet = (
            self.resolver.destination_urn(object_name, destination)
            if destination and object_name
            else None
        )
        if destination and not object_name:
            # Common for webhook-style destinations, so info rather than a warning.
            self.report.unresolved_destinations.append(sync.slug)
            self.report.info(
                title="Could not resolve sync destination object",
                message="The sync configuration names no destination table or object "
                "the connector recognises, so the sync has no outlet.",
                context=f"sync_slug: {sync.slug}, destination_type: {destination.type}",
            )

        fine_grained = (
            self._column_lineage(
                sync, upstreams.inlets[0], upstreams.field_paths, outlet
            )
            if outlet
            and upstreams.field_paths is not None
            and len(upstreams.inlets) == 1
            else []
        )

        datajob = DataJob(
            name=sync.id,
            flow=flow,
            display_name=sync.slug,
            subtype=DataJobSubTypes.HIGHTOUCH_SYNC,
            created=sync.created_at,
            last_modified=sync.updated_at,
            custom_properties=self._custom_properties(
                sync, model, destination, object_name
            ),
            inlets=list(upstreams.inlets),
            outlets=[outlet] if outlet else [],
            fine_grained_lineages=fine_grained or None,
            owners=owners_of(self.config, sync.created_by),
            tags=tags_of(sync.tags),
        )
        yield datajob

        if self.config.include_sync_runs:
            yield from self._runs(sync, datajob)

    def _upstreams(self, sync: HightouchSync, model: HightouchModel) -> SyncUpstreams:
        if model.id in self.resolver.emitted_models:
            fields = self.resolver.model_fields.get(model.id) or []
            return SyncUpstreams(
                inlets=[self.resolver.model_urn(model)],
                field_paths=[f.fieldPath for f in fields],
            )

        source = self.resolver.sources.get(model.source_id)
        if source:
            # The model itself is not emitted, so the sync reads the tables the
            # model reads.
            table_urn = self.resolver.source_table_urn(model, source)
            if table_urn:
                fields = self.resolver.schema_fields(table_urn) or []
                return SyncUpstreams(
                    inlets=[table_urn], field_paths=[f.fieldPath for f in fields]
                )
            parsed = self.resolver.parse_model_sql(model, source)
            if parsed and parsed.upstream_urns:
                # The model's output columns are not those of the tables it
                # queries, so there is no column lineage.
                return SyncUpstreams(inlets=parsed.upstream_urns)

        self.report.warning(
            title="No upstream lineage for sync",
            message="The sync's model is not emitted and the tables it reads could "
            "not be resolved.",
            context=f"sync_slug: {sync.slug}, query_type: {model.query_type}",
            log_category=StructuredLogCategory.LINEAGE,
        )
        return SyncUpstreams(inlets=[])

    def _column_lineage(
        self,
        sync: HightouchSync,
        inlet: str,
        upstream_field_paths: List[str],
        outlet: str,
    ) -> List[FineGrainedLineageClass]:
        destination_paths = [
            f.fieldPath for f in self.resolver.schema_fields(outlet) or []
        ]
        lineage = []
        for mapping in field_mappings(sync, self.report):
            # Mappings use the names as typed in Hightouch; adopt the casing of
            # schemas DataHub already has so the field URNs join.
            source_field = (
                match_field(mapping.source_field, upstream_field_paths)
                or mapping.source_field
            )
            destination_field = (
                match_field(mapping.destination_field, destination_paths)
                or mapping.destination_field
            )
            lineage.append(
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=[make_schema_field_urn(inlet, source_field)],
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=[make_schema_field_urn(outlet, destination_field)],
                )
            )
        self.report.column_lineage_emitted += len(lineage)
        return lineage

    def _custom_properties(
        self,
        sync: HightouchSync,
        model: Optional[HightouchModel],
        destination: Optional[HightouchDestination],
        object_name: Optional[str],
    ) -> Dict[str, str]:
        properties = {
            "sync_id": sync.id,
            "model_id": sync.model_id,
            "destination_id": sync.destination_id,
            "disabled": str(sync.disabled).lower(),
        }
        if sync.schedule:
            properties["schedule"] = json.dumps(sync.schedule, sort_keys=True)
        if model:
            properties["model_name"] = model.name
        if destination:
            properties["destination_name"] = destination.name
            properties["destination_type"] = destination.type
        if object_name:
            properties["destination_object"] = object_name
        return properties

    def _runs(
        self, sync: HightouchSync, datajob: DataJob
    ) -> Iterable[MetadataWorkUnit]:
        # Run history is extra detail on a sync already emitted, so a failure here
        # is a warning and does not discard the sync.
        try:
            runs = self.api_client.get_sync_runs(
                sync.id, limit=self.config.max_sync_runs_per_sync
            )
        except Exception as e:
            self.report.warning(
                title="Failed to fetch sync runs",
                message="Run history for this sync is missing; the sync itself was "
                "ingested.",
                context=f"sync_slug: {sync.slug}",
                exc=e,
            )
            return
        for run in runs:
            yield from self._run_workunits(sync, run, datajob)
            self.report.sync_runs_emitted += 1

    def _run_workunits(
        self, sync: HightouchSync, run: HightouchSyncRun, datajob: DataJob
    ) -> Iterable[MetadataWorkUnit]:
        dpi = DataProcessInstance(
            id=run.id,
            orchestrator=HIGHTOUCH_PLATFORM,
            cluster=self.config.env,
            template_urn=datajob.urn,
            data_platform_instance=self.config.platform_instance,
            inlets=list(datajob.inlets),
            outlets=list(datajob.outlets),
            properties=_run_properties(sync, run),
        )
        started = int(run.started_at.timestamp() * 1000)
        for mcp in dpi.generate_mcp(
            created_ts_millis=started, materialize_iolets=False
        ):
            yield mcp.as_workunit()
        for mcp in dpi.start_event_mcp(started):
            yield mcp.as_workunit()

        status = run.status.lower()
        if status in _IN_PROGRESS_RUN_STATUSES:
            return
        result = _FINISHED_RUN_RESULTS.get(status)
        if result is None:
            self.report.unknown_sync_run_statuses[status] = (
                self.report.unknown_sync_run_statuses.get(status, 0) + 1
            )
            self.report.warning(
                title="Unknown sync run status",
                message="The run is recorded as SKIPPED rather than assuming success.",
                context=f"sync_slug: {sync.slug}, run: {run.id}, status: {run.status}",
            )
            result = InstanceRunResult.SKIPPED
        finished = (
            int(run.finished_at.timestamp() * 1000) if run.finished_at else started
        )
        for mcp in dpi.end_event_mcp(
            end_timestamp_millis=finished,
            result=result,
            result_type=HIGHTOUCH_PLATFORM,
        ):
            yield mcp.as_workunit()


def _run_properties(sync: HightouchSync, run: HightouchSyncRun) -> Dict[str, str]:
    properties = {"sync_id": sync.id, "status": run.status}
    for prefix, rows in (
        ("planned_rows", run.planned_rows),
        ("successful_rows", run.successful_rows),
        ("failed_rows", run.failed_rows),
    ):
        for kind in ("added", "changed", "removed"):
            if rows and f"{kind}Count" in rows:
                properties[f"{prefix}_{kind}"] = str(rows[f"{kind}Count"])
    if run.query_size is not None:
        properties["query_size"] = str(run.query_size)
    if run.completion_ratio is not None:
        properties["completion_ratio"] = str(run.completion_ratio)
    if isinstance(run.error, str):
        properties["error"] = run.error
    elif run.error:
        properties["error"] = str(run.error.get("message") or run.error)
    return properties
