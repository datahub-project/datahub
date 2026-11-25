from typing import Dict, Iterable, List, Optional, Set

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.incremental_lineage_helper import (
    convert_upstream_lineage_to_patch,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.qlik_sense.constants import (
    DATAHUB_LINEAGE_READ_FAILED_TITLE,
    ENTITY_CHECK_BATCH_SIZE,
)
from datahub.ingestion.source.qlik_sense.script_lineage import ScriptTableLineage
from datahub.ingestion.source.qlik_sense.source.incremental import IncrementalMixin
from datahub.ingestion.source.qlik_sense.source_utils import (
    _bounded_list,
    _schema_field_parent,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)


class ColumnLineageMixin(IncrementalMixin):
    @staticmethod
    def _script_column_lineage(
        dataset_urn: str, table: ScriptTableLineage
    ) -> Optional[List[FineGrainedLineage]]:
        return [
            FineGrainedLineage(
                upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                upstreams=upstreams,
                downstreamType=FineGrainedLineageDownstreamType.FIELD,
                downstreams=[builder.make_schema_field_urn(dataset_urn, column)],
            )
            for column, upstreams in table.column_upstreams.items()
            if upstreams
        ] or None

    def _column_lineage(
        self, lineages: Optional[List[FineGrainedLineage]]
    ) -> Optional[List[FineGrainedLineage]]:
        return (lineages or None) if self.config.include_column_lineage else None

    @staticmethod
    def _patch_upstreams(
        dataset_urn: str,
        upstream_urns: List[str],
        lineage_type: str,
        fine_grained: Optional[List[FineGrainedLineage]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """Adds lineage without replacing the edges an earlier run wrote."""
        if not upstream_urns and not fine_grained:
            return
        yield convert_upstream_lineage_to_patch(
            dataset_urn,
            UpstreamLineage(
                upstreams=[
                    Upstream(dataset=urn, type=lineage_type) for urn in upstream_urns
                ],
                fineGrainedLineages=fine_grained or None,
            ),
            None,
        )

    def _current_upstreams_batch(
        self, dataset_urns: List[str]
    ) -> Dict[str, Optional[UpstreamLineage]]:
        """DataHub's current lineage of each dataset; None for those that could
        not be read, whose lineage is then only added to."""
        graph = self.ctx.graph
        if graph is None:
            return {urn: self._current_upstreams(urn) for urn in dataset_urns}
        current: Dict[str, Optional[UpstreamLineage]] = {}
        for start in range(0, len(dataset_urns), ENTITY_CHECK_BATCH_SIZE):
            batch = dataset_urns[start : start + ENTITY_CHECK_BATCH_SIZE]
            try:
                entities = graph.get_entities(
                    "dataset", batch, aspects=[UpstreamLineage.ASPECT_NAME]
                )
            except Exception as e:
                self.reporter.warning(
                    title=DATAHUB_LINEAGE_READ_FAILED_TITLE,
                    message="The lineage of these datasets from apps that store "
                    "into them is only added to.",
                    context=_bounded_list(batch),
                    exc=e,
                )
                current.update(dict.fromkeys(batch))
                continue
            for urn in batch:
                found = entities.get(urn, {}).get(UpstreamLineage.ASPECT_NAME)
                current[urn] = (
                    found[0]
                    if found and isinstance(found[0], UpstreamLineage)
                    else UpstreamLineage(upstreams=[])
                )
        return current

    def _current_upstreams(self, dataset_urn: str) -> Optional[UpstreamLineage]:
        graph = self.ctx.graph
        if graph is None:
            if not self._reported_no_graph:
                self._reported_no_graph = True
                self.reporter.info(
                    title="Qlik dataset lineage only added to",
                    message="Without a DataHub connection, such as with a file "
                    "sink, edges from app tables that no longer store into a "
                    "dataset can't be found, so none are removed.",
                )
            return None
        try:
            return graph.get_aspect(dataset_urn, UpstreamLineage) or UpstreamLineage(
                upstreams=[]
            )
        except Exception as e:
            self.reporter.warning(
                title=DATAHUB_LINEAGE_READ_FAILED_TITLE,
                message="The dataset's lineage from apps that store into it is "
                "only added to.",
                context=dataset_urn,
                exc=e,
            )
            return None

    @staticmethod
    def _stale_field_lineage(
        current: List[FineGrainedLineage],
        stored: List[FineGrainedLineage],
        owned: Set[str],
    ) -> List[FineGrainedLineage]:
        """The column edges from owned tables that this run no longer stores,
        each as a lineage holding only those upstream fields."""
        stored_edges = {
            (downstream, upstream)
            for lineage in stored
            for downstream in lineage.downstreams or []
            for upstream in lineage.upstreams or []
        }
        stale = []
        for lineage in current:
            # A column edge can only be removed by patch through its single
            # downstream field; one another tool wrote with none or several
            # is not this connector's.
            if len(lineage.downstreams or []) != 1:
                continue
            upstreams = [
                upstream
                for upstream in lineage.upstreams or []
                if _schema_field_parent(upstream) in owned
                and not any(
                    (downstream, upstream) in stored_edges
                    for downstream in lineage.downstreams or []
                )
            ]
            if upstreams:
                stale_lineage = FineGrainedLineage.from_obj(lineage.to_obj())
                stale_lineage.upstreams = upstreams
                stale.append(stale_lineage)
        return stale
