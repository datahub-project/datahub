from typing import Dict, List, Optional

from datahub.ingestion.source.qlik_sense.constants import (
    DATAHUB_LINEAGE_READ_FAILED_TITLE,
    ENTITY_CHECK_BATCH_SIZE,
)
from datahub.ingestion.source.qlik_sense.report_text import (
    bounded_list,
)
from datahub.ingestion.source.qlik_sense.source.incremental import IncrementalMixin
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    UpstreamLineage,
)


class ColumnLineageMixin(IncrementalMixin):
    def _column_lineage(
        self, lineages: Optional[List[FineGrainedLineage]]
    ) -> Optional[List[FineGrainedLineage]]:
        return (lineages or None) if self.config.include_column_lineage else None

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
                    context=bounded_list(batch),
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
