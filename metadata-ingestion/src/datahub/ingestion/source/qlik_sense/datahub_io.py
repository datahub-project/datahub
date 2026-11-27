from typing import AbstractSet, Dict, Iterable, List, Optional

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.qlik_sense.constants import (
    ENTITY_CHECK_BATCH_SIZE,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import Status
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    Upstream,
)
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.utilities.urns.urn import guess_entity_type


def patch_workunits(
    patch: MetadataPatchProposal, is_primary_source: bool
) -> Iterable[MetadataWorkUnit]:
    for mcp in patch.build():
        yield MetadataWorkUnit(
            id=MetadataWorkUnit.generate_workunit_id(mcp),
            mcp_raw=mcp,
            is_primary_source=is_primary_source,
        )


def patch_upstreams(
    dataset_urn: str,
    upstream_urns: List[str],
    lineage_type: str,
    fine_grained: Optional[List[FineGrainedLineage]] = None,
    superseded: AbstractSet[str] = frozenset(),
) -> Iterable[MetadataWorkUnit]:
    """Adds lineage without replacing the edges an earlier run wrote, except
    edges to `superseded`, other names of linked files, which can't be right."""
    if not upstream_urns and not fine_grained:
        return
    patch = DatasetPatchBuilder(dataset_urn)
    for urn in upstream_urns:
        patch.add_upstream_lineage(Upstream(dataset=urn, type=lineage_type))
    for lineage in fine_grained or []:
        patch.add_fine_grained_lineage(lineage)
    for name in sorted(superseded - set(upstream_urns)):
        patch.remove_upstream_lineage(name)
    for mcp in patch.build():
        yield MetadataWorkUnit(
            id=MetadataWorkUnit.generate_workunit_id(mcp), mcp_raw=mcp
        )


def entities_present(graph: DataHubGraph, urns: List[str]) -> bool:
    """Whether every entity the app's last read produced is in DataHub and
    not soft-deleted, which also catches entities the sink failed to write
    and hard-deleted ones. Entities stale removal deleted after they left
    Qlik are not in the checkpoint."""
    by_type: Dict[str, List[str]] = {}
    for urn in urns:
        by_type.setdefault(guess_entity_type(urn), []).append(urn)
    for entity_type, typed_urns in by_type.items():
        for start in range(0, len(typed_urns), ENTITY_CHECK_BATCH_SIZE):
            batch = typed_urns[start : start + ENTITY_CHECK_BATCH_SIZE]
            entities = graph.get_entities(
                entity_type, batch, aspects=[Status.ASPECT_NAME]
            )
            for urn in batch:
                status = entities.get(urn, {}).get(Status.ASPECT_NAME)
                if status is None or getattr(status[0], "removed", True):
                    return False
    return True
