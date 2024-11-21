import logging
from typing import Iterable, List

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    FineGrainedLineageClass,
    UpstreamLineageClass,
)

logger = logging.getLogger(__name__)


def to_job_lineage(
    job_urn: str,
    mcps: Iterable[MetadataChangeProposalWrapper],
    ignore_extra_mcps: bool = True,
) -> Iterable[MetadataChangeProposalWrapper]:
    inputDatasets: List[str] = []
    outputDatasets: List[str] = []
    fineGrainedLineages: List[FineGrainedLineageClass] = []
    for mcp in mcps:

        # TODO: Simple write operations without lineage as outputDatasets

        upstream_lineage = mcp.as_workunit().get_aspect_of_type(UpstreamLineageClass)
        if upstream_lineage is not None:

            if mcp.entityUrn and mcp.entityUrn not in outputDatasets:
                outputDatasets.append(mcp.entityUrn)

            for upstream in upstream_lineage.upstreams:
                if upstream.dataset not in inputDatasets:
                    inputDatasets.append(upstream.dataset)

            if upstream_lineage.fineGrainedLineages:
                for fineGrainedLineage in upstream_lineage.fineGrainedLineages:
                    fineGrainedLineages.append(fineGrainedLineage)

        else:
            if ignore_extra_mcps:
                logger.warning(
                    f"Ignoring mcp {mcp.entityUrn}-{mcp.aspectName} with no lineage"
                )
            else:
                yield mcp

    if inputDatasets or outputDatasets:
        # we have job lineage
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInputOutputClass(
                inputDatasets=inputDatasets,
                outputDatasets=outputDatasets,
                fineGrainedLineages=fineGrainedLineages,
            ),
        )
