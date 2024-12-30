import logging
from typing import Iterable, List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    FineGrainedLineageClass,
    UpstreamLineageClass,
)

logger = logging.getLogger(__name__)


def to_datajob_input_output(
    *, mcps: Iterable[MetadataChangeProposalWrapper], ignore_extra_mcps: bool = True
) -> Optional[DataJobInputOutputClass]:
    inputDatasets: List[str] = []
    outputDatasets: List[str] = []
    fineGrainedLineages: List[FineGrainedLineageClass] = []
    for mcp in mcps:
        # TODO: Represent simple write operations without lineage as outputDatasets.

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

        elif ignore_extra_mcps:
            pass
        else:
            raise ValueError(
                f"Expected an upstreamLineage aspect, got {mcp.aspectName} for {mcp.entityUrn}"
            )

    if not inputDatasets and not outputDatasets:
        return None

    return DataJobInputOutputClass(
        inputDatasets=inputDatasets,
        outputDatasets=outputDatasets,
        fineGrainedLineages=fineGrainedLineages,
    )
