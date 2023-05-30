import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)


def datasetUrn(tbl):
    return builder.make_dataset_urn("postgres", tbl)


def fldUrn(tbl, fld):
    return builder.make_schema_field_urn(datasetUrn(tbl), fld)


# Lineage of fields in a dataset
# c1      <-- unknownFunc(bar2.c1, bar4.c1)
# c2      <-- myfunc(bar3.c2)
# {c3,c4} <-- unknownFunc(bar2.c2, bar2.c3, bar3.c1)
# c5      <-- unknownFunc(bar3)
# {c6,c7} <-- unknownFunc(bar4)

# note that the semantic of the "transformOperation" value is contextual.
# In above example, it is regarded as some kind of UDF; but it could also be an expression etc.

fineGrainedLineages = [
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
        upstreams=[fldUrn("bar2", "c1"), fldUrn("bar4", "c1")],
        downstreamType=FineGrainedLineageDownstreamType.FIELD,
        downstreams=[fldUrn("bar", "c1")],
    ),
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
        upstreams=[fldUrn("bar3", "c2")],
        downstreamType=FineGrainedLineageDownstreamType.FIELD,
        downstreams=[fldUrn("bar", "c2")],
        confidenceScore=0.8,
        transformOperation="myfunc",
    ),
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
        upstreams=[fldUrn("bar2", "c2"), fldUrn("bar2", "c3"), fldUrn("bar3", "c1")],
        downstreamType=FineGrainedLineageDownstreamType.FIELD_SET,
        downstreams=[fldUrn("bar", "c3"), fldUrn("bar", "c4")],
        confidenceScore=0.7,
    ),
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.DATASET,
        upstreams=[datasetUrn("bar3")],
        downstreamType=FineGrainedLineageDownstreamType.FIELD,
        downstreams=[fldUrn("bar", "c5")],
    ),
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.DATASET,
        upstreams=[datasetUrn("bar4")],
        downstreamType=FineGrainedLineageDownstreamType.FIELD_SET,
        downstreams=[fldUrn("bar", "c6"), fldUrn("bar", "c7")],
    ),
]


# this is just to check if any conflicts with existing Upstream, particularly the DownstreamOf relationship
upstream = Upstream(dataset=datasetUrn("bar2"), type=DatasetLineageType.TRANSFORMED)

fieldLineages = UpstreamLineage(
    upstreams=[upstream], fineGrainedLineages=fineGrainedLineages
)

lineageMcp = MetadataChangeProposalWrapper(
    entityUrn=datasetUrn("bar"),
    aspect=fieldLineages,
)

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

# Emit metadata!
emitter.emit_mcp(lineageMcp)
