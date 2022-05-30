import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
)
from datahub.metadata.schema_classes import ChangeTypeClass, DataJobInputOutputClass


def datasetUrn(tbl):
    return builder.make_dataset_urn("postgres", tbl)


def fldUrn(tbl, fld):
    return builder.make_schema_field_urn(datasetUrn(tbl), fld)


# Lineage of fields output by a job
# bar.c1          <-- unknownFunc(bar2.c1, bar4.c1)
# bar.c2          <-- myfunc(bar3.c2)
# {bar.c3,bar.c4} <-- unknownFunc(bar2.c2, bar2.c3, bar3.c1)
# bar.c5          <-- unknownFunc(bar3)
# {bar.c6,bar.c7} <-- unknownFunc(bar4)
# bar2.c9 has no upstream i.e. its values are somehow created independently within this job.

# Note that the semantic of the "transformOperation" value is contextual.
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
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.NONE,
        upstreams=[],
        downstreamType=FineGrainedLineageDownstreamType.FIELD,
        downstreams=[fldUrn("bar2", "c9")],
    ),
]

# The lineage of output col bar.c9 is unknown. So there is no lineage for it above.
# Note that bar2 is an input as well as an output dataset, but some fields are inputs while other fields are outputs.

dataJobInputOutput = DataJobInputOutputClass(
    inputDatasets=[datasetUrn("bar2"), datasetUrn("bar3"), datasetUrn("bar4")],
    outputDatasets=[datasetUrn("bar"), datasetUrn("bar2")],
    inputDatajobs=None,
    inputDatasetFields=[
        fldUrn("bar2", "c1"),
        fldUrn("bar2", "c2"),
        fldUrn("bar2", "c3"),
        fldUrn("bar3", "c1"),
        fldUrn("bar3", "c2"),
        fldUrn("bar4", "c1"),
    ],
    outputDatasetFields=[
        fldUrn("bar", "c1"),
        fldUrn("bar", "c2"),
        fldUrn("bar", "c3"),
        fldUrn("bar", "c4"),
        fldUrn("bar", "c5"),
        fldUrn("bar", "c6"),
        fldUrn("bar", "c7"),
        fldUrn("bar", "c9"),
        fldUrn("bar2", "c9"),
    ],
    fineGrainedLineages=fineGrainedLineages,
)

dataJobLineageMcp = MetadataChangeProposalWrapper(
    entityType="dataJob",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=builder.make_data_job_urn("spark", "Flow1", "Task1"),
    aspectName="dataJobInputOutput",
    aspect=dataJobInputOutput,
)

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

# Emit metadata!
emitter.emit_mcp(dataJobLineageMcp)
