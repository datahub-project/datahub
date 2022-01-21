import datahub.emitter.mce_builder as builder
import json

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage   
)
from datahub.metadata.schema_classes import ChangeTypeClass, DataJobInputOutputClass

def datasetUrn(tbl):
    return builder.make_dataset_urn("postgres", tbl)

def fldUrn(tbl, fld):
    return f"urn:li:schemaField:({datasetUrn(tbl)}, {fld})"

# Lineage of fields in a dataset (view)
# c1      <-- unknownFunc(bar2.c1, bar4.c1)
# c2      <-- myfunc(bar3.c2)
# {c3,c4} <-- unknownFunc(bar2.c2, bar2.c3, bar3.c1)
# c5      <-- unknownFunc(bar3)
# {c6,c7} <-- unknownFunc(bar4)

# note that the semantic of the "transformOperation" value is contextual.
# In above example, it is regarded as some kind of UDF; but it could also be an expression etc.

fineGrainedLineages=[
        FineGrainedLineage(
            upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
            upstreams=[fldUrn("bar2", "c1"), fldUrn("bar4", "c1")],
            downstreamType=FineGrainedLineageDownstreamType.FIELD,
            downstreams=[fldUrn("bar", "c1")]),

        FineGrainedLineage(
            upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
            upstreams=[fldUrn("bar3","c2")],
            downstreamType=FineGrainedLineageDownstreamType.FIELD,
            downstreams=[fldUrn("bar", "c2")], 
            confidenceScore = 0.8, transformOperation="myfunc"),

        FineGrainedLineage(
            upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
            upstreams=[fldUrn("bar2","c2"), fldUrn("bar2","c3"), fldUrn("bar3","c1")],
            downstreamType=FineGrainedLineageDownstreamType.FIELD_SET, 
            downstreams=[fldUrn("bar", "c3"), fldUrn("bar", "c4")], 
            confidenceScore = 0.7),

        FineGrainedLineage(
            upstreamType=FineGrainedLineageUpstreamType.DATASET,
            upstreams=[datasetUrn("bar3")],
            downstreamType=FineGrainedLineageDownstreamType.FIELD, 
            downstreams=[fldUrn("bar", "c5")]),

        FineGrainedLineage(
            upstreamType=FineGrainedLineageUpstreamType.DATASET,
            upstreams=[datasetUrn("bar4")],
            downstreamType=FineGrainedLineageDownstreamType.FIELD_SET, 
            downstreams=[fldUrn("bar", "c6"), fldUrn("bar", "c7")])            
    ]


# this is just to check if any conflicts with existing Upstream, particularly the DownstreamOf relationship
upstream = Upstream(dataset=datasetUrn("bar2"), type=DatasetLineageType.TRANSFORMED)

fieldLineages = UpstreamLineage(upstreams=[upstream], fineGrainedLineages=fineGrainedLineages)

viewLineageMcp = MetadataChangeProposalWrapper(
    entityType="dataset",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=datasetUrn("bar"),
    aspectName="upstreamLineage",
    aspect=fieldLineages
)

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

# Emit metadata!
emitter.emit_mcp(viewLineageMcp)

# Lineage of fields output by a job
# The lineages are primarily the same as the above example.
#
# In addition to the earlier lineages, 
# 1. the lineage of output col bar.c9 is unknown. So there is no lineage for it.
# 2. output col bar2.c9 is known to not have any parents i.e. its values are somehow created independently within this job.

# Note that bar2 is an input as well as an output dataset, but some fields are inputs while other fields are outputs.

fineGrainedLineages.append(
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.NONE,
        upstreams=[],
        downstreamType=FineGrainedLineageDownstreamType.FIELD, 
        downstreams=[fldUrn("bar2", "c9")])        
)

dataJobInputOutput = DataJobInputOutputClass(
    inputDatasets=[datasetUrn("bar2"), datasetUrn("bar3"), datasetUrn("bar4")], 
    outputDatasets=[datasetUrn("bar"), datasetUrn("bar2")], 
    inputDatajobs=None,
    inputDatasetFields=[fldUrn("bar2","c1"), fldUrn("bar2","c2"), fldUrn("bar2","c3"), 
                        fldUrn("bar3","c1"), fldUrn("bar3","c2"), fldUrn("bar4","c1")],
    outputDatasetFields=[fldUrn("bar", "c1"), fldUrn("bar", "c2"),fldUrn("bar", "c3"), 
                         fldUrn("bar", "c4"), fldUrn("bar", "c5"),fldUrn("bar", "c6"), 
                         fldUrn("bar", "c7"), fldUrn("bar", "c9"),
                         fldUrn("bar2", "c9")],
    fineGrainedLineages=fineGrainedLineages
    )

dataJobLineageMcp = MetadataChangeProposalWrapper(
    entityType="dataJob",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=builder.make_data_job_urn("spark", "Flow1", "Task1"),
    aspectName="dataJobInputOutput",
    aspect=dataJobInputOutput
)
emitter.emit_mcp(dataJobLineageMcp)