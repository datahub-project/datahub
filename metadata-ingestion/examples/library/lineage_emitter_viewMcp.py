import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FieldLineageDetailsClass,
    ViewPropertiesClass
)
from datahub.metadata.schema_classes import ChangeTypeClass, DataJobInputOutputClass

def fldUrn(tbl, fld):
    datasetUrn = builder.make_dataset_urn("foo", tbl)
    return f"urn:li:datasetField:({datasetUrn}, {fld})"

# Lineage of fields in a view
# view col c1 <-- unknownFunc(foo.bar2.c9, foo.bar4.c2)
# view col c2 <-- myfunc(foo.bar3.c5)

# note that the semantic of the "transform" value is contextual.
# In above example, it is regarded as some kind of UDF; but it could also be an expression etc.

viewProps = ViewPropertiesClass(False, "someLogic", "someLang", {
        fldUrn("bar", "c1"): FieldLineageDetailsClass(parentFields=[fldUrn("bar2", "c9"), fldUrn("bar4", "c2")]),
        fldUrn("bar", "c2"): FieldLineageDetailsClass(parentFields=[fldUrn("bar3","c5")], transform="myfunc")
    })

# Construct a MetadataChangeProposalWrapper object.
viewLineageMcp = MetadataChangeProposalWrapper(
    entityType="dataset",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=builder.make_dataset_urn("foo", "bar"),
    aspectName="viewProperties",
    aspect=viewProps
)

# Lineage of fields output by a job
# output col bar.c1  <-- unknownFunc(bar2.c9, bar4.c2)
# output col bar2.c2 <-- myfunc(bar3.c5)
# (Note that the dataset bar2 is both, an input as well as an output.)
# The lineage of output col bar2.c1 is unknown.
# output col bar.c2 is known to not have any parents i.e. its values are somehow created independently within this job.

dataJobInputOutput = DataJobInputOutputClass(
    inputDatasets=[
        builder.make_dataset_urn("foo", "bar2"),
        builder.make_dataset_urn("foo", "bar3"),
        builder.make_dataset_urn("foo", "bar4")], 
    outputDatasets=[
        builder.make_dataset_urn("foo", "bar"),
        builder.make_dataset_urn("foo", "bar2")], 
    inputDatajobs=None,
    inputDatasetFields=[fldUrn("bar2", "c9"), fldUrn("bar4", "c2"),fldUrn("bar3","c5"), fldUrn("bar3","c6")],
    outputDatasetFields=[fldUrn("bar", "c1"), fldUrn("bar", "c2"),fldUrn("bar2", "c1"), fldUrn("bar2", "c2")],
    fieldsLineages={
        fldUrn("bar", "c1"): FieldLineageDetailsClass([fldUrn("bar2", "c9"), fldUrn("bar4", "c2")]),
        fldUrn("bar2", "c2"): FieldLineageDetailsClass([fldUrn("bar3","c5")], transform="myfunc"),
        fldUrn("bar2", "c1"): FieldLineageDetailsClass([fldUrn("unknown","unknown")])
    })

dataJobLineageMcp = MetadataChangeProposalWrapper(
    entityType="dataJob",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=builder.make_data_job_urn("spark", "Flow1", "Task1"),
    aspectName="dataJobInputOutput",
    aspect=dataJobInputOutput
)

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

# Emit metadata!
emitter.emit_mcp(viewLineageMcp)
emitter.emit_mcp(dataJobLineageMcp)
