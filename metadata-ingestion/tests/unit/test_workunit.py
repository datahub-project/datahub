import json

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    ContainerClass,
    DatasetSnapshotClass,
    GenericAspectClass,
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    StatusClass,
    UpstreamLineageClass,
)


def test_get_aspects_of_type_mcp():
    aspect = StatusClass(False)
    wu = MetadataChangeProposalWrapper(
        entityUrn="urn:li:container:asdf", aspect=aspect
    ).as_workunit()
    assert wu.get_aspects_of_type(StatusClass) == [aspect]
    assert wu.get_aspects_of_type(ContainerClass) == []


def test_get_aspects_of_type_mce():
    status_aspect = StatusClass(False)
    status_aspect_2 = StatusClass(True)
    lineage_aspect = UpstreamLineageClass(upstreams=[])
    mce = MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn="urn:li:dataset:asdf",
            aspects=[status_aspect, lineage_aspect, status_aspect_2],
        )
    )
    wu = MetadataWorkUnit(id="id", mce=mce)
    assert wu.get_aspects_of_type(StatusClass) == [status_aspect, status_aspect_2]
    assert wu.get_aspects_of_type(UpstreamLineageClass) == [lineage_aspect]
    assert wu.get_aspects_of_type(ContainerClass) == []


def test_get_aspects_of_type_mcpc():
    aspect = StatusClass(False)
    mcpc = MetadataChangeProposalClass(
        entityUrn="urn:li:container:asdf",
        entityType="container",
        changeType=ChangeTypeClass.UPSERT,
        aspectName=StatusClass.ASPECT_NAME,
        aspect=GenericAspectClass(
            value=json.dumps(aspect.to_obj()).encode(),
            contentType="application/json",
        ),
    )
    wu = MetadataWorkUnit(id="id", mcp_raw=mcpc)
    assert wu.get_aspects_of_type(StatusClass) == [aspect]
    assert wu.get_aspects_of_type(ContainerClass) == []

    # Failure scenarios
    mcpc = MetadataChangeProposalClass(
        entityUrn="urn:li:container:asdf",
        entityType="container",
        changeType=ChangeTypeClass.UPSERT,
        aspectName="not status",
        aspect=GenericAspectClass(
            value=json.dumps(aspect.to_obj()).encode(),
            contentType="application/json",
        ),
    )
    wu = MetadataWorkUnit(id="id", mcp_raw=mcpc)
    assert wu.get_aspects_of_type(StatusClass) == []

    mcpc = MetadataChangeProposalClass(
        entityUrn="urn:li:container:asdf",
        entityType="container",
        changeType=ChangeTypeClass.PATCH,
        aspectName=StatusClass.ASPECT_NAME,
        aspect=GenericAspectClass(
            value=json.dumps({"not_status": True}).encode(),
            contentType="application/json-patch+json",
        ),
    )
    wu = MetadataWorkUnit(id="id", mcp_raw=mcpc)
    assert wu.get_aspects_of_type(StatusClass) == []

    mcpc = MetadataChangeProposalClass(
        entityUrn="urn:li:container:asdf",
        entityType="container",
        changeType=ChangeTypeClass.UPSERT,
        aspectName=StatusClass.ASPECT_NAME,
        aspect=GenericAspectClass(
            value=(json.dumps(aspect.to_obj()) + "aaa").encode(),
            contentType="application/json",
        ),
    )
    wu = MetadataWorkUnit(id="id", mcp_raw=mcpc)
    assert wu.get_aspects_of_type(StatusClass) == []

    mcpc = MetadataChangeProposalClass(
        entityUrn="urn:li:container:asdf",
        entityType="container",
        changeType=ChangeTypeClass.UPSERT,
        aspectName=StatusClass.ASPECT_NAME,
        aspect=GenericAspectClass(
            value='{"ß": 2}'.encode("latin_1"),
            contentType="application/json",
        ),
    )
    wu = MetadataWorkUnit(id="id", mcp_raw=mcpc)
    assert wu.get_aspects_of_type(StatusClass) == []
