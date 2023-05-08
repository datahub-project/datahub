import json
import unittest
from typing import Optional, List, Any, cast

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import EndOfStream, RecordEnvelope, PipelineContext
from datahub.ingestion.transformer.generic_aspect_transformer import GenericAspectTransformer
from datahub.metadata.schema_classes import GenericAspectClass, MetadataChangeEventClass, DatasetSnapshotClass, \
    StatusClass, DataJobSnapshotClass, MetadataChangeProposalClass
from datahub.utilities.urns.urn import Urn


def make_mce_dataset(
        entity_urn: str = "urn:li:dataset:(urn:li:dataPlatform:bigquery,example1,PROD)",
        aspects: Optional[List[Any]] = None,
) -> MetadataChangeEventClass:
    if aspects is None:
        aspects = [StatusClass(removed=False)]
    return MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=entity_urn,
            aspects=aspects,
        ),
    )

def make_mce_datajob(
        entity_urn: str = "urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_456)",
        aspects: Optional[List[Any]] = None,
) -> MetadataChangeEventClass:
    if aspects is None:
        aspects = [StatusClass(removed=False)]
    return MetadataChangeEventClass(
        proposedSnapshot=DataJobSnapshotClass(
            urn=entity_urn,
            aspects= aspects
        )
    )

def make_mcpw(
        entity_urn: str = "urn:li:dataset:(urn:li:dataPlatform:bigquery,example1,PROD)",
        aspect_name: str = "status",
        aspect: Any = StatusClass(removed=False),
) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        entityType=Urn.create_from_string(entity_urn).get_type(),
        aspectName=aspect_name,
        changeType="UPSERT",
        aspect=aspect,
    )

def make_mcpc(
        entity_urn: str = "urn:li:dataset:(urn:li:dataPlatform:bigquery,example1,PROD)",
        aspect_name: str = "status",
        aspect: Any = StatusClass(removed=False),
) -> MetadataChangeProposalClass:
    return MetadataChangeProposalClass(
        entityUrn=entity_urn,
        entityType=Urn.create_from_string(entity_urn).get_type(),
        aspectName=aspect_name,
        changeType="UPSERT",
        aspect=aspect,
    )

class DummyAddGenericAspectTransformer(GenericAspectTransformer):

    def __init__(self):
        super().__init__()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "GenericAspectTransformer":
        return cls()

    def entity_types(self) -> List[str]:
        return ["dataset"]

    def aspect_name(self) -> str:
        return "customAspect"

    def transform_generic_aspect(self, entity_urn: str, aspect_name: str, aspect: Optional[GenericAspectClass]) -> \
            Optional[GenericAspectClass]:
        result_aspect = GenericAspectClass(
            contentType="application/json",
            value=json.dumps({"customAspect": 10}).encode("utf-8") ,
        )
        return result_aspect


class AddGenericAspectTransformer(unittest.TestCase):

    def test_add_generic_aspect_when_mce_received(self):
        mce_dataset = make_mce_dataset()
        mce_datajob = make_mce_datajob()
        inputs = [mce_dataset, mce_datajob, EndOfStream()]
        outputs = list(
            DummyAddGenericAspectTransformer().transform([RecordEnvelope(input, metadata={}) for input in inputs])
        )

        assert len(outputs) == len(inputs) + 1
        # Verify that the first entry is unchanged.
        assert inputs[0] == outputs[0].record
        # Verify that the second entry is unchanged.
        assert inputs[1] == outputs[1].record
        # Check the first entry generates generic aspect.
        last_event = outputs[2].record
        assert last_event.entityUrn == cast(MetadataChangeEventClass, inputs[0]).proposedSnapshot.urn
        assert isinstance(last_event, MetadataChangeProposalClass)
        assert isinstance(last_event.aspect, GenericAspectClass)
        assert (json.loads(last_event.aspect.value))["customAspect"] == 10
        # Verify that the last entry is EndOfStream
        assert inputs[2] == outputs[3].record

    def test_add_generic_aspect_when_mcpw_received(self):
        mcpw_dataset = make_mcpw()
        mcpw_datajob = make_mcpw(entity_urn="urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_456)")
        inputs = [mcpw_dataset, mcpw_datajob, EndOfStream()]
        outputs = list(
            DummyAddGenericAspectTransformer().transform([RecordEnvelope(input, metadata={}) for input in inputs])
        )

        assert len(outputs) == len(inputs) + 1
        # Verify that the first entry is unchanged.
        assert inputs[0] == outputs[0].record
        # Verify that the second entry is unchanged.
        assert inputs[1] == outputs[1].record
        # Check the first entry generates generic aspect.
        last_event = outputs[2].record
        assert last_event.entityUrn == cast(MetadataChangeProposalWrapper,inputs[0]).entityUrn
        assert isinstance(last_event, MetadataChangeProposalClass)
        assert isinstance(last_event.aspect, GenericAspectClass)
        assert (json.loads(last_event.aspect.value))["customAspect"] == 10
        # Verify that the last entry is EndOfStream
        assert inputs[2] == outputs[3].record

    def test_add_generic_aspect_when_mcpc_received(self):
        mcpc_dataset = make_mcpc()
        mcpc_datajob = make_mcpc(entity_urn="urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_456)")
        inputs = [mcpc_dataset, mcpc_datajob, EndOfStream()]
        outputs = list(
            DummyAddGenericAspectTransformer().transform([RecordEnvelope(input, metadata={}) for input in inputs])
        )

        assert len(outputs) == len(inputs) + 1
        # Verify that the first entry is unchanged.
        assert inputs[0] == outputs[0].record
        # Verify that the second entry is unchanged.
        assert inputs[1] == outputs[1].record
        # Check the first entry generates generic aspect.
        last_event = outputs[2].record
        assert last_event.entityUrn == cast(MetadataChangeProposalClass,inputs[0]).entityUrn
        assert isinstance(last_event, MetadataChangeProposalClass)
        assert isinstance(last_event.aspect, GenericAspectClass)
        assert (json.loads(last_event.aspect.value))["customAspect"] == 10
        # Verify that the last entry is EndOfStream
        assert inputs[2] == outputs[3].record


if __name__ == '__main__':
    unittest.main()
