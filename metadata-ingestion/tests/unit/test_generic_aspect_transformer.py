import json
import unittest
from typing import Optional, List, Any

from datahub.ingestion.api.common import EndOfStream, RecordEnvelope
from datahub.ingestion.transformer.generic_aspect_transformer import GenericAspectTransformer
from datahub.metadata.schema_classes import GenericAspectClass, MetadataChangeEventClass, DatasetSnapshotClass, \
    StatusClass, DataJobSnapshotClass, MetadataChangeProposalClass


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

class DummyAddGenericAspectTransformer(GenericAspectTransformer):

    def __init__(self):
        super().__init__()

    def entity_types(self) -> List[str]:
        return ["dataset"]

    def aspect_name(self) -> str:
        return "customAspect"

    def transform_aspect(self, entity_urn: str, aspect_name: str, aspect: Optional[GenericAspectClass]) -> \
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
        transformer = DummyAddGenericAspectTransformer()
        outputs = list(
            transformer.transform([RecordEnvelope(input, metadata={}) for input in inputs])
        )
        print(outputs)
        assert len(outputs) == len(inputs) + 1

        # Verify that the first entry is unchanged.
        assert inputs[0] == outputs[0].record
        # Verify that the second entry is unchanged.
        assert inputs[1] == outputs[1].record

        # Check the first entry generates generic aspect.
        last_event = outputs[2].record
        assert isinstance(last_event, MetadataChangeProposalClass)
        assert isinstance(last_event.aspect, GenericAspectClass)
        assert (json.loads(last_event.aspect.value))["customAspect"] == 10
        assert last_event.entityUrn == inputs[0].proposedSnapshot.urn

        # Verify that the last entry is EndOfStream
        assert inputs[2] == outputs[3].record


if __name__ == '__main__':
    unittest.main()
