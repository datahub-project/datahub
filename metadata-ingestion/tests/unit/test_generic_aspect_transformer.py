import json
import unittest
from typing import Any, List, Optional

from datahub.emitter.aspect import JSON_CONTENT_TYPE
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import EndOfStream, PipelineContext, RecordEnvelope
from datahub.ingestion.transformer.generic_aspect_transformer import (
    GenericAspectTransformer,
)
from datahub.metadata.schema_classes import (
    DataJobSnapshotClass,
    DatasetSnapshotClass,
    GenericAspectClass,
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    StatusClass,
)
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
        proposedSnapshot=DataJobSnapshotClass(urn=entity_urn, aspects=aspects)
    )


def make_mcpw(
    entity_urn: str = "urn:li:dataset:(urn:li:dataPlatform:bigquery,example1,PROD)",
    aspect_name: str = "status",
    aspect: Any = StatusClass(removed=False),
) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        entityType=Urn.from_string(entity_urn).get_type(),
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
        entityType=Urn.from_string(entity_urn).get_type(),
        aspectName=aspect_name,
        changeType="UPSERT",
        aspect=aspect,
    )


class DummyGenericAspectTransformer(GenericAspectTransformer):
    def __init__(self):
        super().__init__()

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "DummyGenericAspectTransformer":
        return cls()

    def entity_types(self) -> List[str]:
        return ["dataset"]

    def aspect_name(self) -> str:
        return "customAspect"

    def transform_generic_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[GenericAspectClass]
    ) -> Optional[GenericAspectClass]:
        value = (
            aspect.value if aspect else json.dumps({"customAspect": 10}).encode("utf-8")
        )
        result_aspect = GenericAspectClass(
            contentType=JSON_CONTENT_TYPE,
            value=value,
        )
        return result_aspect


class TestDummyGenericAspectTransformer(unittest.TestCase):
    def test_add_generic_aspect_when_mce_received(self):
        mce_dataset = make_mce_dataset()
        mce_datajob = make_mce_datajob()
        inputs = [mce_dataset, mce_datajob, EndOfStream()]
        outputs = list(
            DummyGenericAspectTransformer().transform(
                [RecordEnvelope(i, metadata={}) for i in inputs]
            )
        )

        assert len(outputs) == len(inputs) + 1
        # Verify that the first entry is unchanged.
        assert inputs[0] == outputs[0].record
        # Verify that the second entry is unchanged.
        assert inputs[1] == outputs[1].record
        # Check the first entry generates generic aspect.
        last_event = outputs[2].record
        assert last_event.entityUrn == mce_dataset.proposedSnapshot.urn
        assert isinstance(last_event, MetadataChangeProposalClass)
        assert isinstance(last_event.aspect, GenericAspectClass)
        assert (json.loads(last_event.aspect.value))["customAspect"] == 10
        # Verify that the last entry is EndOfStream
        assert inputs[2] == outputs[3].record

    def test_add_generic_aspect_when_mcpw_received(self):
        mcpw_dataset = make_mcpw()
        mcpw_datajob = make_mcpw(
            entity_urn="urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_456)"
        )
        inputs = [mcpw_dataset, mcpw_datajob, EndOfStream()]
        outputs = list(
            DummyGenericAspectTransformer().transform(
                [RecordEnvelope(i, metadata={}) for i in inputs]
            )
        )

        assert len(outputs) == len(inputs) + 1
        # Verify that the first entry is unchanged.
        assert inputs[0] == outputs[0].record
        # Verify that the second entry is unchanged.
        assert inputs[1] == outputs[1].record
        # Check the first entry generates generic aspect.
        last_event = outputs[2].record
        assert last_event.entityUrn == mcpw_dataset.entityUrn
        assert isinstance(last_event, MetadataChangeProposalClass)
        assert isinstance(last_event.aspect, GenericAspectClass)
        assert (json.loads(last_event.aspect.value))["customAspect"] == 10
        # Verify that the last entry is EndOfStream
        assert inputs[2] == outputs[3].record

    def test_add_generic_aspect_when_mcpc_received(self):
        mcpc_dataset = make_mcpc()
        mcpc_datajob = make_mcpc(
            entity_urn="urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_456)"
        )
        inputs = [mcpc_dataset, mcpc_datajob, EndOfStream()]
        outputs = list(
            DummyGenericAspectTransformer().transform(
                [RecordEnvelope(i, metadata={}) for i in inputs]
            )
        )

        assert len(outputs) == len(inputs) + 1
        # Verify that the first entry is unchanged.
        assert inputs[0] == outputs[0].record
        # Verify that the second entry is unchanged.
        assert inputs[1] == outputs[1].record
        # Check the first entry generates generic aspect.
        last_event = outputs[2].record
        assert last_event.entityUrn == mcpc_dataset.entityUrn
        assert isinstance(last_event, MetadataChangeProposalClass)
        assert isinstance(last_event.aspect, GenericAspectClass)
        assert (json.loads(last_event.aspect.value))["customAspect"] == 10
        # Verify that the last entry is EndOfStream
        assert inputs[2] == outputs[3].record

    def test_modify_generic_aspect_when_mcpc_received(self):
        mcpc_dataset_without_custom_aspect = make_mcpc()
        mcpc_dataset_with_custom_aspect = make_mcpc(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,example1,PROD)",
            aspect_name="customAspect",
            aspect=GenericAspectClass(
                contentType=JSON_CONTENT_TYPE,
                value=json.dumps({"customAspect": 5}).encode("utf-8"),
            ),
        )
        mcpc_datajob = make_mcpc(
            entity_urn="urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_456)"
        )
        inputs = [
            mcpc_dataset_without_custom_aspect,
            mcpc_dataset_with_custom_aspect,
            mcpc_datajob,
            EndOfStream(),
        ]
        outputs = list(
            DummyGenericAspectTransformer().transform(
                [RecordEnvelope(i, metadata={}) for i in inputs]
            )
        )

        assert len(outputs) == len(inputs) + 1
        # Verify that the first entry is unchanged.
        assert inputs[0] == outputs[0].record
        # Check the second entry has original generic aspect.
        assert outputs[1].record.entityUrn == mcpc_dataset_with_custom_aspect.entityUrn
        assert isinstance(outputs[1].record, MetadataChangeProposalClass)
        assert isinstance(outputs[1].record.aspect, GenericAspectClass)
        assert (json.loads(outputs[1].record.aspect.value))["customAspect"] == 5
        # Verify that the third entry is unchanged.
        assert inputs[2] == outputs[2].record
        # Check the first entry generates generic aspect.
        last_event = outputs[3].record
        assert last_event.entityUrn == mcpc_dataset_without_custom_aspect.entityUrn
        assert isinstance(last_event, MetadataChangeProposalClass)
        assert isinstance(last_event.aspect, GenericAspectClass)
        assert (json.loads(last_event.aspect.value))["customAspect"] == 10
        # Verify that the last entry is EndOfStream
        assert inputs[3] == outputs[4].record


class DummyRemoveGenericAspectTransformer(GenericAspectTransformer):
    def __init__(self):
        super().__init__()

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "DummyRemoveGenericAspectTransformer":
        return cls()

    def entity_types(self) -> List[str]:
        return ["dataset"]

    def aspect_name(self) -> str:
        return "customAspect"

    def transform_generic_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[GenericAspectClass]
    ) -> Optional[GenericAspectClass]:
        return None


class TestDummyRemoveGenericAspectTransformer(unittest.TestCase):
    def test_remove_generic_aspect_when_mcpc_received(self):
        mcpc_dataset_without_custom_aspect = make_mcpc()
        mcpc_dataset_with_custom_aspect = make_mcpc(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,example1,PROD)",
            aspect_name="customAspect",
            aspect=GenericAspectClass(
                contentType=JSON_CONTENT_TYPE,
                value=json.dumps({"customAspect": 5}).encode("utf-8"),
            ),
        )
        mcpc_datajob = make_mcpc(
            entity_urn="urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_456)"
        )
        inputs = [
            mcpc_dataset_without_custom_aspect,
            mcpc_dataset_with_custom_aspect,
            mcpc_datajob,
            EndOfStream(),
        ]
        outputs = list(
            DummyRemoveGenericAspectTransformer().transform(
                [RecordEnvelope(i, metadata={}) for i in inputs]
            )
        )

        # Check that the second entry is removed.
        assert len(outputs) == len(inputs) - 1
        # Verify that the first entry is unchanged.
        assert inputs[0] == outputs[0].record
        # Verify that the third entry is unchanged.
        assert inputs[2] == outputs[1].record
        # Verify that the last entry is EndOfStream
        assert inputs[3] == outputs[2].record


if __name__ == "__main__":
    unittest.main()
