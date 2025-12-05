"""Tests for base transformer classes, specifically MultipleAspectTransformer."""

from typing import Iterable, List, Optional, Tuple, cast

from datahub.emitter.mce_builder import Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    MultipleAspectTransformer,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    OwnershipClass,
    TagAssociationClass,
)


class TestMultipleAspectTransformer(BaseTransformer, MultipleAspectTransformer):
    """Test implementation of MultipleAspectTransformer."""

    def __init__(self, emit_additional_aspect: bool = True):
        super().__init__()
        self.emit_additional_aspect = emit_additional_aspect

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "TestMultipleAspectTransformer":
        return cls()

    def aspect_name(self) -> str:
        return "globalTags"

    def entity_types(self) -> List[str]:
        return ["dataset"]

    def transform_aspects(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Iterable[Tuple[str, Optional[Aspect]]]:
        """Return tags plus an ownership aspect."""
        output: List[Tuple[str, Optional[Aspect]]] = [("globalTags", aspect)]

        if self.emit_additional_aspect and aspect:
            # Emit an additional ownership aspect as test
            ownership = OwnershipClass(owners=[], lastModified=None)
            output.append(("ownership", cast(Optional[Aspect], ownership)))

        return output


def test_multiple_aspect_transformer_mcpw():
    """Test that MultipleAspectTransformer emits multiple aspects from MCPW."""
    transformer = TestMultipleAspectTransformer(emit_additional_aspect=True)

    # Create an MCP with tags
    tags = GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:test")])
    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)",
        aspect=tags,
        aspectName="globalTags",
    )

    envelope = RecordEnvelope(record=mcp, metadata={})

    # Transform
    output_envelopes = list(transformer.transform([envelope]))

    # Should have 2 envelopes: globalTags + ownership
    assert len(output_envelopes) == 2

    # First should be globalTags
    assert output_envelopes[0].record.aspectName == "globalTags"
    assert isinstance(output_envelopes[0].record.aspect, GlobalTagsClass)

    # Second should be ownership
    assert output_envelopes[1].record.aspectName == "ownership"
    assert isinstance(output_envelopes[1].record.aspect, OwnershipClass)

    # Both should have same entity URN
    assert (
        output_envelopes[0].record.entityUrn
        == output_envelopes[1].record.entityUrn
        == mcp.entityUrn
    )


def test_multiple_aspect_transformer_removal():
    """Test that MultipleAspectTransformer can remove original aspect."""

    class RemovingTransformer(BaseTransformer, MultipleAspectTransformer):
        @classmethod
        def create(
            cls, config_dict: dict, ctx: PipelineContext
        ) -> "RemovingTransformer":
            return cls()

        def aspect_name(self) -> str:
            return "globalTags"

        def entity_types(self) -> List[str]:
            return ["dataset"]

        def transform_aspects(
            self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
        ) -> Iterable[Tuple[str, Optional[Aspect]]]:
            # Remove original, emit ownership instead
            ownership = OwnershipClass(owners=[], lastModified=None)
            return [
                ("globalTags", None),  # Remove
                ("ownership", cast(Optional[Aspect], ownership)),
            ]

    transformer = RemovingTransformer()

    tags = GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:test")])
    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)",
        aspect=tags,
        aspectName="globalTags",
    )

    envelope = RecordEnvelope(record=mcp, metadata={})
    output_envelopes = list(transformer.transform([envelope]))

    # Should only have ownership (globalTags removed by returning None)
    assert len(output_envelopes) == 1
    assert output_envelopes[0].record.aspectName == "ownership"


def test_multiple_aspect_transformer_single_output():
    """Test that MultipleAspectTransformer works with single output aspect."""
    transformer = TestMultipleAspectTransformer(emit_additional_aspect=False)

    tags = GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:test")])
    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)",
        aspect=tags,
        aspectName="globalTags",
    )

    envelope = RecordEnvelope(record=mcp, metadata={})
    output_envelopes = list(transformer.transform([envelope]))

    # Should have only 1 envelope
    assert len(output_envelopes) == 1
    assert output_envelopes[0].record.aspectName == "globalTags"


def test_multiple_aspect_transformer_work_unit_ids():
    """Test that work unit IDs are properly set for multiple aspects."""
    transformer = TestMultipleAspectTransformer(emit_additional_aspect=True)

    tags = GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:test")])
    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)",
        aspect=tags,
        aspectName="globalTags",
    )

    envelope = RecordEnvelope(record=mcp, metadata={"workunit_id": "original"})
    output_envelopes = list(transformer.transform([envelope]))

    # Work unit IDs should be different for different aspects
    assert len(output_envelopes) == 2

    # Both should have workunit_id metadata
    assert "workunit_id" in output_envelopes[0].metadata
    assert "workunit_id" in output_envelopes[1].metadata

    # Work unit IDs should contain aspect names
    assert "globalTags" in output_envelopes[0].metadata["workunit_id"]
    assert "ownership" in output_envelopes[1].metadata["workunit_id"]
