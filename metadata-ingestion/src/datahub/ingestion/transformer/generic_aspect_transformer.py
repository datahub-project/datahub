from abc import ABCMeta, abstractmethod
from typing import Iterable, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import RecordEnvelope, EndOfStream
from datahub.ingestion.transformer.base_transformer import BaseTransformer, SingleAspectTransformer
from datahub.metadata.schema_classes import MetadataChangeEventClass, MetadataChangeProposalClass, \
    GenericAspectClass
from datahub.utilities.urns.urn import Urn


class GenericAspectTransformer(BaseTransformer, SingleAspectTransformer, metaclass=ABCMeta):
    """Transformer that does transform custom aspects using GenericAspectClass."""

    def __init__(self):
        super().__init__()

    @abstractmethod
    def transform_aspect(
            self, entity_urn: str, aspect_name: str, aspect: Optional[GenericAspectClass]
    ) -> Optional[GenericAspectClass]:
        """Implement this method to transform a single custom aspect for an entity.
        The purpose of this abstract method is to reinforce the use of GenericAspectClass."""
        pass

    def transform(
            self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        """
        This method overrides the original one from BaseTransformer in order to support
        custom aspects. They need to be upserted with MetadataChangeProposalClass instead of
        MetadataChangeProposalWrapper used at the original method.
        """
        for envelope in record_envelopes:
            if not self._should_process(envelope.record):
                pass
            elif isinstance(envelope.record, MetadataChangeEventClass):
                self._record_mce(envelope.record)
            elif isinstance(envelope.record, MetadataChangeProposalWrapper):
                self._record_mcp(envelope.record)
            # elif isinstance(envelope.record, MetadataChangeProposalClass):
            #     return_envelope = self._transform_or_record_mcp(envelope)
            #     if return_envelope is None:
            #         continue
            #     else:
            #         envelope = return_envelope
            elif isinstance(envelope.record, EndOfStream) and isinstance(
                    self, SingleAspectTransformer
            ):
                # walk through state and call transform for any unprocessed entities
                for urn, state in self.entity_map.items():
                    if "seen" in state:
                        # call transform on this entity_urn
                        last_seen_mcp = state["seen"].get("mcp")
                        last_seen_mce_system_metadata = state["seen"].get("mce")

                        transformed_aspect = self.transform_aspect(
                            entity_urn=urn,
                            aspect_name=self.aspect_name(),
                            aspect=last_seen_mcp.aspect
                            if last_seen_mcp
                               and last_seen_mcp.aspectName == self.aspect_name()
                            else None,
                        )
                        if transformed_aspect:
                            # for end of stream records, we modify the workunit-id
                            structured_urn = Urn.create_from_string(urn)
                            simple_name = "-".join(structured_urn.get_entity_id())
                            record_metadata = envelope.metadata.copy()
                            record_metadata.update(
                                {
                                    "workunit_id": f"txform-{simple_name}-{self.aspect_name()}"
                                }
                            )
                            yield RecordEnvelope(
                                record=MetadataChangeProposalClass(
                                    entityType=structured_urn.get_type(),
                                    entityUrn=urn,
                                    changeType="UPSERT",
                                    aspectName=self.aspect_name(),
                                    aspect=transformed_aspect,
                                    systemMetadata=last_seen_mcp.systemMetadata
                                    if last_seen_mcp
                                    else last_seen_mce_system_metadata,
                                ),
                                metadata=record_metadata,
                            )
                    self._mark_processed(urn)
            yield envelope
