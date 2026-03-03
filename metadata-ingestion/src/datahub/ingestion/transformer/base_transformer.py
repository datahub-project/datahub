import logging
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import datahub.emitter.mce_builder as builder
from datahub.emitter.aspect import ASPECT_MAP
from datahub.emitter.mce_builder import Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import ControlRecord, EndOfStream, RecordEnvelope
from datahub.ingestion.api.transform import Transformer
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
)
from datahub.utilities.urns.urn import Urn, guess_entity_type

log = logging.getLogger(__name__)


def _update_work_unit_id(
    envelope: RecordEnvelope, urn: str, aspect_name: str
) -> Dict[Any, Any]:
    structured_urn = Urn.from_string(urn)
    simple_name = "-".join(structured_urn.entity_ids)
    record_metadata = envelope.metadata.copy()
    record_metadata.update({"workunit_id": f"txform-{simple_name}-{aspect_name}"})
    return record_metadata


class HandleEndOfStreamTransformer:
    def handle_end_of_stream(
        self,
    ) -> Sequence[Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]]:
        return []


class LegacyMCETransformer(
    Transformer, HandleEndOfStreamTransformer, metaclass=ABCMeta
):
    @abstractmethod
    def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
        pass


class SingleAspectTransformer(HandleEndOfStreamTransformer, metaclass=ABCMeta):
    @abstractmethod
    def aspect_name(self) -> str:
        """Implement this method to specify a single aspect that the transformer is interested in subscribing to. No default provided."""
        pass

    @abstractmethod
    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        """Implement this method to transform a single aspect for an entity.
        param: entity_urn: the entity that is being processed
        param: aspect_name: the aspect name corresponding to the subscription
        param: aspect: an optional aspect corresponding to the aspect name that the transformer is interested in. Empty if no aspect with this name was produced by the underlying connector
        """
        pass


class MultipleAspectTransformer(HandleEndOfStreamTransformer, metaclass=ABCMeta):
    """
    Transformer that transforms one input aspect into multiple output aspects.

    Unlike SingleAspectTransformer where input and output aspect types must match,
    this allows emitting additional aspects of different types.

    Example: Transform globalTags into both globalTags and structuredProperties.
    """

    @abstractmethod
    def aspect_name(self) -> str:
        """
        Specify the INPUT aspect this transformer subscribes to.
        Example: "globalTags"
        """
        pass

    @abstractmethod
    def transform_aspects(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Iterable[Tuple[str, Optional[Aspect]]]:
        """
        Transform one input aspect into multiple output aspects.

        Args:
            entity_urn: The entity being processed
            aspect_name: The input aspect name (from aspect_name() method)
            aspect: The input aspect value (None if not present)

        Returns:
            Iterable of (output_aspect_name, output_aspect) tuples.
            - First tuple conventionally contains the transformed input aspect
            - Additional tuples contain new aspects to emit
            - Use None as aspect value to remove that aspect

        Example:
            return [
                ("globalTags", modified_tags),      # Same type as input
                ("structuredProperties", props),    # Different type
            ]
        """
        pass


class BaseTransformer(Transformer, metaclass=ABCMeta):
    """
    Base transformer providing common functionality for all transformers.

    Architecture:
        BaseTransformer uses a mixin pattern where concrete transformers must inherit from
        both BaseTransformer and one of the allowed mixins:
        - LegacyMCETransformer: For full MCE transformation
        - SingleAspectTransformer: For transforming a single aspect type
        - MultipleAspectTransformer: For transforming one aspect into multiple aspects

        Example:
            class MyTransformer(BaseTransformer, SingleAspectTransformer):
                def aspect_name(self) -> str:
                    return "globalTags"
                def transform_aspect(self, ...) -> ...:
                    # transformation logic

        The mixins define the interface (abstract methods) while BaseTransformer provides
        the implementation helpers that dispatch based on which mixin is present. This allows
        type-safe polymorphic behavior with runtime assertions validating the class hierarchy.
    """

    allowed_mixins = [
        LegacyMCETransformer,
        SingleAspectTransformer,
        MultipleAspectTransformer,
    ]

    @abstractmethod
    def entity_types(self) -> List[str]:
        """Implement this method to specify which entity types the transformer is interested in subscribing to. Defaults to ALL (encoded as "*")"""
        return ["*"]

    def __init__(self):
        self.entity_map: Dict[str, Dict[str, Any]] = {}
        mixedin = False
        for mixin in [
            LegacyMCETransformer,
            SingleAspectTransformer,
            MultipleAspectTransformer,
        ]:
            mixedin = mixedin or isinstance(self, mixin)
        if not mixedin:
            assert (
                f"Class does not implement one of required traits {self.allowed_mixins}"
            )

    def _should_process(
        self,
        record: Union[
            MetadataChangeEventClass,
            MetadataChangeProposalWrapper,
            MetadataChangeProposalClass,
            ControlRecord,
        ],
    ) -> bool:
        if isinstance(record, ControlRecord):
            # all control events should be processed
            return True

        entity_types = self.entity_types()
        if "*" in entity_types:
            return True
        if isinstance(record, MetadataChangeEventClass):
            entity_type = guess_entity_type(record.proposedSnapshot.urn)
            return entity_type in entity_types
        elif isinstance(
            record, (MetadataChangeProposalWrapper, MetadataChangeProposalClass)
        ):
            return record.entityType in entity_types

        # default to process everything that is not caught by above checks
        return True

    def _record_mce(self, mce: MetadataChangeEventClass) -> None:
        record_entry = self.entity_map.get(mce.proposedSnapshot.urn, {"seen": {}})
        if "seen" in record_entry:
            # we just record the system metadata field from the mce, since we might need it later
            record_entry["seen"]["mce"] = mce.systemMetadata
            self.entity_map[mce.proposedSnapshot.urn] = record_entry

    def _record_mcp(
        self, mcp: Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]
    ) -> None:
        assert mcp.entityUrn
        record_entry = self.entity_map.get(mcp.entityUrn, {"seen": {}})
        if "seen" in record_entry and "mcp" not in record_entry["seen"]:
            # only record the first mcp seen
            record_entry["seen"]["mcp"] = mcp
        self.entity_map[mcp.entityUrn] = record_entry

    def _mark_processed(self, entity_urn: str) -> None:
        self.entity_map[entity_urn] = {"processed": True}

    def _transform_or_record_mce(
        self,
        envelope: RecordEnvelope[MetadataChangeEventClass],
    ) -> RecordEnvelope[MetadataChangeEventClass]:
        mce: MetadataChangeEventClass = envelope.record
        if mce.proposedSnapshot:
            self._record_mce(mce)
        if isinstance(self, SingleAspectTransformer):
            aspect_type = ASPECT_MAP[self.aspect_name()]

            # If we find a type corresponding to the aspect name we look for it in the mce
            # It's possible that the aspect is supported by the entity but not in the MCE
            # snapshot union. In those cases, we just want to record the urn as seen.
            supports_aspect = builder.can_add_aspect(mce, aspect_type)
            if supports_aspect:
                old_aspect = builder.get_aspect_if_available(
                    mce,
                    aspect_type,
                )
                if old_aspect is not None:
                    # TRICKY: If the aspect is not present in the MCE, it might still show up in a
                    # subsequent MCP. As such, we _only_ mark the urn as processed if we actually
                    # find the aspect already in the MCE.

                    transformed_aspect = self.transform_aspect(
                        entity_urn=mce.proposedSnapshot.urn,
                        aspect_name=self.aspect_name(),
                        aspect=old_aspect,
                    )

                    # If transformed_aspect is None, this will remove the aspect.
                    builder.set_aspect(
                        mce,
                        aspect_type=aspect_type,
                        aspect=transformed_aspect,
                    )

                    envelope.record = mce
                    self._mark_processed(mce.proposedSnapshot.urn)
        elif isinstance(self, MultipleAspectTransformer):
            input_aspect_type = ASPECT_MAP.get(self.aspect_name())
            if input_aspect_type and builder.can_add_aspect(mce, input_aspect_type):
                old_aspect = builder.get_aspect_if_available(mce, input_aspect_type)
                if old_aspect is not None:
                    for output_aspect_name, output_aspect in self.transform_aspects(
                        entity_urn=mce.proposedSnapshot.urn,
                        aspect_name=self.aspect_name(),
                        aspect=old_aspect,
                    ):
                        output_aspect_type = ASPECT_MAP.get(output_aspect_name)
                        if output_aspect_type and builder.can_add_aspect(
                            mce, output_aspect_type
                        ):
                            builder.set_aspect(
                                mce,
                                aspect_type=output_aspect_type,
                                aspect=output_aspect,
                            )
                    envelope.record = mce
                    self._mark_processed(mce.proposedSnapshot.urn)
        elif isinstance(self, LegacyMCETransformer):
            # we pass down the full MCE
            envelope.record = self.transform_one(mce)
            self._mark_processed(mce.proposedSnapshot.urn)

        return envelope

    def _transform_or_record_mcpw(
        self,
        envelope: RecordEnvelope[MetadataChangeProposalWrapper],
    ) -> Optional[RecordEnvelope[MetadataChangeProposalWrapper]]:
        """
        Handle SingleAspectTransformer processing for MetadataChangeProposalWrapper records.

        This helper method is defined on BaseTransformer but calls methods from the
        SingleAspectTransformer mixin. The runtime assertion ensures type safety while
        allowing the mixin pattern to work correctly.

        Args:
            envelope: Record envelope containing an MCPW

        Returns:
            The envelope with transformed aspect, or None if aspect should be dropped

        Note:
            This method requires self to be an instance of SingleAspectTransformer,
            which is validated via runtime assertion.
        """
        assert envelope.record.entityUrn
        assert isinstance(self, SingleAspectTransformer)
        if envelope.record.aspectName == self.aspect_name() and envelope.record.aspect:
            # we have a match on the aspect name, call the specific transform function
            transformed_aspect = self.transform_aspect(
                entity_urn=envelope.record.entityUrn,
                aspect_name=envelope.record.aspectName,
                aspect=envelope.record.aspect,
            )
            self._mark_processed(envelope.record.entityUrn)
            if transformed_aspect is None:
                # drop the record
                log.debug(
                    f"Dropping record {envelope} as transformation result is None"
                )
            envelope.record.aspect = transformed_aspect
        else:
            self._record_mcp(envelope.record)
        return envelope if envelope.record.aspect is not None else None

    def _transform_multiple_aspect_mcpw(
        self, envelope: RecordEnvelope[MetadataChangeProposalWrapper]
    ) -> Iterable[RecordEnvelope[MetadataChangeProposalWrapper]]:
        """
        Handle MultipleAspectTransformer processing for MetadataChangeProposalWrapper records.

        This helper method is defined on BaseTransformer but calls methods from the
        MultipleAspectTransformer mixin. It transforms a single input aspect into
        multiple output aspects, emitting them on-the-fly as separate envelopes.

        The first output aspect replaces the original envelope, and additional aspects
        are yielded as new envelopes with updated work unit IDs.

        Args:
            envelope: Record envelope containing an MCPW

        Yields:
            One or more envelopes, each containing a transformed aspect

        Note:
            This method requires self to be an instance of MultipleAspectTransformer,
            which is validated via runtime assertion.
        """
        assert isinstance(self, MultipleAspectTransformer)

        assert envelope.record.entityUrn
        if envelope.record.aspectName == self.aspect_name() and envelope.record.aspect:
            # Collect all output aspects
            output_aspects = list(
                self.transform_aspects(
                    entity_urn=envelope.record.entityUrn,
                    aspect_name=envelope.record.aspectName,
                    aspect=envelope.record.aspect,
                )
            )

            # First aspect replaces current envelope
            if output_aspects:
                first_aspect_name, first_aspect = output_aspects[0]
                if first_aspect is not None:
                    envelope.record.aspectName = first_aspect_name
                    envelope.record.aspect = first_aspect
                    # Update work unit ID for first aspect too
                    envelope.metadata = _update_work_unit_id(
                        envelope=envelope,
                        urn=envelope.record.entityUrn,
                        aspect_name=first_aspect_name,
                    )
                    yield envelope

                # Additional aspects yielded as new envelopes
                for aspect_name, aspect in output_aspects[1:]:
                    if aspect is not None:
                        record_metadata = _update_work_unit_id(
                            envelope=envelope,
                            urn=envelope.record.entityUrn,
                            aspect_name=aspect_name,
                        )
                        yield RecordEnvelope(
                            record=MetadataChangeProposalWrapper(
                                entityUrn=envelope.record.entityUrn,
                                aspectName=aspect_name,
                                aspect=aspect,
                            ),
                            metadata=record_metadata,
                        )
            self._mark_processed(envelope.record.entityUrn)
        else:
            self._record_mcp(envelope.record)

    def _handle_single_aspect_end_of_stream(
        self, envelope: RecordEnvelope
    ) -> Iterable[RecordEnvelope]:
        """
        Process accumulated state at end of stream for SingleAspectTransformer.

        This helper method is defined on BaseTransformer but calls methods from the
        SingleAspectTransformer mixin. It processes any entities that were tracked
        during the ingestion and emits their transformed aspects.

        This is necessary for transformers that need to see all aspects of an entity
        before deciding on the final transformation (e.g., accumulating state across
        multiple aspects).

        Args:
            envelope: The EndOfStream control record envelope

        Yields:
            Envelopes containing transformed aspects for tracked entities

        Note:
            This method requires self to be an instance of SingleAspectTransformer,
            which is validated via runtime assertion. It accesses entity_map which is
            managed by BaseTransformer.
        """
        assert isinstance(self, SingleAspectTransformer)

        for urn, state in self.entity_map.items():
            if "seen" in state:
                last_seen_mcp = state["seen"].get("mcp")
                last_seen_mce_system_metadata = state["seen"].get("mce")

                transformed_aspect = self.transform_aspect(
                    entity_urn=urn,
                    aspect_name=self.aspect_name(),
                    aspect=(
                        last_seen_mcp.aspect
                        if last_seen_mcp
                        and last_seen_mcp.aspectName == self.aspect_name()
                        else None
                    ),
                )
                if transformed_aspect:
                    structured_urn = Urn.from_string(urn)

                    mcp: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                        entityUrn=urn,
                        entityType=structured_urn.get_type(),
                        systemMetadata=(
                            last_seen_mcp.systemMetadata
                            if last_seen_mcp
                            else last_seen_mce_system_metadata
                        ),
                        aspectName=self.aspect_name(),
                        aspect=transformed_aspect,
                    )

                    if mcp.entityUrn:
                        assert mcp.aspect is not None
                        record_metadata = _update_work_unit_id(
                            envelope=envelope,
                            aspect_name=mcp.aspect.get_aspect_name(),
                            urn=mcp.entityUrn,
                        )
                    else:
                        record_metadata = envelope.metadata.copy()

                    yield RecordEnvelope(
                        record=mcp,
                        metadata=record_metadata,
                    )

            self._mark_processed(urn)

    def _handle_multiple_aspect_end_of_stream(
        self, envelope: RecordEnvelope
    ) -> Iterable[RecordEnvelope]:
        """
        Process accumulated state at end of stream for MultipleAspectTransformer.

        This helper method is defined on BaseTransformer but calls methods from the
        MultipleAspectTransformer mixin. It processes any entities that were tracked
        during the ingestion and emits their transformed aspects as multiple outputs.

        Similar to _handle_single_aspect_end_of_stream but supports emitting multiple
        aspects per entity (e.g., transforming tags into both tags and structured properties).

        Args:
            envelope: The EndOfStream control record envelope

        Yields:
            Envelopes containing transformed aspects for tracked entities. Multiple
            envelopes may be yielded per entity if the transformer emits multiple aspects.

        Note:
            This method requires self to be an instance of MultipleAspectTransformer,
            which is validated via runtime assertion. It accesses entity_map which is
            managed by BaseTransformer.
        """
        assert isinstance(self, MultipleAspectTransformer)

        for urn, state in self.entity_map.items():
            if "seen" in state:
                last_seen_mcp = state["seen"].get("mcp")
                last_seen_mce_system_metadata = state["seen"].get("mce")

                output_aspects = self.transform_aspects(
                    entity_urn=urn,
                    aspect_name=self.aspect_name(),
                    aspect=(
                        last_seen_mcp.aspect
                        if last_seen_mcp
                        and last_seen_mcp.aspectName == self.aspect_name()
                        else None
                    ),
                )

                for output_aspect_name, output_aspect in output_aspects:
                    if output_aspect:
                        structured_urn = Urn.from_string(urn)

                        mcp: MetadataChangeProposalWrapper = (
                            MetadataChangeProposalWrapper(
                                entityUrn=urn,
                                entityType=structured_urn.get_type(),
                                systemMetadata=(
                                    last_seen_mcp.systemMetadata
                                    if last_seen_mcp
                                    else last_seen_mce_system_metadata
                                ),
                                aspectName=output_aspect_name,
                                aspect=output_aspect,
                            )
                        )

                        if mcp.entityUrn:
                            record_metadata = _update_work_unit_id(
                                envelope=envelope,
                                aspect_name=output_aspect_name,
                                urn=mcp.entityUrn,
                            )
                        else:
                            record_metadata = envelope.metadata.copy()

                        yield RecordEnvelope(
                            record=mcp,
                            metadata=record_metadata,
                        )

            self._mark_processed(urn)

    def _handle_end_of_stream(
        self, envelope: RecordEnvelope
    ) -> Iterable[RecordEnvelope]:
        assert isinstance(
            self,
            (SingleAspectTransformer, MultipleAspectTransformer, LegacyMCETransformer),
        ), (
            f"Transformer {type(self).__name__} must inherit from one of the allowed mixins"
        )

        mcps: Sequence[
            Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]
        ] = self.handle_end_of_stream()

        for mcp in mcps:
            if (
                mcp.aspect is None or mcp.aspectName is None or mcp.entityUrn is None
            ):  # to silent the lint error
                continue

            record_metadata = _update_work_unit_id(
                envelope=envelope,
                aspect_name=mcp.aspectName,
                urn=mcp.entityUrn,
            )

            yield RecordEnvelope(
                record=mcp,
                metadata=record_metadata,
            )

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        for envelope in record_envelopes:
            if not self._should_process(envelope.record):
                # early exit
                pass
            elif isinstance(envelope.record, MetadataChangeEventClass) and isinstance(
                self,
                (
                    SingleAspectTransformer,
                    MultipleAspectTransformer,
                    LegacyMCETransformer,
                ),
            ):
                envelope = self._transform_or_record_mce(envelope)
            elif isinstance(
                envelope.record, MetadataChangeProposalWrapper
            ) and isinstance(self, SingleAspectTransformer):
                return_envelope = self._transform_or_record_mcpw(envelope)
                if return_envelope is None:
                    continue
                else:
                    envelope = return_envelope
            elif isinstance(
                envelope.record, MetadataChangeProposalWrapper
            ) and isinstance(self, MultipleAspectTransformer):
                # Use helper method for MultipleAspectTransformer MCPW processing
                yield from self._transform_multiple_aspect_mcpw(envelope)
                continue
            elif isinstance(envelope.record, EndOfStream) and isinstance(
                self, SingleAspectTransformer
            ):
                # Use helper method for SingleAspectTransformer end of stream
                yield from self._handle_single_aspect_end_of_stream(envelope)
                yield from self._handle_end_of_stream(envelope=envelope)
            elif isinstance(envelope.record, EndOfStream) and isinstance(
                self, MultipleAspectTransformer
            ):
                # Use helper method for MultipleAspectTransformer end of stream
                yield from self._handle_multiple_aspect_end_of_stream(envelope)
                yield from self._handle_end_of_stream(envelope=envelope)

            yield envelope
