import logging
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, Iterable, List, Optional, Type, Union

import datahub.emitter.mce_builder
from datahub.emitter.mce_builder import Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import ControlRecord, EndOfStream, RecordEnvelope
from datahub.ingestion.api.transform import Transformer
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    ChangeTypeClass,
    DataFlowSnapshotClass,
    DataJobSnapshotClass,
    DataPlatformInstanceClass,
    DatasetDeprecationClass,
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    DatasetUpstreamLineageClass,
    EditableDatasetPropertiesClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    GlossaryTermsClass,
    InstitutionalMemoryClass,
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    OwnershipClass,
    SchemaMetadataClass,
    StatusClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.utilities.urns.urn import Urn

log = logging.getLogger(__name__)


class SnapshotAspectRegistry:
    """A registry of aspect name to aspect type mappings, only for snapshot classes. Do not add non-snapshot aspect classes here."""

    def __init__(self):
        self.aspect_name_type_mapping = {
            "ownership": OwnershipClass,
            "globalTags": GlobalTagsClass,
            "datasetProperties": DatasetPropertiesClass,
            "editableDatasetProperties": EditableDatasetPropertiesClass,
            "glossaryTerms": GlossaryTermsClass,
            "status": StatusClass,
            "browsePaths": BrowsePathsClass,
            "schemaMetadata": SchemaMetadataClass,
            "editableSchemaMetadata": EditableSchemaMetadataClass,
            "datasetDeprecation": DatasetDeprecationClass,
            "datasetUpstreamLineage": DatasetUpstreamLineageClass,
            "upstreamLineage": UpstreamLineageClass,
            "institutionalMemory": InstitutionalMemoryClass,
            "dataPlatformInstance": DataPlatformInstanceClass,
            "viewProperties": ViewPropertiesClass,
        }

    def get_aspect_type(self, aspect_name: str) -> Optional[Type[Aspect]]:
        return self.aspect_name_type_mapping.get(aspect_name)


class LegacyMCETransformer(Transformer, metaclass=ABCMeta):
    @abstractmethod
    def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
        pass


class SingleAspectTransformer(metaclass=ABCMeta):
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
        param: aspect: an optional aspect corresponding to the aspect name that the transformer is interested in. Empty if no aspect with this name was produced by the underlying connector"""
        pass


class BaseTransformer(Transformer, metaclass=ABCMeta):
    """Transformer that offers common functionality that most transformers need"""

    allowed_mixins = [LegacyMCETransformer, SingleAspectTransformer]

    @abstractmethod
    def entity_types(self) -> List[str]:
        """Implement this method to specify which entity types the transformer is interested in subscribing to. Defaults to ALL (encoded as "*")"""
        return ["*"]

    def __init__(self):
        self.entity_map: Dict[str, Dict[str, Any]] = {}
        self.entity_type_mappings: Dict[str, Type] = {
            "dataset": DatasetSnapshotClass,
            "dataFlow": DataFlowSnapshotClass,
            "dataJob": DataJobSnapshotClass,
        }
        self.snapshot_aspect_registry = SnapshotAspectRegistry()
        mixedin = False
        for mixin in [LegacyMCETransformer, SingleAspectTransformer]:
            mixedin = mixedin or isinstance(self, mixin)
        if not mixedin:
            assert (
                "Class does not implement one of required traits {self.allowed_mixins}"
            )

    def _should_process(
        self,
        record: Union[
            MetadataChangeEventClass, MetadataChangeProposalWrapper, ControlRecord
        ],
    ) -> bool:
        if isinstance(record, ControlRecord):
            # all control events should be processed
            return True

        entity_types = self.entity_types()
        if "*" in entity_types:
            return True
        if isinstance(record, MetadataChangeEventClass):
            for e in entity_types:
                assert (
                    e in self.entity_type_mappings
                ), f"Do not have a class mapping for {e}. Subscription to this entity will not work for transforming MCE-s"
                if isinstance(record.proposedSnapshot, self.entity_type_mappings[e]):
                    return True
            # fall through, no entity type matched
            return False
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

    def _record_mcp(self, mcp: MetadataChangeProposalWrapper) -> None:
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
            aspect_type = self.snapshot_aspect_registry.get_aspect_type(  # type: ignore
                self.aspect_name()
            )
            if aspect_type:
                # if we find a type corresponding to the aspect name we look for it in the mce
                old_aspect = datahub.emitter.mce_builder.get_aspect_if_available(
                    mce,
                    aspect_type,
                )
                if old_aspect:
                    if isinstance(self, LegacyMCETransformer):
                        # use the transform_one pathway to transform this MCE
                        envelope.record = self.transform_one(mce)
                    else:
                        transformed_aspect = self.transform_aspect(
                            entity_urn=mce.proposedSnapshot.urn,
                            aspect_name=self.aspect_name(),
                            aspect=old_aspect,
                        )
                        datahub.emitter.mce_builder.set_aspect(
                            mce,
                            aspect_type=aspect_type,
                            aspect=transformed_aspect,
                        )
                        envelope.record = mce
                    self._mark_processed(mce.proposedSnapshot.urn)
            else:
                log.warning(
                    f"Could not locate a snapshot aspect type for aspect {self.aspect_name()}. This can lead to silent drops of messages in transformers."
                )
        elif isinstance(self, LegacyMCETransformer):
            # we pass down the full MCE
            envelope.record = self.transform_one(mce)
            self._mark_processed(mce.proposedSnapshot.urn)

        return envelope

    def _transform_or_record_mcp(
        self,
        envelope: RecordEnvelope[MetadataChangeProposalWrapper],
    ) -> Optional[RecordEnvelope[MetadataChangeProposalWrapper]]:
        # remember stuff
        assert envelope.record.entityUrn
        assert isinstance(self, SingleAspectTransformer)
        if envelope.record.aspectName == self.aspect_name():
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

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        for envelope in record_envelopes:
            if not self._should_process(envelope.record):
                # early exit
                pass
            elif isinstance(envelope.record, MetadataChangeEventClass):
                envelope = self._transform_or_record_mce(envelope)
            elif isinstance(
                envelope.record, MetadataChangeProposalWrapper
            ) and isinstance(self, SingleAspectTransformer):
                return_envelope = self._transform_or_record_mcp(envelope)
                if return_envelope is None:
                    continue
                else:
                    envelope = return_envelope
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
                                record=MetadataChangeProposalWrapper(
                                    entityUrn=urn,
                                    entityType=structured_urn.get_type(),
                                    changeType=ChangeTypeClass.UPSERT,
                                    systemMetadata=last_seen_mcp.systemMetadata
                                    if last_seen_mcp
                                    else last_seen_mce_system_metadata,
                                    aspectName=self.aspect_name(),
                                    aspect=transformed_aspect,
                                ),
                                metadata=record_metadata,
                            )
                    self._mark_processed(urn)
            yield envelope
