import dataclasses
from typing import Union

from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DictWrapper,
    GenericAspectClass,
    KafkaAuditHeaderClass,
    MetadataChangeProposalClass,
    SystemMetadataClass,
)


def _make_generic_aspect(codegen_obj: DictWrapper) -> GenericAspectClass:
    # TODO this
    pass


@dataclasses.dataclass
class MetadataChangeProposalWrapper:
    entityType: str
    entityKey: Union[str, DictWrapper]
    changeType: Union[str, ChangeTypeClass]
    auditHeader: Union[None, KafkaAuditHeaderClass] = None
    aspectName: Union[None, str] = None
    aspect: Union[None, DictWrapper] = None
    systemMetadata: Union[None, SystemMetadataClass] = None

    def make_mcp(self):
        serializedEntityKey: Union[str, GenericAspectClass]
        if isinstance(self.entityKey, DictWrapper):
            serializedEntityKey = _make_generic_aspect(self.entityKey)
        else:
            serializedEntityKey = self.entityKey

        serializedAspect = None
        if self.aspect is not None:
            serializedAspect = _make_generic_aspect(self.aspect)

        return MetadataChangeProposalClass(
            entityType=self.entityType,
            entityKey=serializedEntityKey,
            changeType=self.changeType,
            auditHeader=self.auditHeader,
            aspectName=self.aspectName,
            aspect=serializedAspect,
            systemMetadata=self.systemMetadata,
        )
