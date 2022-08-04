import dataclasses
import json
from typing import Union

from datahub.emitter.serialization_helper import pre_json_transform
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DictWrapper,
    GenericAspectClass,
    KafkaAuditHeaderClass,
    MetadataChangeProposalClass,
    SystemMetadataClass,
    _Aspect,
)


def _make_generic_aspect(codegen_obj: DictWrapper) -> GenericAspectClass:
    serialized = json.dumps(pre_json_transform(codegen_obj.to_obj()))
    return GenericAspectClass(
        value=serialized.encode(),
        contentType="application/json",
    )


@dataclasses.dataclass
class MetadataChangeProposalWrapper:
    # TODO: remove manually aspectName from the codebase
    # TODO: (after) remove aspectName field from this class
    # TODO: infer entityType from entityUrn
    # TODO: set changeType's default to UPSERT

    entityType: str
    changeType: Union[str, ChangeTypeClass]
    entityUrn: Union[None, str] = None
    entityKeyAspect: Union[None, _Aspect] = None
    auditHeader: Union[None, KafkaAuditHeaderClass] = None
    aspectName: Union[None, str] = None
    aspect: Union[None, _Aspect] = None
    systemMetadata: Union[None, SystemMetadataClass] = None

    def __post_init__(self) -> None:
        if not self.aspectName and self.aspect:
            self.aspectName = self.aspect.get_aspect_name()
        elif (
            self.aspectName
            and self.aspect
            and self.aspectName != self.aspect.get_aspect_name()
        ):
            raise ValueError(
                f"aspectName {self.aspectName} does not match aspect type {type(self.aspect)} with name {self.aspect.get_aspect_name()}"
            )

    def make_mcp(self) -> MetadataChangeProposalClass:
        serializedEntityKeyAspect: Union[None, GenericAspectClass] = None
        if isinstance(self.entityKeyAspect, DictWrapper):
            serializedEntityKeyAspect = _make_generic_aspect(self.entityKeyAspect)

        serializedAspect = None
        if self.aspect is not None:
            serializedAspect = _make_generic_aspect(self.aspect)

        return MetadataChangeProposalClass(
            entityType=self.entityType,
            entityUrn=self.entityUrn,
            entityKeyAspect=serializedEntityKeyAspect,
            changeType=self.changeType,
            auditHeader=self.auditHeader,
            aspectName=self.aspectName,
            aspect=serializedAspect,
            systemMetadata=self.systemMetadata,
        )

    def validate(self) -> bool:
        if self.entityUrn is None and self.entityKeyAspect is None:
            return False
        if self.entityUrn is not None and self.entityKeyAspect is not None:
            return False
        if self.entityKeyAspect is not None and not self.entityKeyAspect.validate():
            return False
        if self.aspect and not self.aspect.validate():
            return False
        if not self.make_mcp().validate():
            return False
        return True

    def to_obj(self, tuples: bool = False) -> dict:
        return self.make_mcp().to_obj(tuples=tuples)

    # TODO: add a from_obj method. Implementing this would require us to
    # inspect the aspectName field to determine which class to deserialize into.
