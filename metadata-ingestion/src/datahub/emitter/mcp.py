import dataclasses
import json
from typing import TYPE_CHECKING, Union

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
from datahub.utilities.urns.urn import guess_entity_type

if TYPE_CHECKING:
    from datahub.ingestion.api.workunit import MetadataWorkUnit

_ENTITY_TYPE_UNSET = "ENTITY_TYPE_UNSET"


def _make_generic_aspect(codegen_obj: DictWrapper) -> GenericAspectClass:
    serialized = json.dumps(pre_json_transform(codegen_obj.to_obj()))
    return GenericAspectClass(
        value=serialized.encode(),
        contentType="application/json",
    )


@dataclasses.dataclass
class MetadataChangeProposalWrapper:
    # TODO: remove manually set aspectName from the codebase
    # TODO: (after) remove aspectName field from this class
    # TODO: remove manually set entityType from the codebase

    entityType: str = _ENTITY_TYPE_UNSET
    changeType: Union[str, ChangeTypeClass] = ChangeTypeClass.UPSERT
    entityUrn: Union[None, str] = None
    entityKeyAspect: Union[None, _Aspect] = None
    auditHeader: Union[None, KafkaAuditHeaderClass] = None
    aspectName: Union[None, str] = None
    aspect: Union[None, _Aspect] = None
    systemMetadata: Union[None, SystemMetadataClass] = None

    def __post_init__(self) -> None:
        if self.entityUrn and self.entityType == _ENTITY_TYPE_UNSET:
            self.entityType = guess_entity_type(self.entityUrn)
        elif self.entityUrn and self.entityType:
            guessed_entity_type = guess_entity_type(self.entityUrn).lower()
            # Entity type checking is actually case insensitive.
            # Note that urns are case sensitive, but entity types are not.
            if self.entityType.lower() != guessed_entity_type:
                raise ValueError(
                    f"entityType {self.entityType} does not match the entity type {guessed_entity_type} from entityUrn {self.entityUrn}",
                )
        elif self.entityType == _ENTITY_TYPE_UNSET:
            raise ValueError("entityType must be set if entityUrn is not set")

        if not self.entityUrn and not self.entityKeyAspect:
            raise ValueError("entityUrn or entityKeyAspect must be set")

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

    def as_workunit(self) -> "MetadataWorkUnit":
        from datahub.ingestion.api.workunit import MetadataWorkUnit

        # TODO: If the aspect is a timeseries aspect, we should do some
        # customization of the ID here.

        return MetadataWorkUnit(id=f"{self.entityUrn}-{self.aspectName}", mcp=self)
