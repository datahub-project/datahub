import dataclasses
import json
from typing import TYPE_CHECKING, List, Optional, Sequence, Tuple, Union

from datahub.emitter.aspect import ASPECT_MAP, JSON_CONTENT_TYPE, TIMESERIES_ASPECT_MAP
from datahub.emitter.serialization_helper import post_json_transform, pre_json_transform
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DictWrapper,
    GenericAspectClass,
    KafkaAuditHeaderClass,
    MetadataChangeLogClass,
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
        contentType=JSON_CONTENT_TYPE,
    )


def _try_from_generic_aspect(
    aspectName: Optional[str],
    aspect: Optional[GenericAspectClass],
) -> Tuple[bool, Optional[_Aspect]]:
    # The first value in the tuple indicates the success of the conversion,
    # while the second value is the deserialized aspect.

    if aspect is None:
        return True, None
    assert aspectName is not None, "aspectName must be set if aspect is set"

    if aspect.contentType != JSON_CONTENT_TYPE:
        return False, None

    if aspectName not in ASPECT_MAP:
        return False, None

    aspect_cls = ASPECT_MAP[aspectName]

    serialized = aspect.value.decode()
    obj = post_json_transform(json.loads(serialized))

    return True, aspect_cls.from_obj(obj)


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

    @classmethod
    def construct_many(
        cls, entityUrn: str, aspects: Sequence[Optional[_Aspect]]
    ) -> List["MetadataChangeProposalWrapper"]:
        return [cls(entityUrn=entityUrn, aspect=aspect) for aspect in aspects if aspect]

    def _make_mcp_without_aspects(self) -> MetadataChangeProposalClass:
        return MetadataChangeProposalClass(
            entityType=self.entityType,
            entityUrn=self.entityUrn,
            changeType=self.changeType,
            auditHeader=self.auditHeader,
            aspectName=self.aspectName,
            systemMetadata=self.systemMetadata,
        )

    def make_mcp(self) -> MetadataChangeProposalClass:
        serializedEntityKeyAspect: Union[None, GenericAspectClass] = None
        if isinstance(self.entityKeyAspect, DictWrapper):
            serializedEntityKeyAspect = _make_generic_aspect(self.entityKeyAspect)

        serializedAspect = None
        if self.aspect is not None:
            serializedAspect = _make_generic_aspect(self.aspect)

        mcp = self._make_mcp_without_aspects()
        mcp.entityKeyAspect = serializedEntityKeyAspect
        mcp.aspect = serializedAspect
        return mcp

    def validate(self) -> bool:
        if self.entityUrn is None and self.entityKeyAspect is None:
            return False
        if self.entityUrn is not None and self.entityKeyAspect is not None:
            return False
        if self.entityKeyAspect is not None and not self.entityKeyAspect.validate():
            return False
        if self.aspect and not self.aspect.validate():
            return False
        if not self._make_mcp_without_aspects().validate():
            # PERF: Because we've already validated the aspects above, we can skip
            # re-validating them by using _make_mcp_without_aspects() instead of
            # make_mcp(). This way, we avoid doing unnecessary JSON serialization.
            return False
        return True

    def to_obj(self, tuples: bool = False, simplified_structure: bool = False) -> dict:
        # The simplified_structure parameter is used to make the output
        # not contain nested JSON strings. Instead, it unpacks the JSON
        # string into an object.

        obj = self.make_mcp().to_obj(tuples=tuples)
        if simplified_structure:
            # Undo the double JSON serialization that happens in the MCP aspect.
            if (
                obj.get("aspect")
                and obj["aspect"].get("contentType") == JSON_CONTENT_TYPE
            ):
                obj["aspect"] = {"json": json.loads(obj["aspect"]["value"])}
        return obj

    @classmethod
    def from_obj(
        cls, obj: dict, tuples: bool = False
    ) -> Union["MetadataChangeProposalWrapper", MetadataChangeProposalClass]:
        """
        Attempt to deserialize into an MCPW, but fall back
        to a standard MCP if we're missing codegen'd classes for the
        entity key or aspect.
        """

        # Redo the double JSON serialization so that the rest of deserialization
        # routine works.
        if obj.get("aspect") and obj["aspect"].get("json"):
            obj["aspect"] = {
                "contentType": JSON_CONTENT_TYPE,
                "value": json.dumps(obj["aspect"]["json"]),
            }

        mcpc = MetadataChangeProposalClass.from_obj(obj, tuples=tuples)

        # We don't know how to deserialize the entity key aspects yet.
        if mcpc.entityKeyAspect is not None:
            return mcpc

        # Try to deserialize the aspect.
        return cls.try_from_mcpc(mcpc) or mcpc

    @classmethod
    def try_from_mcpc(
        cls, mcpc: MetadataChangeProposalClass
    ) -> Optional["MetadataChangeProposalWrapper"]:
        """Attempts to create a MetadataChangeProposalWrapper from a MetadataChangeProposalClass.
        Neatly handles unsupported, expected cases, such as unknown aspect types or non-json content type.

        Raises:
            Exception if the generic aspect is invalid, e.g. contains invalid json.
        """

        if mcpc.changeType != ChangeTypeClass.UPSERT:
            # We can only generate MCPWs for upserts.
            return None

        converted, aspect = _try_from_generic_aspect(mcpc.aspectName, mcpc.aspect)
        if converted:
            return cls(
                entityType=mcpc.entityType,
                entityUrn=mcpc.entityUrn,
                changeType=mcpc.changeType,
                auditHeader=mcpc.auditHeader,
                aspectName=mcpc.aspectName,
                aspect=aspect,
                systemMetadata=mcpc.systemMetadata,
            )
        else:
            return None

    @classmethod
    def try_from_mcl(
        cls, mcl: MetadataChangeLogClass
    ) -> Union["MetadataChangeProposalWrapper", MetadataChangeProposalClass]:
        mcpc = MetadataChangeProposalClass(
            entityUrn=mcl.entityUrn,
            entityType=mcl.entityType,
            entityKeyAspect=mcl.entityKeyAspect,
            aspect=mcl.aspect,
            aspectName=mcl.aspectName,
            changeType=mcl.changeType,
            auditHeader=mcl.auditHeader,
            systemMetadata=mcl.systemMetadata,
        )
        return cls.try_from_mcpc(mcpc) or mcpc

    @classmethod
    def from_obj_require_wrapper(
        cls, obj: dict, tuples: bool = False
    ) -> "MetadataChangeProposalWrapper":
        mcp = cls.from_obj(obj, tuples=tuples)
        assert isinstance(mcp, cls)
        return mcp

    def as_workunit(
        self, *, treat_errors_as_warnings: bool = False, is_primary_source: bool = True
    ) -> "MetadataWorkUnit":
        from datahub.ingestion.api.workunit import MetadataWorkUnit

        if self.aspect and self.aspectName in TIMESERIES_ASPECT_MAP:
            # TODO: Make this a cleaner interface.
            ts = getattr(self.aspect, "timestampMillis", None)
            assert ts is not None

            # If the aspect is a timeseries aspect, include the timestampMillis in the ID.
            return MetadataWorkUnit(
                id=f"{self.entityUrn}-{self.aspectName}-{ts}",
                mcp=self,
                treat_errors_as_warnings=treat_errors_as_warnings,
                is_primary_source=is_primary_source,
            )

        return MetadataWorkUnit(
            id=f"{self.entityUrn}-{self.aspectName}",
            mcp=self,
            treat_errors_as_warnings=treat_errors_as_warnings,
            is_primary_source=is_primary_source,
        )
