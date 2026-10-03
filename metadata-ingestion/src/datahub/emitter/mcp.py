import base64
import dataclasses
import gzip
import json
import os
import warnings
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Tuple, Union

from datahub.emitter.aspect import ASPECT_MAP, GZIP_JSON_CONTENT_TYPE, JSON_CONTENT_TYPE
from datahub.emitter.serialization_helper import post_json_transform, pre_json_transform
from datahub.errors import DataHubDeprecationWarning
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

DEFAULT_USE_GZIP = os.environ.get("DATAHUB_USE_GZIP_ENCODING", "true").lower() == "true"

GZIP_COMPRESSED_ASPECTS = {"schemaMetadata"}


def _make_generic_aspect(codegen_obj: DictWrapper, use_gzip: bool = DEFAULT_USE_GZIP) -> GenericAspectClass:
    serialized = json.dumps(pre_json_transform(codegen_obj.to_obj()))
    if use_gzip:
        # Compress the data and then base64 encode it for safe JSON serialization
        compressed = gzip.compress(serialized.encode())
        base64_encoded = base64.b64encode(compressed)
        return GenericAspectClass(
            value=base64_encoded,
            contentType=GZIP_JSON_CONTENT_TYPE,
        )
    else:
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

    if aspect.contentType not in [JSON_CONTENT_TYPE, GZIP_JSON_CONTENT_TYPE]:
        return False, None

    if aspectName not in ASPECT_MAP:
        return False, None

    aspect_cls = ASPECT_MAP[aspectName]

    if aspect.contentType == GZIP_JSON_CONTENT_TYPE:
        # TODO: can we avoid repeating check for aspect.value? According to the schema it should be always bytes...
        if isinstance(aspect.value, str):
            binary_data = base64.b64decode(aspect.value)
            decompressed = gzip.decompress(binary_data)
            serialized = decompressed.decode()
        else:
            decompressed = gzip.decompress(aspect.value)
            serialized = decompressed.decode()
    else:
        # Handle standard JSON content
        if isinstance(aspect.value, bytes):
            serialized = aspect.value.decode()
        else:
            serialized = aspect.value
        
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
    headers: Union[None, Dict[str, str]] = None
    use_gzip: bool = DEFAULT_USE_GZIP

    def __post_init__(self) -> None:
        if self.entityUrn and self.entityType == _ENTITY_TYPE_UNSET:
            self.entityType = guess_entity_type(self.entityUrn)
        elif self.entityUrn and self.entityType:
            guessed_entity_type = guess_entity_type(self.entityUrn)
            if self.entityType.lower() != guessed_entity_type.lower():
                # If they aren't a case-ignored match, raise an error.
                raise ValueError(
                    f"entityType {self.entityType} does not match the entity type {guessed_entity_type} from entityUrn {self.entityUrn}",
                )
            elif self.entityType != guessed_entity_type:
                # If they only differ in case, normalize and print a warning.
                self.entityType = guessed_entity_type
                warnings.warn(
                    f"The passed entityType {self.entityType} differs in case from the expected entity type {guessed_entity_type}. "
                    "This will be automatically corrected for now, but will become an error in a future release. "
                    "Note that the entityType field is optional and will be automatically inferred from the entityUrn.",
                    DataHubDeprecationWarning,
                    stacklevel=3,
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
        cls, entityUrn: str, aspects: Sequence[Optional[_Aspect]], use_gzip: bool = DEFAULT_USE_GZIP
    ) -> List["MetadataChangeProposalWrapper"]:
        return [cls(entityUrn=entityUrn, aspect=aspect, use_gzip=use_gzip) for aspect in aspects if aspect]

    def _make_mcp_without_aspects(self) -> MetadataChangeProposalClass:
        return MetadataChangeProposalClass(
            entityType=self.entityType,
            entityUrn=self.entityUrn,
            changeType=self.changeType,
            auditHeader=self.auditHeader,
            aspectName=self.aspectName,
            systemMetadata=self.systemMetadata,
            headers=self.headers,
        )

    def make_mcp(self, use_gzip: bool = None) -> MetadataChangeProposalClass:
        if use_gzip is None:
            use_gzip = self.use_gzip
            
        serializedEntityKeyAspect: Union[None, GenericAspectClass] = None
        if isinstance(self.entityKeyAspect, DictWrapper):
            serializedEntityKeyAspect = _make_generic_aspect(self.entityKeyAspect, False)

        serializedAspect = None
        if self.aspect is not None:
            # Only compress specific aspects (for testing)
            aspect_name = self.aspect.get_aspect_name()
            should_compress = use_gzip and aspect_name in GZIP_COMPRESSED_ASPECTS
            serializedAspect = _make_generic_aspect(self.aspect, should_compress)

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

    def to_obj(self, tuples: bool = False, simplified_structure: bool = False, use_gzip: bool = None) -> dict:
        # The simplified_structure parameter is used to make the output
        # not contain nested JSON strings. Instead, it unpacks the JSON
        # string into an object.

        obj = self.make_mcp(use_gzip=use_gzip).to_obj(tuples=tuples)
        if simplified_structure:
            # Undo the double JSON serialization that happens in the MCP aspect.
            if obj.get("aspect"):
                content_type = obj["aspect"].get("contentType")
                if content_type == JSON_CONTENT_TYPE:
                    obj["aspect"] = {"json": json.loads(obj["aspect"]["value"])}
                elif content_type == GZIP_JSON_CONTENT_TYPE:
                    # For gzipped content, we need to base64 decode, decompress, then load
                    # the value should be a base64 encoded string
                    value = obj["aspect"]["value"]
                    if isinstance(value, str):
                        binary_data = base64.b64decode(value)
                        decompressed = gzip.decompress(binary_data)
                        obj["aspect"] = {"json": json.loads(decompressed.decode('utf-8'))}
                    else:
                        decompressed = gzip.decompress(value)
                        obj["aspect"] = {"json": json.loads(decompressed.decode('utf-8'))}
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
            content_type = obj["aspect"].get("contentType", JSON_CONTENT_TYPE)
            
            if content_type == GZIP_JSON_CONTENT_TYPE:
                json_str = json.dumps(obj["aspect"]["json"])
                compressed = gzip.compress(json_str.encode('utf-8'))
                base64_encoded = base64.b64encode(compressed).decode('ascii')
                obj["aspect"] = {
                    "contentType": GZIP_JSON_CONTENT_TYPE,
                    "value": base64_encoded,
                }
            else:
                obj["aspect"] = {
                    "contentType": JSON_CONTENT_TYPE,
                    "value": json.dumps(obj["aspect"]["json"]),
                }

        mcpc = MetadataChangeProposalClass.from_obj(obj, tuples=tuples)

        # We don't know how to deserialize the entity key aspects yet.
        if mcpc.entityKeyAspect is not None:
            return mcpc

        if obj.get("aspect") and obj["aspect"].get("contentType") == GZIP_JSON_CONTENT_TYPE:
            use_gzip = True
        else:
            use_gzip = DEFAULT_USE_GZIP
            
        return cls.try_from_mcpc(mcpc, use_gzip=use_gzip) or mcpc

    @classmethod
    def try_from_mcpc(
        cls, mcpc: MetadataChangeProposalClass, use_gzip: bool = DEFAULT_USE_GZIP
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
            # Determine if the source was using gzip based on content type
            if mcpc.aspect and mcpc.aspect.contentType == GZIP_JSON_CONTENT_TYPE:
                inferred_use_gzip = True
            else:
                inferred_use_gzip = use_gzip
                
            return cls(
                entityType=mcpc.entityType,
                entityUrn=mcpc.entityUrn,
                changeType=mcpc.changeType,
                auditHeader=mcpc.auditHeader,
                aspectName=mcpc.aspectName,
                aspect=aspect,
                systemMetadata=mcpc.systemMetadata,
                headers=mcpc.headers,
                use_gzip=inferred_use_gzip,
            )
        else:
            return None

    @classmethod
    def try_from_mcl(
        cls, mcl: MetadataChangeLogClass, use_gzip: bool = DEFAULT_USE_GZIP
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
            headers=mcl.headers,
        )
        return cls.try_from_mcpc(mcpc, use_gzip=use_gzip) or mcpc

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

        id = MetadataWorkUnit.generate_workunit_id(self)
        return MetadataWorkUnit(
            id=id,
            mcp=self,
            treat_errors_as_warnings=treat_errors_as_warnings,
            is_primary_source=is_primary_source,
        )
