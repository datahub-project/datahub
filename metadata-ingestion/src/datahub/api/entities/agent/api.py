from __future__ import annotations

import time
from typing import Callable, ClassVar, Iterable, List, Optional, Set

from pydantic import field_validator

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    ApiPropertiesClass,
    ApiSignatureClass,
    ArrayTypeClass,
    AuditStampClass,
    BooleanTypeClass,
    DataPlatformInstanceClass,
    DateTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    RestApiPropertiesClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StatusClass,
    StringTypeClass,
    SubTypesClass,
)
from datahub.metadata.urns import ApiUrn

# Standard API subtypes, carried on the subTypes aspect. "Being an agent's
# tool" is a relationship, not an intrinsic kind — an MCP tool is just an API.
API_SUBTYPE_MCP_TOOL = "MCP_TOOL"
API_SUBTYPE_REST_ENDPOINT = "REST_ENDPOINT"
API_SUBTYPE_GRPC_METHOD = "GRPC_METHOD"
API_SUBTYPE_GRAPHQL_FIELD = "GRAPHQL_FIELD"
API_SUBTYPE_FUNCTION = "FUNCTION"
API_SUBTYPE_AGENT = "AGENT"
API_SUBTYPE_UI_TOOL = "UI_TOOL"
API_SUBTYPE_BUILTIN = "BUILTIN"

_VALID_SUBTYPES: Set[str] = {
    API_SUBTYPE_MCP_TOOL,
    API_SUBTYPE_REST_ENDPOINT,
    API_SUBTYPE_GRPC_METHOD,
    API_SUBTYPE_GRAPHQL_FIELD,
    API_SUBTYPE_FUNCTION,
    API_SUBTYPE_AGENT,
    API_SUBTYPE_UI_TOOL,
    API_SUBTYPE_BUILTIN,
}

_DEFAULT_ACTOR = "urn:li:corpuser:datahub"

# HTTP methods valid on the restApiProperties aspect (subtype REST_ENDPOINT).
_VALID_HTTP_METHODS: Set[str] = {
    "GET",
    "POST",
    "PUT",
    "PATCH",
    "DELETE",
    "HEAD",
    "OPTIONS",
    "TRACE",
}

# Map a friendly ``data_type`` string onto a SchemaField type-union member.
# The raw string is preserved as ``nativeDataType`` so richer hints like
# "OrderId" or "array<Order>" still display as authored.
_DATA_TYPE_TO_SCHEMA_TYPE: dict[str, type] = {
    "string": StringTypeClass,
    "number": NumberTypeClass,
    "integer": NumberTypeClass,
    "float": NumberTypeClass,
    "boolean": BooleanTypeClass,
    "object": RecordTypeClass,
    "struct": RecordTypeClass,
    "array": ArrayTypeClass,
    "date": DateTypeClass,
    "datetime": DateTypeClass,
}


def _schema_type_for(data_type: str) -> SchemaFieldDataTypeClass:
    """Resolve a friendly ``data_type`` string to a SchemaFieldDataTypeClass.

    Unknown types fall back to ``StringTypeClass`` (the raw string is still kept
    on ``nativeDataType``, so the original hint is not lost). Parameterized
    hints like ``array<Order>`` resolve on their base type (``array``).
    """
    base = data_type.lower().split("<", 1)[0].strip()
    type_cls = _DATA_TYPE_TO_SCHEMA_TYPE.get(base, StringTypeClass)
    return SchemaFieldDataTypeClass(type=type_cls())


class ApiParam(ConfigModel):
    """A single typed parameter (or output field) of an API's signature.

    Args:
        name: The parameter / field name (becomes ``fieldPath``).
        data_type: A friendly type hint (e.g. ``string``, ``integer``,
            ``array<Order>``). The raw value is preserved as ``nativeDataType``;
            the union type is inferred from its lowercased prefix.
        required: Whether the parameter is required. ``False`` maps to a
            nullable field.
        description: What the parameter / field represents.
    """

    name: str
    data_type: str = "string"
    required: bool = False
    description: Optional[str] = None

    def to_schema_field(self) -> SchemaFieldClass:
        return SchemaFieldClass(
            fieldPath=self.name,
            type=_schema_type_for(self.data_type),
            nativeDataType=self.data_type,
            nullable=not self.required,
            description=self.description,
        )


class Api(ConfigModel):
    """High-level helper for registering an API with DataHub.

    An API is a named callable with a typed input and output schema — an MCP
    tool, a REST endpoint, a gRPC method, a GraphQL field, a function, etc.
    Cataloging APIs as first-class entities makes caller -> API dependencies
    (services that compose them, agents that invoke them) visible in the graph.

    Args:
        id: The id of the API (bare id or a full ``urn:li:api:`` urn).
        name: Display name of the API.
        subtypes: Standard API subtypes (e.g. ``MCP_TOOL``, ``REST_ENDPOINT``,
            ``FUNCTION``). Defaults to ``["MCP_TOOL"]``.
        description: What the API does and when to use it.
        schema_definition: Input/output schema (typically JSON Schema) as an
            opaque string, used for discoverability and validation.
        parameters: The API's typed input signature as a list of
            :class:`ApiParam` (each becomes an input ``SchemaField``).
        returns: The API's typed output shape as a list of :class:`ApiParam`
            (each becomes an output ``SchemaField``).
        external_url: Link to the API's registry entry, docs, or source.
        method: For REST endpoints, the HTTP method (e.g. ``GET``, ``POST``).
            Paired with ``path`` it emits the ``restApiProperties`` aspect.
        path: For REST endpoints, the route/path template relative to the
            service base URL (e.g. ``/orders/{orderId}``).
    """

    # Backwards-compatible mapping from the legacy ``type`` values onto the
    # standard subtypes, so callers migrating from the old AgentTool helper
    # can pass the old-style strings.
    _LEGACY_TYPE_TO_SUBTYPE: ClassVar[dict[str, str]] = {
        "MCP": API_SUBTYPE_MCP_TOOL,
        "REST_API": API_SUBTYPE_REST_ENDPOINT,
        "FUNCTION": API_SUBTYPE_FUNCTION,
        "CUSTOM": API_SUBTYPE_FUNCTION,
    }

    id: str
    name: str
    subtypes: List[str] = [API_SUBTYPE_MCP_TOOL]
    description: Optional[str] = None
    schema_definition: Optional[str] = None
    parameters: Optional[List[ApiParam]] = None
    returns: Optional[List[ApiParam]] = None
    external_url: Optional[str] = None
    platform: Optional[str] = None
    # REST-specific (subtype REST_ENDPOINT): the (method, path) identity. Emitted
    # as the restApiProperties aspect when both are provided.
    method: Optional[str] = None
    path: Optional[str] = None

    @field_validator("method", mode="before")
    @classmethod
    def normalize_method(cls, v: object) -> Optional[str]:
        if v is None:
            return None
        method = str(v).upper()
        if method not in _VALID_HTTP_METHODS:
            raise ValueError(
                f"HTTP method {method!r} is not one of {sorted(_VALID_HTTP_METHODS)}"
            )
        return method

    @field_validator("subtypes", mode="before")
    @classmethod
    def normalize_subtypes(cls, v: object) -> List[str]:
        raw: List[str]
        if isinstance(v, str):
            raw = [v]
        elif isinstance(v, list):
            raw = [str(s) for s in v]
        else:
            raise ValueError("subtypes must be a string or a list of strings")
        normalized = [cls._LEGACY_TYPE_TO_SUBTYPE.get(s, s) for s in raw]
        for s in normalized:
            if s not in _VALID_SUBTYPES:
                raise ValueError(
                    f"API subtype {s} is not one of {sorted(_VALID_SUBTYPES)}"
                )
        return normalized

    @property
    def urn(self) -> str:
        if self.id.startswith("urn:li:api:"):
            return self.id
        return str(ApiUrn(self.id))

    def _mint_auditstamp(self) -> AuditStampClass:
        return AuditStampClass(time=int(time.time() * 1000.0), actor=_DEFAULT_ACTOR)

    def generate_mcp(self) -> Iterable[MetadataChangeProposalWrapper]:
        audit_stamp = self._mint_auditstamp()
        input_fields = (
            [p.to_schema_field() for p in self.parameters]
            if self.parameters is not None
            else None
        )
        output_fields = (
            [p.to_schema_field() for p in self.returns]
            if self.returns is not None
            else None
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=self.urn,
            aspect=ApiPropertiesClass(
                name=self.name,
                description=self.description,
                externalUrl=self.external_url,
                created=audit_stamp,
                lastModified=audit_stamp,
            ),
        )
        # The input/output signature is its own aspect so it can be re-ingested
        # independently when the endpoint's contract changes.
        if (
            self.schema_definition is not None
            or input_fields is not None
            or output_fields is not None
        ):
            yield MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=ApiSignatureClass(
                    schemaDefinition=self.schema_definition,
                    inputFields=input_fields,
                    outputFields=output_fields,
                ),
            )
        # REST-specific (method, path) identity, only for REST endpoints.
        if self.method is not None and self.path is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=RestApiPropertiesClass(method=self.method, path=self.path),
            )
        yield MetadataChangeProposalWrapper(
            entityUrn=self.urn, aspect=SubTypesClass(typeNames=list(self.subtypes))
        )
        if self.platform:
            yield MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=DataPlatformInstanceClass(
                    platform=builder.make_data_platform_urn(self.platform)
                ),
            )
        yield MetadataChangeProposalWrapper(
            entityUrn=self.urn, aspect=StatusClass(removed=False)
        )

    def emit(
        self,
        emitter: Emitter,
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> str:
        """Emit the API to DataHub and return its urn."""
        for mcp in self.generate_mcp():
            emitter.emit(mcp, callback)
        return self.urn
