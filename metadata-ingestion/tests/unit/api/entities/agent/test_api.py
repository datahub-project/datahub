from typing import List

import pytest
from pydantic import ValidationError

from datahub.api.entities.agent.api import (
    API_SUBTYPE_FUNCTION,
    API_SUBTYPE_MCP_TOOL,
    API_SUBTYPE_REST_ENDPOINT,
    Api,
    ApiParam,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    ApiPropertiesClass,
    ApiSignatureClass,
    ArrayTypeClass,
    BooleanTypeClass,
    DataPlatformInstanceClass,
    NumberTypeClass,
    RestApiPropertiesClass,
    StatusClass,
    StringTypeClass,
    SubTypesClass,
)


def _mcps(api: Api) -> List[MetadataChangeProposalWrapper]:
    return list(api.generate_mcp())


def _aspects(api: Api) -> list:
    return [m.aspect for m in api.generate_mcp()]


def test_urn_from_bare_id_and_passthrough_full_urn() -> None:
    assert Api(id="get_order", name="Get Order").urn == "urn:li:api:get_order"
    full = "urn:li:api:some.custom.id"
    assert Api(id=full, name="X").urn == full


def test_minimal_api_emits_properties_subtypes_status() -> None:
    api = Api(id="get_order", name="Get Order", description="Fetch an order")
    aspects = _aspects(api)

    props = [a for a in aspects if isinstance(a, ApiPropertiesClass)]
    assert len(props) == 1
    assert props[0].name == "Get Order"
    assert props[0].description == "Fetch an order"

    subtypes = [a for a in aspects if isinstance(a, SubTypesClass)]
    assert len(subtypes) == 1
    assert subtypes[0].typeNames == [API_SUBTYPE_MCP_TOOL]

    status = [a for a in aspects if isinstance(a, StatusClass)]
    assert len(status) == 1 and status[0].removed is False

    # No signature aspect when there is no schema / params / returns.
    assert not any(isinstance(a, ApiSignatureClass) for a in aspects)
    # No REST properties without method+path.
    assert not any(isinstance(a, RestApiPropertiesClass) for a in aspects)


def test_signature_emitted_from_parameters_and_returns() -> None:
    api = Api(
        id="get_order",
        name="Get Order",
        parameters=[ApiParam(name="order_id", data_type="string", required=True)],
        returns=[ApiParam(name="order", data_type="object")],
    )
    sigs = [a for a in _aspects(api) if isinstance(a, ApiSignatureClass)]
    assert len(sigs) == 1
    sig = sigs[0]
    assert sig.inputFields is not None and len(sig.inputFields) == 1
    assert sig.outputFields is not None and len(sig.outputFields) == 1
    in_field = sig.inputFields[0]
    assert in_field.fieldPath == "order_id"
    assert in_field.nullable is False  # required -> not nullable


def test_signature_emitted_from_schema_definition_only() -> None:
    api = Api(id="x", name="X", schema_definition='{"type": "object"}')
    sigs = [a for a in _aspects(api) if isinstance(a, ApiSignatureClass)]
    assert len(sigs) == 1
    assert sigs[0].schemaDefinition == '{"type": "object"}'
    assert sigs[0].inputFields is None
    assert sigs[0].outputFields is None


def test_optional_param_maps_to_nullable_field() -> None:
    field = ApiParam(name="q", data_type="string", required=False).to_schema_field()
    assert field.nullable is True
    assert field.nativeDataType == "string"


def test_data_type_resolution_and_native_type_preserved() -> None:
    # nativeDataType always keeps the raw authored string; the union type is
    # inferred from the lowercased base type.
    cases = {
        "string": StringTypeClass,
        "integer": NumberTypeClass,
        "boolean": BooleanTypeClass,
        "array<Order>": ArrayTypeClass,  # parameterized resolves on base type
    }
    for raw, expected_cls in cases.items():
        field = ApiParam(name="f", data_type=raw).to_schema_field()
        assert isinstance(field.type.type, expected_cls)
        assert field.nativeDataType == raw


def test_unknown_data_type_falls_back_to_string_but_keeps_native() -> None:
    field = ApiParam(name="f", data_type="OrderId").to_schema_field()
    assert isinstance(field.type.type, StringTypeClass)
    assert field.nativeDataType == "OrderId"


def test_rest_endpoint_emits_rest_properties_with_normalized_method() -> None:
    api = Api(
        id="get_order",
        name="Get Order",
        subtypes=[API_SUBTYPE_REST_ENDPOINT],
        method="get",  # lowercase -> normalized to GET
        path="/orders/{orderId}",
    )
    rest = [a for a in _aspects(api) if isinstance(a, RestApiPropertiesClass)]
    assert len(rest) == 1
    assert rest[0].method == "GET"
    assert rest[0].path == "/orders/{orderId}"


def test_rest_properties_not_emitted_without_both_method_and_path() -> None:
    api = Api(id="x", name="X", method="POST")  # path missing
    assert not any(isinstance(a, RestApiPropertiesClass) for a in _aspects(api))


def test_normalize_method_rejects_invalid_verb() -> None:
    with pytest.raises(ValidationError):
        Api(id="x", name="X", method="FETCH")


def test_normalize_method_none_stays_none() -> None:
    assert Api(id="x", name="X").method is None


def test_legacy_subtype_aliases_are_mapped() -> None:
    api = Api(id="x", name="X", subtypes=["MCP", "REST_API", "CUSTOM"])
    assert api.subtypes == [
        API_SUBTYPE_MCP_TOOL,
        API_SUBTYPE_REST_ENDPOINT,
        API_SUBTYPE_FUNCTION,
    ]


def test_single_string_subtype_coerced_to_list() -> None:
    assert Api(id="x", name="X", subtypes="FUNCTION").subtypes == [API_SUBTYPE_FUNCTION]


def test_invalid_subtype_rejected() -> None:
    with pytest.raises(ValidationError):
        Api(id="x", name="X", subtypes=["NOT_A_SUBTYPE"])


def test_platform_emits_data_platform_instance() -> None:
    api = Api(id="x", name="X", platform="openapi")
    dpis = [a for a in _aspects(api) if isinstance(a, DataPlatformInstanceClass)]
    assert len(dpis) == 1
    assert dpis[0].platform == "urn:li:dataPlatform:openapi"


def test_emit_sends_all_mcps_and_returns_urn() -> None:
    class _RecordingEmitter:
        def __init__(self) -> None:
            self.emitted: List[MetadataChangeProposalWrapper] = []

        def emit(self, mcp, callback=None):  # type: ignore[no-untyped-def]
            self.emitted.append(mcp)

    api = Api(id="get_order", name="Get Order")
    emitter = _RecordingEmitter()
    urn = api.emit(emitter)  # type: ignore[arg-type]
    assert urn == "urn:li:api:get_order"
    assert len(emitter.emitted) == len(_mcps(api))
    assert all(m.entityUrn == urn for m in emitter.emitted)


def test_rest_id_is_canonical_and_lossless() -> None:
    # Method is upper-cased (matches restApiProperties.method); leading slash
    # on the path is optional and normalized away.
    assert Api.rest_id("svc", "get", "/orders") == "svc/GET/orders"
    assert Api.rest_id("svc", "GET", "orders") == "svc/GET/orders"

    # A path parameter and a same-shaped literal path do NOT collide — the
    # braces are preserved rather than stripped.
    templated = Api.rest_id("svc", "GET", "/orders/{orderId}")
    literal = Api.rest_id("svc", "GET", "/orders/orderId")
    assert templated == "svc/GET/orders/{orderId}"
    assert templated != literal

    # The id is a valid, round-trippable api urn.
    assert Api(id=templated, name="x").urn == "urn:li:api:svc/GET/orders/{orderId}"


def test_rest_id_rejects_invalid_method() -> None:
    with pytest.raises(ValueError):
        Api.rest_id("svc", "FETCH", "/orders")
