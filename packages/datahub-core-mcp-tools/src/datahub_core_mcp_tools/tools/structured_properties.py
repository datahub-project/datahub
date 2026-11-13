"""Structured property tools: add_structured_properties sets typed property values
on assets; remove_structured_properties takes properties off them."""

from datetime import date
from typing import Annotated, Dict, List, Literal, Union

from datahub.metadata.urns import Urn
from fastmcp.exceptions import ToolError
from pydantic import WithJsonSchema

from .. import graphql_helpers
from ..arg_validation import StrList
from ..label_writes import require_entity_urns
from ..read_backend import JsonObject, get_read_backend
from ..version_requirements import destructive, idempotent, min_version

_PROPERTY_PREFIX = "urn:li:structuredProperty:"
_FIND_PROPERTIES = (
    'Find structured properties with search(query="<name>", '
    'filter="entity_type = structuredProperty").'
)
# Failures named in a partial-failure message; the rest are counted.
_FAILURES_SHOWN = 5

# Published as a plain string: some providers reject a multi-type anyOf, and
# Gemini accepts only a single `type`, so {"type": ["string", "number"]} is not
# safe either. Numbers still work: numeric strings are parsed for numeric
# properties, and the runtime union keeps accepting int and float.
StructuredPropertyValue = Annotated[
    Union[str, float, int], WithJsonSchema({"type": "string"})
]


def _require_property_urn(value: str, *, tool: str, param: str) -> str:
    urn = value.strip()
    if not urn.startswith(_PROPERTY_PREFIX):
        raise ToolError(
            f"{tool}: `{param}` must be a structured property URN "
            f"({_PROPERTY_PREFIX}...), got {value!r}. {_FIND_PROPERTIES}"
        )
    return urn


def _definition(urn: str, *, tool: str) -> JsonObject:
    entity = get_read_backend().structured_property(urn)
    definition = entity.get("definition") if entity else None
    # A missing property still comes back, with exists=false and a placeholder
    # definition that would let any value through.
    if not isinstance(definition, dict) or (entity or {}).get("exists") is False:
        raise ToolError(
            f"{tool}: structured property {urn} does not exist in DataHub. "
            f"{_FIND_PROPERTIES}"
        )
    return definition


def _value_type(definition: JsonObject) -> str:
    value_type = definition.get("valueType")
    info = value_type.get("info") if isinstance(value_type, dict) else None
    name = info.get("qualifiedName") if isinstance(info, dict) else None
    return name.lower() if isinstance(name, str) else ""


def _property_value(
    value: Union[str, float, int], value_type: str, *, urn: str, tool: str
) -> dict:
    """``value`` as a PropertyValueInput for a property of ``value_type``
    (datahub.string, datahub.number, datahub.urn, datahub.date,
    datahub.rich_text)."""
    if any(t in value_type for t in ("number", "int", "float", "double", "long")):
        try:
            return {"numberValue": float(value)}
        except ValueError:
            raise ToolError(
                f"{tool}: {urn} takes numbers ({value_type}), got {value!r}."
            ) from None
    text = str(value)
    if "urn" in value_type:
        try:
            Urn.from_string(text)
        except Exception:
            raise ToolError(
                f"{tool}: {urn} takes DataHub URNs ({value_type}), got {text!r}."
            ) from None
    elif "date" in value_type:
        # GMS stores a plain date and accepts only YYYY-MM-DD, not a datetime.
        try:
            if len(text) != 10:
                raise ValueError(text)
            date.fromisoformat(text)
        except ValueError:
            raise ToolError(
                f"{tool}: {urn} takes dates as YYYY-MM-DD ({value_type}) such as "
                f"2024-12-22, got {text!r}."
            ) from None
    return {"stringValue": text}


def _write_each(
    entity_urns: List[str],
    *,
    tool: str,
    operation_name: str,
    input_for: Dict[str, object],
    verb: Literal["added", "removed"],
    count: int,
) -> dict:
    """Run one write per asset (GMS has no batch form). Every argument was checked
    first, so a failure here is GMS refusing an asset or not answering."""
    graph = graphql_helpers.get_datahub_client()._graph
    query = graphql_helpers.load_gql_document("structured_properties.gql")
    applied: List[str] = []
    failed: List[str] = []
    for urn in entity_urns:
        try:
            result = graphql_helpers.execute_write(
                graph,
                query=query,
                operation_name=operation_name,
                variables={"input": {"assetUrn": urn, **input_for}},
                unknown_outcome_hint=(
                    f"Check {urn}'s structured properties with get_entities."
                ),
            )
        except ToolError as e:
            failed.append(f"{urn}: {e}")
            continue
        if result.get(operation_name):
            applied.append(urn)
        else:
            failed.append(f"{urn}: DataHub returned no result")
    if failed:
        shown = "; ".join(failed[:_FAILURES_SHOWN])
        more = len(failed) - _FAILURES_SHOWN
        raise ToolError(
            f"{tool}: applied to {len(applied)} of {len(entity_urns)} entit(ies)"
            + (f" ({', '.join(applied)})" if applied else "")
            + f". Failed: {shown}"
            + (f"; and {more} more" if more > 0 else "")
            + ". Calling it again for the failed entities is safe."
        )
    preposition = "to" if verb == "added" else "from"
    return {
        "success": True,
        "entities_updated": len(applied),
        "message": (
            f"Successfully {verb} {count} structured propert(ies) {preposition} "
            f"{len(applied)} entit(ies){graphql_helpers.PER_CALL_SCOPE_NOTE}"
        ),
    }


@destructive
@idempotent
@min_version(cloud="0.3.16", oss="1.4.0")
def add_structured_properties(
    property_values: Dict[str, List[StructuredPropertyValue]],
    entity_urns: StrList,
) -> dict:
    """Add structured properties with values to multiple DataHub entities.

    This tool allows you to assign structured properties to multiple entities in a single operation.
    Structured properties are schema-defined metadata fields that can store typed values (strings, numbers, etc.).
    The values given for a property replace that property's current values on each entity;
    other properties are left as they are.

    Args:
        property_values: Dictionary mapping structured property URNs to lists of values.
                        Values are strings; pass numbers as numeric strings ("0.95").
                        Example: {
                            "urn:li:structuredProperty:io.acryl.privacy.retentionTime": ["90"],
                            "urn:li:structuredProperty:io.acryl.common.businessCriticality": ["HIGH"]
                        }
        entity_urns: List of entity URNs to assign properties to (e.g., dataset URNs, dashboard URNs)

    Returns:
        Dictionary with:
        - success: True when every entity was updated
        - entities_updated: How many entities were updated
        - message: What was done

    Examples:
        # Add retention time and criticality to datasets
        add_structured_properties(
            property_values={
                "urn:li:structuredProperty:io.acryl.privacy.retentionTime": ["90"],
                "urn:li:structuredProperty:io.acryl.common.businessCriticality": ["HIGH"]
            },
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
            ]
        )

        # Add numeric property
        add_structured_properties(
            property_values={
                "urn:li:structuredProperty:io.acryl.dataQuality.scoreThreshold": ["0.95"]
            },
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.verified_data,PROD)"
            ]
        )

        # Add multiple values for a multi-valued property
        add_structured_properties(
            property_values={
                "urn:li:structuredProperty:io.acryl.common.dataClassification": ["PII", "SENSITIVE"]
            },
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ]
        )
    """
    tool = "add_structured_properties"
    if not property_values:
        raise ToolError(f"{tool}: `property_values` must name at least one property.")
    entities = require_entity_urns(entity_urns, tool=tool)
    params: List[dict] = []
    for key, values in property_values.items():
        urn = _require_property_urn(key, tool=tool, param="property_values")
        if not values:
            raise ToolError(
                f"{tool}: give at least one value for {urn}, or use "
                "remove_structured_properties to take it off."
            )
        definition = _definition(urn, tool=tool)
        value_type = _value_type(definition)
        if definition.get("cardinality") == "SINGLE" and len(values) > 1:
            raise ToolError(f"{tool}: {urn} takes a single value, got {len(values)}.")
        params.append(
            {
                "structuredPropertyUrn": urn,
                "values": [
                    _property_value(v, value_type, urn=urn, tool=tool) for v in values
                ],
            }
        )
    return _write_each(
        entities,
        tool=tool,
        operation_name="upsertStructuredProperties",
        input_for={"structuredPropertyInputParams": params},
        verb="added",
        count=len(params),
    )


@destructive
@idempotent
@min_version(cloud="0.3.16", oss="1.4.0")
def remove_structured_properties(
    property_urns: StrList,
    entity_urns: StrList,
) -> dict:
    """Remove structured properties from multiple DataHub entities.

    This tool allows you to remove structured property assignments from multiple entities in a single operation.

    Args:
        property_urns: List of structured property URNs to remove
                      Example: ["urn:li:structuredProperty:io.acryl.privacy.retentionTime"]
        entity_urns: List of entity URNs to remove properties from (e.g., dataset URNs, dashboard URNs)

    Returns:
        Dictionary with:
        - success: True when every entity was updated
        - entities_updated: How many entities were updated
        - message: What was done

    Examples:
        # Remove retention time property from datasets
        remove_structured_properties(
            property_urns=["urn:li:structuredProperty:io.acryl.privacy.retentionTime"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_data,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.archived,PROD)"
            ]
        )
    """
    tool = "remove_structured_properties"
    if not property_urns:
        raise ToolError(f"{tool}: `property_urns` must name at least one property.")
    # The properties need not exist: removing a reference to a property that was
    # since deleted is how that reference gets cleaned up.
    properties = list(
        dict.fromkeys(
            _require_property_urn(value, tool=tool, param=f"property_urns[{index}]")
            for index, value in enumerate(property_urns)
        )
    )
    entities = require_entity_urns(entity_urns, tool=tool)
    return _write_each(
        entities,
        tool=tool,
        operation_name="removeStructuredProperties",
        input_for={"structuredPropertyUrns": properties},
        verb="removed",
        count=len(properties),
    )
