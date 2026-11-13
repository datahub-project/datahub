"""update_description: set, extend or clear the description of an asset or column."""

import dataclasses
from typing import Literal, Optional

from datahub.metadata.urns import DatasetUrn, Urn
from fastmcp.exceptions import ToolError

from .. import arg_validation, graphql_helpers
from ..column_paths import (
    read_schema_field_paths,
    resolve_column_path,
    schema_field_paths,
)
from ..read_backend import JsonObject, get_read_backend
from ..sibling_merge import merge_siblings
from ..version_requirements import destructive, min_version

# updateDescription refuses these entity types; their own update mutations set
# the description instead, as the UI does.
_OWN_UPDATE_MUTATION = {
    "chart": "updateChartDescription",
    "dashboard": "updateDashboardDescription",
    "dataFlow": "updateDataFlowDescription",
    "dataJob": "updateDataJobDescription",
}


def _text(holder: object) -> str:
    # GraphQL answers an unset aspect with an explicit null, so a missing key
    # and a null value must read the same.
    if not isinstance(holder, dict):
        return ""
    value = holder.get("description")
    return value if isinstance(value, str) else ""


def _field_description(fields: object, column_path: str) -> str:
    if not isinstance(fields, list):
        return ""
    for field in fields:
        if isinstance(field, dict) and field.get("fieldPath") == column_path:
            return _text(field)
    return ""


def _shown_description(entity: JsonObject, column_path: Optional[str]) -> str:
    """The description DataHub shows: the one edited in DataHub when set, else
    the one from the source system."""
    if column_path is None:
        return _text(entity.get("editableProperties")) or _text(
            entity.get("properties")
        )
    editable = entity.get("editableSchemaMetadata")
    schema = entity.get("schemaMetadata")
    return _field_description(
        editable.get("editableSchemaFieldInfo") if isinstance(editable, dict) else None,
        column_path,
    ) or _field_description(
        schema.get("fields") if isinstance(schema, dict) else None, column_path
    )


@dataclasses.dataclass(frozen=True)
class _Current:
    description: str
    column_path: Optional[str]
    """The column resolved to its schema fieldPath; None for the entity itself."""


def _current_description(urn: str, column: Optional[str], *, tool: str) -> _Current:
    # A failed read must stop the write: appending to a description that could
    # not be read would replace it with the appended text alone. That includes
    # the siblings' read, whose text DataHub may be showing.
    try:
        entity = get_read_backend().description_sources(urn)
    except Exception as e:
        raise ToolError(
            f"{tool}: could not read the current description of {urn}, so nothing "
            f"was appended: {e}"
        ) from e
    if not entity:
        raise ToolError(f"{tool}: {urn} does not exist in DataHub.")
    if not any(
        key in entity for key in ("editableProperties", "properties", "schemaMetadata")
    ):
        raise ToolError(
            f"{tool}: appending is not supported for {entity.get('type') or urn}. "
            'Read its description with get_entities and use operation="replace".'
        )
    # Resolved against the dataset's own schema, before the merge, since GMS
    # writes only to a column of that schema.
    column_path = (
        None
        if column is None
        else resolve_column_path(schema_field_paths(entity), column, urn=urn, tool=tool)
    )
    # Appending to the entity's own text alone would hide the sibling text the
    # UI showed, since the edited description written here then wins.
    merge_siblings(entity)
    return _Current(_shown_description(entity, column_path), column_path)


@destructive
@min_version(cloud="0.3.16", oss="1.4.0")
def update_description(
    entity_urn: str,
    operation: Literal["replace", "append", "remove"] = "replace",
    description: Optional[str] = None,
    column_path: Optional[str] = None,
) -> dict:
    """Update description for a DataHub entity or its column (e.g., schema field).

    This tool allows you to set, append to, or remove a description for an entity or its column.
    Useful for documenting datasets, containers, charts, dashboards, data flows, data jobs,
    ML models, ML model groups, ML features, ML feature tables, ML primary keys, tags,
    glossary terms, glossary nodes, domains, data products, groups, and schema fields.

    The description is the one edited in DataHub. Where an asset also has a description
    from its source system, DataHub shows the edited one when it is set, so "remove"
    brings the source system's description back into view rather than leaving none.

    Args:
        entity_urn: Entity URN to update description for (e.g., dataset URN, container URN)
        operation: The operation to perform:
                  - "replace": Replace the existing description with the new one (default)
                  - "append": Append the new description to the one DataHub shows now,
                    which may come from a sibling such as the dataset's dbt model
                    (the text is added as given, so start it with a space or newline)
                  - "remove": Remove the description (description parameter not needed)
        description: The description text to set or append (supports markdown formatting).
                    Required for "replace" and "append" operations, ignored for "remove".
        column_path: Column_path identifier (e.g., column name for schema field).
                    Only for datasets; use None for entity-level descriptions.
                    For column-level descriptions, provide the column name (e.g., "customer_email").
                    Name a nested column by its dotted path (e.g., "address.city"). The exact
                    fieldPath list_schema_fields returns (e.g., "[version=2.0].[type=string].email")
                    works too; a column name is resolved to it.

    Returns:
        Dictionary with:
        - success: True when the description was changed
        - urn: The entity URN
        - column_path: The column's schema fieldPath (if applicable)
        - message: What was done

    Examples:
        # Update description for a container (entity-level)
        update_description(
            entity_urn="urn:li:container:12345",
            operation="replace",
            description="Production data warehouse"
        )

        # Update description for a dataset field (column-level)
        update_description(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
            operation="replace",
            description="User's primary email address",
            column_path="email"
        )

        # Append to existing field description
        update_description(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
            operation="append",
            description=" (PII)",
            column_path="email"
        )

        # Remove field description
        update_description(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
            operation="remove",
            column_path="old_field"
        )
    """
    tool = "update_description"
    problem = arg_validation.urn_error_message(
        entity_urn, tool=tool, param="entity_urn"
    )
    if problem is not None:
        raise ToolError(problem)
    urn = entity_urn.strip()
    column = (column_path or "").strip() or None
    entity_type = Urn.from_string(urn).entity_type
    if column is not None and not isinstance(Urn.from_string(urn), DatasetUrn):
        raise ToolError(
            f"{tool}: `column_path` is only for datasets; {urn} is a {entity_type}. "
            "Leave it out to update the entity's own description."
        )
    if operation in ("replace", "append") and not description:
        raise ToolError(f"{tool}: `description` is required for {operation!r}.")

    if operation == "append":
        current = _current_description(urn, column, tool=tool)
        column = current.column_path
        final = current.description + (description or "")
    else:
        if column is not None:
            column = resolve_column_path(
                read_schema_field_paths(urn, tool=tool), column, urn=urn, tool=tool
            )
        final = "" if operation == "remove" else description or ""

    target = urn + (f" column {column}" if column else "")
    unknown_outcome_hint = (
        f"Check the description of {target} with get_entities before calling {tool} "
        "again"
    )
    if operation == "append":
        unknown_outcome_hint += ": appending twice would add the text twice."
    else:
        unknown_outcome_hint += "."
    graph = graphql_helpers.get_datahub_client()._graph
    query = graphql_helpers.load_gql_document("descriptions.gql")
    own_mutation = _OWN_UPDATE_MUTATION.get(entity_type)
    if own_mutation is not None:
        result = graphql_helpers.execute_write(
            graph,
            query=query,
            operation_name=own_mutation,
            variables={"urn": urn, "description": final},
            unknown_outcome_hint=unknown_outcome_hint,
        )
        applied = any(isinstance(value, dict) for value in result.values())
    else:
        write_input: dict = {"description": final, "resourceUrn": urn}
        if column is not None:
            write_input["subResource"] = column
            write_input["subResourceType"] = "DATASET_FIELD"
        result = graphql_helpers.execute_write(
            graph,
            query=query,
            operation_name="updateDescription",
            variables={"input": write_input},
            unknown_outcome_hint=unknown_outcome_hint,
        )
        applied = bool(result.get("updateDescription"))
    if not applied:
        raise ToolError(
            f"{tool}: DataHub did not change the description of {target} (the write "
            "returned no result)."
        )
    return {
        "success": True,
        "urn": urn,
        "column_path": column,
        "message": (
            f"Description {'removed' if operation == 'remove' else 'updated'} "
            "successfully"
        ),
    }
