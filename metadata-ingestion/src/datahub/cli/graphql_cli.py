import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click

from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)

# GraphQL introspection queries (split to avoid "bad faith" protection)
QUERY_INTROSPECTION = """
query QueryIntrospection {
  __schema {
    queryType { 
      name
      fields {
        name
        description
        args {
          name
          type {
            name
            kind
            ofType {
              name
              kind
              ofType {
                name
                kind
              }
            }
          }
        }
      }
    }
  }
}
"""

MUTATION_INTROSPECTION = """
query MutationIntrospection {
  __schema {
    mutationType { 
      name
      fields {
        name
        description
        args {
          name
          type {
            name
            kind
            ofType {
              name
              kind
              ofType {
                name
                kind
              }
            }
          }
        }
      }
    }
  }
}
"""


def _is_file_path(value: str) -> bool:
    """Check if a string appears to be a file path and the file exists."""
    if not value or len(value) < 2:
        return False

    resolved_path = Path(value).resolve()
    return resolved_path.exists()


def _load_content_or_file(value: str) -> str:
    """Load content from file if value is a file path, otherwise return value as-is."""
    if _is_file_path(value):
        resolved_path = Path(value).resolve()

        # Security check: prevent path traversal attacks
        if "../" in str(resolved_path) or "..\\" in str(resolved_path):
            raise ValueError("Invalid file path: path traversal detected")

        with open(resolved_path, "r") as f:
            return f.read()
    return value


def _parse_variables(variables_str: Optional[str]) -> Optional[Dict[str, Any]]:
    """Parse variables from JSON string or file."""
    if not variables_str:
        return None

    content = _load_content_or_file(variables_str)
    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        raise click.ClickException(f"Invalid JSON in variables: {e}") from e


def _get_schema_files_path() -> Path:
    """Get the path to DataHub's GraphQL schema files."""
    # Try to find the schema files relative to the current package
    current_file = Path(__file__)
    repo_root = current_file

    # Go up directories until we find the repo root (contains datahub-graphql-core)
    for _ in range(10):  # Safety limit
        repo_root = repo_root.parent
        graphql_core_path = (
            repo_root / "datahub-graphql-core" / "src" / "main" / "resources"
        )
        if graphql_core_path.exists():
            return graphql_core_path

    # Fallback: try common relative paths
    possible_paths = [
        Path("../../../datahub-graphql-core/src/main/resources"),
        Path("../../../../datahub-graphql-core/src/main/resources"),
        Path("datahub-graphql-core/src/main/resources"),
    ]

    for path in possible_paths:
        if path.exists():
            return path.resolve()

    raise FileNotFoundError("Could not find DataHub GraphQL schema files")


def _parse_graphql_operations_from_files(
    custom_schema_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Parse operations from DataHub's GraphQL schema files as fallback."""
    try:
        if custom_schema_path:
            schema_path = Path(custom_schema_path)
            if not schema_path.exists():
                raise FileNotFoundError(
                    f"Custom schema path does not exist: {custom_schema_path}"
                )
            logger.debug(f"Using custom GraphQL schema path: {schema_path}")
        else:
            schema_path = _get_schema_files_path()
            logger.debug(f"Found GraphQL schema files at: {schema_path}")

        queries = []
        mutations = []

        # Parse all .graphql files in the directory
        for graphql_file in schema_path.glob("*.graphql"):
            content = graphql_file.read_text()

            # Extract queries using regex
            query_matches = re.finditer(
                r"extend\s+type\s+Query\s*\{([^}]+)\}|type\s+Query\s*\{([^}]+)\}",
                content,
                re.DOTALL | re.IGNORECASE,
            )

            for match in query_matches:
                query_content = match.group(1) or match.group(2)
                operations = _parse_operations_from_content(query_content, "Query")
                queries.extend(operations)

            # Extract mutations using regex
            mutation_matches = re.finditer(
                r"extend\s+type\s+Mutation\s*\{([^}]+)\}|type\s+Mutation\s*\{([^}]+)\}",
                content,
                re.DOTALL | re.IGNORECASE,
            )

            for match in mutation_matches:
                mutation_content = match.group(1) or match.group(2)
                operations = _parse_operations_from_content(
                    mutation_content, "Mutation"
                )
                mutations.extend(operations)

        logger.debug(
            f"Parsed {len(queries)} queries and {len(mutations)} mutations from schema files"
        )

        return {
            "queryType": {"fields": queries} if queries else None,
            "mutationType": {"fields": mutations} if mutations else None,
        }

    except Exception as e:
        logger.error(f"Failed to parse GraphQL schema files: {e}")
        logger.error("Cannot proceed without valid schema information.")
        logger.error("Please ensure:")
        logger.error("1. DataHub GMS is accessible for schema introspection")
        logger.error("2. Schema files exist and are valid GraphQL")
        logger.error("3. Network connectivity allows GraphQL requests")
        raise click.ClickException(
            f"Schema loading failed: {e}. Cannot determine available GraphQL operations."
        ) from e


def _parse_operations_from_content(
    content: str, operation_type: str
) -> List[Dict[str, Any]]:
    """Parse individual operations from GraphQL content."""
    operations = []

    # Match field definitions with optional descriptions
    # Pattern matches: fieldName(args): ReturnType or "description" fieldName(args): ReturnType
    field_pattern = (
        r'(?:"""([^"]+)"""\s*|"([^"]+)"\s*)?(\w+)(?:\([^)]*\))?\s*:\s*[^,\n]+'
    )

    matches = re.finditer(field_pattern, content, re.MULTILINE)

    for match in matches:
        description1, description2, field_name = match.groups()
        description = description1 or description2 or ""

        # Skip common GraphQL keywords and types
        if field_name.lower() in [
            "query",
            "mutation",
            "subscription",
            "type",
            "input",
            "enum",
        ]:
            continue

        operation: Dict[str, Any] = {
            "name": field_name,
            "description": description.strip(),
            "args": [],  # We could parse args too, but for now keep it simple
        }
        operations.append(operation)

    return operations


def _format_operation_list(
    operations: List[Dict[str, Any]], operation_type: str
) -> str:
    """Format operations list for display."""
    if not operations:
        return f"No {operation_type.lower()} operations found."

    lines = [f"{operation_type}:"]
    for op in operations:
        name = op.get("name", "Unknown")
        description = op.get("description", "")
        if description:
            lines.append(f"  - {name}: {description}")
        else:
            lines.append(f"  - {name}")

    return "\n".join(lines)


def _find_input_type(
    schema: Dict[str, Any], type_name: str
) -> Optional[Dict[str, Any]]:
    """Find an input type definition in the schema."""
    types = schema.get("types", [])
    for type_def in types:
        if type_def.get("name") == type_name and type_def.get("kind") == "INPUT_OBJECT":
            return type_def
    return None


def _format_operation_details(
    operation: Dict[str, Any],
    operation_type: str,
    schema: Optional[Dict[str, Any]] = None,
) -> str:
    """Format detailed operation information."""
    name = operation.get("name", "Unknown")
    description = operation.get("description", "No description available")
    args = operation.get("args", [])

    lines = [
        f"Operation: {name}",
        f"Type: {operation_type}",
        f"Description: {description}",
    ]

    if args:
        lines.append("Arguments:")
        for arg in args:
            arg_name = arg.get("name", "unknown")
            arg_type = _format_graphql_type(arg.get("type", {}))
            lines.append(f"  - {arg_name}: {arg_type}")

            # If we have schema info, try to show input type fields
            if schema:
                # Extract the base type name (remove ! and [] wrappers)
                base_type_name = _extract_base_type_name(arg.get("type", {}))
                if base_type_name:
                    input_type = _find_input_type(schema, base_type_name)
                    if input_type:
                        input_fields = input_type.get("inputFields", [])
                        if input_fields:
                            lines.append(f"    Fields in {base_type_name}:")
                            for field in input_fields:
                                field_name = field.get("name", "unknown")
                                field_type = _format_graphql_type(field.get("type", {}))
                                field_desc = field.get("description", "")
                                if field_desc:
                                    lines.append(
                                        f"      - {field_name}: {field_type} - {field_desc}"
                                    )
                                else:
                                    lines.append(f"      - {field_name}: {field_type}")
    else:
        lines.append("Arguments: None")

    return "\n".join(lines)


def _format_operation_details_recursive(
    operation: Dict[str, Any], operation_type: str, client: Any
) -> str:
    """Format detailed operation information with recursive type exploration."""
    name = operation.get("name", "Unknown")
    description = operation.get("description", "No description available")
    args = operation.get("args", [])

    lines = [
        f"Operation: {name}",
        f"Type: {operation_type}",
        f"Description: {description}",
    ]

    if args:
        lines.append("Arguments:")

        # Collect all input types for recursive exploration
        all_types_to_explore = set()

        for arg in args:
            arg_name = arg.get("name", "unknown")
            arg_type = _format_graphql_type(arg.get("type", {}))
            lines.append(f"  - {arg_name}: {arg_type}")

            # Collect base type name for recursive exploration
            base_type_name = _extract_base_type_name(arg.get("type", {}))
            if base_type_name and base_type_name not in [
                "String",
                "Int",
                "Float",
                "Boolean",
                "ID",
            ]:
                all_types_to_explore.add(base_type_name)

        # Recursively explore all collected types
        if all_types_to_explore:
            lines.append("")  # Empty line before type details
            lines.append("Input Type Details:")

            all_explored_types = {}
            for type_name in all_types_to_explore:
                logger.debug(f"Recursively exploring input type: {type_name}")
                try:
                    explored_types = _fetch_type_recursive(client, type_name)
                    all_explored_types.update(explored_types)
                except Exception as e:
                    logger.debug(f"Failed to explore type {type_name}: {e}")

            # Format all explored types
            if all_explored_types:
                lines.append("")
                for type_name in sorted(all_explored_types.keys()):
                    type_info = all_explored_types[type_name]
                    lines.append(f"{type_name}:")
                    lines.extend(_format_single_type_fields(type_info))
                    lines.append("")  # Empty line between types
    else:
        lines.append("Arguments: None")

    return "\n".join(lines).rstrip()


def _format_type_details(input_type: Dict[str, Any]) -> str:
    """Format detailed input type information."""
    type_name = input_type.get("name", "Unknown")
    type_kind = input_type.get("kind", "")
    input_fields = input_type.get("inputFields", [])
    enum_values = input_type.get("enumValues", [])

    lines = [
        f"Type: {type_name}",
        f"Kind: {type_kind}",
    ]

    if input_fields:
        lines.append("Fields:")
        for field in input_fields:
            field_name = field.get("name", "unknown")
            field_type = _format_graphql_type(field.get("type", {}))
            field_desc = field.get("description", "")
            if field_desc:
                lines.append(f"  - {field_name}: {field_type} - {field_desc}")
            else:
                lines.append(f"  - {field_name}: {field_type}")
    elif enum_values:
        lines.append("Enum Values:")
        for enum_value in enum_values:
            value_name = enum_value.get("name", "unknown")
            value_desc = enum_value.get("description", "")
            is_deprecated = enum_value.get("isDeprecated", False)
            deprecation_reason = enum_value.get("deprecationReason", "")

            value_line = f"  - {value_name}"
            if value_desc:
                value_line += f" - {value_desc}"
            if is_deprecated:
                if deprecation_reason:
                    value_line += f" (DEPRECATED: {deprecation_reason})"
                else:
                    value_line += " (DEPRECATED)"
            lines.append(value_line)
    else:
        if type_kind == "ENUM":
            lines.append("Enum Values: None")
        else:
            lines.append("Fields: None")

    return "\n".join(lines)


def _collect_nested_types(
    type_info: Dict[str, Any], visited: Optional[set] = None
) -> List[str]:
    """Collect all nested type names from a GraphQL type definition."""
    if visited is None:
        visited = set()

    nested_types = []
    input_fields = type_info.get("inputFields", [])

    for field in input_fields:
        field_type = field.get("type", {})
        base_type_name = _extract_base_type_name(field_type)

        if base_type_name and base_type_name not in visited:
            # Only collect custom/complex types (not built-in scalars)
            if base_type_name not in ["String", "Int", "Float", "Boolean", "ID"]:
                nested_types.append(base_type_name)
                # Don't add to visited here - let _fetch_type_recursive handle that

    return nested_types


def _fetch_type_recursive(
    client: Any, type_name: str, visited: Optional[set] = None
) -> Dict[str, Dict[str, Any]]:
    """Recursively fetch a type and all its nested types."""
    if visited is None:
        visited = set()

    if type_name in visited:
        return {}

    visited.add(type_name)
    types_map = {}

    # Fetch the current type
    try:
        targeted_query = f"""
        query DescribeType {{
          __type(name: "{type_name}") {{
            name
            kind
            inputFields {{
              name
              description
              type {{
                name
                kind
                ofType {{
                  name
                  kind
                  ofType {{
                    name
                    kind
                  }}
                }}
              }}
            }}
            enumValues {{
              name
              description
              isDeprecated
              deprecationReason
            }}
          }}
        }}
        """

        type_result = client.execute_graphql(targeted_query)
        type_info = type_result.get("__type")

        if type_info:
            types_map[type_name] = type_info

            # Find nested types
            nested_type_names = _collect_nested_types(type_info, visited)
            logger.debug(f"Type '{type_name}' has nested types: {nested_type_names}")

            # Recursively fetch nested types
            for nested_type_name in nested_type_names:
                logger.debug(f"Recursively fetching nested type: {nested_type_name}")
                nested_types = _fetch_type_recursive(client, nested_type_name, visited)
                types_map.update(nested_types)
                if nested_type_name in nested_types:
                    logger.debug(f"Successfully fetched type: {nested_type_name}")
                else:
                    logger.debug(f"Failed to fetch type: {nested_type_name}")

    except Exception as e:
        logger.debug(f"Failed to fetch type {type_name}: {e}")

    return types_map


def _format_single_type_fields(
    type_info: Dict[str, Any], indent: str = "  "
) -> List[str]:
    """Format fields or enum values for a single type."""
    lines = []
    input_fields = type_info.get("inputFields", [])
    enum_values = type_info.get("enumValues", [])
    type_kind = type_info.get("kind", "")

    if input_fields:
        for field in input_fields:
            field_name = field.get("name", "unknown")
            field_type = _format_graphql_type(field.get("type", {}))
            field_desc = field.get("description", "")
            if field_desc:
                lines.append(f"{indent}{field_name}: {field_type} - {field_desc}")
            else:
                lines.append(f"{indent}{field_name}: {field_type}")
    elif enum_values:
        for enum_value in enum_values:
            value_name = enum_value.get("name", "unknown")
            value_desc = enum_value.get("description", "")
            is_deprecated = enum_value.get("isDeprecated", False)
            deprecation_reason = enum_value.get("deprecationReason", "")

            value_line = f"{indent}{value_name}"
            if value_desc:
                value_line += f" - {value_desc}"
            if is_deprecated:
                if deprecation_reason:
                    value_line += f" (DEPRECATED: {deprecation_reason})"
                else:
                    value_line += " (DEPRECATED)"
            lines.append(value_line)
    else:
        if type_kind == "ENUM":
            lines.append(f"{indent}(no enum values)")
        else:
            lines.append(f"{indent}(no fields)")

    return lines


def _format_recursive_types(
    types_map: Dict[str, Dict[str, Any]], root_type_name: str
) -> str:
    """Format multiple types in a hierarchical display."""
    lines = []

    # Display root type first
    if root_type_name in types_map:
        root_type = types_map[root_type_name]
        lines.append(f"{root_type_name}:")
        lines.extend(_format_single_type_fields(root_type))
        lines.append("")  # Empty line after root type

    # Display nested types
    for type_name, type_info in types_map.items():
        if type_name == root_type_name:
            continue  # Already displayed

        lines.append(f"{type_name}:")
        lines.extend(_format_single_type_fields(type_info))
        lines.append("")  # Empty line between types

    return "\n".join(lines).rstrip()


def _extract_base_type_name(type_info: Dict[str, Any]) -> Optional[str]:
    """Extract the base type name from a GraphQL type (removing NON_NULL and LIST wrappers)."""
    if not type_info:
        return None

    kind = type_info.get("kind", "")
    name = type_info.get("name")
    of_type = type_info.get("ofType")

    if kind in ["NON_NULL", "LIST"] and of_type:
        return _extract_base_type_name(of_type)
    elif name:
        return name
    else:
        return None


def _format_graphql_type(type_info: Dict[str, Any]) -> str:
    """Format GraphQL type information for display."""
    kind = type_info.get("kind", "")
    name = type_info.get("name")
    of_type = type_info.get("ofType")

    if kind == "NON_NULL":
        inner_type = _format_graphql_type(of_type) if of_type else "Unknown"
        return f"{inner_type}!"
    elif kind == "LIST":
        inner_type = _format_graphql_type(of_type) if of_type else "Unknown"
        return f"[{inner_type}]"
    elif name:
        return name
    else:
        return "Unknown"


def _find_operation_by_name(
    schema: Dict[str, Any], operation_name: str
) -> Optional[tuple[Dict[str, Any], str]]:
    """Find an operation by name in queries or mutations."""
    # Search in queries
    query_type = schema.get("queryType", {})
    if query_type:
        for field in query_type.get("fields", []):
            if field.get("name") == operation_name:
                return field, "Query"

    # Search in mutations
    mutation_type = schema.get("mutationType", {})
    if mutation_type:
        for field in mutation_type.get("fields", []):
            if field.get("name") == operation_name:
                return field, "Mutation"

    return None


def _find_type_by_name(client: Any, type_name: str) -> Optional[Dict[str, Any]]:
    """Find a type by name using GraphQL introspection."""
    try:
        targeted_query = f"""
        query DescribeType {{
          __type(name: "{type_name}") {{
            name
            kind
            inputFields {{
              name
              description
              type {{
                name
                kind
                ofType {{
                  name
                  kind
                  ofType {{
                    name
                    kind
                  }}
                }}
              }}
            }}
            enumValues {{
              name
              description
              isDeprecated
              deprecationReason
            }}
          }}
        }}
        """

        type_result = client.execute_graphql(targeted_query)
        return type_result.get("__type")

    except Exception as e:
        logger.debug(f"Failed to fetch type {type_name}: {e}")
        return None


def _search_operation_and_type(
    schema: Dict[str, Any], client: Any, name: str
) -> Tuple[Optional[Tuple[Dict[str, Any], str]], Optional[Dict[str, Any]]]:
    """Search for both operation and type with the given name."""
    operation_info = _find_operation_by_name(schema, name)
    type_info = _find_type_by_name(client, name)
    return operation_info, type_info


def _convert_type_to_json(type_info: Dict[str, Any]) -> Dict[str, Any]:
    """Convert GraphQL type info to LLM-friendly JSON format."""
    if not type_info:
        return {}

    kind = type_info.get("kind", "")
    name = type_info.get("name")
    of_type = type_info.get("ofType")

    result = {"kind": kind}

    if name:
        result["name"] = name

    if kind in ["NON_NULL", "LIST"] and of_type:
        result["ofType"] = _convert_type_to_json(of_type)
    elif kind == "NON_NULL":
        result["nonNull"] = True
    elif kind == "LIST":
        result["list"] = True

    return result


def _convert_operation_to_json(
    operation: Dict[str, Any], operation_type: str
) -> Dict[str, Any]:
    """Convert operation info to LLM-friendly JSON format."""
    result = {
        "name": operation.get("name", ""),
        "type": operation_type,
        "description": operation.get("description", ""),
        "arguments": [],
    }

    for arg in operation.get("args", []):
        arg_json = {
            "name": arg.get("name", ""),
            "type": _convert_type_to_json(arg.get("type", {})),
            "description": arg.get("description", ""),
        }

        # Determine if required based on NON_NULL wrapper
        arg_type = arg.get("type", {})
        arg_json["required"] = arg_type.get("kind") == "NON_NULL"

        result["arguments"].append(arg_json)

    return result


def _convert_type_details_to_json(type_info: Dict[str, Any]) -> Dict[str, Any]:
    """Convert type details to LLM-friendly JSON format."""
    result = {
        "name": type_info.get("name", ""),
        "kind": type_info.get("kind", ""),
        "description": type_info.get("description", ""),
    }

    # Handle input fields for INPUT_OBJECT types
    input_fields = type_info.get("inputFields", [])
    if input_fields:
        result["fields"] = []
        for field in input_fields:
            field_json = {
                "name": field.get("name", ""),
                "type": _convert_type_to_json(field.get("type", {})),
                "description": field.get("description", ""),
            }
            result["fields"].append(field_json)

    # Handle enum values for ENUM types
    enum_values = type_info.get("enumValues", [])
    if enum_values:
        result["values"] = []
        for enum_value in enum_values:
            value_json = {
                "name": enum_value.get("name", ""),
                "description": enum_value.get("description", ""),
                "deprecated": enum_value.get("isDeprecated", False),
            }
            if enum_value.get("deprecationReason"):
                value_json["deprecationReason"] = enum_value.get("deprecationReason")
            result["values"].append(value_json)

    return result


def _convert_operations_list_to_json(schema: Dict[str, Any]) -> Dict[str, Any]:
    """Convert operations list to LLM-friendly JSON format."""
    result: Dict[str, Any] = {"schema": {"queries": [], "mutations": []}}

    # Convert queries
    query_type = schema.get("queryType", {})
    if query_type:
        for field in query_type.get("fields", []):
            result["schema"]["queries"].append(
                _convert_operation_to_json(field, "Query")
            )

    # Convert mutations
    mutation_type = schema.get("mutationType", {})
    if mutation_type:
        for field in mutation_type.get("fields", []):
            result["schema"]["mutations"].append(
                _convert_operation_to_json(field, "Mutation")
            )

    return result


def _convert_describe_to_json(
    operation_info: Optional[tuple[Dict[str, Any], str]],
    type_info: Optional[Dict[str, Any]],
    types_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Convert describe output to LLM-friendly JSON format."""
    result = {}

    if operation_info:
        operation_details, operation_type = operation_info
        result["operation"] = _convert_operation_to_json(
            operation_details, operation_type
        )

    if type_info:
        result["type"] = _convert_type_details_to_json(type_info)

    if types_map:
        result["relatedTypes"] = {}
        for type_name, type_data in types_map.items():
            result["relatedTypes"][type_name] = _convert_type_details_to_json(type_data)

    return result


def _dict_to_graphql_input(obj: Dict[str, Any]) -> str:
    """Convert a Python dict to GraphQL input syntax."""
    if not isinstance(obj, dict):
        return str(obj)

    items = []
    for key, value in obj.items():
        if isinstance(value, str):
            items.append(f'{key}: "{value}"')
        elif isinstance(value, dict):
            items.append(f"{key}: {_dict_to_graphql_input(value)}")
        elif isinstance(value, list):
            list_items = []
            for item in value:
                if isinstance(item, str):
                    list_items.append(f'"{item}"')
                elif isinstance(item, dict):
                    list_items.append(_dict_to_graphql_input(item))
                else:
                    list_items.append(str(item))
            items.append(f"{key}: [{', '.join(list_items)}]")
        elif isinstance(value, bool):
            items.append(f"{key}: {str(value).lower()}")
        else:
            items.append(f"{key}: {value}")

    return "{" + ", ".join(items) + "}"


def _generate_operation_query(
    operation_field: Dict[str, Any],
    operation_type: str,
    variables: Optional[Dict[str, Any]] = None,
) -> str:
    """Generate a GraphQL query string from an operation field definition."""
    operation_name = operation_field.get("name", "unknown")
    args = operation_field.get("args", [])

    # Build arguments string
    args_string = ""
    if args:
        # Check for required arguments
        required_args = []
        optional_args = []

        for arg in args:
            arg_name = arg.get("name")
            arg_type = arg.get("type", {})
            if arg_type.get("kind") == "NON_NULL":
                required_args.append(arg_name)
            else:
                optional_args.append(arg_name)

        if variables:
            # Build arguments from provided variables
            valid_args = []
            for arg in args:
                arg_name = arg.get("name")
                if arg_name and arg_name in variables:
                    # Use inline value instead of variable syntax for simplicity
                    value = variables[arg_name]
                    if isinstance(value, str):
                        # Handle string values with quotes
                        formatted_value = f'"{value}"'
                    elif isinstance(value, dict):
                        # Handle object/input types - convert to GraphQL syntax
                        formatted_value = _dict_to_graphql_input(value)
                    else:
                        # Handle numbers, booleans, etc.
                        formatted_value = (
                            str(value).lower()
                            if isinstance(value, bool)
                            else str(value)
                        )

                    valid_args.append(f"{arg_name}: {formatted_value}")

            if valid_args:
                args_string = f"({', '.join(valid_args)})"

        # Check if all required arguments are provided
        if required_args:
            missing_required = [
                arg for arg in required_args if not variables or arg not in variables
            ]
            if missing_required:
                raise click.ClickException(
                    f"Operation '{operation_name}' requires arguments: {', '.join(missing_required)}. "
                    f'Provide them using --variables \'{{"{missing_required[0]}": "value", ...}}\''
                )

    # Generate basic field selection based on common patterns
    if operation_name == "me":
        # Special case for 'me' query - we know it returns AuthenticatedUser
        field_selection = "{ corpUser { urn username properties { displayName email firstName lastName title } } }"
    elif operation_name.startswith("list"):
        # List operations typically return paginated results
        entity_name = operation_name.replace("list", "").lower()
        if entity_name == "users":
            field_selection = (
                "{ total users { urn username properties { displayName email } } }"
            )
        else:
            # Generic list response
            field_selection = "{ total }"
    elif operation_name in ["corpUser", "dataset", "dashboard", "chart"]:
        # Entity queries typically return the entity with basic fields
        field_selection = "{ urn }"
    else:
        # Default minimal selection
        field_selection = ""

    # Construct the query
    operation_keyword = operation_type.lower()
    query = f"{operation_keyword} {{ {operation_name}{args_string} {field_selection} }}"

    return query


def _get_schema_via_introspection(client: Any) -> Dict[str, Any]:
    """Get GraphQL schema via introspection only (no fallback for explicit requests)."""
    try:
        # Make two separate requests to avoid "bad faith" introspection protection
        query_result = client.execute_graphql(QUERY_INTROSPECTION)
        mutation_result = client.execute_graphql(MUTATION_INTROSPECTION)

        # Combine results
        schema = {}
        if query_result and "__schema" in query_result:
            schema.update(query_result["__schema"])
        if mutation_result and "__schema" in mutation_result:
            schema.update(mutation_result["__schema"])

        logger.debug("Successfully fetched schema via introspection")
        return schema
    except Exception as e:
        logger.error(f"GraphQL introspection failed: {e}")
        logger.error("Cannot perform introspection. Please ensure:")
        logger.error("1. DataHub GMS is running and accessible")
        logger.error("2. Network connectivity allows GraphQL requests")
        logger.error("3. Authentication credentials are valid")
        raise click.ClickException(
            f"Schema introspection failed: {e}. Cannot retrieve live schema information."
        ) from e


def _handle_list_operations(
    schema: Dict[str, Any],
    format: str,
    pretty: bool,
) -> None:
    """Handle --list-operations and combined --list-queries --list-mutations."""
    if format == "json":
        json_output = _convert_operations_list_to_json(schema)
        click.echo(
            json.dumps(json_output, indent=2 if pretty else None, sort_keys=True)
        )
    else:
        query_fields = (
            schema.get("queryType", {}).get("fields", [])
            if schema.get("queryType")
            else []
        )
        mutation_fields = (
            schema.get("mutationType", {}).get("fields", [])
            if schema.get("mutationType")
            else []
        )

        output = []
        if query_fields:
            output.append(_format_operation_list(query_fields, "Queries"))
        if mutation_fields:
            output.append(_format_operation_list(mutation_fields, "Mutations"))

        click.echo("\n\n".join(output))


def _handle_list_queries(
    schema: Dict[str, Any],
    format: str,
    pretty: bool,
) -> None:
    """Handle --list-queries only."""
    if format == "json":
        query_type = schema.get("queryType", {})
        json_output: Dict[str, Any] = {
            "schema": {
                "queries": [
                    _convert_operation_to_json(field, "Query")
                    for field in query_type.get("fields", [])
                ]
            }
        }
        click.echo(
            json.dumps(json_output, indent=2 if pretty else None, sort_keys=True)
        )
    else:
        query_fields = (
            schema.get("queryType", {}).get("fields", [])
            if schema.get("queryType")
            else []
        )
        click.echo(_format_operation_list(query_fields, "Queries"))


def _handle_list_mutations(
    schema: Dict[str, Any],
    format: str,
    pretty: bool,
) -> None:
    """Handle --list-mutations only."""
    if format == "json":
        mutation_type = schema.get("mutationType", {})
        json_output: Dict[str, Any] = {
            "schema": {
                "mutations": [
                    _convert_operation_to_json(field, "Mutation")
                    for field in mutation_type.get("fields", [])
                ]
            }
        }
        click.echo(
            json.dumps(json_output, indent=2 if pretty else None, sort_keys=True)
        )
    else:
        mutation_fields = (
            schema.get("mutationType", {}).get("fields", [])
            if schema.get("mutationType")
            else []
        )
        click.echo(_format_operation_list(mutation_fields, "Mutations"))


def _get_recursive_types_for_describe(
    client: Any,
    operation_info: Optional[Tuple[Dict[str, Any], str]],
    type_info: Optional[Dict[str, Any]],
    describe: str,
) -> Optional[Dict[str, Any]]:
    """Get recursive types for describe functionality."""
    types_map = None
    try:
        if operation_info:
            # Collect input types from operation arguments
            operation_details, _ = operation_info
            all_types = set()
            for arg in operation_details.get("args", []):
                base_type_name = _extract_base_type_name(arg.get("type", {}))
                if base_type_name and base_type_name not in [
                    "String",
                    "Int",
                    "Float",
                    "Boolean",
                    "ID",
                ]:
                    all_types.add(base_type_name)

            # Fetch all related types recursively
            all_related_types = {}
            for type_name in all_types:
                try:
                    related_types = _fetch_type_recursive(client, type_name)
                    all_related_types.update(related_types)
                except Exception as e:
                    logger.debug(
                        f"Failed to fetch recursive types for {type_name}: {e}"
                    )
            types_map = all_related_types

        elif type_info:
            # Fetch recursive types starting from the type itself
            try:
                types_map = _fetch_type_recursive(client, describe)
            except Exception as e:
                logger.debug(f"Recursive type fetching failed: {e}")
                types_map = None
    except Exception as e:
        logger.debug(f"Recursive exploration failed: {e}")
        types_map = None

    return types_map


def _handle_describe_json_output(
    operation_info: Optional[Tuple[Dict[str, Any], str]],
    type_info: Optional[Dict[str, Any]],
    types_map: Optional[Dict[str, Dict[str, Any]]],
    describe: str,
    recurse: bool,
    pretty: bool,
) -> None:
    """Handle JSON output for describe functionality."""
    json_output = _convert_describe_to_json(operation_info, type_info, types_map)

    # Add metadata
    json_output["meta"] = {"query": describe, "recursive": recurse}

    click.echo(json.dumps(json_output, indent=2 if pretty else None, sort_keys=True))


def _handle_describe_human_output(
    schema: Dict[str, Any],
    client: Any,
    operation_info: Optional[Tuple[Dict[str, Any], str]],
    type_info: Optional[Dict[str, Any]],
    describe: str,
    recurse: bool,
) -> None:
    """Handle human-readable output for describe functionality."""
    output_sections = []

    # Show operation details if found
    if operation_info:
        operation_details, operation_type = operation_info

        if recurse:
            try:
                operation_output = _format_operation_details_recursive(
                    operation_details, operation_type, client
                )
            except Exception as e:
                logger.debug(
                    f"Recursive operation details failed ({e}), falling back to standard format"
                )
                operation_output = _format_operation_details(
                    operation_details, operation_type, schema
                )
        else:
            operation_output = _format_operation_details(
                operation_details, operation_type, schema
            )

        output_sections.append(f"=== OPERATION ===\n{operation_output}")

    # Show type details if found
    if type_info:
        if recurse:
            try:
                types_map = _fetch_type_recursive(client, describe)
                if types_map and describe in types_map:
                    type_output = _format_recursive_types(types_map, describe)
                else:
                    type_output = _format_type_details(type_info)
            except Exception as e:
                logger.debug(
                    f"Recursive type details failed ({e}), falling back to standard format"
                )
                type_output = _format_type_details(type_info)
        else:
            type_output = _format_type_details(type_info)

        output_sections.append(f"=== TYPE ===\n{type_output}")

    # Output results
    if len(output_sections) > 1:
        # Both operation and type found - show both with separators
        click.echo("\n\n".join(output_sections))
    else:
        # Only one found - show without section header
        output = output_sections[0]
        # Remove the section header
        if output.startswith("=== OPERATION ===\n"):
            output = output[len("=== OPERATION ===\n") :]
        elif output.startswith("=== TYPE ===\n"):
            output = output[len("=== TYPE ===\n") :]
        click.echo(output)


def _handle_describe(
    schema: Dict[str, Any],
    client: Any,
    describe: str,
    recurse: bool,
    format: str,
    pretty: bool,
) -> None:
    """Handle --describe operation/type."""
    operation_info, type_info = _search_operation_and_type(schema, client, describe)

    if not operation_info and not type_info:
        raise click.ClickException(
            f"'{describe}' not found as an operation or type. Use --list-operations to see available operations or try a specific type name."
        )

    if format == "json":
        types_map = None
        if recurse:
            types_map = _get_recursive_types_for_describe(
                client, operation_info, type_info, describe
            )

        _handle_describe_json_output(
            operation_info,
            type_info,
            types_map,
            describe,
            recurse,
            pretty,
        )
    else:
        _handle_describe_human_output(
            schema,
            client,
            operation_info,
            type_info,
            describe,
            recurse,
        )


def _execute_operation(
    client: Any, operation: str, variables: Optional[str], schema_path: Optional[str]
) -> Dict[str, Any]:
    """Execute a named GraphQL operation."""
    if schema_path:
        schema = _parse_graphql_operations_from_files(schema_path)
    else:
        schema = _get_schema_via_introspection(client)

    # Find the operation
    operation_info = _find_operation_by_name(schema, operation)
    if not operation_info:
        raise click.ClickException(
            f"Operation '{operation}' not found. Use --list-operations to see available operations."
        )

    operation_field, operation_type = operation_info
    variables_dict = _parse_variables(variables)

    try:
        # Generate the GraphQL query from the operation
        generated_query = _generate_operation_query(
            operation_field, operation_type, variables_dict
        )
        logger.debug(f"Generated query for operation '{operation}': {generated_query}")

        # Execute the generated query
        return client.execute_graphql(query=generated_query, variables=variables_dict)
    except Exception as e:
        raise click.ClickException(
            f"Failed to execute operation '{operation}': {e}"
        ) from e


def _execute_query(client: Any, query: str, variables: Optional[str]) -> Dict[str, Any]:
    """Execute a raw GraphQL query."""
    query_content = _load_content_or_file(query)
    variables_dict = _parse_variables(variables)

    try:
        return client.execute_graphql(query=query_content, variables=variables_dict)
    except Exception as e:
        raise click.ClickException(f"Failed to execute GraphQL query: {e}") from e


@click.command()
@click.option(
    "--query",
    "-q",
    help="GraphQL query string or path to .graphql file",
)
@click.option(
    "--variables",
    "-v",
    help="GraphQL variables as JSON string or path to .json file",
)
@click.option(
    "--operation",
    "-o",
    help="Execute a named GraphQL operation from the schema",
)
@click.option(
    "--list-operations",
    is_flag=True,
    help="List all available GraphQL operations (queries and mutations)",
)
@click.option(
    "--list-queries",
    is_flag=True,
    help="List available GraphQL queries",
)
@click.option(
    "--list-mutations",
    is_flag=True,
    help="List available GraphQL mutations",
)
@click.option(
    "--describe",
    "-d",
    help="Describe a specific GraphQL operation",
)
@click.option(
    "--recurse",
    is_flag=True,
    help="Recursively describe nested types when using --describe",
)
@click.option(
    "--schema-path",
    help="Path to GraphQL schema files directory (uses local files instead of live introspection)",
)
@click.option(
    "--no-pretty",
    is_flag=True,
    help="Disable pretty-printing of JSON output",
)
@click.option(
    "--format",
    type=click.Choice(["human", "json"]),
    default="human",
    help="Output format: human-readable or JSON for LLM consumption",
)
@upgrade.check_upgrade
def graphql(
    query: Optional[str],
    variables: Optional[str],
    operation: Optional[str],
    list_operations: bool,
    list_queries: bool,
    list_mutations: bool,
    describe: Optional[str],
    recurse: bool,
    schema_path: Optional[str],
    no_pretty: bool,
    format: str,
) -> None:
    """Execute GraphQL queries and mutations against DataHub."""

    pretty = not no_pretty
    client = get_default_graph(ClientMode.CLI)

    # Schema introspection commands
    if list_operations or list_queries or list_mutations or describe:
        if schema_path:
            schema = _parse_graphql_operations_from_files(schema_path)
        else:
            schema = _get_schema_via_introspection(client)

        if list_operations or (list_queries and list_mutations):
            _handle_list_operations(schema, format, pretty)
            return
        elif list_queries:
            _handle_list_queries(schema, format, pretty)
            return
        elif list_mutations:
            _handle_list_mutations(schema, format, pretty)
            return
        elif describe:
            _handle_describe(
                schema,
                client,
                describe,
                recurse,
                format,
                pretty,
            )
            return

    # Execution commands
    if operation:
        result = _execute_operation(client, operation, variables, schema_path)
    elif query:
        result = _execute_query(client, query, variables)
    else:
        raise click.ClickException(
            "Must specify either --query, --operation, or a discovery option "
            "(--list-operations, --list-queries, --list-mutations, --describe)"
        )

    # Output result
    if pretty:
        click.echo(json.dumps(result, indent=2, sort_keys=True))
    else:
        click.echo(json.dumps(result))


if __name__ == "__main__":
    graphql()
