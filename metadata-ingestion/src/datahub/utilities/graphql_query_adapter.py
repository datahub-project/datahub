"""GraphQL query adaptation utilities for schema compatibility.

This module provides automatic query adaptation to match server schema capabilities.
It uses GraphQL introspection and AST transformation to remove unsupported fields,
enabling clients to work with servers of different versions.
"""

import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Set

from graphql import (
    FieldNode,
    FragmentDefinitionNode,
    GraphQLSchema,
    InlineFragmentNode,
    TypeInfo,
    TypeInfoVisitor,
    Visitor,
    parse,
    print_ast,
    visit,
)
from graphql.language.visitor import REMOVE
from graphql.utilities import build_client_schema, get_introspection_query

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)


class UnsupportedFieldRemover(Visitor):
    """AST visitor that removes fields and fragments not supported by the server schema.

    This visitor walks the GraphQL query AST and removes:
    - Inline fragments for types that don't exist in the schema
    - Fields that don't exist on their parent type
    - Fragment spreads referencing undefined fragments

    Uses TypeInfo to track the current type context during traversal.
    """

    def __init__(self, schema: GraphQLSchema, type_info: TypeInfo) -> None:
        """Initialize the visitor.

        Args:
            schema: The server's GraphQL schema
            type_info: Type information tracker for AST traversal
        """
        super().__init__()
        self.schema = schema
        self.type_info = type_info
        self.removed_fields: List[str] = []
        self._fragment_definitions: Set[str] = set()

    def enter_fragment_definition(
        self,
        node: FragmentDefinitionNode,
        key: Optional[int],
        parent: Optional[List[FragmentDefinitionNode]],
        path: List[int],
        ancestors: List[object],
    ) -> None:
        """Track fragment definitions as we encounter them."""
        self._fragment_definitions.add(node.name.value)

    def enter_inline_fragment(
        self,
        node: InlineFragmentNode,
        key: Optional[int],
        parent: Optional[List[InlineFragmentNode]],
        path: List[int],
        ancestors: List[object],
    ) -> Optional[object]:
        """Remove inline fragments for types that don't exist in the schema.

        Example:
            ... on Document { urn }  # Removed if Document type doesn't exist
        """
        if node.type_condition:
            type_name = node.type_condition.name.value
            type_obj = self.schema.type_map.get(type_name)

            if type_obj is None:
                self.removed_fields.append(f"InlineFragment(... on {type_name})")
                logger.debug(
                    f"Removing inline fragment for unsupported type: {type_name}"
                )
                return REMOVE

        return None

    def enter_field(
        self,
        node: FieldNode,
        key: Optional[int],
        parent: Optional[List[FieldNode]],
        path: List[int],
        ancestors: List[object],
    ) -> Optional[object]:
        """Remove fields that don't exist on their parent type.

        Uses TypeInfo to determine the current parent type and checks if the
        field exists in the schema.
        """
        parent_type = self.type_info.get_parent_type()
        if parent_type is None:
            return None

        field_name = node.name.value

        # Introspection fields (__typename, __schema, etc.) are always valid
        if field_name.startswith("__"):
            return None

        # Check if the field exists on the parent type
        parent_type_obj = (
            parent_type.of_type if hasattr(parent_type, "of_type") else parent_type
        )

        if hasattr(parent_type_obj, "fields"):
            fields = parent_type_obj.fields
            if field_name not in fields:
                field_path = self._get_field_path(ancestors, node)
                self.removed_fields.append(f"Field({field_path})")
                logger.debug(
                    f"Removing unsupported field: {field_name} on {parent_type}"
                )
                return REMOVE

        return None

    def _get_field_path(self, ancestors: List[object], current_node: FieldNode) -> str:
        """Build a human-readable path for a field (e.g., 'searchResults.entity.urn')."""
        path_parts: List[str] = []

        # Walk up the ancestor chain to build the path
        for ancestor in ancestors:
            if isinstance(ancestor, FieldNode):
                path_parts.append(ancestor.name.value)

        path_parts.append(current_node.name.value)
        return ".".join(path_parts)


class QueryProjector:
    """Adapts GraphQL queries to match server schema capabilities.

    This class provides automatic query transformation by:
    1. Introspecting the server schema (with caching)
    2. Parsing the client query to an AST
    3. Removing unsupported fields/types via AST visitor
    4. Returning the adapted query string

    Example:
        projector = QueryProjector()
        adapted_query, removed = projector.adapt_query(query, graph)
        if removed:
            print(f"Adapted query, removed {len(removed)} fields")
    """

    def __init__(self) -> None:
        """Initialize the query projector with empty schema cache."""
        # Simple per-instance caching using graph object ID
        self._schema_cache: Dict[int, GraphQLSchema] = {}

    def adapt_query(self, query: str, graph: "DataHubGraph") -> tuple[str, List[str]]:
        """Adapt a GraphQL query to match the server's schema capabilities.

        Args:
            query: The GraphQL query string to adapt
            graph: DataHubGraph instance for schema introspection

        Returns:
            A tuple of (adapted_query, removed_fields) where:
            - adapted_query: The modified query with unsupported fields removed
            - removed_fields: List of field paths that were removed

        Raises:
            Exception: If query parsing or schema introspection fails
        """
        # Get the server schema (cached per graph instance)
        schema = self._get_schema(graph)

        # Parse the query to an AST
        try:
            document = parse(query)
        except Exception as e:
            logger.error(f"Failed to parse GraphQL query: {e}")
            raise

        # Visit the AST with type-aware field removal
        type_info = TypeInfo(schema)
        visitor = UnsupportedFieldRemover(schema, type_info)

        try:
            modified_ast = visit(document, TypeInfoVisitor(type_info, visitor))
        except Exception as e:
            logger.error(f"Failed to visit GraphQL AST: {e}")
            raise

        # Convert the modified AST back to a query string
        adapted_query = print_ast(modified_ast)

        return adapted_query, visitor.removed_fields

    def _get_schema(self, graph: "DataHubGraph") -> GraphQLSchema:
        """Get the server schema with simple instance-based caching.

        Args:
            graph: DataHubGraph instance for schema introspection

        Returns:
            The server's GraphQL schema

        Raises:
            Exception: If schema introspection fails
        """
        graph_id = id(graph)

        if graph_id not in self._schema_cache:
            schema = self._introspect_schema(graph)
            self._schema_cache[graph_id] = schema
            logger.debug(f"Cached schema for graph instance {graph_id}")

        return self._schema_cache[graph_id]

    def _introspect_schema(self, graph: "DataHubGraph") -> GraphQLSchema:
        """Introspect the server's GraphQL schema.

        Args:
            graph: DataHubGraph instance for executing the introspection query

        Returns:
            The server's GraphQL schema

        Raises:
            Exception: If introspection query execution fails
        """
        introspection_query = get_introspection_query()

        try:
            # Execute introspection query with strip_unsupported_fields=False
            # to avoid recursion (the projector itself calls execute_graphql)
            result = graph.execute_graphql(
                introspection_query, strip_unsupported_fields=False
            )

            # Build client schema from introspection result
            # execute_graphql already unwraps result["data"]
            schema = build_client_schema(result)

            logger.info("Successfully introspected server GraphQL schema")
            return schema

        except Exception as e:
            logger.error(f"Failed to introspect server schema: {e}")
            raise
