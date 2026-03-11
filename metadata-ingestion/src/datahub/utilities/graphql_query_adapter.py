"""GraphQL query adaptation utilities for schema compatibility.

This module provides automatic query adaptation to match server schema capabilities.
It uses GraphQL introspection and AST transformation to remove unsupported fields,
enabling clients to work with servers of different versions.
"""

import hashlib
import json
import logging
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple, cast

from graphql import (
    FieldNode,
    FragmentDefinitionNode,
    GraphQLSchema,
    InlineFragmentNode,
    IntrospectionQuery,
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

SCHEMA_TTL_SECONDS: float = 600.0  # 10 min backstop TTL
SCHEMA_FAILURE_TTL_SECONDS: float = 60.0  # 1 min negative cache
QUERY_CACHE_MAX_SIZE: int = 128
DISK_CACHE_DIR: str = os.path.join(os.path.expanduser("~"), ".datahub", "schema_cache")


class RequiredFieldUnsupportedError(Exception):
    """Raised when required fields are not supported by the server schema.

    Attributes:
        unsupported_fields: The required field paths that the server cannot fulfill.
    """

    def __init__(self, unsupported_fields: List[str]) -> None:
        self.unsupported_fields = unsupported_fields
        fields_str = ", ".join(unsupported_fields)
        super().__init__(
            f"Server schema does not support required fields: {fields_str}"
        )


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
        self.removed_structural_paths: List[str] = []
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
                self.removed_structural_paths.append(type_name)
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
                self.removed_structural_paths.append(
                    self._get_structural_path(ancestors, node)
                )
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

    def _get_structural_path(
        self, ancestors: List[object], current_node: FieldNode
    ) -> str:
        """Build a dot-path for required_fields matching.

        The path is anchored at the innermost inline fragment type boundary,
        or at the root query field if there is no enclosing inline fragment.

        Examples:
            - Field 'name' inside '... on Dataset { properties { name } }'
              → 'Dataset.properties.name'
            - Root field 'searchAcrossEntities' with no inline fragment
              → 'searchAcrossEntities'
        """
        # Find the innermost InlineFragmentNode to anchor the path
        last_inline_idx = -1
        last_inline_type: Optional[str] = None
        for i, ancestor in enumerate(ancestors):
            if (
                isinstance(ancestor, InlineFragmentNode)
                and ancestor.type_condition is not None
            ):
                last_inline_idx = i
                last_inline_type = ancestor.type_condition.name.value

        parts: List[str] = []
        if last_inline_type is not None:
            parts.append(last_inline_type)

        # Collect FieldNode names after the inline fragment (or from start)
        start = last_inline_idx + 1 if last_inline_idx >= 0 else 0
        for ancestor in ancestors[start:]:
            if isinstance(ancestor, FieldNode):
                parts.append(ancestor.name.value)

        parts.append(current_node.name.value)
        return ".".join(parts)


def _check_required_fields(
    required_fields: List[str], removed_paths: List[str]
) -> None:
    """Raise RequiredFieldUnsupportedError if any required path was removed.

    A required path R is violated when any removed path D satisfies:
    - D == R (exact match), or
    - R starts with D + "." (a parent of R was removed)
    """
    unsupported: List[str] = []
    for req in required_fields:
        for removed in removed_paths:
            if req == removed or req.startswith(removed + "."):
                unsupported.append(req)
                break
    if unsupported:
        raise RequiredFieldUnsupportedError(unsupported)


class QueryProjector:
    """Adapts GraphQL queries to match server schema capabilities.

    This class provides automatic query transformation by:
    1. Introspecting the server schema (with caching)
    2. Parsing the client query to an AST
    3. Removing unsupported fields/types via AST visitor
    4. Returning the adapted query string

    Caching strategy:
    - Commit-hash invalidation: schema is refetched when server commit hash changes
    - TTL backstop (10 min): guards against stale cache when config refresh is off
    - Negative caching (1 min): avoids hammering a failing server
    - On-disk cache: avoids cold-start introspection cost across process restarts
    - Query result cache: avoids re-parsing identical queries against the same schema

    Example:
        projector = QueryProjector()
        adapted_query, removed = projector.adapt_query(query, graph)
        if removed:
            print(f"Adapted query, removed {len(removed)} fields")
    """

    def __init__(self) -> None:
        """Initialize the query projector with empty caches."""
        # Schema state (single schema, 1:1 with graph)
        self._cached_schema: Optional[GraphQLSchema] = None
        self._schema_commit_hash: Optional[str] = None
        self._schema_fetched_at: float = 0.0
        self._schema_fetch_failed: bool = False
        self._schema_lock: threading.Lock = threading.Lock()
        self._schema_generation: int = 0

        # Query result cache: (query_string, generation) ->
        #   (adapted_query, removed_fields, removed_structural_paths)
        self._query_cache: Dict[Tuple[str, int], Tuple[str, List[str], List[str]]] = {}

    def adapt_query(
        self,
        query: str,
        graph: "DataHubGraph",
        required_fields: Optional[List[str]] = None,
    ) -> Tuple[str, List[str]]:
        """Adapt a GraphQL query to match the server's schema capabilities.

        Args:
            query: The GraphQL query string to adapt.
            graph: DataHubGraph instance for schema introspection.
            required_fields: Optional list of dot-separated field paths that
                must survive projection. If any would be removed, raises
                RequiredFieldUnsupportedError instead of silently stripping them.
                Paths can be type-anchored (``Dataset.properties.name``) or
                root-anchored (``searchAcrossEntities``). A path is violated
                when any prefix of it was removed.

        Returns:
            A tuple of (adapted_query, removed_fields) where:
            - adapted_query: The modified query with unsupported fields removed
            - removed_fields: List of human-readable field paths that were removed

        Raises:
            RequiredFieldUnsupportedError: If any required_fields would be removed.
            Exception: If query parsing or schema introspection fails.
        """
        # Get the server schema (cached with commit-hash + TTL invalidation)
        schema = self._get_schema(graph)

        # Check query result cache
        cache_key = (query, self._schema_generation)
        cached_result = self._query_cache.get(cache_key)
        if cached_result is not None:
            adapted_query, removed_fields, removed_paths = cached_result
            if required_fields:
                _check_required_fields(required_fields, removed_paths)
            return adapted_query, removed_fields

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

        removed_paths = visitor.removed_structural_paths
        result = (adapted_query, visitor.removed_fields, removed_paths)

        # Store in query cache, evicting oldest if over capacity
        if len(self._query_cache) >= QUERY_CACHE_MAX_SIZE:
            oldest_key = next(iter(self._query_cache))
            del self._query_cache[oldest_key]
        self._query_cache[cache_key] = result

        if required_fields:
            _check_required_fields(required_fields, removed_paths)

        return adapted_query, visitor.removed_fields

    def _get_commit_hash(self, graph: "DataHubGraph") -> Optional[str]:
        """Safely get the server's commit hash. Returns None on failure."""
        try:
            return graph.server_config.commit_hash
        except Exception:
            return None

    def _get_schema(self, graph: "DataHubGraph") -> GraphQLSchema:
        """Get the server schema with commit-hash invalidation, TTL, and negative caching.

        Uses double-checked locking to prevent thundering herd on cold cache.

        Raises:
            Exception: If schema introspection fails (after negative cache expires)
        """
        now = time.monotonic()
        current_hash = self._get_commit_hash(graph)

        # Fast path (no lock): check if cached schema is still valid
        hash_unchanged = (
            current_hash is not None and current_hash == self._schema_commit_hash
        ) or (current_hash is None and self._schema_commit_hash is None)
        within_ttl = (now - self._schema_fetched_at) < SCHEMA_TTL_SECONDS

        if self._cached_schema is not None and hash_unchanged and within_ttl:
            return self._cached_schema

        # Fast path: negative cache — don't retry failed introspection too quickly
        if (
            self._schema_fetch_failed
            and hash_unchanged
            and (now - self._schema_fetched_at) < SCHEMA_FAILURE_TTL_SECONDS
        ):
            raise RuntimeError(
                "Schema introspection recently failed; retry after backoff"
            )

        # Slow path: acquire lock and double-check
        with self._schema_lock:
            # Re-check after acquiring lock (another thread may have fetched)
            now = time.monotonic()
            current_hash = self._get_commit_hash(graph)
            hash_unchanged = (
                current_hash is not None and current_hash == self._schema_commit_hash
            ) or (current_hash is None and self._schema_commit_hash is None)
            within_ttl = (now - self._schema_fetched_at) < SCHEMA_TTL_SECONDS

            if self._cached_schema is not None and hash_unchanged and within_ttl:
                return self._cached_schema

            if (
                self._schema_fetch_failed
                and hash_unchanged
                and (now - self._schema_fetched_at) < SCHEMA_FAILURE_TTL_SECONDS
            ):
                raise RuntimeError(
                    "Schema introspection recently failed; retry after backoff"
                )

            # Fetch the schema
            try:
                schema = self._introspect_schema(graph)
                self._cached_schema = schema
                self._schema_commit_hash = current_hash
                self._schema_fetched_at = time.monotonic()
                self._schema_fetch_failed = False
                self._schema_generation += 1
                self._query_cache.clear()
                logger.debug(
                    f"Schema cached (generation={self._schema_generation}, hash={current_hash})"
                )
                return schema
            except Exception:
                self._schema_fetch_failed = True
                self._cached_schema = None
                self._schema_commit_hash = current_hash
                self._schema_fetched_at = time.monotonic()
                raise

    def _disk_cache_path(self, server_url: str, commit_hash: str) -> Path:
        """Get the disk cache file path for a given server + commit hash."""
        cache_dir = Path(DISK_CACHE_DIR)
        key = hashlib.sha256(f"{server_url}:{commit_hash}".encode()).hexdigest()[:32]
        return cache_dir / f"{key}.json"

    def _load_from_disk(
        self, server_url: str, commit_hash: str
    ) -> Optional[GraphQLSchema]:
        """Try to load cached introspection from disk. Returns None on miss/error."""
        try:
            path = self._disk_cache_path(server_url, commit_hash)
            # Reject symlinks to prevent symlink attacks
            if path.is_symlink():
                logger.warning(f"Ignoring symlinked cache file: {path}")
                return None
            # Use direct read (no exists() check) to avoid TOCTOU race
            data: IntrospectionQuery = json.loads(path.read_text(encoding="utf-8"))
            schema = build_client_schema(data)
            logger.debug(f"Loaded schema from disk cache: {path}")
            return schema
        except FileNotFoundError:
            return None
        except Exception as e:
            logger.debug(f"Disk cache miss or error: {e}")
        return None

    def _save_to_disk(
        self, server_url: str, commit_hash: str, introspection_data: dict
    ) -> None:
        """Save introspection result to disk cache. Best-effort, never raises.

        Uses atomic write (tempfile + rename) to prevent truncated files on crash.
        Rejects symlinks to prevent symlink attacks.
        """
        try:
            path = self._disk_cache_path(server_url, commit_hash)
            # Reject symlinks to prevent writing through to unintended targets
            if path.is_symlink():
                logger.warning(f"Refusing to write to symlinked cache path: {path}")
                return
            path.parent.mkdir(parents=True, exist_ok=True)
            # Atomic write: write to temp file then rename to avoid truncated files
            fd, tmp_path = tempfile.mkstemp(
                dir=str(path.parent), suffix=".tmp", prefix=".schema_"
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(introspection_data, f)
                os.replace(tmp_path, str(path))
                logger.debug(f"Saved schema to disk cache: {path}")
            except BaseException:
                # Clean up temp file on any failure (including KeyboardInterrupt)
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
            self._cleanup_old_cache_files(path.parent, max_age_days=7)
        except Exception as e:
            logger.debug(f"Failed to write disk cache: {e}")

    def _cleanup_old_cache_files(self, cache_dir: Path, max_age_days: int) -> None:
        """Remove cache files older than max_age_days. Best-effort, per-file error handling.

        Also removes stale .tmp files from interrupted atomic writes (e.g., on
        Windows where os.replace can fail if the target is locked).
        """
        try:
            cutoff = time.time() - (max_age_days * 86400)
            for f in cache_dir.glob("*.json"):
                try:
                    if f.stat().st_mtime < cutoff:
                        f.unlink()
                        logger.debug(f"Cleaned up old cache file: {f}")
                except FileNotFoundError:
                    pass  # Another process already deleted it
                except Exception:
                    pass  # Permission errors, etc. — skip this file
            # Clean up stale temp files (older than 1 hour)
            tmp_cutoff = time.time() - 3600
            for f in cache_dir.glob(".schema_*.tmp"):
                try:
                    if f.stat().st_mtime < tmp_cutoff:
                        f.unlink()
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"Cache cleanup error: {e}")

    def _introspect_schema(self, graph: "DataHubGraph") -> GraphQLSchema:
        """Introspect the server's GraphQL schema, with on-disk caching."""
        server_url = getattr(graph, "_gms_server", None)
        commit_hash = self._get_commit_hash(graph)

        # Try disk cache first (requires both server_url and commit_hash)
        if server_url and commit_hash:
            disk_schema = self._load_from_disk(server_url, commit_hash)
            if disk_schema is not None:
                return disk_schema

        introspection_query = get_introspection_query()

        try:
            # Execute introspection query with strip_unsupported_fields=False
            # to avoid recursion (the projector itself calls execute_graphql)
            result = graph.execute_graphql(
                introspection_query, strip_unsupported_fields=False
            )

            # Build client schema from introspection result
            # execute_graphql already unwraps result["data"]
            schema = build_client_schema(cast(IntrospectionQuery, result))

            # Save to disk cache (requires both server_url and commit_hash)
            if server_url and commit_hash:
                self._save_to_disk(server_url, commit_hash, result)

            logger.info("Successfully introspected server GraphQL schema")
            return schema

        except Exception as e:
            logger.error(f"Failed to introspect server schema: {e}")
            raise
