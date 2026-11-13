"""Register DataHub's MCP tools on a host's FastMCP instance.

There is no module-level server: each host (the datahub-ai-service, the local
stdio wrapper, DataHub Cloud) creates its own FastMCP instance, owns transport
and authentication, and calls ``register_tools`` on it. Registering on one
instance never affects another.
"""

import functools
import inspect
from enum import Enum
from typing import Awaitable, Callable, Collection, Optional, ParamSpec, TypeVar

from anyio.lowlevel import checkpoint_if_cancelled
from datahub.cli.env_utils import get_boolean_env_variable
from fastmcp import FastMCP
from loguru import logger
from mcp.server.lowlevel.server import NotificationOptions

from .background_execution import run_in_background
from .graphql_helpers import get_mcp_context
from .progress_heartbeat import with_progress_heartbeat
from .result_policy import trust_label_tags
from .schema_matching import SchemaFieldMatcher
from .tool_context import BackgroundExecution
from .tools.aspect_history import get_aspect_history
from .tools.dataset_queries import get_dataset_queries
from .tools.dataset_usage import get_dataset_usage_history
from .tools.descriptions import update_description
from .tools.documents import grep_documents, search_documents
from .tools.domains import create_domain, remove_domains, set_domains
from .tools.entities import (
    SchemaFieldsTool,
    get_entities,
    get_entities_description,
    list_schema_fields,
)
from .tools.get_me import get_me
from .tools.glossary import (
    add_related_terms,
    add_related_terms_cloud,
    create_glossary_term,
)
from .tools.glossary_versioning import (
    compare_glossary_term_versions,
    create_glossary_term_version,
    get_glossary_term_versions,
    is_entity_versioning_enabled,
)
from .tools.grep_entities import grep_entities
from .tools.ingestion_recipes import (
    get_recipe_schema,
    list_connector_types,
    validate_recipe,
)
from .tools.lineage import get_lineage, get_lineage_paths_between
from .tools.owners import add_owners, remove_owners
from .tools.save_document import is_save_document_enabled, save_document
from .tools.search import search, search_description
from .tools.structured_properties import (
    add_structured_properties,
    remove_structured_properties,
)
from .tools.tags import add_tags, create_tag, remove_tags
from .tools.terms import add_glossary_terms, remove_glossary_terms
from .tools.update_document import update_document
from .version_requirements import tool_annotations

_P = ParamSpec("_P")
_R = TypeVar("_R")


class ToolType(Enum):
    """Tags that sort tools into the read and write groups."""

    SEARCH = "search"
    MUTATION = "mutation"
    USER = "user"
    # Read-only tools outside the search surface. Like SEARCH, they belong to the
    # read group, so they are selected by default rather than left ungrouped.
    READ_ONLY = "read_only"
    # The opt-in ingestion group, reads and writes alike: groups stay disjoint,
    # so a write here carries readOnlyHint false rather than MUTATION.
    INGESTION = "ingestion"


def _tool_fn_label(fn: Callable) -> str:
    """Best-effort name for logging (handles functools.partial wrappers)."""
    name = getattr(fn, "__name__", None) or getattr(
        getattr(fn, "func", None), "__name__", None
    )
    return name if isinstance(name, str) and name else repr(fn)


def async_background(fn: Callable[_P, _R]) -> Callable[_P, Awaitable[_R]]:
    """Wrap a synchronous tool so it runs in a worker thread.

    FastMCP runs sync tools on the event loop, where one slow GMS call would block
    every other request (https://github.com/jlowin/fastmcp/issues/864).
    """
    if inspect.iscoroutinefunction(fn):
        raise RuntimeError("async_background can only be used on non-async functions")

    @functools.wraps(fn)
    async def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        execution = None
        try:
            execution = get_mcp_context().tool_context.get(BackgroundExecution)
        except LookupError:
            pass
        try:
            return await run_in_background(fn, *args, **kwargs)
        except Exception:
            # A host that supplies BackgroundExecution owns exception logging.
            # Arguments stay out of the log: they can carry user data.
            if execution is None:
                logger.exception(f"Tool function {_tool_fn_label(fn)} failed")
            raise
        finally:
            # The worker thread can't be interrupted, so a cancellation that arrives
            # while it runs waits for it. Deliver that cancellation now, instead of
            # returning a result the MCP SDK would then try to send for a request
            # the client already cancelled, which breaks the session.
            await checkpoint_if_cancelled()

    return wrapper


def disable_list_changed_capabilities(mcp_instance: FastMCP) -> None:
    """Stop advertising ``listChanged`` for tools, prompts and resources.

    FastMCP advertises ``listChanged: true`` unconditionally, which tells clients
    their cached list stays valid until the server says otherwise. A stateless HTTP
    host has no session to send ``notifications/*/list_changed`` over, so clients
    that trust the advertisement keep a stale tool list after an admin changes it.
    ``false`` is the honest signal: re-list rather than cache indefinitely.

    On the 2026-07-28 protocol the flags follow whether ``subscriptions/listen`` is
    served instead (FastMCP serves it from 4.2.5), so drop that handler too: its
    in-process bus can't reach clients connected to another replica.
    """
    server = mcp_instance._mcp_server
    server.notification_options = NotificationOptions(
        prompts_changed=False,
        resources_changed=False,
        tools_changed=False,
    )
    server._request_handlers.pop("subscriptions/listen", None)
    server.subscriptions = None


def register_tool(
    mcp_instance: FastMCP,
    name: str,
    fn: Callable,
    *,
    tool_type: ToolType,
    description: Optional[str] = None,
    slow: bool = False,
) -> None:
    """Register one synchronous tool function on ``mcp_instance``.

    The function runs in a worker thread (see ``async_background``), is tagged
    with its tool type, and carries MCP annotations from its @read_only,
    @destructive, @idempotent and @open_world markers. A ``slow`` tool also reports progress
    while it runs, to clients that ask for it.
    """
    mcp_instance.tool(
        name=name,
        description=description or fn.__doc__,
        tags={tool_type.value, *trust_label_tags(fn)},
        annotations=tool_annotations(fn),
    )(with_progress_heartbeat(fn) if slow else async_background(fn))


def register_tools(
    mcp_instance: FastMCP,
    *,
    is_oss: bool,
    groups: Optional[Collection[str]] = None,
    schema_field_matcher: Optional[SchemaFieldMatcher] = None,
) -> list[str]:
    """Register every DataHub tool this deployment supports on ``mcp_instance``.

    Args:
        mcp_instance: The host's FastMCP instance.
        is_oss: Whether the tools talk to DataHub Core (True) or DataHub Cloud
            (False). Some tools describe themselves differently on each.
        groups: Register only these tool groups (see tool_groups), plus the
            system tools every server serves. None registers every group
            except the opt-in ones (``ingestion``), which are registered only
            when named.
        schema_field_matcher: How the schema tools rank columns against names
            and keywords on this server; substring matching by default.

    Returns:
        The names of the tools registered.

    TOOLS_IS_MUTATION_ENABLED=false registers no tool that changes metadata,
    including the ingestion group's writes. Writes are on by default: each one
    still runs with the caller's own privileges, so it can do only what that
    caller could do in the UI. INGESTION_TOOLS_ENABLED=false registers no
    ingestion tool, whatever ``groups`` names.
    """
    # tool_groups builds on ToolType from this module.
    from .tool_groups import SYSTEM_TOOL_IDS, default_group_ids, groups_for_tags

    mutations_enabled = get_boolean_env_variable(
        "TOOLS_IS_MUTATION_ENABLED", default=True
    )
    ingestion_enabled = get_boolean_env_variable(
        "INGESTION_TOOLS_ENABLED", default=True
    )
    wanted_groups = set(default_group_ids() if groups is None else groups)
    logger.info(
        f"Registering DataHub MCP tools (is_oss={is_oss}, groups={groups}, "
        f"mutations={'on' if mutations_enabled else 'off'}, "
        f"ingestion={'on' if ingestion_enabled else 'off'})"
    )
    registered: list[str] = []

    def add(
        name: str,
        fn: Callable,
        tool_type: ToolType,
        *,
        description: Optional[str] = None,
        slow: bool = False,
    ) -> None:
        writes = tool_type is ToolType.MUTATION or (
            tool_type is ToolType.INGESTION and not tool_annotations(fn).read_only_hint
        )
        if writes and not mutations_enabled:
            return
        if tool_type is ToolType.INGESTION and not ingestion_enabled:
            return
        if name not in SYSTEM_TOOL_IDS:
            if not set(groups_for_tags({tool_type.value})) & wanted_groups:
                return
        register_tool(
            mcp_instance,
            name,
            fn,
            tool_type=tool_type,
            description=description,
            slow=slow,
        )
        registered.append(name)

    add("get_me", get_me, ToolType.USER)
    add(
        "search",
        search,
        ToolType.SEARCH,
        description=search_description(is_oss=is_oss),
    )
    # A multi-hop walk over a large graph can take a while.
    add("get_lineage", get_lineage, ToolType.SEARCH, slow=True)
    add("get_dataset_queries", get_dataset_queries, ToolType.SEARCH)
    add("get_dataset_usage_history", get_dataset_usage_history, ToolType.SEARCH)
    # Up to ~30 rounds of batchGet, one per version stepped through.
    add("get_aspect_history", get_aspect_history, ToolType.SEARCH, slow=True)
    schema_tools = (
        SchemaFieldsTool(matcher=schema_field_matcher)
        if schema_field_matcher is not None
        else None
    )
    add(
        "get_entities",
        schema_tools.get_entities if schema_tools else get_entities,
        ToolType.SEARCH,
        description=get_entities_description(is_oss=is_oss),
    )
    if get_boolean_env_variable("LIST_SCHEMA_FIELDS_ENABLED", default=True):
        add(
            "list_schema_fields",
            schema_tools.list_schema_fields if schema_tools else list_schema_fields,
            ToolType.SEARCH,
        )
    # Up to two full-depth upstream searches when the direction is not given.
    add(
        "get_lineage_paths_between",
        get_lineage_paths_between,
        ToolType.SEARCH,
        slow=True,
    )
    add("search_documents", search_documents, ToolType.SEARCH)
    # Regex over whole document bodies: many large documents can take a while.
    add("grep_documents", grep_documents, ToolType.SEARCH, slow=True)
    if get_boolean_env_variable("GREP_ENTITIES_ENABLED", default=True):
        add("grep_entities", grep_entities, ToolType.SEARCH, slow=True)
    add("create_tag", create_tag, ToolType.MUTATION)
    add("add_tags", add_tags, ToolType.MUTATION)
    add("remove_tags", remove_tags, ToolType.MUTATION)
    add("add_terms", add_glossary_terms, ToolType.MUTATION)
    add("remove_terms", remove_glossary_terms, ToolType.MUTATION)
    add("create_glossary_term", create_glossary_term, ToolType.MUTATION)
    # DataHub Cloud relates terms in more ways than Core's isA / hasA.
    add(
        "add_related_terms",
        add_related_terms if is_oss else add_related_terms_cloud,
        ToolType.MUTATION,
    )
    add("create_domain", create_domain, ToolType.MUTATION)
    add("set_domains", set_domains, ToolType.MUTATION)
    add("remove_domains", remove_domains, ToolType.MUTATION)
    add("add_owners", add_owners, ToolType.MUTATION)
    add("remove_owners", remove_owners, ToolType.MUTATION)
    add("update_description", update_description, ToolType.MUTATION)
    add("add_structured_properties", add_structured_properties, ToolType.MUTATION)
    add(
        "remove_structured_properties",
        remove_structured_properties,
        ToolType.MUTATION,
    )
    if is_save_document_enabled():
        # They ship together: an agent that can create a document but not edit
        # it is not a configuration anyone needs.
        add("save_document", save_document, ToolType.MUTATION)
        add("update_document", update_document, ToolType.MUTATION)
    else:
        logger.info(
            "Document tools not registered (SAVE_DOCUMENT_TOOL_ENABLED is false)"
        )
    if is_entity_versioning_enabled():
        add(
            "create_glossary_term_version",
            create_glossary_term_version,
            ToolType.MUTATION,
        )
        add("get_glossary_term_versions", get_glossary_term_versions, ToolType.SEARCH)
        add(
            "compare_glossary_term_versions",
            compare_glossary_term_versions,
            ToolType.SEARCH,
        )
    else:
        logger.info(
            "Glossary term versioning tools not registered "
            "(ENTITY_VERSIONING_ENABLED is not set or false)"
        )
    # The opt-in ingestion group: registered only when `groups` names it.
    add("list_connector_types", list_connector_types, ToolType.INGESTION)
    add("get_recipe_schema", get_recipe_schema, ToolType.INGESTION)
    add("validate_recipe", validate_recipe, ToolType.INGESTION)
    return registered
