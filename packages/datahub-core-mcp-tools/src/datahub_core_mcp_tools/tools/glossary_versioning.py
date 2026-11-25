"""Glossary term versioning: create_glossary_term_version,
get_glossary_term_versions and compare_glossary_term_versions.

A versioned glossary term is one GlossaryTerm entity per version, linked into a
VersionSet. These tools create a version and read that history; they are
registered only when ENTITY_VERSIONING_ENABLED is true. The glossary writes that
need no versioning are in glossary.py.
"""

from typing import Annotated, Dict, Optional

from datahub.cli.env_utils import get_boolean_env_variable
from fastmcp.exceptions import ToolError
from pydantic import Field

from .. import arg_validation, graphql_helpers
from ..read_backend import get_read_backend, require_existing
from ..version_requirements import min_version, read_only
from .glossary import new_term_id, require_parent_node, require_term_urn

# GlossaryTerm.versionProperties reached OSS master after the 1.9.0 release branch
# was cut, so 1.10.0 is the first release that can have it. Cloud: 0.3.16.
VERSIONING_MIN_OSS = "1.10.0"
VERSIONING_MIN_CLOUD = "0.3.16"

# One page of versions; a concept with more is not expected.
_MAX_VERSIONS = 50

_TERM_PREFIX = "urn:li:glossaryTerm:"
_VERSION_SET_PREFIX = "urn:li:versionSet:"


def is_entity_versioning_enabled() -> bool:
    """Whether the entity versioning tools are registered (default off)."""
    return get_boolean_env_variable("ENTITY_VERSIONING_ENABLED", default=False)


def _version_fields(entity: dict) -> dict:
    version_props = entity.get("versionProperties") or {}
    return {
        "version_tag": (version_props.get("version") or {}).get("versionTag"),
        "is_latest": version_props.get("isLatest"),
        "comment": version_props.get("comment"),
    }


def _format_version_entry(entity: dict) -> dict:
    """Flatten a versionsSearch entity entry into a clean dict."""
    props = entity.get("properties") or {}
    created = (entity.get("versionProperties") or {}).get("created") or {}
    return {
        "urn": entity.get("urn"),
        "hierarchical_name": entity.get("hierarchicalName"),
        "name": props.get("name"),
        "definition": props.get("description"),
        "term_source": props.get("termSource"),
        **_version_fields(entity),
        "created_timestamp_ms": created.get("time"),
    }


def _term_summary(entity: dict) -> dict:
    props = entity.get("properties") or {}
    deprecation = entity.get("deprecation") or {}
    return {
        "urn": entity.get("urn"),
        "hierarchical_name": entity.get("hierarchicalName"),
        "name": props.get("name"),
        "definition": props.get("description"),
        "term_source": props.get("termSource"),
        "custom_properties": {
            cp["key"]: cp["value"] for cp in props.get("customProperties") or []
        },
        "owners": [
            {"urn": (o.get("owner") or {}).get("urn"), "type": o.get("type")}
            for o in (entity.get("ownership") or {}).get("owners") or []
        ],
        "tags": [
            (t.get("tag") or {}).get("urn")
            for t in (entity.get("tags") or {}).get("tags") or []
        ],
        "deprecated": deprecation.get("deprecated", False),
        "deprecation_note": deprecation.get("note"),
        **_version_fields(entity),
    }


_COMPARABLE_FIELDS = (
    "name",
    "definition",
    "term_source",
    "custom_properties",
    "owners",
    "tags",
    "deprecated",
    "deprecation_note",
)


@read_only
@min_version(cloud=VERSIONING_MIN_CLOUD, oss=VERSIONING_MIN_OSS)
def get_glossary_term_versions(
    urn: Annotated[str, Field(validation_alias=arg_validation.URN_ALIASES)],
) -> dict:
    """Retrieve the full version history for a GlossaryTerm concept.

    Accepts either a GlossaryTerm URN (looks up its VersionSet automatically)
    or a VersionSet URN directly. Versions are returned sorted newest-first.

    Args:
        urn: A GlossaryTerm URN (e.g., "urn:li:glossaryTerm:Revenue") or a
             VersionSet URN (e.g., "urn:li:versionSet:(revenue,glossaryTerm)").

    Returns:
        Dictionary with:
        - version_set_urn: The VersionSet URN grouping all versions
        - total: Total number of versions
        - versions: List of dicts, each with urn, name, definition, version_tag,
                    is_latest, comment, created_timestamp_ms

    Examples:
        # By term URN — looks up its version set automatically
        get_glossary_term_versions("urn:li:glossaryTerm:Revenue")

        # By VersionSet URN — fetches all versions directly
        get_glossary_term_versions("urn:li:versionSet:(revenue,glossaryTerm)")
    """
    tool = "get_glossary_term_versions"
    urn = arg_validation.require_urn(urn, tool=tool)
    if not urn.startswith((_TERM_PREFIX, _VERSION_SET_PREFIX)):
        raise ToolError(
            f"{tool}: unsupported URN type {urn!r}. Provide a GlossaryTerm or "
            "VersionSet URN."
        )

    backend = get_read_backend()
    # A URN GMS has never seen would otherwise read as "not versioned" or as a
    # version set with no versions.
    require_existing(backend, urn)

    if urn.startswith(_VERSION_SET_PREFIX):
        version_set_urn = urn
    else:
        term: dict = backend.glossary_term_version(urn) or {}
        version_set = (term.get("versionProperties") or {}).get("versionSet") or {}
        if not version_set.get("urn"):
            raise ToolError(
                f"GlossaryTerm {urn} is not versioned. Use get_entities to inspect "
                "it, or search to find a versioned term."
            )
        version_set_urn = version_set["urn"]

    history: dict = backend.version_set_history(version_set_urn, 0, _MAX_VERSIONS) or {}
    versions_search = history.get("versionsSearch") or {}
    versions = [
        _format_version_entry(item["entity"])
        for item in versions_search.get("searchResults") or []
        if item.get("entity")
    ]
    return {
        "version_set_urn": version_set_urn,
        "total": versions_search.get("total", len(versions)),
        "versions": versions,
    }


@read_only
@min_version(cloud=VERSIONING_MIN_CLOUD, oss=VERSIONING_MIN_OSS)
def compare_glossary_term_versions(term_urn_a: str, term_urn_b: str) -> dict:
    """Compare two versions of a GlossaryTerm and show what changed between them.

    Fetches both terms and diffs their key fields: name, definition, term_source,
    custom_properties, owners, tags, and deprecation status.

    Args:
        term_urn_a: URN of the first term (typically the older version).
        term_urn_b: URN of the second term (typically the newer version).

    Returns:
        Dictionary with:
        - term_a: Summary of the first term's fields
        - term_b: Summary of the second term's fields
        - changes: List of dicts with {field, value_a, value_b} for every field that differs

    Examples:
        compare_glossary_term_versions(
            "urn:li:glossaryTerm:Revenue",
            "urn:li:glossaryTerm:Revenue_v2"
        )
    """
    tool = "compare_glossary_term_versions"
    urn_a = require_term_urn(term_urn_a, tool=tool, param="term_urn_a")
    urn_b = require_term_urn(term_urn_b, tool=tool, param="term_urn_b")

    by_urn: Dict[str, dict] = {}
    for entity in get_read_backend().glossary_terms([urn_a, urn_b]):
        if isinstance(entity, dict) and isinstance(entity.get("urn"), str):
            by_urn[str(entity["urn"])] = entity

    def summary(urn: str) -> dict:
        entity = by_urn.get(urn)
        # entities(urns:) answers a URN it has never seen with a stub that has
        # only its urn.
        if entity is None or entity.get("properties") is None:
            raise ToolError(f"Term {urn!r} was not found or is not a GlossaryTerm.")
        return _term_summary(entity)

    term_a = summary(urn_a)
    term_b = summary(urn_b)
    changes = [
        {"field": field, "value_a": term_a.get(field), "value_b": term_b.get(field)}
        for field in _COMPARABLE_FIELDS
        if term_a.get(field) != term_b.get(field)
    ]
    return {"term_a": term_a, "term_b": term_b, "changes": changes}


@min_version(cloud=VERSIONING_MIN_CLOUD, oss=VERSIONING_MIN_OSS)
def create_glossary_term_version(
    name: str,
    description: str,
    version: str,
    version_set_urn: Optional[str] = None,
    previous_term_urn: Optional[str] = None,
    comment: Optional[str] = None,
    parent_node_urn: Optional[str] = None,
    id: Optional[str] = None,
) -> dict:
    """Create a new version of an existing GlossaryTerm concept.

    This is the primary tool for KPI versioning workflows. It creates a new
    GlossaryTerm entity and links it to a VersionSet in several writes (not one
    transaction); inputs are validated before the first write, but an unexpected
    server error part-way through can leave the previous term linked as the
    first version or the new term created but not linked. With
    version_set_urn or neither option, the new version becomes the latest
    (isLatest=true) right away.

    Downstream consumers that reference an older term URN continue to see the
    previous definition until they explicitly migrate to the new URN.

    When previous_term_urn is given, the new version is placed under the same parent,
    inherits the previous version's metadata (domain, owners, tags, links, structured
    properties and so on, but not its deprecation or form responses), and starts
    in the DRAFT lifecycle stage when DataHub defines one for glossary terms. The
    previous version then stays the latest visible version until the new one is
    published. No tool publishes a version: tell the user to publish it in the
    DataHub UI by changing the term's Lifecycle stage. Creating a version is rejected
    while the set holds an unpublished version newer than its latest; ask the user to
    publish or delete that one in the UI first.

    To specify which VersionSet to use, provide exactly one of:
    - version_set_urn: The existing VersionSet URN to add to.
    - previous_term_urn: A URN of an existing term — its VersionSet is looked up
      automatically. If the term is not versioned yet, it becomes the first version
      ("v1") of a new version set.
    - Neither: A new VersionSet is created automatically (use for the very
      first version of a concept).

    Use get_glossary_term_versions to see a concept's versions, and
    create_glossary_term for a term that needs no versions.

    Args:
        name: Display name for the new version (e.g., "Revenue_v2").
        description: Updated business definition for this version.
        version: Version label (e.g., "v2", "2024-Q1", "2.0"). Should be
                 unique within the VersionSet.
        version_set_urn: Optional existing VersionSet URN to add this version
                         to (e.g., "urn:li:versionSet:(revenue,glossaryTerm)").
        previous_term_urn: Optional URN of an existing GlossaryTerm. If it is not
                           versioned yet, it becomes the first version ("v1") of a
                           new version set. The new version joins the same VersionSet.
                           Alternative to version_set_urn.
        comment: Optional explanation of what changed in this version
                 (e.g., "Excluding internal accounts per Q2 policy").
        parent_node_urn: Optional parent GlossaryNode URN for hierarchy placement.
        id: Optional custom slug for the new term entity. A UUID is used if omitted.

    Returns:
        Dictionary with:
        - success: True if creation succeeded
        - version_set_urn: URN of the VersionSet grouping all versions
        - term_urn: URN of the newly created GlossaryTerm

    Examples:
        # Create v2 of an existing versioned term
        create_glossary_term_version(
            name="Revenue_v2",
            description="Revenue = sum of closed-won deals excluding internal accounts.",
            version="v2",
            previous_term_urn="urn:li:glossaryTerm:Revenue",
            comment="Updated to exclude internal accounts per Q2 policy",
            parent_node_urn="urn:li:glossaryNode:FinancialKPIs"
        )

        # Create v1 from an unversioned term (it becomes the first version)
        create_glossary_term_version(
            name="Revenue",
            description="Revenue = sum of all closed-won deals in the period.",
            version="v1",
            previous_term_urn="urn:li:glossaryTerm:RevenueUnversioned",
        )

        # Create the very first versioned entry for a concept
        create_glossary_term_version(
            name="Revenue",
            description="Revenue = sum of all closed-won deals in the period.",
            version="v1",
            comment="Initial versioned definition"
        )
    """
    tool = "create_glossary_term_version"
    term_name = name.strip()
    version_label = version.strip()
    if not term_name:
        raise ToolError(f"{tool}: `name` is required.")
    if not version_label:
        raise ToolError(f'{tool}: `version` is required, e.g. "v2".')
    version_set = (version_set_urn or "").strip()
    previous = (previous_term_urn or "").strip()
    if version_set and previous:
        raise ToolError(
            f"{tool}: give either `version_set_urn` or `previous_term_urn`, not both."
        )
    # Always send an id: with previous_term_urn the new term is not the version
    # set's latest, so its URN can't be looked up afterwards, and a write that
    # gets no reply still names the term to check.
    term_id = new_term_id(id, tool=tool)
    term_input: dict = {
        "id": term_id,
        "name": term_name,
        "description": description,
        "version": version_label,
    }
    backend = get_read_backend()
    if version_set:
        version_set = arg_validation.require_urn(
            version_set, tool=tool, param="version_set_urn"
        )
        if not version_set.startswith(_VERSION_SET_PREFIX):
            raise ToolError(
                f"{tool}: `version_set_urn` must be a VersionSet URN "
                f"({_VERSION_SET_PREFIX}...), got {version_set!r}."
            )
        require_existing(backend, version_set)
        term_input["versionSet"] = version_set
    if previous:
        previous = require_term_urn(previous, tool=tool, param="previous_term_urn")
        require_existing(backend, previous)
        term_input["previousTermUrn"] = previous
    if comment and comment.strip():
        term_input["comment"] = comment.strip()
    if parent_node_urn and parent_node_urn.strip():
        term_input["parentNode"] = require_parent_node(parent_node_urn, tool=tool)

    term_urn = f"{_TERM_PREFIX}{term_id}"
    result = graphql_helpers.execute_write(
        graphql_helpers.get_datahub_client()._graph,
        query=graphql_helpers.load_gql_document("glossary_versioning.gql"),
        operation_name="createGlossaryTermVersion",
        variables={"input": term_input},
        unknown_outcome_hint=(
            f"Check whether {term_urn} exists with get_glossary_term_versions "
            f"before calling {tool} again."
        ),
    )
    created = (result or {}).get("createGlossaryTermVersion") or {}
    if not created.get("urn"):
        raise ToolError(
            f"{tool}: DataHub returned no version set, so {term_urn} may not be "
            "linked as a version. Check it with get_glossary_term_versions."
        )
    return {"success": True, "version_set_urn": created["urn"], "term_urn": term_urn}
