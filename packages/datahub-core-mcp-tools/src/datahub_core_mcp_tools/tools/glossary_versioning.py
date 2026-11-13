"""Glossary term versioning: get_glossary_term_versions, compare_glossary_term_versions.

A versioned glossary term is one GlossaryTerm entity per version, linked into a
VersionSet. These tools read that history; they are registered only when
ENTITY_VERSIONING_ENABLED is true. The write tool for the same feature
(create_glossary_term_version) belongs in this module too; the glossary writes
that need no versioning are in glossary.py.
"""

from typing import Annotated, Dict

from datahub.cli.env_utils import get_boolean_env_variable
from fastmcp.exceptions import ToolError
from pydantic import Field

from .. import arg_validation
from ..read_backend import get_read_backend, require_existing
from ..version_requirements import min_version, read_only

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


def _require_term_urn(value: str, *, tool: str, param: str) -> str:
    urn = arg_validation.require_urn(value, tool=tool, param=param)
    if not urn.startswith(_TERM_PREFIX):
        raise ToolError(
            f"{tool}: `{param}` must be a glossary term URN "
            f"({_TERM_PREFIX}...), got {urn}."
        )
    return urn


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
    urn_a = _require_term_urn(term_urn_a, tool=tool, param="term_urn_a")
    urn_b = _require_term_urn(term_urn_b, tool=tool, param="term_urn_b")

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
