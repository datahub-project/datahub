"""Business glossary writes: create_glossary_term and add_related_terms.

add_related_terms has two variants with the same name: ``add_related_terms``
for DataHub Core and ``add_related_terms_cloud`` for DataHub Cloud, whose
schema has more relationship kinds. ``register_tools`` picks one.
"""

import uuid
from typing import Annotated, Literal, Optional

from fastmcp.exceptions import ToolError
from pydantic import BeforeValidator

from .. import arg_validation, graphql_helpers
from ..arg_validation import StrList
from ..read_backend import get_read_backend
from ..version_requirements import idempotent, min_version

_TERM_PREFIX = "urn:li:glossaryTerm:"
_NODE_PREFIX = "urn:li:glossaryNode:"
# An id becomes part of the term's URN, where these characters delimit parts.
_RESERVED_ID_CHARS = ("(", ")", ",")

# DataHub Core's TermRelationshipType has only isA and hasA. DataHub Cloud's
# adds the rest, so the tool offers a different set on each.
_CLOUD_ONLY_RELATIONSHIPS = frozenset(
    {"synonymOf", "antonymOf", "translatesTo", "hasValue", "isRelatedTo"}
)
_RELATIONSHIP_ALIASES = {
    "isa": "isA",
    "is_a": "isA",
    "is a": "isA",
    "inherits": "isA",
    "hasa": "hasA",
    "has_a": "hasA",
    "has a": "hasA",
    "contains": "hasA",
    **{kind.lower(): kind for kind in _CLOUD_ONLY_RELATIONSHIPS},
}


def _coerce_relationship_type(value: object) -> object:
    if not isinstance(value, str):
        return value
    return _RELATIONSHIP_ALIASES.get(value.strip().lower(), value)


def _coerce_core_relationship_type(value: object) -> object:
    coerced = _coerce_relationship_type(value)
    if coerced in _CLOUD_ONLY_RELATIONSHIPS:
        raise ValueError(
            f"{coerced} relationships are not available on DataHub Core, "
            'which has "isA" (inherits from) and "hasA" (contains).'
        )
    return coerced


RelationshipType = Annotated[
    Literal["isA", "hasA"], BeforeValidator(_coerce_core_relationship_type)
]
CloudRelationshipType = Annotated[
    Literal[
        "synonymOf",
        "antonymOf",
        "translatesTo",
        "hasValue",
        "isRelatedTo",
        "isA",
        "hasA",
    ],
    BeforeValidator(_coerce_relationship_type),
]


def _require_urn(value: str, *, tool: str, param: str, example: str) -> str:
    problem = arg_validation.urn_error_message(
        value, tool=tool, param=param, example=example
    )
    if problem is not None:
        raise ToolError(problem)
    return value.strip()


def require_term_urn(value: str, *, tool: str, param: str) -> str:
    urn = _require_urn(value, tool=tool, param=param, example=f"{_TERM_PREFIX}Revenue")
    if not urn.startswith(_TERM_PREFIX):
        raise ToolError(
            f"{tool}: `{param}` must be a glossary term URN ({_TERM_PREFIX}...), "
            f'got {urn!r}. Find terms with search(query="<name>", '
            'filter="entity_type = glossaryTerm").'
        )
    return urn


def new_term_id(value: Optional[str], *, tool: str) -> str:
    """``value`` stripped, or a new UUID when it is blank.

    The id is chosen here rather than by GMS, so a create that gets no reply
    still names the term to look for.
    """
    term_id = (value or "").strip() or str(uuid.uuid4())
    reserved = [c for c in _RESERVED_ID_CHARS if c in term_id]
    if reserved:
        raise ToolError(
            f"{tool}: `id` may not contain {' '.join(repr(c) for c in reserved)}: "
            f"it becomes part of the term's URN. Got {term_id!r}."
        )
    return term_id


def require_parent_node(value: str, *, tool: str) -> str:
    """``value`` as the URN of an existing glossary node, or a ToolError."""
    parent = _require_urn(
        value,
        tool=tool,
        param="parent_node_urn",
        example=f"{_NODE_PREFIX}FinancialKPIs",
    )
    # GMS records any parent URN it is given, so a missing node would leave the
    # term outside the visible hierarchy.
    if not parent.startswith(_NODE_PREFIX) or not get_read_backend().exists(parent):
        raise ToolError(
            f"{tool}: `parent_node_urn` {parent!r} is not an existing glossary "
            'node. Find nodes with search(query="<name>", filter="entity_type = '
            'glossaryNode"), or leave it out to create the term at the top level.'
        )
    return parent


@min_version(cloud="0.3.16", oss="1.4.0")
def create_glossary_term(
    name: str,
    description: str,
    parent_node_urn: Optional[str] = None,
    id: Optional[str] = None,
) -> dict:
    """Create a new (unversioned) GlossaryTerm in the Business Glossary.

    Use this to create a standalone term, then attach it to assets or columns
    with add_terms (pass the term_urn this returns). DataHub refuses a second term
    with the same name under the same parent, so search for the term first
    (search(query=name, filter="entity_type = glossaryTerm")) rather than calling
    this to check whether it exists.

    Args:
        name: Display name for the term (e.g., "Revenue").
        description: Business definition / description for the term.
        parent_node_urn: Optional URN of the parent GlossaryNode (e.g.,
                         "urn:li:glossaryNode:KPIs"). Places the term under
                         that node in the glossary hierarchy.
        id: Optional custom slug used as the URN key (e.g., "Revenue"). A UUID
            is generated if omitted. Avoid spaces; use underscores or CamelCase.

    Returns:
        Dictionary with:
        - success: True if creation succeeded
        - term_urn: URN of the newly created GlossaryTerm

    Examples:
        create_glossary_term(
            name="Revenue",
            description="Total revenue = sum of closed-won deals in the reporting period.",
            parent_node_urn="urn:li:glossaryNode:FinancialKPIs",
            id="Revenue"
        )
    """
    tool = "create_glossary_term"
    term_name = name.strip()
    if not term_name:
        raise ToolError(f"{tool}: `name` is required.")
    term_id = new_term_id(id, tool=tool)
    term_input: dict = {"id": term_id, "name": term_name, "description": description}
    if parent_node_urn and parent_node_urn.strip():
        term_input["parentNode"] = require_parent_node(parent_node_urn, tool=tool)

    term_urn = f"{_TERM_PREFIX}{term_id}"
    result = graphql_helpers.execute_write(
        graphql_helpers.get_datahub_client()._graph,
        query=graphql_helpers.load_gql_document("glossary_terms.gql"),
        operation_name="createGlossaryTerm",
        variables={"input": term_input},
        unknown_outcome_hint=(
            f"Check whether {term_urn} exists with get_entities before calling "
            f"{tool} again."
        ),
    )
    return {"success": True, "term_urn": result.get("createGlossaryTerm") or term_urn}


def _add_related_terms(
    term_urn: str, related_term_urns: list[str], relationship_type: str
) -> dict:
    tool = "add_related_terms"
    source = require_term_urn(term_urn, tool=tool, param="term_urn")
    if not related_term_urns:
        raise ToolError(f"{tool}: `related_term_urns` must name at least one term.")
    related = list(
        dict.fromkeys(
            require_term_urn(value, tool=tool, param=f"related_term_urns[{index}]")
            for index, value in enumerate(related_term_urns)
        )
    )
    if source in related:
        raise ToolError(f"{tool}: a term cannot be related to itself ({source}).")

    # GMS checks that every term exists and names any that does not.
    result = graphql_helpers.execute_write(
        graphql_helpers.get_datahub_client()._graph,
        query=graphql_helpers.load_gql_document("glossary_terms.gql"),
        operation_name="addRelatedTerms",
        variables={
            "input": {
                "urn": source,
                "termUrns": related,
                "relationshipType": relationship_type,
            }
        },
        unknown_outcome_hint=(
            f"Calling {tool} again is safe: a relationship that already exists is "
            "left as is."
        ),
    )
    if not result.get("addRelatedTerms"):
        raise ToolError(
            f"{tool}: DataHub did not add the relationship (addRelatedTerms "
            "returned false)."
        )
    return {
        "success": True,
        "term_urn": source,
        "related_term_urns": related,
        "relationship_type": relationship_type,
    }


@idempotent
@min_version(cloud="0.3.16", oss="1.4.0")
def add_related_terms(
    term_urn: str,
    related_term_urns: StrList,
    relationship_type: RelationshipType,
) -> dict:
    """Relate a GlossaryTerm to one or more other GlossaryTerms.

    Relationship types (use the camelCase values exactly as shown):
    - "isA"  — inheritance: term_urn is a kind of each related term
               (e.g. CreditCardNumber isA PII). Shown as "Inherits" in DataHub.
    - "hasA" — composition: term_urn contains each related term
               (e.g. Address hasA PostalCode). Shown as "Contains" in DataHub.

    A relationship that already exists is left as is, so calling this again is safe.

    Args:
        term_urn: URN of the source GlossaryTerm (e.g., "urn:li:glossaryTerm:CreditCardNumber").
        related_term_urns: List of target GlossaryTerm URNs to link to.
        relationship_type: "isA" or "hasA".

    Returns:
        Dictionary with:
        - success: True if the relationship(s) were added
        - term_urn: The source term URN
        - related_term_urns: The target term URNs
        - relationship_type: The relationship type used

    Examples:
        # CreditCardNumber inherits from PII
        add_related_terms(
            term_urn="urn:li:glossaryTerm:CreditCardNumber",
            related_term_urns=["urn:li:glossaryTerm:PII"],
            relationship_type="isA"
        )

        # Address contains its parts
        add_related_terms(
            term_urn="urn:li:glossaryTerm:Address",
            related_term_urns=["urn:li:glossaryTerm:Street", "urn:li:glossaryTerm:PostalCode"],
            relationship_type="hasA"
        )
    """
    return _add_related_terms(term_urn, related_term_urns, relationship_type)


@idempotent
@min_version(cloud="0.3.16", oss="1.4.0")
def add_related_terms_cloud(
    term_urn: str,
    related_term_urns: StrList,
    relationship_type: CloudRelationshipType,
) -> dict:
    """Add semantic relationship(s) from a GlossaryTerm to one or more other GlossaryTerms.

    Use this to establish governed semantic relationships between terms:
    synonyms (aliases that drive search), antonyms (conflicting definitions),
    translations (multi-language equivalents), valid values (controlled vocabulary),
    and other structural relationships.

    Relationship types and their semantics (use the camelCase values exactly as shown):
    - "synonymOf"    — terms that mean the same thing (drives cross-term search resolution)
    - "antonymOf"    — terms that conflict in business meaning (triggers disambiguation in Ask DataHub)
    - "translatesTo" — term_urn is the canonical term; related_term_urns are translations in other languages
    - "hasValue"     — term_urn is a dimension/concept; related_term_urns are its valid column values
                       (e.g. Country hasValue CH, DE, FR — enables SQL value grounding)
    - "isRelatedTo"  — general bidirectional relationship
    - "isA"          — inheritance / "is a subtype of" (shown as "Inherits")
    - "hasA"         — composition / "contains"

    A relationship that already exists is left as is, so calling this again is safe.

    Args:
        term_urn: URN of the source GlossaryTerm (e.g., "urn:li:glossaryTerm:Revenue").
        related_term_urns: List of target GlossaryTerm URNs to link to.
        relationship_type: One of "synonymOf", "antonymOf", "translatesTo", "hasValue",
                           "isRelatedTo", "isA", "hasA".

    Returns:
        Dictionary with:
        - success: True if the relationship(s) were added
        - term_urn: The source term URN
        - related_term_urns: The target term URNs
        - relationship_type: The relationship type used

    Examples:
        # Add a German translation
        add_related_terms(
            term_urn="urn:li:glossaryTerm:MonthlyRecurringRevenue",
            related_term_urns=["urn:li:glossaryTerm:MonatlichWiederkehrenderUmsatz"],
            relationship_type="translatesTo"
        )

        # Mark two terms as synonyms
        add_related_terms(
            term_urn="urn:li:glossaryTerm:Revenue",
            related_term_urns=["urn:li:glossaryTerm:MRR", "urn:li:glossaryTerm:MonthlyRevenue"],
            relationship_type="synonymOf"
        )

        # Add valid values for a dimension term
        add_related_terms(
            term_urn="urn:li:glossaryTerm:Country",
            related_term_urns=["urn:li:glossaryTerm:CH", "urn:li:glossaryTerm:DE"],
            relationship_type="hasValue"
        )
    """
    return _add_related_terms(term_urn, related_term_urns, relationship_type)
