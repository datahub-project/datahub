"""Host-injected trust labels and optional disclosure decisions, preserving ranking.

A host binds a ResultPolicy in the request's ToolContext; tools shape their
results through these helpers, so every tool labels and withholds the same way.
Without a binding, results pass through unchanged. Configuration documents are
the sole exclusion: they are settings for the tools, never knowledge offered to
an agent.
"""

from dataclasses import dataclass
from typing import Callable, Mapping, Protocol, Sequence

from pydantic import JsonValue

from . import graphql_helpers

ANNOTATION_KEY = "trust"
HEADER_KEY = "trust_policy"
# The identity of retrieval-policy configuration. Hosts and the curator import these
# so that renaming one cannot quietly expose the policy document to agents.
POLICY_DOCUMENT_SUBTYPE = "Retrieval Policy"
DEFAULT_POLICY_DOCUMENT_URN = "urn:li:document:retrieval-policy-default"
# The tag a tool carries when its results can label the entities they return. Hosts
# key on the tag rather than on tool names, so a renamed or re-registered tool keeps it.
TRUST_LABELED_TAG = "trust_labeled"


def trust_labeled(fn: Callable) -> Callable:
    """Declare that a tool's results carry ``trust`` labels on the entities they return
    when the host labels them.

    Like ``@read_only``, it sets an attribute instead of wrapping, and
    ``_register_tool`` turns it into ``TRUST_LABELED_TAG``. Leave it off tools that
    label only under a reader policy.
    """
    fn._trust_labeled = True  # type: ignore[attr-defined]
    return fn


def trust_label_tags(fn: Callable) -> set[str]:
    """The tags ``@trust_labeled`` adds to a tool's registration."""
    return {TRUST_LABELED_TAG} if getattr(fn, "_trust_labeled", False) else set()


class ResultDecisions(Protocol):
    def annotation(
        self, urn: str, *, explain: bool = False
    ) -> Mapping[str, JsonValue] | None: ...

    def header(self, urns: Sequence[str]) -> Mapping[str, JsonValue] | None: ...

    def is_configuration(self, urn: str) -> bool: ...


class ResultPolicy(Protocol):
    def decide(self, urns: Sequence[str]) -> ResultDecisions | None: ...


@dataclass(frozen=True)
class ResultPolicyBinding:
    policy: ResultPolicy
    reserved_urns: tuple[str, ...] = ()


def current_decisions(urns: Sequence[str | None]) -> ResultDecisions | None:
    try:
        binding = graphql_helpers.get_mcp_context().tool_context.get(
            ResultPolicyBinding
        )
    except LookupError:
        return None
    if not isinstance(binding, ResultPolicyBinding):
        return None
    return binding.policy.decide([urn for urn in dict.fromkeys(urns) if urn])


def current_reader_decisions(urns: Sequence[str]) -> ResultDecisions | None:
    decisions = current_decisions(urns)
    header = decisions.header(urns) if decisions is not None else None
    return (
        decisions
        if header and ("reader" in header or "reader_policy_state" in header)
        else None
    )


def label_reader_response(
    response: dict,
    urn: str,
    *,
    aspects: Mapping[str, JsonValue] | None = None,
) -> dict:
    if aspects is not None:
        try:
            binding = graphql_helpers.get_mcp_context().tool_context.get(
                ResultPolicyBinding
            )
        except LookupError:
            binding = None
        record = getattr(binding.policy, "record_aspects", None) if binding else None
        if record is not None:
            record(urn, aspects)
    decisions = current_reader_decisions([urn])
    if decisions is not None:
        response = apply_decision(response, urn, decisions)
        attach_header(response, decisions, [urn], reader_only=True)
    return response


def configuration_response(urn: str) -> dict[str, str]:
    return {"urn": urn, "error": "Retrieval policy configuration is not agent context."}


def is_configuration(
    urn: str, decisions: ResultDecisions | None = None, entity: dict | None = None
) -> bool:
    try:
        binding = graphql_helpers.get_mcp_context().tool_context.get(
            ResultPolicyBinding
        )
    except LookupError:
        binding = None
    return (
        urn == DEFAULT_POLICY_DOCUMENT_URN
        or (isinstance(binding, ResultPolicyBinding) and urn in binding.reserved_urns)
        or (decisions is not None and decisions.is_configuration(urn))
        or (entity is not None and entity.get("subType") == POLICY_DOCUMENT_SUBTYPE)
    )


def apply_decision(
    item: dict, urn: str, decisions: ResultDecisions, *, explain: bool = False
) -> dict:
    label = decisions.annotation(urn, explain=explain)
    if label is not None:
        item[ANNOTATION_KEY] = dict(label)
    # OSS hosts may supply labels only; Cloud adds reader decisions at this seam.
    transform = getattr(decisions, "transform", None)
    return transform(item, urn) if transform is not None else item


def annotate_items(
    items: Sequence[dict],
    urn_of: Callable[[dict], str | None],
    *,
    decisions: ResultDecisions | None = None,
    explain: bool = False,
) -> tuple[list[dict], ResultDecisions | None]:
    decisions = decisions or current_decisions([urn_of(item) for item in items])
    result: list[dict] = []
    for item in items:
        urn = urn_of(item)
        if urn and is_configuration(urn, decisions, item.get("entity") or item):
            continue
        if urn and decisions is not None:
            item = apply_decision(item, urn, decisions, explain=explain)
        result.append(item)
    return result, decisions


def label_entity_results(items: Sequence[dict]) -> ResultDecisions | None:
    """Label ``{"entity": {...}}`` results in place, keeping every position.

    A configuration document becomes an error stub where it was, so the offsets a
    caller pages by still count it.
    """
    decisions = current_decisions(
        [(item.get("entity") or {}).get("urn") for item in items]
    )
    for item in items:
        entity = item.get("entity") or {}
        urn = entity.get("urn")
        if not urn:
            continue
        # The shared checks (default policy document, reserved URNs, subType) apply
        # with or without a bound policy, as they do for search results.
        if is_configuration(urn, decisions, entity):
            item["entity"] = configuration_response(urn)
            continue
        if decisions is None:
            continue
        transformed = apply_decision(item, urn, decisions)
        if transformed is not item:
            item.clear()
            item.update(transformed)
    return decisions


def attach_header(
    response: dict,
    decisions: ResultDecisions | None,
    urns: Sequence[str],
    *,
    reader_only: bool = False,
) -> None:
    if decisions is not None and (header := decisions.header(urns)) is not None:
        if not reader_only or "reader" in header or "reader_policy_state" in header:
            response[HEADER_KEY] = dict(header)


def label_search_response(response: dict) -> dict:
    results = response.get("searchResults") or []
    results, decisions = annotate_items(
        results, lambda item: (item.get("entity") or {}).get("urn")
    )
    if "searchResults" in response:
        response["searchResults"] = results
    attach_header(
        response,
        decisions,
        [
            item["entity"]["urn"]
            for item in results
            if (item.get("entity") or {}).get("urn")
        ],
    )
    return response
