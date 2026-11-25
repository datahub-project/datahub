from typing import Tuple
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.repository_documents.okf_mapping import (
    OkfLookupCache,
    resolve_reference,
)
from datahub.metadata.schema_classes import GlossaryTermInfoClass
from datahub.utilities.urns.error import InvalidUrnError


def _ctx(
    *, user_exists: bool = False, term_urns: Tuple[str, ...] = ()
) -> PipelineContext:
    ctx = PipelineContext(run_id="test")
    graph = MagicMock()
    graph.exists.return_value = user_exists
    graph.get_urns_by_filter.return_value = list(term_urns)
    graph.get_aspect.return_value = GlossaryTermInfoClass(
        name="Active User", definition="", termSource="INTERNAL"
    )
    ctx.graph = graph
    return ctx


def test_urn_values_are_kept_only_for_the_matching_entity_type() -> None:
    ctx, cache = _ctx(), OkfLookupCache()
    group = "urn:li:corpGroup:data"
    assert resolve_reference(group, "owners", ctx, cache) == group
    assert resolve_reference(group, "tags", ctx, cache) is None
    assert resolve_reference("urn:li:tag:pii", "tags", ctx, cache) == "urn:li:tag:pii"


def test_owners_map_emails_directly_and_names_only_when_the_user_exists() -> None:
    cache = OkfLookupCache()
    assert (
        resolve_reference("a@example.com", "owners", _ctx(), cache)
        == "urn:li:corpuser:a@example.com"
    )
    assert resolve_reference("alice", "owners", _ctx(user_exists=False), cache) is None
    # The lookup is cached per name, so a later graph answer is not consulted.
    assert resolve_reference("alice", "owners", _ctx(user_exists=True), cache) is None


def test_tags_need_not_exist_and_terms_resolve_by_name() -> None:
    term = "urn:li:glossaryTerm:active-user"
    ctx, cache = _ctx(term_urns=(term,)), OkfLookupCache()
    assert resolve_reference("metrics", "tags", ctx, cache) == "urn:li:tag:metrics"
    assert resolve_reference("Active User", "glossary_terms", ctx, cache) == term


def test_malformed_urn_raises() -> None:
    with pytest.raises(InvalidUrnError):
        resolve_reference("urn:li:", "owners", _ctx(), OkfLookupCache())
