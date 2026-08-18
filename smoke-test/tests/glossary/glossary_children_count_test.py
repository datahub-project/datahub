import logging
import uuid
from typing import Any, Dict, List, Optional

import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.utilities.domains import Domain
from tests.utils import delete_entity, execute_graphql, with_test_retry

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)

# One request that resolves childrenCount for the parent and for every child at the next
# level, so the children are batched together by GlossaryNodeChildrenCountBatchLoader.
NODE_WITH_CHILD_COUNTS_QUERY = """query getNodeWithChildCounts($urn: String!) {
    glossaryNode(urn: $urn) {
        urn
        childrenCount {
            termsCount
            nodesCount
        }
        glossaryChildrenSearch(input: { query: "*", count: 100, types: [GLOSSARY_NODE] }) {
            total
            searchResults {
                entity {
                    urn
                    ... on GlossaryNode {
                        childrenCount {
                            termsCount
                            nodesCount
                        }
                    }
                }
            }
        }
    }
}"""

CREATE_NODE_MUTATION = """mutation createGlossaryNode($input: CreateGlossaryEntityInput!) {
    createGlossaryNode(input: $input)
}"""

CREATE_TERM_MUTATION = """mutation createGlossaryTerm($input: CreateGlossaryEntityInput!) {
    createGlossaryTerm(input: $input)
}"""


def _create_node(auth_session, node_id: str, parent: Optional[str] = None) -> str:
    entity_input: Dict[str, Any] = {"id": node_id, "name": node_id}
    if parent:
        entity_input["parentNode"] = parent
    res = execute_graphql(
        auth_session,
        CREATE_NODE_MUTATION,
        {"input": entity_input},
        no_sync_wait=True,
    )
    return res["data"]["createGlossaryNode"]


def _create_term(auth_session, term_id: str, parent: str) -> str:
    res = execute_graphql(
        auth_session,
        CREATE_TERM_MUTATION,
        {"input": {"id": term_id, "name": term_id, "parentNode": parent}},
        no_sync_wait=True,
    )
    return res["data"]["createGlossaryTerm"]


@pytest.fixture(scope="module")
def glossary_tree(auth_session):
    """A parent node with three child nodes and two child terms.

    One child node has children of its own, the other two are leaves, so a single request
    exercises a multi-parent batch that mixes populated and empty parents.
    """
    suffix = f"cc-{uuid.uuid4().hex[:8]}"
    logger.info(f"creating glossary tree {suffix}")
    created: List[str] = []

    def track(urn: str) -> str:
        created.append(urn)
        return urn

    parent = track(_create_node(auth_session, f"{suffix}-parent"))
    branch = track(_create_node(auth_session, f"{suffix}-branch", parent))
    leaf_a = track(_create_node(auth_session, f"{suffix}-leaf-a", parent))
    leaf_b = track(_create_node(auth_session, f"{suffix}-leaf-b", parent))
    for i in range(2):
        track(_create_term(auth_session, f"{suffix}-parent-term-{i}", parent))
    for i in range(3):
        track(_create_term(auth_session, f"{suffix}-branch-term-{i}", branch))
    track(_create_node(auth_session, f"{suffix}-branch-child", branch))

    wait_for_writes_to_sync(mae_only=True)

    yield {"parent": parent, "branch": branch, "leaf_a": leaf_a, "leaf_b": leaf_b}

    logger.info(f"removing glossary tree {suffix}")
    # Deepest entities first so a delete never orphans a child. Nothing reads
    # this state afterward, so the deletes don't need to wait for each other.
    for urn in reversed(created):
        delete_entity(auth_session, urn, no_sync_wait=True)


@with_test_retry()
def _fetch_counts(auth_session, parent_urn: str) -> Dict[str, Dict[str, int]]:
    """Returns {urn: {termsCount, nodesCount}} for the parent and each of its child nodes."""
    res = execute_graphql(
        auth_session, NODE_WITH_CHILD_COUNTS_QUERY, {"urn": parent_urn}
    )
    node = res["data"]["glossaryNode"]
    counts = {node["urn"]: node["childrenCount"]}
    for result in node["glossaryChildrenSearch"]["searchResults"]:
        entity = result["entity"]
        counts[entity["urn"]] = entity["childrenCount"]
    # The three child nodes must all be indexed before the counts mean anything.
    assert len(counts) == 4, f"expected parent + 3 children, got {sorted(counts)}"
    return counts


def test_children_count_is_correct_for_a_batch_of_sibling_nodes(
    auth_session, glossary_tree
):
    counts = _fetch_counts(auth_session, glossary_tree["parent"])

    assert counts[glossary_tree["parent"]] == {"termsCount": 2, "nodesCount": 3}
    assert counts[glossary_tree["branch"]] == {"termsCount": 3, "nodesCount": 1}
    # Leaves must report zero rather than inherit a sibling's counts -- a batched
    # aggregation returns no buckets for childless parents, and those have to be
    # distinguished from parents whose buckets were simply not read.
    assert counts[glossary_tree["leaf_a"]] == {"termsCount": 0, "nodesCount": 0}
    assert counts[glossary_tree["leaf_b"]] == {"termsCount": 0, "nodesCount": 0}
