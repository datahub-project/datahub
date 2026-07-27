"""
Smoke tests for GMS entity graph cache via GraphQL hierarchy resolvers.

Exercises bundled domain (FULL / search / SCHEDULED), glossary (PARTIAL / graph /
LAZY), and container (PARTIAL / graph / LAZY) graphs through parentDomains,
parentNodes, and parentContainers. Membership tests cover session-user group
relationship labeling (``IsMemberOfGroup`` vs ``IsMemberOfNativeGroup``) and
corpGroup incoming member listing after UI ``addGroupMembers``. The Playwright-parity
test ``test_corp_group_incoming_members_immediately_after_add_second_member`` polls
INCOMING members with a short timeout after a no-sync-wait ``addGroupMembers``.

Hierarchy setup uses batched ``graph_client.emit_mcp`` plus one ``wait_for_hierarchy_writes``
per test. GraphQL mutations are reserved for sync invalidation cases (moveDomain /
updateParentNode). Set ``DATAHUB_SKIP_CLEANUP=true`` to retain entities for inspection.
"""

import logging
from typing import Optional

import pytest

from tests.entity_graph_cache.helpers import (
    DEFAULT_DATAHUB_USER_URN,
    IS_MEMBER_OF_GROUP,
    IS_MEMBER_OF_NATIVE_GROUP,
    add_corp_group_membership,
    add_users_to_native_group,
    cleanup_containers,
    cleanup_domains,
    cleanup_glossary_entities,
    create_native_group,
    delete_native_group,
    emit_container,
    emit_domain,
    emit_glossary_node,
    emit_glossary_term,
    fetch_prometheus_metrics,
    move_domain,
    prometheus_counter_total,
    prometheus_isolated,
    query_container_child_urns,
    query_corpuser_group_relationships,
    query_parent_container_urns_on_container,
    query_parent_domain_urns,
    query_parent_node_urns_on_term,
    remove_corp_group_membership,
    remove_users_from_native_group,
    unique_id,
    update_parent_node,
    wait_for_corp_group_incoming_members,
    wait_for_hierarchy_writes,
    wait_for_session_group_membership_labels,
)

logger = logging.getLogger(__name__)

METRIC_SEARCH_SCROLL = "entity.graph.build.search_scroll"
METRIC_GRAPH_SCROLL = "entity.graph.build.graph_scroll"


def test_domain_parent_domains_hierarchy(auth_session, graph_client):
    run_id = unique_id("egc-domain")
    grandparent_id = f"egc-grand-{run_id}"
    parent_id = f"egc-parent-{run_id}"
    child_id = f"egc-child-{run_id}"
    created: list[str] = []

    try:
        grandparent_urn = emit_domain(
            graph_client, grandparent_id, f"EGC Grand {run_id}"
        )
        created.append(grandparent_urn)

        parent_urn = emit_domain(
            graph_client,
            parent_id,
            f"EGC Parent {run_id}",
            parent_domain_urn=grandparent_urn,
        )
        created.append(parent_urn)

        child_urn = emit_domain(
            graph_client,
            child_id,
            f"EGC Child {run_id}",
            parent_domain_urn=parent_urn,
        )
        created.append(child_urn)

        wait_for_hierarchy_writes()

        parent_urns = query_parent_domain_urns(auth_session, child_urn)
        assert len(parent_urns) >= 2
        assert parent_urns[0] == parent_urn
        assert grandparent_urn in parent_urns
    finally:
        cleanup_domains(auth_session, created)


def test_domain_sync_move_reflects_in_parent_domains(auth_session, graph_client):
    run_id = unique_id("egc-domain-move")
    root_id = f"egc-root-{run_id}"
    child_id = f"egc-child-{run_id}"
    new_parent_id = f"egc-new-parent-{run_id}"
    created: list[str] = []

    try:
        root_urn = emit_domain(graph_client, root_id, f"EGC Root {run_id}")
        created.append(root_urn)

        child_urn = emit_domain(
            graph_client,
            child_id,
            f"EGC Child {run_id}",
            parent_domain_urn=root_urn,
        )
        created.append(child_urn)

        wait_for_hierarchy_writes()

        before_move = query_parent_domain_urns(auth_session, child_urn)
        assert root_urn in before_move

        new_parent_urn = emit_domain(
            graph_client, new_parent_id, f"EGC New Parent {run_id}"
        )
        created.append(new_parent_urn)

        move_domain(auth_session, child_urn, new_parent_urn)

        after_move = query_parent_domain_urns(auth_session, child_urn)
        assert after_move[0] == new_parent_urn
        assert root_urn not in after_move
    finally:
        cleanup_domains(auth_session, created)


def test_glossary_parent_nodes_hierarchy(auth_session, graph_client):
    run_id = unique_id("egc-glossary")
    root_id = f"egc-gnode-root-{run_id}"
    child_id = f"egc-gnode-child-{run_id}"
    term_id = f"egc-gterm-{run_id}"
    created: list[str] = []

    try:
        root_urn = emit_glossary_node(graph_client, root_id, f"EGC GRoot {run_id}")
        created.append(root_urn)

        child_urn = emit_glossary_node(
            graph_client,
            child_id,
            f"EGC GChild {run_id}",
            parent_node_urn=root_urn,
        )
        created.append(child_urn)

        term_urn = emit_glossary_term(
            graph_client,
            term_id,
            f"EGC GTerm {run_id}",
            parent_node_urn=child_urn,
        )
        created.append(term_urn)

        wait_for_hierarchy_writes()

        parent_urns = query_parent_node_urns_on_term(auth_session, term_urn)
        assert len(parent_urns) >= 2
        assert parent_urns[0] == child_urn
        assert root_urn in parent_urns
    finally:
        cleanup_glossary_entities(graph_client, created)


def test_glossary_sync_reparent_reflects_in_parent_nodes(auth_session, graph_client):
    run_id = unique_id("egc-glossary-reparent")
    root_id = f"egc-gnode-root-{run_id}"
    middle_id = f"egc-gnode-mid-{run_id}"
    leaf_id = f"egc-gnode-leaf-{run_id}"
    new_parent_id = f"egc-gnode-new-{run_id}"
    term_id = f"egc-gterm-{run_id}"
    created: list[str] = []

    try:
        root_urn = emit_glossary_node(graph_client, root_id, f"EGC GRoot {run_id}")
        created.append(root_urn)

        middle_urn = emit_glossary_node(
            graph_client,
            middle_id,
            f"EGC GMid {run_id}",
            parent_node_urn=root_urn,
        )
        created.append(middle_urn)

        leaf_urn = emit_glossary_node(
            graph_client,
            leaf_id,
            f"EGC GLeaf {run_id}",
            parent_node_urn=middle_urn,
        )
        created.append(leaf_urn)

        term_urn = emit_glossary_term(
            graph_client,
            term_id,
            f"EGC GTerm {run_id}",
            parent_node_urn=leaf_urn,
        )
        created.append(term_urn)

        wait_for_hierarchy_writes()

        before = query_parent_node_urns_on_term(auth_session, term_urn)
        assert leaf_urn in before
        assert middle_urn in before
        assert root_urn in before

        new_parent_urn = emit_glossary_node(
            graph_client, new_parent_id, f"EGC GNew {run_id}"
        )
        created.append(new_parent_urn)

        update_parent_node(auth_session, leaf_urn, new_parent_urn)

        after = query_parent_node_urns_on_term(auth_session, term_urn)
        assert after[0] == leaf_urn
        assert new_parent_urn in after
        assert middle_urn not in after
        assert root_urn not in after
    finally:
        cleanup_glossary_entities(graph_client, created)


def test_glossary_deep_hierarchy_within_bundled_max_depth(auth_session, graph_client):
    run_id = unique_id("egc-glossary-deep")
    node_ids = [f"egc-gnode-{level}-{run_id}" for level in range(6)]
    term_id = f"egc-gterm-deep-{run_id}"
    created: list[str] = []
    node_urns: list[str] = []

    try:
        node_urns.append(
            emit_glossary_node(graph_client, node_ids[0], f"EGC Level0 {run_id}")
        )
        created.append(node_urns[0])

        for index in range(1, 6):
            urn = emit_glossary_node(
                graph_client,
                node_ids[index],
                f"EGC Level{index} {run_id}",
                parent_node_urn=node_urns[index - 1],
            )
            node_urns.append(urn)
            created.append(urn)

        term_urn = emit_glossary_term(
            graph_client,
            term_id,
            f"EGC Deep Term {run_id}",
            parent_node_urn=node_urns[-1],
        )
        created.append(term_urn)

        wait_for_hierarchy_writes()

        parent_urns = query_parent_node_urns_on_term(auth_session, term_urn)
        assert len(parent_urns) == 6
        for expected in reversed(node_urns):
            assert expected in parent_urns
    finally:
        cleanup_glossary_entities(graph_client, created)


def test_domain_cache_metrics_when_isolated(auth_session, graph_client):
    if not prometheus_isolated(auth_session):
        pytest.skip(
            "Prometheus cache metrics require isolated run (management URL resolvable "
            "from auth_session, BATCH_COUNT=1, no pytest-xdist worker)"
        )

    run_id = unique_id("egc-domain-metrics")
    grandparent_id = f"egc-metrics-grand-{run_id}"
    parent_id = f"egc-metrics-parent-{run_id}"
    child_id = f"egc-metrics-child-{run_id}"
    created: list[str] = []

    try:
        grandparent_urn = emit_domain(
            graph_client, grandparent_id, f"EGC Metrics Grand {run_id}"
        )
        created.append(grandparent_urn)

        parent_urn = emit_domain(
            graph_client,
            parent_id,
            f"EGC Metrics Parent {run_id}",
            parent_domain_urn=grandparent_urn,
        )
        created.append(parent_urn)

        child_urn = emit_domain(
            graph_client,
            child_id,
            f"EGC Metrics Child {run_id}",
            parent_domain_urn=parent_urn,
        )
        created.append(child_urn)

        wait_for_hierarchy_writes()

        metrics_before = fetch_prometheus_metrics(auth_session)
        counter_before = prometheus_counter_total(metrics_before, METRIC_SEARCH_SCROLL)

        query_parent_domain_urns(auth_session, child_urn)
        metrics_after_first = fetch_prometheus_metrics(auth_session)
        counter_after_first = prometheus_counter_total(
            metrics_after_first, METRIC_SEARCH_SCROLL
        )

        query_parent_domain_urns(auth_session, child_urn)
        metrics_after_second = fetch_prometheus_metrics(auth_session)
        counter_after_second = prometheus_counter_total(
            metrics_after_second, METRIC_SEARCH_SCROLL
        )

        assert counter_after_second == counter_after_first
        logger.info(
            "domain search_scroll counter: before=%s after_first=%s after_second=%s",
            counter_before,
            counter_after_first,
            counter_after_second,
        )
    finally:
        cleanup_domains(auth_session, created)


def test_glossary_cache_metrics_when_isolated(auth_session, graph_client):
    if not prometheus_isolated(auth_session):
        pytest.skip(
            "Prometheus cache metrics require isolated run (management URL resolvable "
            "from auth_session, BATCH_COUNT=1, no pytest-xdist worker)"
        )

    run_id = unique_id("egc-glossary-metrics")
    root_id = f"egc-metrics-groot-{run_id}"
    child_id = f"egc-metrics-gchild-{run_id}"
    term_id = f"egc-metrics-gterm-{run_id}"
    created: list[str] = []

    try:
        root_urn = emit_glossary_node(
            graph_client, root_id, f"EGC Metrics GRoot {run_id}"
        )
        created.append(root_urn)

        child_urn = emit_glossary_node(
            graph_client,
            child_id,
            f"EGC Metrics GChild {run_id}",
            parent_node_urn=root_urn,
        )
        created.append(child_urn)

        term_urn = emit_glossary_term(
            graph_client,
            term_id,
            f"EGC Metrics GTerm {run_id}",
            parent_node_urn=child_urn,
        )
        created.append(term_urn)

        wait_for_hierarchy_writes()

        metrics_before = fetch_prometheus_metrics(auth_session)
        counter_before = prometheus_counter_total(metrics_before, METRIC_GRAPH_SCROLL)

        query_parent_node_urns_on_term(auth_session, term_urn)
        metrics_after_first = fetch_prometheus_metrics(auth_session)
        counter_after_first = prometheus_counter_total(
            metrics_after_first, METRIC_GRAPH_SCROLL
        )

        query_parent_node_urns_on_term(auth_session, term_urn)
        metrics_after_second = fetch_prometheus_metrics(auth_session)
        counter_after_second = prometheus_counter_total(
            metrics_after_second, METRIC_GRAPH_SCROLL
        )

        assert counter_after_second == counter_after_first
        logger.info(
            "glossary graph_scroll counter: before=%s after_first=%s after_second=%s",
            counter_before,
            counter_after_first,
            counter_after_second,
        )
    finally:
        cleanup_glossary_entities(graph_client, created)


def test_container_parent_containers_hierarchy(auth_session, graph_client):
    run_id = unique_id("egc-container")
    grandparent_id = f"egc-c-grand-{run_id}"
    parent_id = f"egc-c-parent-{run_id}"
    child_id = f"egc-c-child-{run_id}"
    created: list[str] = []

    try:
        grandparent_urn = emit_container(
            graph_client, grandparent_id, f"EGC Grand Container {run_id}"
        )
        created.append(grandparent_urn)

        parent_urn = emit_container(
            graph_client,
            parent_id,
            f"EGC Parent Container {run_id}",
            parent_container_urn=grandparent_urn,
        )
        created.append(parent_urn)

        child_urn = emit_container(
            graph_client,
            child_id,
            f"EGC Child Container {run_id}",
            parent_container_urn=parent_urn,
        )
        created.append(child_urn)

        wait_for_hierarchy_writes()

        parent_urns = query_parent_container_urns_on_container(auth_session, child_urn)
        assert len(parent_urns) >= 2
        assert parent_urns[0] == parent_urn
        assert grandparent_urn in parent_urns
    finally:
        cleanup_containers(graph_client, created)


def test_container_relationships_direct_children(auth_session, graph_client):
    run_id = unique_id("egc-container-rel")
    grandparent_id = f"egc-cr-grand-{run_id}"
    parent_id = f"egc-cr-parent-{run_id}"
    child_id = f"egc-cr-child-{run_id}"
    created: list[str] = []

    try:
        grandparent_urn = emit_container(
            graph_client, grandparent_id, f"EGC Rel Grand Container {run_id}"
        )
        created.append(grandparent_urn)

        parent_urn = emit_container(
            graph_client,
            parent_id,
            f"EGC Rel Parent Container {run_id}",
            parent_container_urn=grandparent_urn,
        )
        created.append(parent_urn)

        child_urn = emit_container(
            graph_client,
            child_id,
            f"EGC Rel Child Container {run_id}",
            parent_container_urn=parent_urn,
        )
        created.append(child_urn)

        wait_for_hierarchy_writes()

        child_urns = query_container_child_urns(auth_session, grandparent_urn)
        assert parent_urn in child_urns
        assert child_urn not in child_urns
    finally:
        cleanup_containers(graph_client, created)


def test_membership_relationships_idempotent_for_session_user(auth_session):
    """GraphQL corpuser group relationships should be stable across repeated reads."""
    user_urn = auth_session._upstream.cookies["actor"]

    first = query_corpuser_group_relationships(auth_session, user_urn)
    second = query_corpuser_group_relationships(auth_session, user_urn)
    assert first == second


def test_session_user_dual_group_types_labeled_correctly(auth_session, graph_client):
    """Session-user outgoing membership must label corp vs native groups separately."""
    user_urn = auth_session._upstream.cookies["actor"]
    run_id = unique_id("egc-membership")
    native_group_id = f"egc-native-{run_id}"
    corp_group_id = f"egc-corp-{run_id}"
    native_group_urn: Optional[str] = None
    corp_group_urn: Optional[str] = None

    try:
        native_group_urn = create_native_group(auth_session, native_group_id)
        add_users_to_native_group(auth_session, native_group_urn, [user_urn])

        # Corp SSO-style membership uses groupMembership; the group entity must exist
        # or GraphQL filters the relationship in mapEntityRelationships.
        corp_group_urn = create_native_group(auth_session, corp_group_id)
        add_corp_group_membership(graph_client, user_urn, corp_group_urn)
        wait_for_hierarchy_writes()

        wait_for_session_group_membership_labels(
            auth_session,
            user_urn,
            {
                native_group_urn: IS_MEMBER_OF_NATIVE_GROUP,
                corp_group_urn: IS_MEMBER_OF_GROUP,
            },
        )
    finally:
        if native_group_urn is not None:
            remove_users_from_native_group(auth_session, native_group_urn, [user_urn])
            delete_native_group(auth_session, native_group_urn)
        if corp_group_urn is not None:
            remove_corp_group_membership(graph_client, user_urn, corp_group_urn)
            delete_native_group(auth_session, corp_group_urn)
        wait_for_hierarchy_writes()


def test_corp_group_incoming_members_after_native_add(auth_session):
    """Group profile member listing must reflect UI addGroupMembers immediately."""
    user_urn = auth_session._upstream.cookies["actor"]
    run_id = unique_id("egc-group-members")
    group_id = f"egc-members-{run_id}"
    group_urn: Optional[str] = None

    try:
        group_urn = create_native_group(auth_session, group_id)
        add_users_to_native_group(auth_session, group_urn, [user_urn])

        members = wait_for_corp_group_incoming_members(
            auth_session, group_urn, [user_urn]
        )
        assert user_urn in members
    finally:
        if group_urn is not None:
            remove_users_from_native_group(auth_session, group_urn, [user_urn])
            delete_native_group(auth_session, group_urn)


def test_corp_group_incoming_members_immediately_after_add_second_member(
    auth_session,
):
    """
    Playwright parity for v2-managing-groups "create group and add member".

    Mirrors the UI sequence:
      1. createGroup
      2. addGroupMembers(admin) — CreateGroupModal auto-adds the logged-in user
      3. addGroupMembers(datahub)
      4. getGroup → corpGroup INCOMING IsMemberOf* relationships (page reload)

    Step 3 bypasses ``TestSessionWrapper`` post-mutation sync waits. Member listing
    is polled with a short timeout (Playwright reload + ``expect`` retry budget),
    not a single-shot read.
    """
    admin_urn = auth_session._upstream.cookies["actor"]
    datahub_urn = DEFAULT_DATAHUB_USER_URN
    run_id = unique_id("egc-playwright-member")
    group_urn: Optional[str] = None

    try:
        group_urn = create_native_group(auth_session, f"egc-grp-{run_id}")
        add_users_to_native_group(
            auth_session, group_urn, [admin_urn], wait_for_sync=True
        )
        add_users_to_native_group(
            auth_session, group_urn, [datahub_urn], wait_for_sync=False
        )

        members = wait_for_corp_group_incoming_members(
            auth_session,
            group_urn,
            [admin_urn, datahub_urn],
            timeout_seconds=10.0,
            poll_interval_seconds=0.5,
            use_no_sync_wait=True,
        )
        logger.info(
            "corpGroup INCOMING members after addGroupMembers (playwright parity): %s",
            members,
        )

        assert admin_urn in members, f"admin missing from INCOMING members: {members}"
        assert datahub_urn in members, (
            "datahub missing after addGroupMembers (Playwright parity); "
            f"members={members}"
        )
    finally:
        if group_urn is not None:
            try:
                remove_users_from_native_group(
                    auth_session, group_urn, [admin_urn, datahub_urn]
                )
            except Exception:
                pass
            delete_native_group(auth_session, group_urn)
