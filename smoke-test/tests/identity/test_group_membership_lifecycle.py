import uuid

import pytest
import tenacity

from datahub.metadata.schema_classes import NativeGroupMembershipClass
from tests.consistency_utils import wait_for_writes_to_sync
from tests.privileges.utils import assign_user_to_group, remove_group
from tests.utils import execute_graphql, get_frontend_url

_POLL_TIMEOUT_SECONDS = 60
_POLL_INTERVAL_SECONDS = 2

_GROUP_STATE_QUERY = """
query getGroupState($urn: String!) {
  corpGroup(urn: $urn) {
    exists
    relationships(input: {
      types: ["IsMemberOfNativeGroup"], direction: INCOMING, start: 0, count: 100
    }) {
      total
    }
  }
}
"""

_CREATE_GROUP_WITH_ID_MUTATION = """
mutation createGroup($input: CreateGroupInput!) {
  createGroup(input: $input)
}
"""

_MEMBER_USER_URN = "urn:li:corpuser:datahub"


def _create_group_with_id(auth_session, group_id: str) -> str:
    # create_group() in tests.privileges.utils omits `id`, so the resolver mints a random
    # UUID as the key and the URN changes on every call. This test needs the same URN to
    # survive a delete/recreate cycle, so it must pass `id` explicitly.
    response = auth_session.post(
        f"{get_frontend_url()}/api/v2/graphql",
        json={
            "query": _CREATE_GROUP_WITH_ID_MUTATION,
            "variables": {"input": {"id": group_id, "name": group_id}},
        },
    )
    response.raise_for_status()
    res_data = response.json()
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createGroup"]
    wait_for_writes_to_sync()
    return res_data["data"]["createGroup"]


def _native_member_total(auth_session, group_urn: str) -> int:
    res = execute_graphql(auth_session, _GROUP_STATE_QUERY, {"urn": group_urn})
    return res["data"]["corpGroup"]["relationships"]["total"]


def _native_groups(graph_client, user_urn: str) -> list:
    aspect = graph_client.get_aspect(user_urn, NativeGroupMembershipClass)
    return list(aspect.nativeGroups) if aspect else []


@tenacity.retry(
    stop=tenacity.stop_after_delay(_POLL_TIMEOUT_SECONDS),
    wait=tenacity.wait_fixed(_POLL_INTERVAL_SECONDS),
    reraise=True,
)
def _assert_group_absent_from_native_groups(
    graph_client, user_urn: str, group_urn: str
) -> None:
    # The member-side cleanup after a group delete runs on CompletableFuture.runAsync inside
    # GMS. That task isn't tracked by any Kafka consumer, so wait_for_writes_to_sync's lag-based
    # polling gives no signal about whether it has finished; only polling the aspect itself does.
    assert group_urn not in _native_groups(graph_client, user_urn), (
        f"Expected {group_urn} to be removed from {user_urn}'s nativeGroupMembership aspect "
        f"within {_POLL_TIMEOUT_SECONDS}s of the group delete."
    )


@tenacity.retry(
    stop=tenacity.stop_after_delay(_POLL_TIMEOUT_SECONDS),
    wait=tenacity.wait_fixed(_POLL_INTERVAL_SECONDS),
    reraise=True,
)
def _assert_group_present_in_native_groups(
    graph_client, user_urn: str, group_urn: str
) -> None:
    assert group_urn in _native_groups(graph_client, user_urn), (
        f"Expected {group_urn} to appear in {user_urn}'s nativeGroupMembership aspect "
        f"within {_POLL_TIMEOUT_SECONDS}s of being re-added."
    )


@tenacity.retry(
    stop=tenacity.stop_after_delay(_POLL_TIMEOUT_SECONDS),
    wait=tenacity.wait_fixed(_POLL_INTERVAL_SECONDS),
    reraise=True,
)
def _assert_native_member_total(auth_session, group_urn: str, expected: int) -> None:
    actual = _native_member_total(auth_session, group_urn)
    assert actual == expected, (
        f"Expected {expected} native member(s) of {group_urn} within "
        f"{_POLL_TIMEOUT_SECONDS}s, found {actual} instead."
    )


@pytest.fixture(scope="module")
def group_id() -> str:
    return f"test-group-lifecycle-{uuid.uuid4().hex[:8]}"


def test_membership_converges_after_group_delete_and_recreate(
    auth_session, graph_client, group_id
):
    """A group deleted and recreated under the same URN must accept its members again.

    Previously, deleting a group reaped its IsMemberOfNativeGroup edges but left each member's
    nativeGroupMembership aspect still naming it. Re-adding the member then wrote identical aspect
    content, which is suppressed both at MCL emission and by graph diff mode, so the edge could
    never be rebuilt and the group stayed empty while addGroupMembers reported success.

    What this test pins is the cleanup half: that the member's aspect no longer references the
    group after deletion, so the sequence converges. The repair half — rebuilding an edge when the
    aspect still names the group — cannot be reached from this sequence and is covered by unit
    tests in GroupServiceTest instead.
    """
    group_urn = _create_group_with_id(auth_session, group_id)
    try:
        assign_user_to_group(auth_session, group_urn, [_MEMBER_USER_URN])
        wait_for_writes_to_sync()
        assert _native_member_total(auth_session, group_urn) == 1
        assert group_urn in _native_groups(graph_client, _MEMBER_USER_URN)

        remove_group(auth_session, group_urn)
        wait_for_writes_to_sync()

        # Half B: the member-side aspect must not keep a dangling reference to the deleted group.
        # Polled rather than asserted once: the cleanup that clears this aspect runs
        # asynchronously in-process (see the helper above), so a single read can race it.
        _assert_group_absent_from_native_groups(
            graph_client, _MEMBER_USER_URN, group_urn
        )

        recreated_urn = _create_group_with_id(auth_session, group_id)
        assert recreated_urn == group_urn
        assert _native_member_total(auth_session, group_urn) == 0

        # The re-add must succeed and be visible on both sides. Note this exercises the ordinary
        # add path, not the missing-edge repair: the assertion above proved the aspect no longer
        # names the group, so this add changes aspect content and never reaches the repair branch.
        # Repair is only reachable when the aspect still names the group while the edge is gone,
        # which requires a recreate to beat the asynchronous cleanup — see GroupServiceTest for
        # that path. Both checks are polled: the edge side of this update is still subject to the
        # same asynchronous graph-index refresh as the rest of this test.
        assign_user_to_group(auth_session, group_urn, [_MEMBER_USER_URN])
        wait_for_writes_to_sync()
        _assert_native_member_total(auth_session, group_urn, 1)
        _assert_group_present_in_native_groups(
            graph_client, _MEMBER_USER_URN, group_urn
        )
    finally:
        remove_group(auth_session, group_urn)
