import logging
import time
import uuid
from typing import Any, Callable, Dict, List, Tuple

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DomainsClass,
    GlobalTagsClass,
    TagAssociationClass,
)
from tests.consistency_utils import wait_for_writes_to_sync
from tests.privileges.utils import create_user, remove_user
from tests.utils import (
    delete_urns,
    get_admin_credentials,
    get_frontend_url,
    login_as,
)

logger = logging.getLogger(__name__)

PERF_NUM_QUERIES = 25
PERF_LATENCY_RATIO_THRESHOLD = 2
PERF_LATENCY_ABS_FLOOR_SECONDS = 0.05
PERF_USER_EMAIL = "perf_eval_user@example.com"
PERF_USER_PASSWORD = "perf_eval_user_pw"
PERF_GROUP_PREFIX = "PerfEvalGroup-"
PERF_DECOY_PREFIX = "PerfEvalDecoy-"
PERF_NUM_DATASETS = 15
PERF_PAGE_COUNT = 20
PERF_NUM_DECOY_GROUPS = 10
PERF_TAG_URN = "urn:li:tag:PerfEvalTag"

_CREATE_POLICY_QUERY = """mutation createPolicy($input: PolicyUpdateInput!) {
        createPolicy(input: $input) }"""
_DELETE_POLICY_QUERY = """mutation deletePolicy($urn: String!) {
        deletePolicy(urn: $urn) }"""
_CREATE_GROUP_QUERY = """mutation createGroup($input: CreateGroupInput!) {
        createGroup(input: $input) }"""
_ADD_GROUP_MEMBERS_QUERY = """mutation addGroupMembers($groupUrn: String!, $userUrns: [String!]!) {
        addGroupMembers(input: { groupUrn: $groupUrn, userUrns: $userUrns }) }"""
_REMOVE_GROUP_QUERY = """mutation removeGroup($urn: String!) {
        removeGroup(urn: $urn) }"""
_CREATE_DOMAIN_QUERY = """mutation createDomain($input: CreateDomainInput!) {
        createDomain(input: $input) }"""
_DELETE_DOMAIN_QUERY = """mutation deleteDomain($urn: String!) {
        deleteDomain(urn: $urn) }"""

# Mirrors a content/search page rendering per-result action affordances: each result resolves the
# EntityPrivileges object, which fans out into ~14-22 authorize() calls per entity.
_SEARCH_PRIVILEGES_QUERY = """query searchPerf($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    total
    searchResults {
      entity {
        urn
        ... on Dataset {
          privileges {
            canEditLineage
            canEditTags
            canEditOwners
            canEditDomains
            canEditDeprecation
            canEditDescription
          }
        }
      }
    }
  }
}"""


def _post_graphql(session, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    response = session.post(
        f"{get_frontend_url()}/api/v2/graphql",
        json={"query": query, "variables": variables},
    )
    response.raise_for_status()
    res_data = response.json()
    assert res_data, "GraphQL response is empty"
    assert "errors" not in res_data, f"GraphQL errors: {res_data.get('errors')}"
    return res_data


def _timed_query(session, query: str, variables: Dict[str, Any]) -> List[float]:
    """Return sorted, outlier-trimmed per-query durations (seconds) over PERF_NUM_QUERIES posts."""
    durations = []
    payload = {"query": query, "variables": variables}
    for _ in range(PERF_NUM_QUERIES):
        t0 = time.perf_counter()
        response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=payload)
        response.raise_for_status()
        durations.append(time.perf_counter() - t0)
    durations.sort()
    trim = max(1, PERF_NUM_QUERIES // 10)
    return durations[:-trim]


def _median(values: List[float]) -> float:
    return values[len(values) // 2]


def _assert_page_not_slowed_by_scale(
    timed_fn: Callable[[], List[float]], scale_setup: Callable[[], None], *, label: str
) -> None:
    """Assert ``timed_fn`` median latency after ``scale_setup`` adds its policies stays within
    max(baseline x ratio, baseline + abs floor). The floor keeps the check meaningful on a few-ms
    baseline dominated by jitter; a discarded warm-up precedes each measurement."""
    timed_fn()
    baseline_median = _median(timed_fn())

    scale_setup()
    wait_for_writes_to_sync()
    time.sleep(3)

    timed_fn()
    scaled_median = _median(timed_fn())
    ratio = scaled_median / baseline_median if baseline_median > 0 else 0
    allowed = max(
        baseline_median * PERF_LATENCY_RATIO_THRESHOLD,
        baseline_median + PERF_LATENCY_ABS_FLOOR_SECONDS,
    )
    logger.info(
        "%s perf: baseline median=%.2fms, scaled median=%.2fms (ratio=%.2fx, allowed<=%.2fms)",
        label,
        baseline_median * 1000,
        scaled_median * 1000,
        ratio,
        allowed * 1000,
    )
    assert scaled_median <= allowed, (
        f"{label} median grew from {baseline_median * 1000:.2f}ms to "
        f"{scaled_median * 1000:.2f}ms after adding policies ({ratio:.2f}x), "
        f"exceeding allowed {allowed * 1000:.2f}ms."
    )


def _create_scoped_policy(
    session,
    *,
    name: str,
    privileges: List[str],
    group_urns: List[str],
    resource_owners: bool,
    domain_urn: str,
) -> str:
    """Create an ACTIVE METADATA policy scoped to a DOMAIN criterion. Group-scoped policies match
    via ``group_urns``; owner-scoped policies set ``resource_owners`` and leave groups empty."""
    variables: Dict[str, Any] = {
        "input": {
            "type": "METADATA",
            "name": name,
            "description": "Perf eval scoped policy",
            "state": "ACTIVE",
            "resources": {
                "filter": {
                    "criteria": [
                        {"field": "DOMAIN", "values": [domain_urn], "condition": "EQUALS"}
                    ]
                }
            },
            "privileges": privileges,
            "actors": {
                "users": [],
                "groups": group_urns,
                "resourceOwners": resource_owners,
                "allUsers": False,
                "allGroups": False,
            },
        }
    }
    res = _post_graphql(session, _CREATE_POLICY_QUERY, variables)
    return res["data"]["createPolicy"]


def _provision_user_and_groups(
    admin_session, *, num_groups: int, num_decoy: int
) -> Tuple[Any, str, List[str], List[str]]:
    """Create the perf user in ``num_groups`` membership groups plus ``num_decoy`` decoy groups the
    user is not in (used to scope policies so they evaluate but never match the actor). Returns the
    refreshed admin session, the user URN, membership group URNs, and decoy group URNs."""
    admin_session = create_user(admin_session, PERF_USER_EMAIL, PERF_USER_PASSWORD)
    user_urn = f"urn:li:corpuser:{PERF_USER_EMAIL}"
    group_urns: List[str] = []
    decoy_group_urns: List[str] = []
    for i in range(num_groups):
        res = _post_graphql(
            admin_session,
            _CREATE_GROUP_QUERY,
            {"input": {"name": f"{PERF_GROUP_PREFIX}{num_groups}-{i}"}},
        )
        gurn = res["data"]["createGroup"]
        group_urns.append(gurn)
        _post_graphql(
            admin_session,
            _ADD_GROUP_MEMBERS_QUERY,
            {"groupUrn": gurn, "userUrns": [user_urn]},
        )
    for i in range(num_decoy):
        res = _post_graphql(
            admin_session,
            _CREATE_GROUP_QUERY,
            {"input": {"name": f"{PERF_DECOY_PREFIX}{num_groups}-{i}"}},
        )
        decoy_group_urns.append(res["data"]["createGroup"])
    return admin_session, user_urn, group_urns, decoy_group_urns


def _ingest_perf_datasets(graph_client, domain_urn: str, run_id: str) -> List[str]:
    """Emit PERF_NUM_DATASETS datasets into ``domain_urn``, tagged with PERF_TAG_URN, so the
    heavy-page search returns results whose domain must be resolved during per-result authorization."""
    dataset_urns: List[str] = []
    for i in range(PERF_NUM_DATASETS):
        name = f"perf_eval_ds_{run_id}_{i}"
        urn = f"urn:li:dataset:(urn:li:dataPlatform:kafka,{name},PROD)"
        dataset_urns.append(urn)
        graph_client.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=DatasetPropertiesClass(name=name)
            )
        )
        graph_client.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=DomainsClass(domains=[domain_urn])
            )
        )
        graph_client.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=GlobalTagsClass(tags=[TagAssociationClass(tag=PERF_TAG_URN)]),
            )
        )
    return dataset_urns


def _cleanup(
    admin_session,
    graph_client,
    *,
    policy_urns: List[str],
    group_urns: List[str],
    user_urn: str,
    dataset_urns: List[str],
    domain_urns: List[str],
) -> None:
    for urn in policy_urns:
        try:
            _post_graphql(admin_session, _DELETE_POLICY_QUERY, {"urn": urn})
        except Exception:
            logger.warning("Failed to clean up perf policy %s", urn)
    try:
        delete_urns(graph_client, dataset_urns)
    except Exception:
        logger.warning("Failed to clean up perf datasets")
    for urn in domain_urns:
        try:
            _post_graphql(admin_session, _DELETE_DOMAIN_QUERY, {"urn": urn})
        except Exception:
            logger.warning("Failed to clean up perf domain %s", urn)
    for urn in group_urns:
        try:
            _post_graphql(admin_session, _REMOVE_GROUP_QUERY, {"urn": urn})
        except Exception:
            logger.warning("Failed to clean up perf group %s", urn)
    try:
        remove_user(admin_session, user_urn)
    except Exception:
        logger.warning("Failed to clean up perf user %s", user_urn)


@pytest.mark.parametrize("num_groups,num_policies", [(10, 20), (100, 50)])
def test_domains_page_perf_with_domain_scoped_policies(
    auth_session, graph_client, num_groups, num_policies
):
    """Loading a domain's contents (search filtered by domain, rendering per-result privileges) for
    a many-group user must not blow up as DOMAIN-scoped policies accumulate. Each policy is scoped
    to the parent domain via a decoy group, so it is evaluated on every per-result check but never
    matches the actor -- the case that previously forced a recursive domain walk per policy."""
    admin_username, admin_password = get_admin_credentials()
    admin_session = login_as(admin_username, admin_password)

    run_id = uuid.uuid4().hex[:8]
    parent_domain_id = f"perf_parent_{run_id}"
    child_domain_id = f"perf_child_{run_id}"
    parent_domain_urn = f"urn:li:domain:{parent_domain_id}"
    child_domain_urn = f"urn:li:domain:{child_domain_id}"

    admin_session, user_urn, group_urns, decoy_group_urns = _provision_user_and_groups(
        admin_session, num_groups=num_groups, num_decoy=PERF_NUM_DECOY_GROUPS
    )

    policy_urns: List[str] = []
    dataset_urns: List[str] = []
    try:
        _post_graphql(
            admin_session,
            _CREATE_DOMAIN_QUERY,
            {"input": {"id": parent_domain_id, "name": parent_domain_id}},
        )
        _post_graphql(
            admin_session,
            _CREATE_DOMAIN_QUERY,
            {
                "input": {
                    "id": child_domain_id,
                    "name": child_domain_id,
                    "parentDomain": parent_domain_urn,
                }
            },
        )
        dataset_urns = _ingest_perf_datasets(graph_client, child_domain_urn, run_id)
        wait_for_writes_to_sync()
        time.sleep(3)

        user_session = login_as(PERF_USER_EMAIL, PERF_USER_PASSWORD)

        def _timed():
            return _timed_query(
                user_session,
                _SEARCH_PRIVILEGES_QUERY,
                {
                    "input": {
                        "types": ["DATASET"],
                        "query": "*",
                        "count": PERF_PAGE_COUNT,
                        "orFilters": [
                            {"and": [{"field": "domains", "values": [child_domain_urn]}]}
                        ],
                    }
                },
            )

        def _setup():
            for i in range(num_policies):
                policy_urns.append(
                    _create_scoped_policy(
                        admin_session,
                        name=f"PerfDomainScoped-{num_groups}-{i}",
                        privileges=["EDIT_ENTITY_TAGS", "EDIT_ENTITY_OWNERS"],
                        group_urns=[decoy_group_urns[i % len(decoy_group_urns)]],
                        resource_owners=False,
                        domain_urn=parent_domain_urn,
                    )
                )

        _assert_page_not_slowed_by_scale(
            _timed, _setup, label=f"domains-page[g={num_groups},p={num_policies}]"
        )
    finally:
        _cleanup(
            admin_session,
            graph_client,
            policy_urns=policy_urns,
            group_urns=group_urns + decoy_group_urns,
            user_urn=user_urn,
            dataset_urns=dataset_urns,
            domain_urns=[child_domain_urn, parent_domain_urn],
        )

