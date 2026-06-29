"""
Helpers for entity graph cache smoke tests.

Hierarchy **setup** uses batched ``graph_client.emit_mcp`` (no per-entity sync wait).
Call ``wait_for_hierarchy_writes()`` once after emitting all aspects, then query via
``auth_session``. Sync invalidation tests use GraphQL ``moveDomain`` / ``updateParentNode``
only for the mutation under test (each triggers one ``TestSessionWrapper`` wait).

Prometheus counter probes are only valid when this module runs alone — see
``prometheus_isolated``.
"""

import logging
import os
import time
import uuid
from typing import Any, Dict, List, Optional

import requests
from requests.structures import CaseInsensitiveDict

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    ContainerClass,
    ContainerPropertiesClass,
    DomainPropertiesClass,
    GlossaryNodeInfoClass,
    GlossaryTermInfoClass,
    GroupMembershipClass,
)
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utilities import env_vars
from tests.utils import (
    delete_entity,
    execute_graphql,
    get_gms_prometheus_base_url,
    with_test_retry,
)

logger = logging.getLogger(__name__)

MOVE_DOMAIN = """
mutation moveDomain($input: MoveDomainInput!) {
  moveDomain(input: $input)
}
"""

UPDATE_PARENT_NODE = """
mutation updateParentNode($input: UpdateParentNodeInput!) {
  updateParentNode(input: $input)
}
"""

PARENT_DOMAINS_QUERY = """
query parentDomains($urn: String!) {
  domain(urn: $urn) {
    parentDomains {
      count
      domains {
        urn
        ... on Domain {
          properties {
            name
          }
        }
      }
    }
  }
}
"""

PARENT_NODES_ON_TERM_QUERY = """
query parentNodesOnTerm($urn: String!) {
  glossaryTerm(urn: $urn) {
    parentNodes {
      count
      nodes {
        urn
        properties {
          name
        }
      }
    }
  }
}
"""

PARENT_CONTAINERS_ON_DATASET_QUERY = """
query parentContainersOnDataset($urn: String!) {
  dataset(urn: $urn) {
    parentContainers {
      count
      containers {
        urn
        properties {
          name
        }
      }
    }
  }
}
"""

PARENT_CONTAINERS_ON_CONTAINER_QUERY = """
query parentContainersOnContainer($urn: String!) {
  container(urn: $urn) {
    parentContainers {
      count
      containers {
        urn
        properties {
          name
        }
      }
    }
  }
}
"""

CONTAINER_CHILD_RELATIONSHIPS_QUERY = """
query containerChildRelationships($urn: String!) {
  container(urn: $urn) {
    relationships(
      input: {
        types: ["IsPartOf"]
        direction: INCOMING
        start: 0
        count: 100
      }
    ) {
      total
      relationships {
        type
        entity {
          urn
        }
      }
    }
  }
}
"""

CORPUSER_GROUP_RELATIONSHIPS_QUERY = """
query corpUserGroupRelationships($urn: String!) {
  corpUser(urn: $urn) {
    relationships(
      input: {
        types: ["IsMemberOfGroup", "IsMemberOfNativeGroup"]
        direction: OUTGOING
        start: 0
        count: 100
      }
    ) {
      total
      relationships {
        type
        entity {
          urn
        }
      }
    }
  }
}
"""

CORP_GROUP_INCOMING_MEMBERS_QUERY = """
query corpGroupIncomingMembers($urn: String!) {
  corpGroup(urn: $urn) {
    relationships(
      input: {
        types: ["IsMemberOfGroup", "IsMemberOfNativeGroup"]
        direction: INCOMING
        start: 0
        count: 100
        includeSoftDelete: false
      }
    ) {
      total
      relationships {
        type
        entity {
          urn
        }
      }
    }
  }
}
"""

CREATE_GROUP = """
mutation createGroup($input: CreateGroupInput!) {
  createGroup(input: $input)
}
"""

ADD_GROUP_MEMBERS = """
mutation addGroupMembers($input: AddGroupMembersInput!) {
  addGroupMembers(input: $input)
}
"""

REMOVE_GROUP_MEMBERS = """
mutation removeGroupMembers($input: RemoveGroupMembersInput!) {
  removeGroupMembers(input: $input)
}
"""

REMOVE_GROUP = """
mutation removeGroup($urn: String!) {
  removeGroup(urn: $urn)
}
"""

IS_MEMBER_OF_GROUP = "IsMemberOfGroup"
IS_MEMBER_OF_NATIVE_GROUP = "IsMemberOfNativeGroup"

# Default quickstart / Playwright E2E user (v2-managing-groups adds this member).
DEFAULT_DATAHUB_USER_URN = "urn:li:corpuser:datahub"


def unique_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def execute_graphql_no_sync_wait(
    auth_session,
    query: str,
    variables: Optional[Dict[str, Any]] = None,
) -> dict:
    """GraphQL POST without ``TestSessionWrapper`` kafka/ES sync wait.

    Smoke tests wrap ``auth_session`` so every POST waits for consumer lag + ES
    refresh. The Playwright groups test reloads immediately after
    ``addGroupMembers``; use this helper for write→read timing parity.
    """
    json_payload: Dict[str, Any] = {"query": query}
    if variables:
        json_payload["variables"] = variables
    headers = CaseInsensitiveDict(
        {"Authorization": f"Bearer {auth_session.gms_token()}"}
    )
    response = auth_session._upstream.post(
        f"{auth_session.frontend_url()}/api/v2/graphql",
        json=json_payload,
        headers=headers,
    )
    response.raise_for_status()
    res_data = response.json()
    assert res_data, "GraphQL response is empty"
    assert res_data.get("data") is not None, (
        f"GraphQL response.data is None. Errors: {res_data.get('errors')}"
    )
    assert "errors" not in res_data, f"GraphQL errors: {res_data.get('errors')}"
    return res_data


def skip_cleanup_enabled() -> bool:
    return os.environ.get("DATAHUB_SKIP_CLEANUP", "").lower() in ("1", "true", "yes")


def domain_urn(domain_id: str) -> str:
    return f"urn:li:domain:{domain_id}"


def glossary_node_urn(node_id: str) -> str:
    return f"urn:li:glossaryNode:{node_id}"


def glossary_term_urn(term_id: str) -> str:
    return f"urn:li:glossaryTerm:{term_id}"


def container_urn(container_id: str) -> str:
    return f"urn:li:container:{container_id}"


def corp_group_urn(group_id: str) -> str:
    return f"urn:li:corpGroup:{group_id}"


def wait_for_hierarchy_writes() -> None:
    """Single Kafka/ES sync wait after a batch of MCP emits."""
    wait_for_writes_to_sync()


def prometheus_isolated(auth_session) -> bool:
    """Return True when Prometheus counter deltas are meaningful for this process."""
    if get_gms_prometheus_base_url() is None:
        return False
    if env_vars.get_batch_count() > 1:
        return False
    if os.getenv("PYTEST_XDIST_WORKER"):
        return False
    return True


def fetch_prometheus_metrics(auth_session) -> str:
    base_url = get_gms_prometheus_base_url()
    if base_url is None:
        raise RuntimeError("GMS Prometheus base URL is not available")
    prometheus_url = f"{base_url}/actuator/prometheus"
    headers = {"Authorization": f"Bearer {auth_session.gms_token()}"}
    response = requests.get(prometheus_url, headers=headers)
    response.raise_for_status()
    return response.text


def prometheus_counter_total(
    metrics_text: str, metric_name: str, **tag_filters: str
) -> float:
    """Sum Micrometer counter values exported to Prometheus text format."""
    base = metric_name.replace(".", "_")
    candidates = sorted({f"{base}_total", base}, key=len, reverse=True)
    total = 0.0

    for line in metrics_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        matched_name: Optional[str] = None
        for candidate in candidates:
            if stripped.startswith(candidate) and (
                stripped == candidate or stripped[len(candidate)] in ("{", " ")
            ):
                matched_name = candidate
                break
        if matched_name is None:
            continue

        remainder = stripped[len(matched_name) :]
        if remainder.startswith("{") and "}" in remainder:
            tags_part, _, value_part = remainder.partition("}")
            tags_part = tags_part.lstrip("{")
            tags: Dict[str, str] = {}
            for piece in tags_part.split(","):
                if "=" not in piece:
                    continue
                key, raw_value = piece.split("=", 1)
                tags[key.strip()] = raw_value.strip().strip('"')
            if tag_filters and not all(
                tags.get(key) == value for key, value in tag_filters.items()
            ):
                continue
            total += float(value_part.strip())
        elif not tag_filters:
            total += float(remainder.strip())

    return total


def emit_domain(
    graph_client: DataHubGraph,
    domain_id: str,
    name: str,
    parent_domain_urn: Optional[str] = None,
) -> str:
    """Emit domain properties via MCP (no sync wait — batch then ``wait_for_hierarchy_writes``)."""
    urn = domain_urn(domain_id)
    props = DomainPropertiesClass(name=name, description=name)
    if parent_domain_urn is not None:
        props.parentDomain = parent_domain_urn
    graph_client.emit_mcp(MetadataChangeProposalWrapper(entityUrn=urn, aspect=props))
    return urn


def emit_glossary_node(
    graph_client: DataHubGraph,
    node_id: str,
    name: str,
    parent_node_urn: Optional[str] = None,
) -> str:
    urn = glossary_node_urn(node_id)
    info = GlossaryNodeInfoClass(definition=name, name=name)
    if parent_node_urn is not None:
        info.parentNode = parent_node_urn
    graph_client.emit_mcp(MetadataChangeProposalWrapper(entityUrn=urn, aspect=info))
    return urn


def emit_glossary_term(
    graph_client: DataHubGraph,
    term_id: str,
    name: str,
    parent_node_urn: str,
) -> str:
    urn = glossary_term_urn(term_id)
    info = GlossaryTermInfoClass(
        definition=name,
        name=name,
        termSource="INTERNAL",
        parentNode=parent_node_urn,
    )
    graph_client.emit_mcp(MetadataChangeProposalWrapper(entityUrn=urn, aspect=info))
    return urn


def emit_container(
    graph_client: DataHubGraph,
    container_id: str,
    name: str,
    parent_container_urn: Optional[str] = None,
) -> str:
    urn = container_urn(container_id)
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=ContainerPropertiesClass(name=name, description=name),
        )
    )
    if parent_container_urn is not None:
        graph_client.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=ContainerClass(container=parent_container_urn),
            )
        )
    return urn


def move_domain(auth_session, child_urn: str, parent_domain_urn: Optional[str]) -> None:
    execute_graphql(
        auth_session,
        MOVE_DOMAIN,
        {"input": {"resourceUrn": child_urn, "parentDomain": parent_domain_urn}},
    )


def update_parent_node(
    auth_session, resource_urn: str, parent_node_urn: Optional[str]
) -> None:
    execute_graphql(
        auth_session,
        UPDATE_PARENT_NODE,
        {"input": {"resourceUrn": resource_urn, "parentNode": parent_node_urn}},
    )


@with_test_retry()
def query_parent_domain_urns(auth_session, domain_urn: str) -> List[str]:
    result = execute_graphql(auth_session, PARENT_DOMAINS_QUERY, {"urn": domain_urn})
    parents = result["data"]["domain"]["parentDomains"]
    assert parents is not None
    return [entry["urn"] for entry in parents["domains"]]


@with_test_retry()
def query_parent_node_urns_on_term(auth_session, term_urn: str) -> List[str]:
    result = execute_graphql(
        auth_session, PARENT_NODES_ON_TERM_QUERY, {"urn": term_urn}
    )
    parents = result["data"]["glossaryTerm"]["parentNodes"]
    assert parents is not None
    return [entry["urn"] for entry in parents["nodes"]]


@with_test_retry()
def query_parent_container_urns_on_dataset(auth_session, dataset_urn: str) -> List[str]:
    result = execute_graphql(
        auth_session, PARENT_CONTAINERS_ON_DATASET_QUERY, {"urn": dataset_urn}
    )
    parents = result["data"]["dataset"]["parentContainers"]
    assert parents is not None
    return [entry["urn"] for entry in parents["containers"]]


@with_test_retry()
def query_parent_container_urns_on_container(
    auth_session, container_urn: str
) -> List[str]:
    result = execute_graphql(
        auth_session, PARENT_CONTAINERS_ON_CONTAINER_QUERY, {"urn": container_urn}
    )
    parents = result["data"]["container"]["parentContainers"]
    assert parents is not None
    return [entry["urn"] for entry in parents["containers"]]


@with_test_retry()
def query_container_child_urns(auth_session, container_urn: str) -> List[str]:
    result = execute_graphql(
        auth_session, CONTAINER_CHILD_RELATIONSHIPS_QUERY, {"urn": container_urn}
    )
    relationships = result["data"]["container"]["relationships"]
    assert relationships is not None
    return [
        entry["entity"]["urn"]
        for entry in relationships["relationships"]
        if entry.get("entity") and entry["entity"].get("urn")
    ]


@with_test_retry()
def query_corpuser_group_relationships(auth_session, user_urn: str) -> List[dict]:
    result = execute_graphql(
        auth_session, CORPUSER_GROUP_RELATIONSHIPS_QUERY, {"urn": user_urn}
    )
    relationships = result["data"]["corpUser"]["relationships"]
    assert relationships is not None
    return relationships["relationships"]


def _parse_corp_group_incoming_member_urns(result: dict) -> List[str]:
    relationships = result["data"]["corpGroup"]["relationships"]
    assert relationships is not None
    return [
        entry["entity"]["urn"]
        for entry in relationships["relationships"]
        if entry.get("entity") and entry["entity"].get("urn")
    ]


@with_test_retry()
def query_corp_group_incoming_member_urns(auth_session, group_urn: str) -> List[str]:
    result = execute_graphql(
        auth_session, CORP_GROUP_INCOMING_MEMBERS_QUERY, {"urn": group_urn}
    )
    return _parse_corp_group_incoming_member_urns(result)


def query_corp_group_incoming_member_urns_immediate(
    auth_session, group_urn: str
) -> List[str]:
    """Single-shot INCOMING members read — no retry, no post-mutation sync wait."""
    result = execute_graphql_no_sync_wait(
        auth_session, CORP_GROUP_INCOMING_MEMBERS_QUERY, {"urn": group_urn}
    )
    return _parse_corp_group_incoming_member_urns(result)


def wait_for_corp_group_incoming_members(
    auth_session,
    group_urn: str,
    expected_member_urns: List[str],
    timeout_seconds: float = 25.0,
    poll_interval_seconds: float = 1.0,
    *,
    use_no_sync_wait: bool = False,
) -> List[str]:
    """Poll corpGroup INCOMING membership until listed members match."""
    expected = set(expected_member_urns)
    deadline = time.monotonic() + timeout_seconds
    last: List[str] = []
    query_fn = (
        query_corp_group_incoming_member_urns_immediate
        if use_no_sync_wait
        else query_corp_group_incoming_member_urns
    )
    while time.monotonic() < deadline:
        last = query_fn(auth_session, group_urn)
        if expected.issubset(set(last)):
            return last
        time.sleep(poll_interval_seconds)
    raise AssertionError(
        "Timed out waiting for corpGroup incoming members: "
        f"expected={sorted(expected)}, last={last}"
    )


def group_relationship_type_by_urn(relationships: List[dict]) -> Dict[str, str]:
    return {
        entry["entity"]["urn"]: entry["type"]
        for entry in relationships
        if entry.get("entity") and entry["entity"].get("urn") and entry.get("type")
    }


def wait_for_session_group_membership_labels(
    auth_session,
    user_urn: str,
    expected_labels: Dict[str, str],
    timeout_seconds: float = 25.0,
    poll_interval_seconds: float = 1.0,
) -> Dict[str, str]:
    """Poll session-user GraphQL until corp/native group labels match.

    GMS caches ``groupMembership`` on the entity client (~20s TTL). MCP writes
    update storage immediately but GraphQL session identity reads through that cache,
    so labels can lag briefly after ``add_corp_group_membership``.
    """
    deadline = time.monotonic() + timeout_seconds
    last: Dict[str, str] = {}
    while time.monotonic() < deadline:
        relationships = query_corpuser_group_relationships(auth_session, user_urn)
        last = group_relationship_type_by_urn(relationships)
        if all(
            last.get(group_urn) == rel_type
            for group_urn, rel_type in expected_labels.items()
        ):
            return last
        time.sleep(poll_interval_seconds)
    raise AssertionError(
        "Timed out waiting for session-user group relationship labels: "
        f"expected={expected_labels}, last={last}"
    )


def create_native_group(auth_session, group_id: str) -> str:
    result = execute_graphql(auth_session, CREATE_GROUP, {"input": {"name": group_id}})
    group_urn = result["data"]["createGroup"]
    assert group_urn
    wait_for_writes_to_sync()
    return group_urn


def add_users_to_native_group(
    auth_session,
    group_urn: str,
    user_urns: List[str],
    *,
    wait_for_sync: bool = True,
) -> None:
    variables = {"input": {"groupUrn": group_urn, "userUrns": user_urns}}
    if wait_for_sync:
        execute_graphql(auth_session, ADD_GROUP_MEMBERS, variables)
        wait_for_writes_to_sync()
    else:
        execute_graphql_no_sync_wait(auth_session, ADD_GROUP_MEMBERS, variables)


def remove_users_from_native_group(
    auth_session, group_urn: str, user_urns: List[str]
) -> None:
    execute_graphql(
        auth_session,
        REMOVE_GROUP_MEMBERS,
        {"input": {"groupUrn": group_urn, "userUrns": user_urns}},
    )
    wait_for_writes_to_sync()


def delete_native_group(auth_session, group_urn: str) -> None:
    execute_graphql(auth_session, REMOVE_GROUP, {"urn": group_urn})
    wait_for_writes_to_sync()


def add_corp_group_membership(
    graph_client: DataHubGraph, user_urn: str, group_urn: str
) -> None:
    existing = graph_client.get_aspect(user_urn, GroupMembershipClass)
    groups = (
        [str(group) for group in existing.groups]
        if existing and existing.groups
        else []
    )
    if group_urn not in groups:
        groups.append(group_urn)
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=user_urn,
            aspect=GroupMembershipClass(groups=groups),
        )
    )
    wait_for_writes_to_sync()


def remove_corp_group_membership(
    graph_client: DataHubGraph, user_urn: str, group_urn: str
) -> None:
    existing = graph_client.get_aspect(user_urn, GroupMembershipClass)
    if not existing or not existing.groups:
        return
    groups = [group for group in existing.groups if group != group_urn]
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=user_urn,
            aspect=GroupMembershipClass(groups=groups),
        )
    )
    wait_for_writes_to_sync()


def cleanup_domains(auth_session, urns: List[str]) -> None:
    if skip_cleanup_enabled():
        logger.info("DATAHUB_SKIP_CLEANUP=true — leaving domains: %s", urns)
        return
    for urn in reversed(urns):
        try:
            delete_entity(auth_session, urn)
        except Exception as exc:
            logger.warning("Domain cleanup failed for %s: %s", urn, exc)


def cleanup_glossary_entities(graph_client: DataHubGraph, urns: List[str]) -> None:
    if skip_cleanup_enabled():
        logger.info("DATAHUB_SKIP_CLEANUP=true — leaving glossary entities: %s", urns)
        return
    for urn in reversed(urns):
        try:
            graph_client.hard_delete_entity(urn)
        except Exception as exc:
            logger.warning("Glossary cleanup failed for %s: %s", urn, exc)


def cleanup_containers(graph_client: DataHubGraph, urns: List[str]) -> None:
    if skip_cleanup_enabled():
        logger.info("DATAHUB_SKIP_CLEANUP=true — leaving containers: %s", urns)
        return
    for urn in reversed(urns):
        try:
            graph_client.hard_delete_entity(urn)
        except Exception as exc:
            logger.warning("Container cleanup failed for %s: %s", urn, exc)
