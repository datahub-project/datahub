"""Shared setup and comparison for the hidden-vs-missing smoke tests.

The rule under test: a caller who can't view an entity gets exactly the answer a caller gets for
an entity that doesn't exist. Each hidden entity has an absent twin, a urn of the same shape that
is never written. The tests send every request twice, once naming the hidden urn and once naming
its twin, and require the same answer after both urns are replaced by one placeholder.

The hidden entities carry data on as many aspects as practical, and visible entities point at
them, so a response that reads anything about a hidden entity differs from its twin's.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.parse
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionTypeClass,
    AuditStampClass,
    ChangeAuditStampsClass,
    ChartInfoClass,
    ContainerClass,
    ContainerPropertiesClass,
    CustomAssertionInfoClass,
    DashboardInfoClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataProductAssociationClass,
    DataProductPropertiesClass,
    DatasetLineageTypeClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    DatasetUsageStatisticsClass,
    DomainPropertiesClass,
    DomainsClass,
    GlobalTagsClass,
    GlossaryNodeInfoClass,
    GlossaryTermAssociationClass,
    GlossaryTermInfoClass,
    GlossaryTermsClass,
    IncidentInfoClass,
    IncidentStatusClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    OperationClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    TagAssociationClass,
    TagPropertiesClass,
    TimeWindowSizeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from tests.e2e.privileges.utils import (
    clear_polices,
    create_user,
    remove_policy,
    remove_user,
    set_base_platform_privileges_policy_status,
    set_view_dataset_sensitive_info_policy_status,
    set_view_entity_profile_privileges_policy_status,
)
from tests.e2e.utils import (
    delete_urns,
    get_frontend_session,
    get_frontend_url,
    login_as,
    unique_suffix,
)
from utilities.consistency_utils import wait_for_writes_to_sync

logger = logging.getLogger(__name__)

PLACEHOLDER = "<ENTITY>"
POLICY_PREFIX = "Smoke hidden vs missing"
USER_PASSWORD = "user"

_ACTOR = "urn:li:corpuser:datahub"


@dataclass(frozen=True)
class HiddenPair:
    """A hidden entity and its never-written twin, keyed by entity type."""

    entity_type: str
    hidden_id: str
    absent_id: str
    hidden_urn: str
    absent_urn: str


@dataclass
class HiddenWorld:
    suffix: str
    user_email: str
    user_urn: str
    pairs: List[HiddenPair]
    written_urns: List[str] = field(default_factory=list)
    policy_urn: Optional[str] = None

    def user_session(self) -> Any:
        return login_as(self.user_email, USER_PASSWORD)


def _stamp() -> AuditStampClass:
    return AuditStampClass(time=int(time.time() * 1000), actor=_ACTOR)


def _dataset_urn(name: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:hive,{name},PROD)"


def _pair(entity_type: str, urn_for: Callable[[str], str], base: str) -> HiddenPair:
    hidden_id = f"{base}-hidden"
    absent_id = f"{base}-absent"
    return HiddenPair(
        entity_type=entity_type,
        hidden_id=hidden_id,
        absent_id=absent_id,
        hidden_urn=urn_for(hidden_id),
        absent_urn=urn_for(absent_id),
    )


def _build_pairs(suffix: str) -> Dict[str, HiddenPair]:
    base = f"hvm-{suffix}"
    return {
        "dataset": _pair("dataset", _dataset_urn, f"{base}-ds"),
        "container": _pair("container", lambda i: f"urn:li:container:{i}", f"{base}-ct"),
        "domain": _pair("domain", lambda i: f"urn:li:domain:{i}", f"{base}-dm"),
        "tag": _pair("tag", lambda i: f"urn:li:tag:{i}", f"{base}-tg"),
        "glossaryTerm": _pair(
            "glossaryTerm", lambda i: f"urn:li:glossaryTerm:{i}", f"{base}-gt"
        ),
        "glossaryNode": _pair(
            "glossaryNode", lambda i: f"urn:li:glossaryNode:{i}", f"{base}-gn"
        ),
        "chart": _pair("chart", lambda i: f"urn:li:chart:(looker,{i})", f"{base}-ch"),
        "dashboard": _pair(
            "dashboard", lambda i: f"urn:li:dashboard:(looker,{i})", f"{base}-db"
        ),
        "dataFlow": _pair(
            "dataFlow", lambda i: f"urn:li:dataFlow:(airflow,{i},PROD)", f"{base}-df"
        ),
        "dataProduct": _pair(
            "dataProduct", lambda i: f"urn:li:dataProduct:{i}", f"{base}-dp"
        ),
    }


def _mcps(suffix: str, pairs: Dict[str, HiddenPair]) -> List[MetadataChangeProposalWrapper]:
    now = int(time.time() * 1000)
    base = f"hvm-{suffix}"
    visible_container = f"urn:li:container:{base}-visible-ct"
    visible_domain = f"urn:li:domain:{base}-visible-dm"
    visible_tag = f"urn:li:tag:{base}-visible-tg"
    visible_node = f"urn:li:glossaryNode:{base}-visible-gn"
    visible_term = f"urn:li:glossaryTerm:{base}-visible-gt"
    upstream = _dataset_urn(f"{base}-upstream")
    downstream = _dataset_urn(f"{base}-downstream")
    child = _dataset_urn(f"{base}-child")
    visible_chart = f"urn:li:chart:(looker,{base}-visible-ch)"
    visible_product = f"urn:li:dataProduct:{base}-visible-dp"
    assertion = f"urn:li:assertion:{base}-assertion"
    incident = f"urn:li:incident:{base}-incident"
    job = f"urn:li:dataJob:({pairs['dataFlow'].hidden_urn},{base}-job)"

    ds = pairs["dataset"].hidden_urn
    ct = pairs["container"].hidden_urn
    dm = pairs["domain"].hidden_urn
    tg = pairs["tag"].hidden_urn
    gt = pairs["glossaryTerm"].hidden_urn
    gn = pairs["glossaryNode"].hidden_urn
    ch = pairs["chart"].hidden_urn
    db = pairs["dashboard"].hidden_urn
    df = pairs["dataFlow"].hidden_urn
    dp = pairs["dataProduct"].hidden_urn
    secret = f"secret-{suffix}"
    audit = ChangeAuditStampsClass()

    def mcp(urn: str, aspect: Any) -> MetadataChangeProposalWrapper:
        return MetadataChangeProposalWrapper(entityUrn=urn, aspect=aspect)

    return [
        # Visible entities the hidden ones hang off.
        mcp(visible_container, ContainerPropertiesClass(name=f"{base} visible container")),
        mcp(visible_domain, DomainPropertiesClass(name=f"{base} visible domain")),
        mcp(visible_tag, TagPropertiesClass(name=f"{base} visible tag")),
        mcp(
            visible_node,
            GlossaryNodeInfoClass(definition="visible node", name=f"{base} visible node"),
        ),
        mcp(
            visible_term,
            GlossaryTermInfoClass(
                definition="visible term",
                termSource="INTERNAL",
                name=f"{base} visible term",
                parentNode=visible_node,
            ),
        ),
        mcp(upstream, DatasetPropertiesClass(name=f"{base} upstream")),
        mcp(
            visible_chart,
            ChartInfoClass(title=f"{base} visible chart", description="", lastModified=audit),
        ),
        # The hidden dataset, with data on as many aspects as practical.
        mcp(
            ds,
            DatasetPropertiesClass(
                name=secret, description=secret, customProperties={"secret": secret}
            ),
        ),
        mcp(
            ds,
            SchemaMetadataClass(
                schemaName=secret,
                platform="urn:li:dataPlatform:hive",
                version=0,
                hash="",
                platformSchema=OtherSchemaClass(rawSchema=""),
                fields=[
                    SchemaFieldClass(
                        fieldPath="col_a",
                        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                        nativeDataType="string",
                        description=secret,
                    )
                ],
            ),
        ),
        mcp(ds, ContainerClass(container=visible_container)),
        mcp(ds, DomainsClass(domains=[visible_domain])),
        mcp(ds, GlobalTagsClass(tags=[TagAssociationClass(tag=visible_tag)])),
        mcp(
            ds,
            GlossaryTermsClass(
                terms=[GlossaryTermAssociationClass(urn=visible_term)], auditStamp=_stamp()
            ),
        ),
        mcp(
            ds,
            OwnershipClass(
                owners=[OwnerClass(owner=_ACTOR, type=OwnershipTypeClass.TECHNICAL_OWNER)]
            ),
        ),
        mcp(
            ds,
            UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(dataset=upstream, type=DatasetLineageTypeClass.TRANSFORMED)
                ]
            ),
        ),
        mcp(ds, SubTypesClass(typeNames=["Table"])),
        mcp(
            ds,
            InstitutionalMemoryClass(
                elements=[
                    InstitutionalMemoryMetadataClass(
                        url="https://example.com/doc",
                        description=secret,
                        createStamp=_stamp(),
                    )
                ]
            ),
        ),
        mcp(ds, DatasetProfileClass(timestampMillis=now, rowCount=42, columnCount=1)),
        mcp(
            ds,
            DatasetUsageStatisticsClass(
                timestampMillis=now - now % 86400000,
                eventGranularity=TimeWindowSizeClass(unit="DAY"),
                uniqueUserCount=3,
                totalSqlQueries=7,
                topSqlQueries=[f"select '{secret}'"],
            ),
        ),
        mcp(
            ds,
            OperationClass(
                timestampMillis=now, operationType="INSERT", lastUpdatedTimestamp=now
            ),
        ),
        mcp(
            downstream,
            UpstreamLineageClass(
                upstreams=[UpstreamClass(dataset=ds, type=DatasetLineageTypeClass.TRANSFORMED)]
            ),
        ),
        mcp(
            visible_product,
            DataProductPropertiesClass(
                name=f"{base} visible product",
                assets=[DataProductAssociationClass(destinationUrn=ds)],
            ),
        ),
        mcp(
            assertion,
            AssertionInfoClass(
                type=AssertionTypeClass.CUSTOM,
                customAssertion=CustomAssertionInfoClass(type="custom", entity=ds),
                description=secret,
            ),
        ),
        mcp(
            incident,
            IncidentInfoClass(
                type="OPERATIONAL",
                entities=[ds],
                status=IncidentStatusClass(state="ACTIVE", lastUpdated=_stamp()),
                created=_stamp(),
                title=secret,
            ),
        ),
        # Hidden container with a visible parent and a visible child.
        mcp(ct, ContainerPropertiesClass(name=secret, description=secret)),
        mcp(ct, ContainerClass(container=visible_container)),
        mcp(child, ContainerClass(container=ct)),
        mcp(child, DomainsClass(domains=[dm])),
        mcp(child, GlobalTagsClass(tags=[TagAssociationClass(tag=tg)])),
        mcp(
            child,
            GlossaryTermsClass(terms=[GlossaryTermAssociationClass(urn=gt)], auditStamp=_stamp()),
        ),
        # Hidden domain under a visible domain, holding the visible child.
        mcp(dm, DomainPropertiesClass(name=secret, description=secret, parentDomain=visible_domain)),
        # Hidden tag and glossary entries, attached to the visible child.
        mcp(tg, TagPropertiesClass(name=secret, description=secret)),
        mcp(gn, GlossaryNodeInfoClass(definition=secret, name=secret, parentNode=visible_node)),
        mcp(
            gt,
            GlossaryTermInfoClass(
                definition=secret, termSource="INTERNAL", name=secret, parentNode=gn
            ),
        ),
        # Hidden chart and dashboard, wired to visible ones.
        mcp(
            ch,
            ChartInfoClass(title=secret, description=secret, lastModified=audit, inputs=[upstream]),
        ),
        mcp(
            db,
            DashboardInfoClass(
                title=secret, description=secret, lastModified=audit, charts=[visible_chart]
            ),
        ),
        # Hidden flow with a visible job.
        mcp(df, DataFlowInfoClass(name=secret, description=secret)),
        mcp(job, DataJobInfoClass(name=f"{base} job", type="COMMAND")),
        mcp(
            job,
            DataJobInputOutputClass(inputDatasets=[upstream], outputDatasets=[downstream]),
        ),
        # Hidden data product holding the visible upstream.
        mcp(
            dp,
            DataProductPropertiesClass(
                name=secret,
                description=secret,
                assets=[DataProductAssociationClass(destinationUrn=upstream)],
            ),
        ),
        mcp(dp, DomainsClass(domains=[visible_domain])),
    ]


def _metadata_privileges(admin_session: Any) -> List[str]:
    response = admin_session.post(
        f"{get_frontend_url()}/api/v2/graphql",
        json={
            "query": """{ getAppConfig { policiesConfig { resourcePrivileges {
                privileges { type } } } } }"""
        },
    )
    response.raise_for_status()
    groups = response.json()["data"]["getAppConfig"]["policiesConfig"]["resourcePrivileges"]
    return sorted({p["type"] for group in groups for p in group["privileges"]})


def _create_hiding_policy(admin_session: Any, user_urn: str, hidden_urns: List[str]) -> str:
    """Grants the user every metadata privilege on everything except the hidden urns.

    This is the policy shape that made existence checkable: a urn that was never written matches
    "everything except", so the caller is allowed to ask about it.
    """
    response = admin_session.post(
        f"{get_frontend_url()}/api/v2/graphql",
        json={
            "query": """mutation createPolicy($input: PolicyUpdateInput!) {
                createPolicy(input: $input) }""",
            "variables": {
                "input": {
                    "type": "METADATA",
                    "name": f"{POLICY_PREFIX} {user_urn}",
                    "description": "Hides the entities of a hidden-vs-missing smoke test",
                    "state": "ACTIVE",
                    "resources": {
                        "filter": {
                            "criteria": [
                                {
                                    "field": "URN",
                                    "values": hidden_urns,
                                    "condition": "NOT_EQUALS",
                                }
                            ]
                        }
                    },
                    "privileges": _metadata_privileges(admin_session),
                    "actors": {
                        "users": [user_urn],
                        "resourceOwners": False,
                        "allUsers": False,
                        "allGroups": False,
                    },
                }
            },
        },
    )
    response.raise_for_status()
    data = response.json()
    assert data.get("data") and data["data"].get("createPolicy"), data
    wait_for_writes_to_sync(mae_only=True)
    return data["data"]["createPolicy"]


def hidden_world(graph_client: Any) -> Iterator[HiddenWorld]:
    """Module fixture body: writes the fixtures, hides them from a new user, cleans up."""
    suffix = unique_suffix()
    pairs = _build_pairs(suffix)
    mcps = _mcps(suffix, pairs)
    world = HiddenWorld(
        suffix=suffix,
        user_email=f"hidden.vs.missing.{suffix}@smoke.datahub.test",
        user_urn=f"urn:li:corpuser:hidden.vs.missing.{suffix}@smoke.datahub.test",
        pairs=list(pairs.values()),
        written_urns=sorted({m.entityUrn for m in mcps if m.entityUrn}),
    )
    admin_session = get_frontend_session()
    try:
        for proposal in mcps:
            graph_client.emit_mcp(proposal)
        wait_for_writes_to_sync()

        clear_polices(admin_session, name_prefixes=[POLICY_PREFIX])
        set_base_platform_privileges_policy_status("INACTIVE", admin_session)
        set_view_dataset_sensitive_info_policy_status("INACTIVE", admin_session)
        set_view_entity_profile_privileges_policy_status("INACTIVE", admin_session)
        admin_session = create_user(admin_session, world.user_email, USER_PASSWORD)
        world.policy_urn = _create_hiding_policy(
            admin_session, world.user_urn, [p.hidden_urn for p in world.pairs]
        )
        yield world
    finally:
        try:
            if world.policy_urn:
                remove_policy(world.policy_urn, admin_session)
            remove_user(admin_session, world.user_urn)
        except Exception:
            logger.warning("hidden-vs-missing user cleanup failed", exc_info=True)
        set_base_platform_privileges_policy_status("ACTIVE", admin_session)
        set_view_dataset_sensitive_info_policy_status("ACTIVE", admin_session)
        set_view_entity_profile_privileges_policy_status("ACTIVE", admin_session)
        wait_for_writes_to_sync(mae_only=True)
        try:
            delete_urns(graph_client, world.written_urns)
        except Exception:
            logger.warning("hidden-vs-missing entity cleanup failed", exc_info=True)


def normalize(value: Any, pair: HiddenPair) -> Any:
    """Replaces both urns of ``pair``, raw or url-encoded, with one placeholder."""
    text = json.dumps(value, sort_keys=True)
    for entity_id in (pair.hidden_id, pair.absent_id):
        for form in {entity_id, urllib.parse.quote(entity_id, safe="")}:
            text = text.replace(form, PLACEHOLDER)
    return json.loads(text)


def _leaves(value: Any, path: Tuple[Any, ...] = ()) -> Dict[Tuple[Any, ...], Any]:
    if isinstance(value, dict):
        out: Dict[Tuple[Any, ...], Any] = {}
        for key, child in value.items():
            out.update(_leaves(child, path + (key,)))
        return out or {path: {}}
    if isinstance(value, list):
        out = {path + ("#len",): len(value)}
        for index, child in enumerate(value):
            out.update(_leaves(child, path + (index,)))
        return out
    return {path: value}


def differences(hidden: Any, absent: Any, absent_again: Any) -> List[str]:
    """Paths where the hidden answer differs from the absent one.

    Paths where two answers for the absent urn already differ (timestamps, generated ids) are
    ignored.
    """
    hidden_leaves = _leaves(hidden)
    absent_leaves = _leaves(absent)
    again_leaves = _leaves(absent_again)
    unstable = {
        path
        for path in set(absent_leaves) | set(again_leaves)
        if absent_leaves.get(path) != again_leaves.get(path)
    }
    diffs = []
    for path in sorted(set(hidden_leaves) | set(absent_leaves), key=str):
        if any(path[: len(u)] == u for u in unstable):
            continue
        if hidden_leaves.get(path, "<absent>") != absent_leaves.get(path, "<absent>"):
            diffs.append(
                f"{'/'.join(map(str, path))}: hidden={hidden_leaves.get(path, '<absent>')!r} "
                f"missing={absent_leaves.get(path, '<absent>')!r}"
            )
    return diffs
