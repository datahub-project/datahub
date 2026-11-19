"""
Structured property and tag writes from users allowed to write without Edit privileges.

The structured property and tag privilege-constraint validators run on every write API. They
must only deny a write when a privilegeConstraints policy narrows the user, not when the user was
allowed to write through another privilege or by a service's own rule. Each scenario uses a user
holding only the privilege named in its test:

  - Manage Documents: Rest.li create and UPSERT, tag PATCH, OpenAPI v3 PATCH on documents
  - Manage Users & Groups: OpenAPI v3 upsert on a group, Rest.li tag PATCH on a user
  - Create Entity: new dataset with structured properties and tags (OpenAPI v3 and Rest.li)
  - Delete Entity: OpenAPI v3 DELETE of the structuredProperties aspect
  - Form assignee (no privileges): GraphQL submitFormPrompt
  - Edit Entity constrained to one property: that property is allowed, any other is
    denied on Rest.li and OpenAPI v3

Setup uses only GMS and the admin token, so it also runs against a stack without a frontend.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any, Callable, Dict, Iterator, List
from urllib.parse import quote

import pytest
import requests

from datahub.configuration.common import OperationalError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    CorpGroupInfoClass,
    CorpUserInfoClass,
    DataHubActorFilterClass,
    DataHubPolicyInfoClass,
    DataHubResourceFilterClass,
    DatasetPropertiesClass,
    DocumentContentsClass,
    DocumentInfoClass,
    DocumentStatusClass,
    GlobalTagsClass,
    PolicyMatchCriterionClass,
    PolicyMatchFilterClass,
    StructuredPropertiesClass,
    StructuredPropertyDefinitionClass,
    StructuredPropertyValueAssignmentClass,
    TagAssociationClass,
)
from datahub.metadata.urns import Urn
from datahub.specific.aspect_helpers.tags import HasTagsPatch
from tests.e2e.utils import get_gms_url
from utilities.consistency_utils import wait_for_writes_to_sync
from utilities.domains import Domain

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.domain(Domain.PLATFORM)]

_UNIQUE = uuid.uuid4().hex[:8]
# New policies reach GMS through its policy cache (refreshed every 10s in CI); allow-cases
# retry until they land.
_POLICY_WAIT_SECONDS = 60.0

PROP_A = f"urn:li:structuredProperty:io.datahub.test.constraintsA{_UNIQUE}"
PROP_B = f"urn:li:structuredProperty:io.datahub.test.constraintsB{_UNIQUE}"
TAG_A = f"urn:li:tag:constraints-a-{_UNIQUE}"

DOCUMENT = f"urn:li:document:constraints-doc-{_UNIQUE}"
GROUP = f"urn:li:corpGroup:constraints-group-{_UNIQUE}"
CORP_USER = f"urn:li:corpuser:constraints-target-{_UNIQUE}"
DATASET_EXISTING = (
    f"urn:li:dataset:(urn:li:dataPlatform:hive,constraints.existing_{_UNIQUE},PROD)"
)
DATASET_FORM = (
    f"urn:li:dataset:(urn:li:dataPlatform:hive,constraints.form_{_UNIQUE},PROD)"
)

DOCS_WRITER = "docs"
GROUP_MANAGER = "groups"
CREATOR = "creator"
DELETER = "deleter"
CONSTRAINED = "constrained"
ASSIGNEE = "assignee"
ROLES = (DOCS_WRITER, GROUP_MANAGER, CREATOR, DELETER, CONSTRAINED, ASSIGNEE)


def _email(role: str) -> str:
    return f"constraints.{role}.{_UNIQUE}@smoke.datahub.test"


def _user_urn(role: str) -> str:
    return f"urn:li:corpuser:{_email(role)}"


def _new_dataset(label: str) -> str:
    return (
        f"urn:li:dataset:(urn:li:dataPlatform:hive,"
        f"constraints.{label}_{uuid.uuid4().hex[:6]}_{_UNIQUE},PROD)"
    )


def _props(*assignments: tuple[str, str]) -> StructuredPropertiesClass:
    return StructuredPropertiesClass(
        properties=[
            StructuredPropertyValueAssignmentClass(propertyUrn=urn, values=[value])
            for urn, value in assignments
        ]
    )


def _stamp() -> AuditStampClass:
    return AuditStampClass(
        time=int(time.time() * 1000), actor="urn:li:corpuser:datahub"
    )


@pytest.fixture(scope="module")
def setup(graph_client: DataHubGraph) -> Iterator[Dict[str, Any]]:
    """Everything is set up through GMS with the admin token, so no frontend is needed."""
    created: List[str] = [DOCUMENT, GROUP, CORP_USER, DATASET_EXISTING, DATASET_FORM]
    created += [_user_urn(role) for role in ROLES]
    policies: List[str] = []
    tokens: List[str] = []
    emitters: Dict[str, DatahubRestEmitter] = {}
    try:
        _seed_metadata(graph_client)
        policies.extend(_create_policies(graph_client))
        form_urn, prompt_id = _create_assigned_form(graph_client)
        created.append(form_urn)

        bearer: Dict[str, str] = {}
        for role in ROLES:
            token, token_id = _mint_token(graph_client, _user_urn(role))
            tokens.append(token_id)
            bearer[role] = token
            emitters[role] = DatahubRestEmitter(
                gms_server=get_gms_url(), token=token, openapi_ingestion=False
            )
        yield {
            "bearer": bearer,
            "emitters": emitters,
            "form_urn": form_urn,
            "prompt_id": prompt_id,
            "created": created,
            "graph": graph_client,
        }
    finally:
        for emitter in emitters.values():
            emitter.close()
        for token_id in tokens:
            _revoke_token(graph_client, token_id)
        for urn in policies + created + [PROP_A, PROP_B, TAG_A]:
            try:
                graph_client.hard_delete_entity(urn=urn)
            except Exception as e:
                logger.warning("Failed to delete %s during cleanup: %s", urn, e)
        wait_for_writes_to_sync()


def _seed_metadata(graph: DataHubGraph) -> None:
    for urn in (PROP_A, PROP_B):
        graph.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=StructuredPropertyDefinitionClass(
                    qualifiedName=Urn.from_string(urn).entity_ids[0],
                    valueType=Urn.make_data_type_urn("string"),
                    entityTypes=[
                        Urn.make_entity_type_urn(t)
                        for t in ("dataset", "document", "corpGroup", "corpuser")
                    ],
                    cardinality="SINGLE",
                ),
            )
        )
    now = _stamp()
    mcps = [
        MetadataChangeProposalWrapper(
            entityUrn=DOCUMENT,
            aspect=DocumentInfoClass(
                title=f"Constraints document {_UNIQUE}",
                contents=DocumentContentsClass(text="Smoke test document"),
                status=DocumentStatusClass(state="PUBLISHED"),
                created=now,
                lastModified=now,
            ),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=DOCUMENT, aspect=_props((PROP_A, "seed"), (PROP_B, "seed"))
        ),
        MetadataChangeProposalWrapper(
            entityUrn=GROUP,
            aspect=CorpGroupInfoClass(
                displayName=f"Constraints group {_UNIQUE}",
                admins=[],
                members=[],
                groups=[],
            ),
        ),
    ]
    for user in [CORP_USER] + [_user_urn(role) for role in ROLES]:
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=user,
                aspect=CorpUserInfoClass(active=True, displayName=user.split(":")[-1]),
            )
        )
    for urn in (DATASET_EXISTING, DATASET_FORM):
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=DatasetPropertiesClass(name=urn.split(",")[1])
            )
        )
    for mcp in mcps:
        graph.emit_mcp(mcp)
    wait_for_writes_to_sync()


def _create_policies(graph: DataHubGraph) -> List[str]:
    """Each user gets exactly one policy. Written as aspects because privilegeConstraints are
    API-only."""
    every_dataset = DataHubResourceFilterClass(type="dataset", allResources=True)
    specs = [
        ("docs", DOCS_WRITER, "PLATFORM", ["MANAGE_DOCUMENTS"], None),
        ("groups", GROUP_MANAGER, "PLATFORM", ["MANAGE_USERS_AND_GROUPS"], None),
        ("create", CREATOR, "METADATA", ["CREATE_ENTITY"], every_dataset),
        ("delete", DELETER, "METADATA", ["DELETE_ENTITY"], every_dataset),
        (
            "prop-a",
            CONSTRAINED,
            "METADATA",
            # The API write check needs Edit Entity for any update, so this is the shape that
            # reaches the validator: Edit Entity granted only through a constrained policy.
            ["EDIT_ENTITY"],
            DataHubResourceFilterClass(
                type="dataset",
                allResources=True,
                privilegeConstraints=PolicyMatchFilterClass(
                    criteria=[
                        PolicyMatchCriterionClass(
                            field="URN", values=[PROP_A], condition="EQUALS"
                        )
                    ]
                ),
            ),
        ),
    ]
    policies = []
    for name, role, policy_type, privileges, resources in specs:
        urn = f"urn:li:dataHubPolicy:constraints-{name}-{_UNIQUE}"
        graph.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=DataHubPolicyInfoClass(
                    displayName=f"Test constraints {name} {_UNIQUE}",
                    description=f"Smoke test policy granting only {privileges}",
                    type=policy_type,
                    state="ACTIVE",
                    privileges=privileges,
                    actors=DataHubActorFilterClass(users=[_user_urn(role)]),
                    resources=resources,
                    editable=True,
                ),
            )
        )
        policies.append(urn)
    wait_for_writes_to_sync()
    return policies


def _create_assigned_form(graph: DataHubGraph) -> tuple[str, str]:
    prompt_id = f"constraints-prompt-{_UNIQUE}"
    res = graph.execute_graphql(
        """mutation createForm($input: CreateFormInput!) { createForm(input: $input) { urn } }""",
        {
            "input": {
                "id": f"constraints-form-{_UNIQUE}",
                "name": f"Constraints form {_UNIQUE}",
                "type": "COMPLETION",
                "prompts": [
                    {
                        "id": prompt_id,
                        "title": "Property A",
                        "type": "STRUCTURED_PROPERTY",
                        "structuredPropertyParams": {"urn": PROP_A},
                        "required": False,
                    }
                ],
                "actors": {"owners": False, "users": [_user_urn(ASSIGNEE)]},
            }
        },
    )
    form_urn = res["createForm"]["urn"]
    graph.execute_graphql(
        """mutation batchAssignForm($input: BatchAssignFormInput!) {
            batchAssignForm(input: $input) }""",
        {"input": {"formUrn": form_urn, "entityUrns": [DATASET_FORM]}},
    )
    wait_for_writes_to_sync()
    return form_urn, prompt_id


def _mint_token(graph: DataHubGraph, actor_urn: str) -> tuple[str, str]:
    res = graph.execute_graphql(
        """mutation createAccessToken($input: CreateAccessTokenInput!) {
            createAccessToken(input: $input) { accessToken metadata { id } } }""",
        {
            "input": {
                "type": "PERSONAL",
                "actorUrn": actor_urn,
                "duration": "ONE_HOUR",
                "name": f"constraints-{_UNIQUE}",
            }
        },
    )
    token = res["createAccessToken"]
    return token["accessToken"], token["metadata"]["id"]


def _revoke_token(graph: DataHubGraph, token_id: str) -> None:
    try:
        graph.execute_graphql(
            "mutation r($id: String!) { revokeAccessToken(tokenId: $id) }",
            {"id": token_id},
        )
    except Exception:
        logger.warning("Failed to revoke token %s", token_id)


# --- Transports ---


class WriteDenied(Exception):
    pass


def _restli(setup: Dict[str, Any], role: str, mcps: List[Any]) -> None:
    try:
        setup["emitters"][role].emit_mcps(mcps)
    except OperationalError as e:
        raise WriteDenied(f"restli {e.info.get('status')}: {e.message}") from e


def _openapi(
    setup: Dict[str, Any], role: str, method: str, path: str, body: Any = None
) -> None:
    resp = requests.request(
        method,
        f"{get_gms_url()}/openapi/v3/{path}",
        params={"async": "false"},
        headers={"Authorization": f"Bearer {setup['bearer'][role]}"},
        json=body,
    )
    if resp.status_code >= 400:
        raise WriteDenied(f"openapi {resp.status_code}: {resp.text[:400]}")


def _graphql(setup: Dict[str, Any], role: str, query: str, variables: Dict) -> None:
    resp = requests.post(
        f"{get_gms_url()}/api/graphql",
        headers={"Authorization": f"Bearer {setup['bearer'][role]}"},
        json={"query": query, "variables": variables},
    )
    payload = resp.json() if resp.status_code == 200 else {}
    if resp.status_code != 200 or payload.get("errors"):
        raise WriteDenied(f"graphql {resp.status_code}: {resp.text[:400]}")


def _add_tag_patch(urn: str) -> List[Any]:
    return list(HasTagsPatch(urn).add_tag(TagAssociationClass(tag=TAG_A)).build())


def _sp_patch(prop: str, value: str) -> Dict[str, Any]:
    return {
        "patch": [
            {
                "op": "add",
                "path": f"/properties/{prop}",
                "value": {"propertyUrn": prop, "values": [{"string": value}]},
            }
        ],
        "arrayPrimaryKeys": {"properties": ["propertyUrn"]},
    }


def _eventually_allowed(write: Callable[[], None], description: str) -> None:
    """Retries until the write succeeds, so a cold policy cache is not mistaken for a denial."""
    deadline = time.time() + _POLICY_WAIT_SECONDS
    last = ""
    while time.time() < deadline:
        try:
            write()
            return
        except WriteDenied as e:
            last = str(e)
        time.sleep(2.0)
    raise AssertionError(f"{description} was denied: {last}")


def _stored_props(graph: DataHubGraph, urn: str) -> Dict[str, List[Any]]:
    aspect = graph.get_aspect(urn, StructuredPropertiesClass)
    if aspect is None:
        return {}
    return {a.propertyUrn: list(a.values) for a in aspect.properties}


# --- Manage Documents ---


def test_manage_documents_restli_upsert(setup):
    _eventually_allowed(
        lambda: _restli(
            setup,
            DOCS_WRITER,
            [
                MetadataChangeProposalWrapper(
                    entityUrn=DOCUMENT,
                    aspect=_props((PROP_A, "seed"), (PROP_B, "restli-upsert")),
                )
            ],
        ),
        "Rest.li UPSERT of structured properties on a document with Manage Documents",
    )
    assert _stored_props(setup["graph"], DOCUMENT)[PROP_B] == ["restli-upsert"]


def test_manage_documents_restli_create_document(setup):
    """A new document and its structured properties in one Rest.li batch, as a publisher sends."""
    created_urns: List[str] = []

    def _create() -> None:
        urn = f"urn:li:document:constraints-new-{uuid.uuid4().hex[:6]}-{_UNIQUE}"
        created_urns.append(urn)
        setup["created"].append(urn)
        now = _stamp()
        _restli(
            setup,
            DOCS_WRITER,
            [
                MetadataChangeProposalWrapper(
                    entityUrn=urn,
                    aspect=DocumentInfoClass(
                        title="Published document",
                        contents=DocumentContentsClass(
                            text="Published by a service account"
                        ),
                        status=DocumentStatusClass(state="PUBLISHED"),
                        created=now,
                        lastModified=now,
                    ),
                ),
                MetadataChangeProposalWrapper(
                    entityUrn=urn, aspect=_props((PROP_A, "published"))
                ),
            ],
        )

    _eventually_allowed(
        _create, "Rest.li create of a new document with structured properties"
    )
    assert _stored_props(setup["graph"], created_urns[-1])[PROP_A] == ["published"]


def test_manage_documents_openapi_patch(setup):
    _eventually_allowed(
        lambda: _openapi(
            setup,
            DOCS_WRITER,
            "PATCH",
            f"entity/document/{quote(DOCUMENT, safe='')}/structuredProperties",
            _sp_patch(PROP_B, "openapi-patch"),
        ),
        "OpenAPI v3 PATCH of structured properties on a document with Manage Documents",
    )
    assert _stored_props(setup["graph"], DOCUMENT)[PROP_B] == ["openapi-patch"]


def test_manage_documents_restli_tag_patch(setup):
    _eventually_allowed(
        lambda: _restli(setup, DOCS_WRITER, _add_tag_patch(DOCUMENT)),
        "Rest.li PATCH adding a tag to a document with Manage Documents",
    )
    tags = setup["graph"].get_aspect(DOCUMENT, GlobalTagsClass)
    assert tags is not None and TAG_A in [t.tag for t in tags.tags]


# --- Manage Users & Groups ---


def test_manage_groups_openapi_upsert(setup):
    _eventually_allowed(
        lambda: _openapi(
            setup,
            GROUP_MANAGER,
            "POST",
            "entity/corpgroup",
            [
                {
                    "urn": GROUP,
                    "structuredProperties": {
                        "value": {
                            "properties": [
                                {"propertyUrn": PROP_A, "values": [{"string": "group"}]}
                            ]
                        }
                    },
                }
            ],
        ),
        "OpenAPI v3 upsert of structured properties on a group with Manage Users & Groups",
    )
    assert _stored_props(setup["graph"], GROUP)[PROP_A] == ["group"]


def test_manage_users_restli_tag_patch(setup):
    _eventually_allowed(
        lambda: _restli(setup, GROUP_MANAGER, _add_tag_patch(CORP_USER)),
        "Rest.li PATCH adding a tag to a user with Manage Users & Groups",
    )
    tags = setup["graph"].get_aspect(CORP_USER, GlobalTagsClass)
    assert tags is not None and TAG_A in [t.tag for t in tags.tags]


# --- Create Entity ---


@pytest.mark.parametrize("transport", ["openapi", "restli"])
def test_create_entity_new_dataset_with_properties_and_tags(setup, transport):
    created_urns: List[str] = []

    def _create() -> None:
        urn = _new_dataset(f"create_{transport}")
        created_urns.append(urn)
        setup["created"].append(urn)
        if transport == "openapi":
            _openapi(
                setup,
                CREATOR,
                "POST",
                "entity/dataset",
                [
                    {
                        "urn": urn,
                        "datasetProperties": {"value": {"name": "created"}},
                        "structuredProperties": {
                            "value": {
                                "properties": [
                                    {
                                        "propertyUrn": PROP_A,
                                        "values": [{"string": "new"}],
                                    }
                                ]
                            }
                        },
                        "globalTags": {"value": {"tags": [{"tag": TAG_A}]}},
                    }
                ],
            )
        else:
            _restli(
                setup,
                CREATOR,
                [
                    MetadataChangeProposalWrapper(
                        entityUrn=urn, aspect=DatasetPropertiesClass(name="created")
                    ),
                    MetadataChangeProposalWrapper(
                        entityUrn=urn, aspect=_props((PROP_A, "new"))
                    ),
                    MetadataChangeProposalWrapper(
                        entityUrn=urn,
                        aspect=GlobalTagsClass(tags=[TagAssociationClass(tag=TAG_A)]),
                    ),
                ],
            )

    _eventually_allowed(
        _create, f"{transport} create of a new dataset with Create Entity only"
    )
    assert _stored_props(setup["graph"], created_urns[-1])[PROP_A] == ["new"]


# --- Delete Entity ---


def test_delete_entity_openapi_delete_aspect(setup):
    graph = setup["graph"]

    def _delete() -> None:
        urn = _new_dataset("delete")
        setup["created"].append(urn)
        graph.emit_mcp(
            MetadataChangeProposalWrapper(entityUrn=urn, aspect=_props((PROP_A, "x")))
        )
        _openapi(
            setup,
            DELETER,
            "DELETE",
            f"entity/dataset/{quote(urn, safe='')}/structuredProperties",
        )
        assert _stored_props(graph, urn) == {}

    _eventually_allowed(
        _delete,
        "OpenAPI v3 DELETE of the structuredProperties aspect with Delete Entity",
    )


# --- Forms ---


def test_form_assignee_graphql_submit_prompt(setup):
    _eventually_allowed(
        lambda: _graphql(
            setup,
            ASSIGNEE,
            """mutation submitFormPrompt($urn: String!, $input: SubmitFormPromptInput!) {
                submitFormPrompt(urn: $urn, input: $input) }""",
            {
                "urn": DATASET_FORM,
                "input": {
                    "promptId": setup["prompt_id"],
                    "formUrn": setup["form_urn"],
                    "type": "STRUCTURED_PROPERTY",
                    "structuredPropertyParams": {
                        "structuredPropertyUrn": PROP_A,
                        "values": [{"stringValue": "answered"}],
                    },
                },
            },
        ),
        "GraphQL submitFormPrompt by a form assignee without Edit privileges",
    )
    assert _stored_props(setup["graph"], DATASET_FORM)[PROP_A] == ["answered"]


# --- privilegeConstraints still enforced ---


@pytest.mark.parametrize("transport", ["restli", "openapi_patch"])
def test_constrained_user_limited_to_allowed_property(setup, transport):
    def _write(prop: str, value: str) -> None:
        if transport == "restli":
            # An UPSERT replaces the aspect, so writing prop_b alone also removes prop_a;
            # either change is outside the constraint for prop_b.
            _restli(
                setup,
                CONSTRAINED,
                [
                    MetadataChangeProposalWrapper(
                        entityUrn=DATASET_EXISTING, aspect=_props((prop, value))
                    )
                ],
            )
        else:
            _openapi(
                setup,
                CONSTRAINED,
                "PATCH",
                f"entity/dataset/{quote(DATASET_EXISTING, safe='')}/structuredProperties",
                _sp_patch(prop, value),
            )

    _eventually_allowed(
        lambda: _write(PROP_A, f"allowed-{transport}"),
        f"{transport} write of the property the constraint allows",
    )
    with pytest.raises(
        WriteDenied, match="Unauthorized to modify one or more structured property"
    ):
        _write(PROP_B, f"denied-{transport}")
    assert PROP_B not in _stored_props(setup["graph"], DATASET_EXISTING)
