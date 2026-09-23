"""
E2E authorization for domain-scoped CREATE_ENTITY / entity create writes.

Marks: global_policy_mutator — disables shared All-Users default policies for the
module so concurrent non-mutator smoke tests are not affected (smoke.sh phase 2).

Scenario (single class / module fixture):
  - Two domains (allowed vs other)
  - Restricted user with CREATE_ENTITY (+ optional EDIT) scoped only to the allowed domain
  - Matrix: OpenAPI sync, OpenAPI async (+ write-trace confirmation on allow), Rest.li sync

OpenAPI async allows return 202 with a trace id; we poll /openapi/v1/trace/write/{id}
(admin session — READ required) until primary storage is ACTIVE_STATE. Auth denials still
fail at the edge with HTTP 403 before async accept, so deny cases assert 403 for both
sync and async (no write trace on deny).
"""

from __future__ import annotations

import logging
import time
import uuid
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Optional

import pytest
from requests.exceptions import HTTPError

from datahub.configuration.common import OperationalError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetPropertiesClass,
    DomainPropertiesClass,
    DomainsClass,
)
from tests.consistency_utils import wait_for_writes_to_sync
from tests.privileges.utils import (
    clear_polices,
    create_domain_scoped_metadata_policy,
    create_user,
    remove_policy,
    remove_user,
    set_base_platform_privileges_policy_status,
    set_view_dataset_sensitive_info_policy_status,
    set_view_entity_profile_privileges_policy_status,
)
from tests.utilities.domains import Domain
from tests.utils import get_frontend_session, get_frontend_url, get_gms_url, login_as

logger = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.no_cypress_suite1,
    pytest.mark.global_policy_mutator,
    pytest.mark.domain(Domain.PLATFORM),
]

_UNIQUE = uuid.uuid4().hex[:8]
POLICY_PREFIX = "Test DomainScoped CREATE_ENTITY"

TEST_USER_EMAIL = f"domain.create.auth.{_UNIQUE}@smoke.datahub.test"
TEST_USER_URN = f"urn:li:corpuser:{TEST_USER_EMAIL}"
TEST_USER_PASSWORD = "user"

DOMAIN_A_URN = f"urn:li:domain:domain-create-a-{_UNIQUE}"
DOMAIN_B_URN = f"urn:li:domain:domain-create-b-{_UNIQUE}"

_DEFAULT_POLICY_CACHE_WAIT_SECONDS = 60.0
_DEFAULT_TRACE_WAIT_SECONDS = 60.0

API_OPENAPI = "openapi"
API_RESTLI = "restli"
MODE_SYNC = "sync"
MODE_ASYNC = "async"

# OpenAPI covers sync + async (trace on allow). Rest.li stays sync-only.
TRANSPORTS = (
    (API_OPENAPI, MODE_SYNC),
    (API_OPENAPI, MODE_ASYNC),
    (API_RESTLI, MODE_SYNC),
)


def _transport_id(api: str, mode: str) -> str:
    return f"{api}-{mode}"


def _dataset_urn(suffix: str) -> str:
    return (
        f"urn:li:dataset:(urn:li:dataPlatform:kafka,"
        f"domain-create-{suffix}-{_UNIQUE},PROD)"
    )


@pytest.fixture(scope="module", autouse=True)
def domain_create_auth_setup(graph_client):
    yield from _setup_impl(graph_client)


def _setup_impl(graph_client):
    for urn, name in (
        (DOMAIN_A_URN, f"Domain Create A {_UNIQUE}"),
        (DOMAIN_B_URN, f"Domain Create B {_UNIQUE}"),
    ):
        graph_client.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=DomainPropertiesClass(
                    name=name,
                    description="Domain for domain-scoped CREATE_ENTITY smoke test",
                ),
            )
        )
    wait_for_writes_to_sync()

    admin_session = get_frontend_session()
    clear_polices(admin_session, name_prefixes=[POLICY_PREFIX])
    set_base_platform_privileges_policy_status("INACTIVE", admin_session)
    set_view_dataset_sensitive_info_policy_status("INACTIVE", admin_session)
    set_view_entity_profile_privileges_policy_status("INACTIVE", admin_session)
    wait_for_writes_to_sync()

    admin_session = create_user(admin_session, TEST_USER_EMAIL, TEST_USER_PASSWORD)
    created_urns: list[str] = [DOMAIN_A_URN, DOMAIN_B_URN]
    token_id: Optional[str] = None
    rest_emitter: Optional[DatahubRestEmitter] = None
    try:
        access_token, token_id = _mint_user_access_token(admin_session)
        rest_emitter = DatahubRestEmitter(gms_server=get_gms_url(), token=access_token)
        yield {
            "admin_session": admin_session,
            "created_urns": created_urns,
            "rest_emitter": rest_emitter,
        }
    finally:
        # Keep teardown resilient: a revoke failure must not skip user/policy cleanup.
        try:
            if token_id:
                _revoke_access_token(get_frontend_session(), token_id)
        except Exception:
            logger.warning("Failed to revoke access token %s during cleanup", token_id)
        if rest_emitter is not None:
            rest_emitter.close()
        admin_session = get_frontend_session()
        remove_user(admin_session, TEST_USER_URN)
        clear_polices(admin_session, name_prefixes=[POLICY_PREFIX])
        set_base_platform_privileges_policy_status("ACTIVE", admin_session)
        set_view_dataset_sensitive_info_policy_status("ACTIVE", admin_session)
        set_view_entity_profile_privileges_policy_status("ACTIVE", admin_session)
        wait_for_writes_to_sync()
        for urn in created_urns:
            try:
                graph_client.hard_delete_entity(urn=urn)
            except Exception:
                logger.warning("Failed to delete %s during cleanup", urn)


def _mint_user_access_token(admin_session) -> tuple[str, str]:
    token_payload = {
        "query": """mutation createAccessToken($input: CreateAccessTokenInput!) {
            createAccessToken(input: $input) {
              accessToken
              metadata { id actorUrn }
            }
          }""",
        "variables": {
            "input": {
                "type": "PERSONAL",
                "actorUrn": TEST_USER_URN,
                "duration": "ONE_HOUR",
                "name": f"domain-create-token-{_UNIQUE}",
            }
        },
    }
    token_resp = admin_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=token_payload
    )
    token_resp.raise_for_status()
    token_data = token_resp.json()
    assert token_data.get("data") and token_data["data"].get("createAccessToken"), (
        f"createAccessToken failed: {token_data}"
    )
    result = token_data["data"]["createAccessToken"]
    return result["accessToken"], result["metadata"]["id"]


def _revoke_access_token(admin_session, token_id: str) -> None:
    resp = admin_session.post(
        f"{get_frontend_url()}/api/v2/graphql",
        json={
            "query": """mutation revokeAccessToken($tokenId: String!) {
                revokeAccessToken(tokenId: $tokenId)
            }""",
            "variables": {"tokenId": token_id},
        },
    )
    if resp.status_code != 200:
        raise AssertionError(
            f"revokeAccessToken HTTP {resp.status_code}: {resp.text[:400]}"
        )
    payload = resp.json()
    if payload.get("errors"):
        raise AssertionError(f"revokeAccessToken GraphQL errors: {payload['errors']}")
    if not (payload.get("data") or {}).get("revokeAccessToken"):
        raise AssertionError(
            f"revokeAccessToken returned unexpected payload: {payload}"
        )


def _user_session():
    return login_as(TEST_USER_EMAIL, TEST_USER_PASSWORD)


def _create_policy(
    admin_session,
    *,
    name: str,
    privileges: list[str],
    domain_urn: str,
) -> str:
    return create_domain_scoped_metadata_policy(
        admin_session,
        name=name,
        description=f"{name} ({_UNIQUE})",
        privileges=privileges,
        user_urn=TEST_USER_URN,
        domain_urn=domain_urn,
        resource_type="dataset",
    )


@contextmanager
def _policy(
    admin_session,
    *,
    name: str,
    privileges: list[str],
    domain_urn: str,
) -> Iterator[str]:
    policy_urn = _create_policy(
        admin_session, name=name, privileges=privileges, domain_urn=domain_urn
    )
    try:
        yield policy_urn
    finally:
        remove_policy(policy_urn, admin_session)


def _openapi_create_dataset(
    session,
    urn: str,
    *,
    domain_urn: Optional[str] = None,
    async_write: bool = False,
) -> Any:
    """OpenAPI v3 entity create (UPSERT). Missing entities use the CREATE privilege path."""
    name = urn.split(",")[1] if "," in urn else urn
    body: dict[str, Any] = {
        "urn": urn,
        "datasetProperties": {
            "value": {
                "name": name,
                "description": "domain-scoped create smoke",
            }
        },
    }
    if domain_urn is not None:
        body["domains"] = {"value": {"domains": [domain_urn]}}

    params: dict[str, str] = {
        "async": "true" if async_write else "false",
    }
    if async_write:
        # Needed so the response body carries telemetryTraceId alongside traceparent.
        params["systemMetadata"] = "true"

    return session.post(
        f"{get_frontend_url()}/openapi/v3/entity/dataset",
        params=params,
        json=[body],
    )


def _restli_create_dataset(
    emitter: DatahubRestEmitter,
    urn: str,
    *,
    domain_urn: Optional[str] = None,
    change_type: str = ChangeTypeClass.CREATE_ENTITY,
) -> None:
    """Rest.li ingestProposal with CREATE_ENTITY (or UPSERT for overwrite cases)."""
    name = urn.split(",")[1] if "," in urn else urn
    mcps = [
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            changeType=change_type,
            aspect=DatasetPropertiesClass(
                name=name, description="domain-scoped create smoke"
            ),
        )
    ]
    if domain_urn is not None:
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                changeType=change_type,
                aspect=DomainsClass(domains=[domain_urn]),
            )
        )
    emitter.emit_mcps(mcps)


# Auth denials: Rest.li UnauthorizedException and OpenAPI AUTHORIZATION ValidationException
# both map to HTTP 403. OpenAPI deny assertions require 403 (not 401) so auth-token failures
# are not mistaken for domain-scoped authorization. Rest.li may still surface 401 in edge cases.
_AUTH_DENIED_STATUS_CODES = frozenset({401, 403})
_OPENAPI_AUTH_DENIED_STATUS_CODES = frozenset({403})


def _auth_denied_status_codes(api: str) -> frozenset[int]:
    if api == API_OPENAPI:
        return _OPENAPI_AUTH_DENIED_STATUS_CODES
    return _AUTH_DENIED_STATUS_CODES


def _extract_trace_id(resp) -> str:
    """Prefer W3C traceparent; fall back to systemMetadata.telemetryTraceId."""
    traceparent = resp.headers.get("traceparent")
    if traceparent:
        parts = traceparent.split("-")
        if len(parts) >= 2 and parts[1]:
            return parts[1]

    try:
        payload = resp.json()
    except Exception as exc:  # noqa: BLE001
        raise AssertionError(
            f"async accept missing traceparent and unreadable body: {resp.text[:400]}"
        ) from exc

    entities = payload if isinstance(payload, list) else [payload]
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        for aspect in entity.values():
            if not isinstance(aspect, dict):
                continue
            props = (aspect.get("systemMetadata") or {}).get("properties") or {}
            trace_id = props.get("telemetryTraceId")
            if trace_id:
                return trace_id

    raise AssertionError(
        f"async accept missing trace id (header/body); "
        f"headers={dict(resp.headers)} body={resp.text[:400]}"
    )


def _wait_for_openapi_write_trace(
    admin_session,
    *,
    trace_id: str,
    urn: str,
    aspect_names: list[str],
    timeout_seconds: float = _DEFAULT_TRACE_WAIT_SECONDS,
) -> str:
    """Poll write-trace until each aspect was applied (ACTIVE or HISTORIC).

    HISTORIC_STATE still means this trace's write succeeded; later overwrites
    (e.g. policy-cache retries on the same URN) demote ACTIVE → HISTORIC.
    """
    applied_states = frozenset({"ACTIVE_STATE", "HISTORIC_STATE"})
    # Transient while Kafka consumers race; keep polling rather than fail-fast.
    transient_error_messages = (
        "Consumer has processed past the offset.",
        "Pending primary storage write.",
    )

    deadline = time.time() + timeout_seconds
    last_detail = ""
    while time.time() < deadline:
        resp = admin_session.post(
            f"{get_frontend_url()}/openapi/v1/trace/write/{trace_id}",
            params={
                "onlyIncludeErrors": "false",
                "detailed": "true",
                "skipCache": "true",
            },
            json={urn: aspect_names},
        )
        last_detail = f"trace status={resp.status_code} body={resp.text[:500]}"
        if resp.status_code == 200:
            aspects = (resp.json() or {}).get(urn) or {}
            hard_errors = []
            pending = []
            for name in aspect_names:
                aspect = aspects.get(name) or {}
                primary = aspect.get("primaryStorage") or {}
                write_status = primary.get("writeStatus")
                write_message = primary.get("writeMessage") or ""
                if write_status in applied_states and aspect.get("success") is True:
                    continue
                if write_status == "ERROR" and not any(
                    msg in write_message for msg in transient_error_messages
                ):
                    hard_errors.append((name, aspect))
                else:
                    pending.append((name, write_status, aspect.get("success")))
            if hard_errors:
                raise AssertionError(
                    f"async write trace reported ERROR for {hard_errors}; {last_detail}"
                )
            if not pending:
                return last_detail
        time.sleep(1.0)
    raise AssertionError(
        f"Timed out waiting for async write trace {trace_id} urn={urn} "
        f"aspects={aspect_names}; last={last_detail}"
    )


def _attempt_create(
    api: str,
    mode: str,
    setup: dict,
    urn: str,
    *,
    domain_urn: Optional[str],
    change_type: str = ChangeTypeClass.CREATE_ENTITY,
) -> tuple[bool, Optional[int], str]:
    """
    Returns (allowed, status_code, detail).
    allowed=True means write succeeded (and for OpenAPI async, write-trace confirmed).
    """
    if api == API_OPENAPI:
        async_write = mode == MODE_ASYNC
        # OpenAPI v3 batch create uses UPSERT; existence-aware auth maps missing → CREATE.
        resp = _openapi_create_dataset(
            _user_session(),
            urn,
            domain_urn=domain_urn,
            async_write=async_write,
        )
        detail = f"openapi mode={mode} status={resp.status_code} body={resp.text[:400]}"
        if resp.status_code in _AUTH_DENIED_STATUS_CODES:
            return False, resp.status_code, detail
        if async_write:
            # Auth edge reject is 403 above. Accept is 202; confirm via write-trace.
            if resp.status_code not in (202, 200, 201):
                return False, resp.status_code, detail
            aspects = ["datasetProperties"]
            if domain_urn is not None:
                aspects.append("domains")
            try:
                trace_id = _extract_trace_id(resp)
                trace_detail = _wait_for_openapi_write_trace(
                    setup["admin_session"],
                    trace_id=trace_id,
                    urn=urn,
                    aspect_names=aspects,
                )
            except AssertionError as exc:
                return False, resp.status_code, f"{detail}; {exc}"
            return (
                True,
                resp.status_code,
                f"openapi mode=async status={resp.status_code} "
                f"trace_id={trace_id} {trace_detail}",
            )
        if resp.status_code in (200, 201):
            return True, resp.status_code, detail
        return False, resp.status_code, detail

    if api == API_RESTLI:
        if mode != MODE_SYNC:
            raise AssertionError(f"Rest.li only supports sync mode, got mode={mode}")
        try:
            _restli_create_dataset(
                setup["rest_emitter"],
                urn,
                domain_urn=domain_urn,
                change_type=change_type,
            )
            return True, 200, "restli ok"
        except HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            body = exc.response.text[:400] if exc.response is not None else str(exc)
            return False, status, f"restli status={status} body={body}"
        except OperationalError as exc:
            # Rest emitter wraps Rest.li 403 as OperationalError(info={"status": 403, ...}).
            raw_status = exc.info.get("status")
            restli_status: Optional[int]
            if isinstance(raw_status, int):
                restli_status = raw_status
            elif isinstance(raw_status, str) and raw_status.isdigit():
                restli_status = int(raw_status)
            else:
                restli_status = None
            return (
                False,
                restli_status,
                f"restli status={restli_status} body={exc.message}",
            )
        except Exception as exc:  # noqa: BLE001
            return False, None, f"restli error={exc}"

    raise AssertionError(f"Unknown api={api}")


def _wait_until(
    attempt: Callable[[], tuple[bool, Optional[int], str]],
    *,
    want_allowed: bool,
    description: str,
    timeout_seconds: float = _DEFAULT_POLICY_CACHE_WAIT_SECONDS,
    require_auth_denied: bool = False,
    auth_denied_status_codes: frozenset[int] = _AUTH_DENIED_STATUS_CODES,
) -> tuple[Optional[int], str]:
    """Poll until allowed matches want_allowed.

    When require_auth_denied=True (deny cases), only auth_denied_status_codes count as denied —
    a 400 Validation Error is not treated as success.
    """
    deadline = time.time() + timeout_seconds
    last_status: Optional[int] = None
    last_detail = ""
    while time.time() < deadline:
        allowed, last_status, last_detail = attempt()
        if want_allowed and allowed:
            return last_status, last_detail
        if not want_allowed and not allowed:
            if not require_auth_denied or last_status in auth_denied_status_codes:
                return last_status, last_detail
        logger.info(
            "Waiting for %s (got %s; %.0fs remaining)",
            description,
            last_detail,
            max(0.0, deadline - time.time()),
        )
        time.sleep(1.0)
    raise AssertionError(f"Timed out waiting for {description}; last={last_detail}")


def _assert_auth_denied(
    status: Optional[int],
    detail: str,
    *,
    api: str = API_OPENAPI,
) -> None:
    allowed = _auth_denied_status_codes(api)
    assert status in allowed, (
        f"expected HTTP {sorted(allowed)} authorization denial, got status={status}; {detail}"
    )


def _track_urn(setup: dict, urn: str) -> str:
    setup["created_urns"].append(urn)
    return urn


def _wait_until_policy_allows_domain_a(setup: dict, api: str, mode: str) -> None:
    """Probe create in Domain A so deny cases are not false-positives from cold policy cache."""
    tid = _transport_id(api, mode)

    def _attempt() -> tuple[bool, Optional[int], str]:
        # Fresh URN per attempt so async accepts are not overwritten by retries.
        probe = _track_urn(setup, _dataset_urn(f"probe-{tid}-{uuid.uuid4().hex[:6]}"))
        return _attempt_create(api, mode, setup, probe, domain_urn=DOMAIN_A_URN)

    _wait_until(
        _attempt,
        want_allowed=True,
        description=f"{tid} policy probe create in Domain A",
        timeout_seconds=(
            _DEFAULT_POLICY_CACHE_WAIT_SECONDS + _DEFAULT_TRACE_WAIT_SECONDS
            if mode == MODE_ASYNC
            else _DEFAULT_POLICY_CACHE_WAIT_SECONDS
        ),
    )
    wait_for_writes_to_sync()


@pytest.mark.parametrize(
    "api,mode", TRANSPORTS, ids=[_transport_id(a, m) for a, m in TRANSPORTS]
)
class TestDomainScopedCreateEntityAuth:
    """Domain-separated writer matrix on OpenAPI sync/async and Rest.li sync."""

    def test_create_in_allowed_domain_succeeds(
        self, domain_create_auth_setup, graph_client, api, mode
    ):
        setup = domain_create_auth_setup
        admin_session = setup["admin_session"]
        tid = _transport_id(api, mode)
        created: dict[str, Optional[str]] = {"urn": None}

        with _policy(
            admin_session,
            name=f"{POLICY_PREFIX} allow A {tid}",
            privileges=["CREATE_ENTITY", "EDIT_ENTITY"],
            domain_urn=DOMAIN_A_URN,
        ):

            def _attempt() -> tuple[bool, Optional[int], str]:
                # Fresh URN per attempt avoids async overwrite → HISTORIC races.
                urn = _track_urn(
                    setup, _dataset_urn(f"allow-a-{tid}-{uuid.uuid4().hex[:6]}")
                )
                allowed, status, detail = _attempt_create(
                    api, mode, setup, urn, domain_urn=DOMAIN_A_URN
                )
                if allowed:
                    created["urn"] = urn
                return allowed, status, detail

            _, detail = _wait_until(
                _attempt,
                want_allowed=True,
                description=f"{tid} create in allowed domain",
                timeout_seconds=(
                    _DEFAULT_POLICY_CACHE_WAIT_SECONDS + _DEFAULT_TRACE_WAIT_SECONDS
                    if mode == MODE_ASYNC
                    else _DEFAULT_POLICY_CACHE_WAIT_SECONDS
                ),
            )
            logger.info("allow Domain A (%s): %s", tid, detail)
            wait_for_writes_to_sync()
            assert created["urn"] is not None
            assert graph_client.exists(created["urn"])

    def test_create_in_other_domain_denied(self, domain_create_auth_setup, api, mode):
        setup = domain_create_auth_setup
        admin_session = setup["admin_session"]
        tid = _transport_id(api, mode)
        urn = _track_urn(setup, _dataset_urn(f"deny-b-{tid}"))

        with _policy(
            admin_session,
            name=f"{POLICY_PREFIX} deny B {tid}",
            privileges=["CREATE_ENTITY", "EDIT_ENTITY"],
            domain_urn=DOMAIN_A_URN,
        ):
            _wait_until_policy_allows_domain_a(setup, api, mode)

            def _attempt() -> tuple[bool, Optional[int], str]:
                return _attempt_create(api, mode, setup, urn, domain_urn=DOMAIN_B_URN)

            status, detail = _wait_until(
                _attempt,
                want_allowed=False,
                require_auth_denied=True,
                auth_denied_status_codes=_auth_denied_status_codes(api),
                description=f"{tid} deny create in other domain",
            )
            _assert_auth_denied(status, detail, api=api)

    def test_create_without_domains_denied(self, domain_create_auth_setup, api, mode):
        setup = domain_create_auth_setup
        admin_session = setup["admin_session"]
        tid = _transport_id(api, mode)
        urn = _track_urn(setup, _dataset_urn(f"deny-nodomain-{tid}"))

        with _policy(
            admin_session,
            name=f"{POLICY_PREFIX} deny no domain {tid}",
            privileges=["CREATE_ENTITY", "EDIT_ENTITY"],
            domain_urn=DOMAIN_A_URN,
        ):
            _wait_until_policy_allows_domain_a(setup, api, mode)

            def _attempt() -> tuple[bool, Optional[int], str]:
                return _attempt_create(api, mode, setup, urn, domain_urn=None)

            status, detail = _wait_until(
                _attempt,
                want_allowed=False,
                require_auth_denied=True,
                auth_denied_status_codes=_auth_denied_status_codes(api),
                description=f"{tid} deny create without domains",
            )
            _assert_auth_denied(status, detail, api=api)

    def test_create_entity_only_succeeds_in_allowed_domain(
        self, domain_create_auth_setup, graph_client, api, mode
    ):
        setup = domain_create_auth_setup
        admin_session = setup["admin_session"]
        tid = _transport_id(api, mode)
        created: dict[str, Optional[str]] = {"urn": None}

        with _policy(
            admin_session,
            name=f"{POLICY_PREFIX} create-only allow A {tid}",
            privileges=["CREATE_ENTITY"],
            domain_urn=DOMAIN_A_URN,
        ):

            def _attempt() -> tuple[bool, Optional[int], str]:
                urn = _track_urn(
                    setup, _dataset_urn(f"create-only-a-{tid}-{uuid.uuid4().hex[:6]}")
                )
                allowed, status, detail = _attempt_create(
                    api, mode, setup, urn, domain_urn=DOMAIN_A_URN
                )
                if allowed:
                    created["urn"] = urn
                return allowed, status, detail

            _, detail = _wait_until(
                _attempt,
                want_allowed=True,
                description=f"{tid} CREATE_ENTITY-only create in allowed domain",
                timeout_seconds=(
                    _DEFAULT_POLICY_CACHE_WAIT_SECONDS + _DEFAULT_TRACE_WAIT_SECONDS
                    if mode == MODE_ASYNC
                    else _DEFAULT_POLICY_CACHE_WAIT_SECONDS
                ),
            )
            logger.info("CREATE_ENTITY-only Domain A (%s): %s", tid, detail)
            wait_for_writes_to_sync()
            assert created["urn"] is not None
            assert graph_client.exists(created["urn"])
            domains = graph_client.get_aspect(created["urn"], DomainsClass)
            assert domains is not None
            assert DOMAIN_A_URN in [str(d) for d in (domains.domains or [])]

    def test_create_entity_only_cannot_overwrite_existing(
        self, domain_create_auth_setup, graph_client, api, mode
    ):
        setup = domain_create_auth_setup
        admin_session = setup["admin_session"]
        tid = _transport_id(api, mode)
        urn = _track_urn(setup, _dataset_urn(f"no-overwrite-{tid}"))

        graph_client.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=DatasetPropertiesClass(name="preexisting"),
            )
        )
        graph_client.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=DomainsClass(domains=[DOMAIN_A_URN]),
            )
        )
        wait_for_writes_to_sync()

        with _policy(
            admin_session,
            name=f"{POLICY_PREFIX} create-only {tid}",
            privileges=["CREATE_ENTITY"],
            domain_urn=DOMAIN_A_URN,
        ):
            _wait_until_policy_allows_domain_a(setup, api, mode)

            def _attempt() -> tuple[bool, Optional[int], str]:
                # Existing entity: OpenAPI UPSERT and Rest.li CREATE_ENTITY both require EDIT.
                return _attempt_create(
                    api,
                    mode,
                    setup,
                    urn,
                    domain_urn=DOMAIN_A_URN,
                )

            status, detail = _wait_until(
                _attempt,
                want_allowed=False,
                require_auth_denied=True,
                auth_denied_status_codes=_auth_denied_status_codes(api),
                description=f"{tid} deny overwrite with CREATE_ENTITY-only",
            )
            _assert_auth_denied(status, detail, api=api)
