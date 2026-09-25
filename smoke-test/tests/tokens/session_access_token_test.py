import logging
import os
import time
from collections.abc import Iterator

import pytest
from requests.exceptions import HTTPError

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import AuditStampClass, CorpUserStatusClass
from tests.utilities.domains import Domain
from tests.utilities.multi_user import cleanup_step_actor_user, make_step_actor_user
from tests.utils import TestSessionWrapper, wait_for_writes_to_sync

from .token_utils import getUserId

pytestmark = [
    pytest.mark.no_cypress_suite1,
    pytest.mark.domain(Domain.PLATFORM),
    pytest.mark.p0,
]

logger = logging.getLogger(__name__)

# Disable telemetry
os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"


@pytest.fixture
def custom_user(
    auth_session, request: pytest.FixtureRequest
) -> Iterator[tuple[str, TestSessionWrapper]]:
    user_urn, user_session = make_step_actor_user(
        auth_session, f"session-access-token-{request.node.name}"
    )
    try:
        yield user_urn, user_session
    finally:
        try:
            user_session.destroy()
        except Exception:
            logger.warning("Failed to revoke test user token", exc_info=True)
        cleanup_step_actor_user(auth_session, user_urn)


def test_01_soft_delete(auth_session, graph_client, custom_user):
    user_urn, user_session = custom_user

    # assert initial access
    assert getUserId(user_session) == {"urn": user_urn}

    graph_client.soft_delete_entity(urn=user_urn)
    wait_for_writes_to_sync(auth_session=auth_session)

    with pytest.raises(HTTPError) as req_info:
        getUserId(user_session)
    assert "401 Client Error: Unauthorized" in str(req_info.value)

    # undo soft delete
    graph_client.set_soft_delete_status(urn=user_urn, delete=False)
    wait_for_writes_to_sync(auth_session=auth_session)


def test_02_suspend(auth_session, graph_client, custom_user):
    user_urn, user_session = custom_user

    # assert initial access
    assert getUserId(user_session) == {"urn": user_urn}

    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityType="corpuser",
            entityUrn=user_urn,
            changeType="UPSERT",
            aspectName="corpUserStatus",
            aspect=CorpUserStatusClass(
                status="SUSPENDED",
                lastModified=AuditStampClass(
                    time=int(time.time() * 1000.0), actor="urn:li:corpuser:unknown"
                ),
            ),
        )
    )
    wait_for_writes_to_sync(auth_session=auth_session)

    with pytest.raises(HTTPError) as req_info:
        getUserId(user_session)
    assert "401 Client Error: Unauthorized" in str(req_info.value)

    # undo suspend
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityType="corpuser",
            entityUrn=user_urn,
            changeType="UPSERT",
            aspectName="corpUserStatus",
            aspect=CorpUserStatusClass(
                status="ACTIVE",
                lastModified=AuditStampClass(
                    time=int(time.time() * 1000.0), actor="urn:li:corpuser:unknown"
                ),
            ),
        )
    )
    wait_for_writes_to_sync(auth_session=auth_session)


def test_03_hard_delete(auth_session, graph_client, custom_user):
    user_urn, user_session = custom_user

    # assert initial access
    assert getUserId(user_session) == {"urn": user_urn}

    graph_client.hard_delete_entity(urn=user_urn)
    wait_for_writes_to_sync(auth_session=auth_session)

    with pytest.raises(HTTPError) as req_info:
        getUserId(user_session)
    assert "401 Client Error: Unauthorized" in str(req_info.value)
