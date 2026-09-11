"""Live-stack behaviour of the OpenLineage REST endpoint.

The fixture-driven cases in ``tests/openapi/openlineage/`` cover status codes and the
resulting entity shapes. Authorization needs more than that harness can express, because
it needs a second, unprivileged actor.
"""

import logging
import urllib.parse
import uuid

import pytest

from tests.utilities.domains import Domain
from tests.utilities.multi_user import cleanup_step_actor_user, make_step_actor_user

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.PLATFORM)

LINEAGE_ENDPOINT = "/openapi/openlineage/api/v1/lineage"
PRODUCER = "https://github.com/apache/airflow/tree/1.0.0"


def _delete_entity(auth_session, entity_type: str, urn: str) -> None:
    """Best-effort teardown; a failure here must not mask the test's own assertion."""
    try:
        auth_session.delete(
            f"{auth_session.gms_url()}/openapi/v3/entity/{entity_type}/"
            f"{urllib.parse.quote(urn, safe='')}"
        )
    except Exception:  # noqa: BLE001 - teardown is advisory
        logger.warning("Could not clean up %s %s", entity_type, urn, exc_info=True)


def test_openlineage_ingest_requires_privileges(auth_session):
    """The endpoint writes through the entity service, which performs no privilege check
    of its own, so the controller has to enforce one."""
    # Run-unique throughout: parallel workers must never collide, and nothing here may
    # touch a dataset another test shares.
    suffix = uuid.uuid4().hex[:8]
    namespace = f"smoke_ol_{suffix}"
    job_name = "authz_probe"
    dataset_name = f"my_db.my_schema.smoke_ol_{suffix}"
    run_id = str(uuid.uuid4())

    event = {
        "eventType": "COMPLETE",
        "eventTime": "2024-06-01T10:00:00.000Z",
        "run": {"runId": run_id},
        "job": {"namespace": namespace, "name": job_name},
        "inputs": [{"namespace": "postgres://my-host:5432", "name": dataset_name}],
        "producer": PRODUCER,
    }

    flow_urn = f"urn:li:dataFlow:(airflow,{job_name},{namespace})"
    job_urn = f"urn:li:dataJob:({flow_urn},{job_name})"
    dataset_urn = (
        f"urn:li:dataset:(urn:li:dataPlatform:postgres,{dataset_name},PROD)"
    )
    dpi_urn = f"urn:li:dataProcessInstance:{run_id}"

    user_urn, unprivileged_session = make_step_actor_user(auth_session, "ol-authz")
    try:
        denied = unprivileged_session.post(
            f"{unprivileged_session.gms_url()}{LINEAGE_ENDPOINT}", json=event
        )
        assert denied.status_code == 403, (
            f"an actor without lineage privileges must be refused, got "
            f"{denied.status_code}: {denied.text[:200]}"
        )

        allowed = auth_session.post(
            f"{auth_session.gms_url()}{LINEAGE_ENDPOINT}", json=event
        )
        assert allowed.status_code == 201, (
            f"a privileged actor must still be able to ingest, got "
            f"{allowed.status_code}: {allowed.text[:200]}"
        )
    finally:
        cleanup_step_actor_user(auth_session, user_urn)
        for entity_type, urn in (
            ("dataprocessinstance", dpi_urn),
            ("datajob", job_urn),
            ("dataflow", flow_urn),
            ("dataset", dataset_urn),
        ):
            _delete_entity(auth_session, entity_type, urn)
