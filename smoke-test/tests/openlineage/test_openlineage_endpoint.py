"""Live-stack behaviour of the OpenLineage REST endpoint.

The fixture-driven cases in ``tests/openapi/openlineage/`` cover status codes and
the resulting entity shapes. Two things need more than that harness can express:
authorization, which needs a second, unprivileged actor, and "a JobEvent creates
no run", which is an assertion about absence and is cleanest through GraphQL.
"""

import logging
import uuid

import pytest

from tests.utilities.domains import Domain
from tests.utilities.multi_user import cleanup_step_actor_user, make_step_actor_user
from tests.utils import execute_graphql, with_test_retry

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.PLATFORM)

LINEAGE_ENDPOINT = "/openapi/openlineage/api/v1/lineage"
PRODUCER = "https://github.com/apache/airflow/tree/1.0.0"
JOB_EVENT_SCHEMA = (
    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent"
)


def _namespace() -> str:
    # Run-unique so parallel workers and reruns never assert on each other's edges.
    return f"smoke_ol_{uuid.uuid4().hex[:8]}"


def _run_event(namespace: str, job_name: str) -> dict:
    return {
        "eventType": "COMPLETE",
        "eventTime": "2024-06-01T10:00:00.000Z",
        "run": {"runId": str(uuid.uuid4())},
        "job": {"namespace": namespace, "name": job_name},
        "inputs": [
            {
                "namespace": "postgres://my-host:5432",
                "name": "my_db.my_schema.events",
            }
        ],
        "producer": PRODUCER,
    }


def _job_urn(namespace: str, job_name: str) -> str:
    flow = f"urn:li:dataFlow:(airflow,{job_name},{namespace})"
    return f"urn:li:dataJob:({flow},{job_name})"


@with_test_retry()
def _runs_total(auth_session, job_urn: str) -> int:
    data = execute_graphql(
        auth_session,
        """query dataJobRuns($urn: String!) {
             dataJob(urn: $urn) { urn runs(start: 0, count: 10) { total } }
           }""",
        variables={"urn": job_urn},
    )
    data_job = data["dataJob"]
    assert data_job is not None, f"DataJob {job_urn} was not created"
    return data_job["runs"]["total"]


def test_openlineage_ingest_requires_privileges(auth_session):
    """The endpoint writes through the entity service, which performs no privilege
    check of its own, so the controller has to enforce one."""
    namespace = _namespace()
    event = _run_event(namespace, "authz_probe")

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


def test_job_event_creates_no_run(auth_session):
    """A JobEvent is the spec's static-lineage path: job metadata with no run, so it
    must not manufacture a DataProcessInstance. The RunEvent alongside it is the
    control that proves the assertion can fail."""
    namespace = _namespace()

    job_event = {
        "eventTime": "2024-06-01T10:00:00.000Z",
        "schemaURL": JOB_EVENT_SCHEMA,
        "job": {"namespace": namespace, "name": "static_job"},
        "inputs": [
            {"namespace": "postgres://my-host:5432", "name": "my_db.my_schema.events"}
        ],
        "producer": PRODUCER,
    }
    response = auth_session.post(
        f"{auth_session.gms_url()}{LINEAGE_ENDPOINT}", json=job_event
    )
    assert response.status_code == 201, response.text[:200]

    run_event = _run_event(namespace, "run_job")
    response = auth_session.post(
        f"{auth_session.gms_url()}{LINEAGE_ENDPOINT}", json=run_event
    )
    assert response.status_code == 201, response.text[:200]

    assert _runs_total(auth_session, _job_urn(namespace, "static_job")) == 0, (
        "a JobEvent carries no run, so its DataJob must have no DataProcessInstance"
    )
    assert _runs_total(auth_session, _job_urn(namespace, "run_job")) == 1, (
        "the RunEvent control must produce exactly one DataProcessInstance"
    )
