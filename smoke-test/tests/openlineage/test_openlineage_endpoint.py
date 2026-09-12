"""Live-stack behaviour of the OpenLineage REST endpoint.

The fixture-driven cases in ``tests/openapi/openlineage/`` cover status codes and the
resulting entity shapes. Two things need more than that harness can express: authorization,
which needs a second unprivileged actor, and "a JobEvent creates no run", which is an
assertion about absence.
"""

import logging
import urllib.parse
import uuid

import pytest

from tests.utilities.domains import Domain
from tests.utilities.multi_user import cleanup_step_actor_user, make_step_actor_user
from tests.utils import execute_graphql, with_test_retry

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


JOB_EVENT_SCHEMA = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent"


def _job_urn(namespace: str, job_name: str) -> str:
    return f"urn:li:dataJob:(urn:li:dataFlow:(airflow,{job_name},{namespace}),{job_name})"


@with_test_retry()
def _runs_total(auth_session, job_urn: str) -> int:
    response = execute_graphql(
        auth_session,
        """query dataJobRuns($urn: String!) {
             dataJob(urn: $urn) { urn runs(start: 0, count: 10) { total } }
           }""",
        variables={"urn": job_urn},
    )
    # execute_graphql returns the whole GraphQL envelope, not just its "data" member.
    data_job = response["data"]["dataJob"]
    assert data_job is not None, f"DataJob {job_urn} was not created"
    return data_job["runs"]["total"]


@with_test_retry()
def _assert_run_counts(auth_session, namespace: str) -> None:
    """Both assertions live inside one retry: the DataJob can be readable before its
    DataProcessInstance is, which would make the control look like a pass for the wrong reason."""
    assert _runs_total(auth_session, _job_urn(namespace, "static_job")) == 0, (
        "a JobEvent carries no run, so its DataJob must have no DataProcessInstance"
    )
    assert _runs_total(auth_session, _job_urn(namespace, "run_job")) == 1, (
        "the RunEvent control must produce exactly one DataProcessInstance"
    )


def test_job_event_creates_no_run(auth_session):
    """A JobEvent is the spec's static-lineage path: job metadata with no run, so it must not
    manufacture a DataProcessInstance. The RunEvent alongside it is the control that proves the
    assertion can fail."""
    suffix = uuid.uuid4().hex[:8]
    namespace = f"smoke_ol_{suffix}"
    dataset_name = f"my_db.my_schema.smoke_ol_{suffix}"
    run_id = str(uuid.uuid4())

    job_event = {
        "eventTime": "2024-06-01T10:00:00.000Z",
        "schemaURL": JOB_EVENT_SCHEMA,
        "job": {"namespace": namespace, "name": "static_job"},
        "inputs": [{"namespace": "postgres://my-host:5432", "name": dataset_name}],
        "producer": PRODUCER,
    }
    run_event = {
        "eventType": "COMPLETE",
        "eventTime": "2024-06-01T10:00:00.000Z",
        "run": {"runId": run_id},
        "job": {"namespace": namespace, "name": "run_job"},
        "inputs": [{"namespace": "postgres://my-host:5432", "name": dataset_name}],
        "producer": PRODUCER,
    }

    try:
        for event in (job_event, run_event):
            response = auth_session.post(
                f"{auth_session.gms_url()}{LINEAGE_ENDPOINT}", json=event
            )
            assert response.status_code == 201, response.text[:200]

        _assert_run_counts(auth_session, namespace)
    finally:
        for job_name in ("static_job", "run_job"):
            _delete_entity(auth_session, "datajob", _job_urn(namespace, job_name))
            _delete_entity(
                auth_session, "dataflow", f"urn:li:dataFlow:(airflow,{job_name},{namespace})"
            )
        _delete_entity(
            auth_session, "dataprocessinstance", f"urn:li:dataProcessInstance:{run_id}"
        )
        _delete_entity(
            auth_session,
            "dataset",
            f"urn:li:dataset:(urn:li:dataPlatform:postgres,{dataset_name},PROD)",
        )


def test_empty_body_is_rejected_as_a_client_error(auth_session):
    """An empty body parses to null rather than throwing, which used to reach the classifier
    and surface as a 500."""
    response = auth_session.post(
        f"{auth_session.gms_url()}{LINEAGE_ENDPOINT}",
        data="",
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 400, (
        f"an empty body is a client error, got {response.status_code}: {response.text[:200]}"
    )
