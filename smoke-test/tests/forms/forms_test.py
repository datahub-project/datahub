"""End-to-end coverage for the batchAssignForm / batchRemoveForm mutations.

Most of these assert behaviour that is deliberately unchanged by batching, and so
pass both before and after it: they exist to prove the batched implementation kept
the original semantics, not to prove the batching happened. The number of calls the
server makes is not observable over GraphQL, so the batching contract itself is
asserted in FormServiceTest instead.

test_batch_assign_form_reports_every_missing_entity is the exception — it pins
behaviour that only holds after batching.
"""

import logging
import uuid
from typing import Any, Dict, List

import pytest

from conftest import _ingest_cleanup_data_impl
from tests.utilities.domains import Domain
from tests.utils import delete_entity, execute_graphql, wait_for_writes_to_sync

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)

DATASET_URNS = [
    f"urn:li:dataset:(urn:li:dataPlatform:kafka,test-forms-sample-kafka-{i},PROD)"
    for i in range(1, 4)
]
CHART_URN = "urn:li:chart:(looker,test-forms-sample-chart)"
MISSING_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:kafka,test-forms-does-not-exist,PROD)"
)
SECOND_MISSING_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:kafka,test-forms-also-does-not-exist,PROD)"
)
# Both FormPromptType values require structured property params, so prompts need a
# real structured property to point at. Seeded alongside the entities in data.json.
STRUCTURED_PROPERTY_URN = "urn:li:structuredProperty:io.datahub.test.formsSmokeProperty"

CREATE_FORM = """mutation createForm($input: CreateFormInput!) {
    createForm(input: $input) { urn }
}"""

BATCH_ASSIGN_FORM = """mutation batchAssignForm($input: BatchAssignFormInput!) {
    batchAssignForm(input: $input)
}"""

BATCH_REMOVE_FORM = """mutation batchRemoveForm($input: BatchRemoveFormInput!) {
    batchRemoveForm(input: $input)
}"""

GET_DATASET_FORMS = """query getDatasetForms($urn: String!) {
    dataset(urn: $urn) {
        forms {
            incompleteForms { form { urn } }
            completedForms { form { urn } }
        }
    }
}"""

GET_CHART_FORMS = """query getChartForms($urn: String!) {
    chart(urn: $urn) {
        forms {
            incompleteForms { form { urn } }
            completedForms { form { urn } }
        }
    }
}"""


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    yield from _ingest_cleanup_data_impl(
        auth_session, graph_client, "tests/forms/data.json", "forms"
    )


@pytest.fixture(scope="module")
def form_urn(auth_session):
    """Creates a form with run-unique form and prompt IDs.

    Prompt IDs must be globally unique and FormPromptValidator enforces that with a
    search, so a fixed ID can collide with a stale index entry from a previous run's
    deleted form. A fresh UUID per run keeps the check deterministic.
    """
    unique_id = str(uuid.uuid4())
    res_data = execute_graphql(
        auth_session,
        CREATE_FORM,
        {
            "input": {
                "id": f"test-forms-{unique_id}",
                "name": f"Test Form {unique_id}",
                "description": "Form fixture for batch assign/remove smoke tests",
                "type": "COMPLETION",
                "prompts": [
                    {
                        "id": f"test-forms-prompt-{unique_id}",
                        "title": "Sample prompt",
                        "type": "STRUCTURED_PROPERTY",
                        "structuredPropertyParams": {"urn": STRUCTURED_PROPERTY_URN},
                        "required": False,
                    }
                ],
                "actors": {"owners": True},
            }
        },
    )
    urn = res_data["data"]["createForm"]["urn"]
    assert urn, "createForm did not return a urn"
    wait_for_writes_to_sync()

    yield urn

    delete_entity(auth_session, urn)


def _assign(
    auth_session,
    form: str,
    urns: List[str],
    expect_errors: bool = False,
    no_sync_wait: bool = False,
):
    return execute_graphql(
        auth_session,
        BATCH_ASSIGN_FORM,
        {"input": {"formUrn": form, "entityUrns": urns}},
        expect_errors=expect_errors,
        no_sync_wait=no_sync_wait,
    )


def _remove(
    auth_session,
    form: str,
    urns: List[str],
    expect_errors: bool = False,
    no_sync_wait: bool = False,
):
    return execute_graphql(
        auth_session,
        BATCH_REMOVE_FORM,
        {"input": {"formUrn": form, "entityUrns": urns}},
        expect_errors=expect_errors,
        no_sync_wait=no_sync_wait,
    )


def _assigned_forms(auth_session, urn: str) -> List[str]:
    # Read-only -- never has state to sync, so always skip the wait. Called in
    # loops (directly and via _count_assigned) across most tests below.
    query, key = (
        (GET_CHART_FORMS, "chart")
        if urn.startswith("urn:li:chart")
        else (GET_DATASET_FORMS, "dataset")
    )
    res_data = execute_graphql(auth_session, query, {"urn": urn}, no_sync_wait=True)
    forms: Dict[str, Any] = res_data["data"][key]["forms"] or {}
    return [
        association["form"]["urn"]
        for association in (forms.get("incompleteForms") or [])
        + (forms.get("completedForms") or [])
    ]


def _count_assigned(auth_session, form: str, urns: List[str]) -> int:
    return sum(1 for urn in urns if form in _assigned_forms(auth_session, urn))


@pytest.fixture(autouse=True)
def unassigned_before_each(auth_session, form_urn):
    """Each test starts with the form assigned to nothing."""
    _remove(auth_session, form_urn, DATASET_URNS + [CHART_URN])
    wait_for_writes_to_sync()


def test_batch_assign_and_remove_form(auth_session, form_urn):
    res_data = _assign(auth_session, form_urn, DATASET_URNS)
    assert res_data["data"]["batchAssignForm"] is True
    wait_for_writes_to_sync()
    assert _count_assigned(auth_session, form_urn, DATASET_URNS) == len(DATASET_URNS)

    res_data = _remove(auth_session, form_urn, DATASET_URNS)
    assert res_data["data"]["batchRemoveForm"] is True
    wait_for_writes_to_sync()
    assert _count_assigned(auth_session, form_urn, DATASET_URNS) == 0


def test_batch_assign_form_across_entity_types(auth_session, form_urn):
    """A batch spanning entity types is read per type, so both types must be assigned."""
    urns = DATASET_URNS + [CHART_URN]
    assert _assign(auth_session, form_urn, urns)["data"]["batchAssignForm"] is True
    wait_for_writes_to_sync()

    assert _count_assigned(auth_session, form_urn, urns) == len(urns)

    assert _remove(auth_session, form_urn, urns)["data"]["batchRemoveForm"] is True
    wait_for_writes_to_sync()
    assert _count_assigned(auth_session, form_urn, urns) == 0


def test_batch_assign_form_is_idempotent(auth_session, form_urn):
    """Re-assigning an already-assigned form succeeds and does not duplicate it."""
    assert (
        _assign(auth_session, form_urn, DATASET_URNS)["data"]["batchAssignForm"] is True
    )
    wait_for_writes_to_sync()

    assert (
        _assign(auth_session, form_urn, DATASET_URNS)["data"]["batchAssignForm"] is True
    )
    wait_for_writes_to_sync()

    for urn in DATASET_URNS:
        assigned = _assigned_forms(auth_session, urn)
        assert assigned.count(form_urn) == 1, (
            f"form assigned {assigned.count(form_urn)} times to {urn}, expected once"
        )


def test_batch_assign_form_tolerates_duplicate_urns(auth_session, form_urn):
    """A urn repeated in the input is assigned once, not twice."""
    duplicated = DATASET_URNS + DATASET_URNS
    assert (
        _assign(auth_session, form_urn, duplicated)["data"]["batchAssignForm"] is True
    )
    wait_for_writes_to_sync()

    for urn in DATASET_URNS:
        assert _assigned_forms(auth_session, urn).count(form_urn) == 1


def test_batch_assign_form_rejects_batch_with_missing_entity(auth_session, form_urn):
    """A nonexistent entity fails the whole batch, leaving the valid entities untouched."""
    res_data = _assign(
        auth_session, form_urn, DATASET_URNS + [MISSING_URN], expect_errors=True
    )
    assert res_data.get("errors"), "expected an error for a nonexistent entity urn"
    wait_for_writes_to_sync()

    assert _count_assigned(auth_session, form_urn, DATASET_URNS) == 0, (
        "no entity should be assigned when the batch contains a nonexistent urn"
    )


def test_batch_assign_form_reports_every_missing_entity(auth_session, form_urn):
    """The failure names all missing urns, not just the first one encountered."""
    res_data = _assign(
        auth_session,
        form_urn,
        [MISSING_URN, DATASET_URNS[0], SECOND_MISSING_URN],
        expect_errors=True,
    )
    errors = " ".join(
        error.get("message", "") for error in res_data.get("errors") or []
    )
    assert MISSING_URN in errors, f"first missing urn absent from error: {errors}"
    assert SECOND_MISSING_URN in errors, (
        f"second missing urn absent from error: {errors}"
    )


def test_batch_remove_form_never_assigned_is_noop(auth_session, form_urn):
    """Removing a form that was never assigned succeeds without changing anything."""
    assert (
        _remove(auth_session, form_urn, DATASET_URNS)["data"]["batchRemoveForm"] is True
    )
    wait_for_writes_to_sync()
    assert _count_assigned(auth_session, form_urn, DATASET_URNS) == 0


def test_batch_remove_form_only_affects_named_form(auth_session, form_urn):
    """Removing one form leaves other forms on the entity in place."""
    other_id = str(uuid.uuid4())
    other_urn = execute_graphql(
        auth_session,
        CREATE_FORM,
        {
            "input": {
                "id": f"test-forms-other-{other_id}",
                "name": f"Other Test Form {other_id}",
                "type": "COMPLETION",
                "prompts": [
                    {
                        "id": f"test-forms-other-prompt-{other_id}",
                        "title": "Sample prompt",
                        "type": "STRUCTURED_PROPERTY",
                        "structuredPropertyParams": {"urn": STRUCTURED_PROPERTY_URN},
                        "required": False,
                    }
                ],
                "actors": {"owners": True},
            }
        },
    )["data"]["createForm"]["urn"]
    wait_for_writes_to_sync()

    try:
        # Both assigns are back-to-back writes with no intermediate read -- the
        # explicit wait_for_writes_to_sync() below is the single wait for the batch.
        _assign(auth_session, form_urn, DATASET_URNS, no_sync_wait=True)
        _assign(auth_session, other_urn, DATASET_URNS, no_sync_wait=True)
        wait_for_writes_to_sync()

        _remove(auth_session, form_urn, DATASET_URNS)
        wait_for_writes_to_sync()

        for urn in DATASET_URNS:
            assigned = _assigned_forms(auth_session, urn)
            assert form_urn not in assigned
            assert other_urn in assigned, (
                f"removing {form_urn} should not have removed {other_urn} from {urn}"
            )
    finally:
        # Teardown -- nothing reads this state afterward, so both writes skip the wait.
        _remove(auth_session, other_urn, DATASET_URNS, no_sync_wait=True)
        delete_entity(auth_session, other_urn, no_sync_wait=True)
