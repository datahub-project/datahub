from typing import Any, Dict, cast

import pytest
from google.auth.exceptions import RefreshError
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError

import datahub.ingestion.agent.probe_methods as pm
from datahub.ingestion.agent.probe_methods import _iter_specs
from datahub.ingestion.agent.verdicts import ProbeConnectionError
from datahub.ingestion.source.google_drive.google_drive_config import (
    GoogleDriveSourceConfig,
)
from datahub.ingestion.source.google_drive.google_drive_probe import (
    GoogleDriveMetadataProbe,
)
from tests.test_helpers.google_drive_fakes import (
    CREDENTIAL,
    DOC_A1,
    ROOT_A,
    SHARED_B,
    TEAM,
    FakeDriveService,
    build_fixture_service,
    http_error,
)

MISSING = "missingFolder01"
DELEGATED_USER = "reader@example.com"


def make_probe(
    svc: FakeDriveService, **config_overrides: Any
) -> GoogleDriveMetadataProbe:
    config_dict: Dict[str, Any] = {"root_folder_ids": [ROOT_A, SHARED_B]}
    config_dict.update(config_overrides)
    config = GoogleDriveSourceConfig.model_validate(config_dict)
    return GoogleDriveMetadataProbe(config, service=cast(Resource, svc))


def token_error(code: str) -> RefreshError:
    # The description is Google's text; the probe must never repeat it.
    return RefreshError(
        f"{code}: description-from-google", {"error": code}, retryable=False
    )


def test_identity_without_delegation_reports_the_service_account() -> None:
    svc = build_fixture_service()
    svc.about_email = CREDENTIAL["client_email"]

    identity = make_probe(svc, credential=CREDENTIAL).identity()

    assert identity == {
        "acting_as": CREDENTIAL["client_email"],
        "credential": "service_account",
        "service_account": CREDENTIAL["client_email"],
        "delegated_user": None,
    }


@pytest.mark.parametrize(
    "acting_as, in_effect", [(DELEGATED_USER, True), ("someone@example.com", False)]
)
def test_identity_says_whether_delegation_is_in_effect(
    acting_as: str, in_effect: bool
) -> None:
    svc = build_fixture_service()
    svc.about_email = acting_as

    identity = make_probe(
        svc, credential=CREDENTIAL, delegated_user=DELEGATED_USER
    ).identity()

    assert identity["delegation_in_effect"] is in_effect


@pytest.mark.parametrize(
    "code, delegated, expected",
    [
        ("unauthorized_client", True, "domain-wide delegation is not authorized"),
        ("invalid_grant", True, "not an active user"),
        ("invalid_grant", False, "rejected the service account key"),
        ("server_error", False, "refused to issue an access token"),
    ],
)
def test_token_failures_explain_the_fix_without_googles_text(
    code: str, delegated: bool, expected: str
) -> None:
    svc = build_fixture_service()
    svc.token_error = token_error(code)
    overrides: Dict[str, Any] = {"credential": CREDENTIAL}
    if delegated:
        overrides["delegated_user"] = DELEGATED_USER

    with pytest.raises(ProbeConnectionError) as excinfo:
        make_probe(svc, **overrides).identity()

    assert expected in str(excinfo.value)
    assert "description-from-google" not in str(excinfo.value)


def test_roots_report_each_root_without_failing_the_others() -> None:
    svc = build_fixture_service()
    svc.get_errors["forbiddenFolder"] = http_error(403, "domainPolicy")

    roots = make_probe(
        svc,
        credential=CREDENTIAL,
        root_folder_ids=[ROOT_A, SHARED_B, TEAM, MISSING, DOC_A1, "forbiddenFolder"],
    ).roots()

    by_id = {root["id"]: root for root in roots}
    assert [root["id"] for root in roots] == [
        ROOT_A,
        SHARED_B,
        TEAM,
        MISSING,
        DOC_A1,
        "forbiddenFolder",
    ]
    assert by_id[ROOT_A] == {
        "name": "Root A",
        "id": ROOT_A,
        "accessible": True,
        "kind": "my_drive_folder",
    }
    assert by_id[SHARED_B]["name"] == "Shared Drive B"
    assert by_id[SHARED_B]["kind"] == "shared_drive"
    assert by_id[TEAM]["kind"] == "shared_drive_folder"
    assert by_id[MISSING]["problem"] == "not_found_or_not_shared"
    # The fix names who to share with, so the caller can act on it.
    assert CREDENTIAL["client_email"] in str(by_id[MISSING]["fix"])
    assert by_id[DOC_A1]["problem"] == "not_a_folder"
    assert by_id["forbiddenFolder"]["problem"] == "forbidden"
    assert not any(by_id[r]["accessible"] for r in (MISSING, DOC_A1))


def test_unshared_root_asks_to_share_with_the_delegated_user() -> None:
    roots = make_probe(
        build_fixture_service(),
        credential=CREDENTIAL,
        delegated_user=DELEGATED_USER,
        root_folder_ids=[MISSING],
    ).roots()

    assert DELEGATED_USER in str(roots[0]["fix"])


@pytest.mark.parametrize(
    "error", [http_error(403, "userRateLimitExceeded"), http_error(500)]
)
def test_roots_fail_on_errors_that_say_nothing_about_the_root(
    error: HttpError,
) -> None:
    svc = build_fixture_service()
    svc.get_errors[ROOT_A] = error

    with pytest.raises(HttpError):
        make_probe(svc).roots()


def test_shared_drives_page_through_and_mark_configured_ones() -> None:
    svc = build_fixture_service()
    svc.shared_drives["sharedCCCCCCC"] = "Shared Drive C"
    svc.shared_drives["sharedDDDDDDD"] = "Shared Drive D"

    drives = make_probe(svc).shared_drives(limit=10)

    assert drives == [
        {"name": "Shared Drive B", "id": SHARED_B, "configured": True},
        {"name": "Shared Drive C", "id": "sharedCCCCCCC", "configured": False},
        {"name": "Shared Drive D", "id": "sharedDDDDDDD", "configured": False},
    ]
    assert len(svc.calls_for("drives.list")) == 2


def test_shared_drives_stop_paging_at_the_limit() -> None:
    svc = build_fixture_service()
    for suffix in "CDEF":
        svc.shared_drives[f"shared{suffix * 7}"] = f"Shared Drive {suffix}"

    drives = make_probe(svc).shared_drives(limit=1)

    assert len(drives) == 1
    assert len(svc.calls_for("drives.list")) == 1


def test_no_command_takes_free_text_that_could_reach_a_drive_query() -> None:
    # files.list's `q` accepts fullText searches, a content oracle. Every query is
    # built from validated IDs, so no command may take a free-form parameter or
    # offer a raw `api` passthrough.
    specs = dict(_iter_specs(GoogleDriveMetadataProbe))

    assert "api" not in specs
    assert {param.name for spec in specs.values() for param in spec.params} <= {"limit"}


def test_probe_error_code_reads_googles_http_status() -> None:
    assert GoogleDriveMetadataProbe.probe_error_code(http_error(404)) == "HTTP 404"
    assert GoogleDriveMetadataProbe.probe_error_code(ValueError("x")) is None


def test_run_probe_method_opens_and_closes_the_drive_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    svc = build_fixture_service()

    def for_config(
        cls: type, config: GoogleDriveSourceConfig
    ) -> GoogleDriveMetadataProbe:
        return GoogleDriveMetadataProbe(config, service=cast(Resource, svc))

    monkeypatch.setattr(GoogleDriveMetadataProbe, "for_config", classmethod(for_config))

    result = pm.run_probe_method(
        "google-drive", {"root_folder_ids": [ROOT_A]}, "roots", {}
    )

    assert result.failures == []
    assert result.result == [
        {"name": "Root A", "id": ROOT_A, "accessible": True, "kind": "my_drive_folder"}
    ]
    assert svc.closed
