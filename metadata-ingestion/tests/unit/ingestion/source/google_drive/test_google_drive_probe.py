from typing import Any, Dict, List, Optional, Tuple, cast

import pytest
from google.auth.exceptions import RefreshError
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError

import datahub.ingestion.agent.probe_methods as pm
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import _iter_specs
from datahub.ingestion.agent.verdicts import ProbeArgumentError, ProbeConnectionError
from datahub.ingestion.source.google_drive.google_drive_config import (
    GoogleDriveSourceConfig,
)
from datahub.ingestion.source.google_drive.google_drive_probe import (
    GoogleDriveMetadataProbe,
)
from tests.test_helpers.google_drive_fakes import (
    ARCHIVE,
    CREDENTIAL,
    DOC_A1,
    DOC_COPY,
    DOC_EMPTY,
    DOC_HUGE,
    DOC_OLD,
    DOC_RUNBOOK,
    DOC_TRASHED,
    ENG,
    MIME_DOC,
    ROOT_A,
    SHARED_B,
    SHEET,
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


def test_no_raw_api_and_only_known_parameters() -> None:
    # files.list's `q` accepts fullText searches, a content oracle, so there is no
    # raw `api` passthrough. `folder` is matched against listed names (next test)
    # and `document` must be a Drive ID before it is used
    # (test_explain_refuses_what_it_cannot_find); neither reaches a query.
    specs = dict(_iter_specs(GoogleDriveMetadataProbe))

    assert "api" not in specs
    assert {param.name for spec in specs.values() for param in spec.params} <= {
        "limit",
        "folder",
        "document",
    }


@pytest.mark.parametrize(
    "folder",
    [
        "Root A' or fullText contains 'secret",
        "Root A/Eng' or fullText contains 'secret",
    ],
)
def test_a_folder_path_is_matched_against_listed_names_never_queried(
    folder: str,
) -> None:
    svc = build_fixture_service()

    with pytest.raises(ProbeArgumentError):
        make_probe(svc).documents(folder=folder)

    # Only folders the listing itself returned were ever queried.
    assert set(svc.calls_for("files.list")) <= set(svc.files_by_id)


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


def test_folders_lists_the_whole_tree_breadth_first_with_each_chain() -> None:
    folders = make_probe(build_fixture_service()).folders(limit=100)

    assert [(f["name"], f["depth"]) for f in folders] == [
        ("Root A", 0),
        ("Shared Drive B", 0),
        ("Root A/Eng", 1),
        ("Shared Drive B/Team", 1),
        # Listed although the recipes below deny it: a skipped folder is
        # reported so its verdict can be explained.
        ("Root A/Eng/Archive", 2),
        ("Root A/Eng/Specs", 2),
    ]
    archive = next(f for f in folders if f["name"] == "Root A/Eng/Archive")
    assert archive["id"] == ARCHIVE
    assert archive["ancestors"] == "Root A\nRoot A/Eng"


def test_folders_stops_listing_at_the_limit() -> None:
    svc = build_fixture_service()

    folders = make_probe(svc).folders(limit=2)

    assert [f["name"] for f in folders] == ["Root A", "Shared Drive B"]
    assert svc.calls_for("files.list") == []


def test_documents_lists_docs_in_one_folder_with_its_chain() -> None:
    probe = make_probe(build_fixture_service())

    in_root = probe.documents(folder="Root A")
    in_eng = probe.documents(folder="Root A/Eng")

    # The sheet, the shortcut and the trashed Doc are not Google Docs in scope.
    assert [d["name"] for d in in_root] == ["Doc A1", "Copy of Doc A1", "Doc Empty"]
    assert in_eng == [
        {
            "name": "Doc Runbook",
            "id": DOC_RUNBOOK,
            "modified_time": "2026-02-03T04:05:06+00:00",
            "folders": "Root A\nRoot A/Eng",
        }
    ]
    # Metadata only: no owners and no text.
    assert not any("owners" in d for d in in_root)


def test_documents_never_exports() -> None:
    svc = build_fixture_service()

    make_probe(svc).documents(folder="Root A/Eng/Specs")

    assert svc.calls_for("files.export") == []


def test_unknown_folder_path_is_the_callers_error() -> None:
    with pytest.raises(ProbeArgumentError):
        make_probe(build_fixture_service()).documents(folder="Root A/Nope")


def test_unreadable_folder_is_a_failure_not_an_empty_listing() -> None:
    svc = build_fixture_service()
    svc.list_errors[ENG] = http_error(403, "insufficientFilePermissions")
    probe = make_probe(svc)

    folders = probe.folders(limit=100)

    assert "Root A/Eng/Archive" not in [f["name"] for f in folders]
    assert len(probe.failures) == 1
    assert "Root A/Eng" in probe.failures[0] and "403" in probe.failures[0]


@pytest.mark.parametrize(
    "error", [http_error(403, "userRateLimitExceeded"), http_error(500)]
)
def test_a_rate_limited_listing_raises_rather_than_reading_as_access(
    error: HttpError,
) -> None:
    svc = build_fixture_service()
    svc.list_errors[ENG] = error

    with pytest.raises(HttpError):
        make_probe(svc).folders(limit=100)


def test_scope_counts_what_a_run_would_pick_up() -> None:
    svc = build_fixture_service()

    scope = make_probe(
        svc,
        folder_pattern={"deny": [".*/Archive$"]},
        document_pattern={"deny": ["^Copy of "]},
        max_documents=8,
    ).scope()

    assert scope == {
        "roots": 2,
        # Root A, Eng, Specs, Shared Drive B, Team; Archive is never entered.
        "folders_in_scope": 5,
        "folders_discovered": 4,
        "folders_filtered": 1,
        # Doc Old sits in Archive, so it is never discovered.
        "documents_discovered": 9,
        "documents_filtered": 1,
        "documents_in_scope": 8,
        "documents_unparseable": 0,
        "shortcuts_skipped": 1,
        "max_documents": 8,
        "may_reach_max_documents": True,
    }
    assert svc.calls_for("files.export") == []


def test_scope_reports_an_unreachable_root_as_a_failure() -> None:
    probe = make_probe(build_fixture_service(), root_folder_ids=[ROOT_A, MISSING])

    scope = probe.scope()

    assert scope["roots"] == 1
    assert probe.failures == [
        f"root '{MISSING}' could not be opened (not_found_or_not_shared); "
        "run `roots` for the fix"
    ]


def _judge(
    kind: str,
    names: List[str],
    parent: Optional[str] = None,
    **config_overrides: Any,
) -> List[Tuple[bool, Optional[str]]]:
    config_dict: Dict[str, object] = {
        "root_folder_ids": [ROOT_A],
        "folder_pattern": {"deny": [".*/Archive$"]},
        "document_pattern": {"deny": ["^Copy of "]},
    }
    config_dict.update(config_overrides)
    result = check_filters(
        source_type="google-drive",
        config_dict=config_dict,
        kind=kind,
        parent_path=[parent] if parent else [],
        names=names,
    )
    return [(v.included, v.excluded_by) for v in result.results]


def test_folder_verdicts_follow_the_walk() -> None:
    assert _judge(
        "Folder",
        ["Root A", "Root A/Eng", "Root A/Eng/Archive", "Root A/Eng/Archive/Deep"],
    ) == [
        (True, None),
        (True, None),
        (False, "folder_pattern"),
        # The pattern does not match this path, but the walk never enters Archive.
        (False, "folder_pattern"),
    ]


def test_folder_verdicts_for_recursive_and_root_documents() -> None:
    assert _judge("Folder", ["Root A", "Root A/Eng"], recursive=False) == [
        (True, None),
        (False, "recursive"),
    ]
    assert _judge("Folder", ["Root A"], create_root_documents=False) == [
        (False, "create_root_documents")
    ]


def test_doc_verdicts_judge_the_folder_chain_then_the_title() -> None:
    assert _judge("Google Doc", ["Doc A1", "Copy of Doc A1"], parent="Root A") == [
        (True, None),
        (False, "document_pattern"),
    ]
    assert _judge("Google Doc", ["Doc Old"], parent="Root A/Eng/Archive") == [
        (False, "folder_pattern")
    ]
    assert _judge(
        "Google Doc", ["Doc Runbook"], parent="Root A/Eng", recursive=False
    ) == [(False, "recursive")]


PATTERNS: Dict[str, Any] = {
    "folder_pattern": {"deny": [".*/Archive$"]},
    "document_pattern": {"deny": ["^Copy of "]},
}


def test_explain_an_ingested_doc_reports_length_and_owners_never_text() -> None:
    explained = make_probe(build_fixture_service(), **PATTERNS).explain(
        document=DOC_RUNBOOK
    )

    assert explained == {
        "name": "Doc Runbook",
        "id": DOC_RUNBOOK,
        "mime_type": MIME_DOC,
        "folder": "Root A/Eng",
        "owners": ["alice@example.com", "bob@example.com"],
        "used_html_fallback": False,
        "text_length": len(
            "# Runbook\n\n1. Check the dashboard.\n2. Page the secondary."
        ),
        "ingested": True,
        "max_documents": 10_000,
    }
    assert "Check the dashboard" not in str(explained)


def test_explain_accepts_a_docs_url() -> None:
    explained = make_probe(build_fixture_service()).explain(
        document=f"https://docs.google.com/document/d/{DOC_A1}/edit?tab=t.0"
    )

    assert explained["id"] == DOC_A1
    assert explained["ingested"] is True


def test_explain_withholds_owners_when_ingestion_would_not_emit_them() -> None:
    explained = make_probe(build_fixture_service(), extract_owners=False).explain(
        document=DOC_RUNBOOK
    )

    assert "owners" not in explained
    assert explained["owners_withheld"] == 2


@pytest.mark.parametrize(
    "file_id, overrides, excluded_by",
    [
        (SHEET, {}, "not_a_google_doc"),
        (DOC_TRASHED, {}, "trashed"),
        (DOC_COPY, PATTERNS, "document_pattern"),
        (DOC_OLD, PATTERNS, "folder_pattern"),
        (DOC_RUNBOOK, {"recursive": False}, "recursive"),
        (DOC_A1, {"root_folder_ids": [SHARED_B]}, "outside_roots"),
    ],
)
def test_explain_decides_before_exporting(
    file_id: str, overrides: Dict[str, Any], excluded_by: str
) -> None:
    svc = build_fixture_service()

    explained = make_probe(svc, **overrides).explain(document=file_id)

    assert explained["ingested"] is False
    assert explained["excluded_by"] == excluded_by
    assert explained["explanation"]
    assert svc.calls_for("files.export") == []


@pytest.mark.parametrize(
    "file_id, overrides",
    [(SHEET, {}), (DOC_TRASHED, {}), (DOC_A1, {"root_folder_ids": [SHARED_B]})],
)
def test_explain_names_a_file_only_under_a_configured_root(
    file_id: str, overrides: Dict[str, Any]
) -> None:
    explained = make_probe(build_fixture_service(), **overrides).explain(
        document=file_id
    )

    assert "name" not in explained


@pytest.mark.parametrize(
    "error", [http_error(403, "userRateLimitExceeded"), http_error(500)]
)
def test_explain_raises_when_a_folder_above_is_rate_limited(
    error: HttpError,
) -> None:
    svc = build_fixture_service()
    svc.get_errors[ENG] = error

    with pytest.raises(HttpError):
        make_probe(svc).explain(document=DOC_RUNBOOK)


def test_explain_names_the_folder_that_decided() -> None:
    explained = make_probe(build_fixture_service(), **PATTERNS).explain(
        document=DOC_OLD
    )

    assert explained["decided_by_folder"] == "Root A/Eng/Archive"


@pytest.mark.parametrize(
    "file_id, overrides, excluded_by",
    [
        (DOC_EMPTY, {}, "skip_empty_documents"),
        (DOC_A1, {"min_text_length": 500}, "min_text_length"),
        (DOC_A1, {"max_text_length": 5}, "max_text_length"),
        (DOC_HUGE, {}, "too_large"),
    ],
)
def test_explain_applies_the_text_rules_after_exporting(
    file_id: str, overrides: Dict[str, Any], excluded_by: str
) -> None:
    explained = make_probe(build_fixture_service(), **overrides).explain(
        document=file_id
    )

    assert explained["ingested"] is False
    assert explained["excluded_by"] == excluded_by


@pytest.mark.parametrize("document", ["not an id", "missingFile0001"])
def test_explain_refuses_what_it_cannot_find(document: str) -> None:
    with pytest.raises(ProbeArgumentError):
        make_probe(build_fixture_service()).explain(document=document)
