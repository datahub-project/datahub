import json
import logging
from typing import List
from unittest.mock import Mock, call, patch

import pytest
from databricks.sdk.errors import PermissionDenied, Unauthenticated

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.genie_diagnostics import (
    LOG_CHUNK_SIZE,
    log_genie_spaces,
)
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.ingestion.source.unity.source import UnityCatalogSource
from datahub.utilities.logging_manager import (
    BASE_LOGGING_FORMAT,
    IN_MEMORY_LOG_BUFFER_MAX_LINE_LENGTH,
)


def test_proxy_quotes_space_id_and_requests_serialized_space() -> None:
    workspace_client = Mock()
    proxy = UnityCatalogApiProxy(
        workspace_client=workspace_client, report=UnityCatalogReport()
    )
    proxy.list_genie_spaces_raw()
    proxy.list_genie_spaces_raw(page_token="next")
    proxy.get_genie_space_raw("space/1")
    assert workspace_client.api_client.do.call_args_list == [
        call("GET", "/api/2.0/genie/spaces", query={}),
        call("GET", "/api/2.0/genie/spaces", query={"page_token": "next"}),
        call(
            "GET",
            "/api/2.0/genie/spaces/space%2F1",
            query={"include_serialized_space": True},
        ),
    ]


def test_paginated_raw_responses_and_large_exports(
    caplog: pytest.LogCaptureFixture,
) -> None:
    proxy = Mock(spec=UnityCatalogApiProxy)
    detail = {
        "serialized_space": json.dumps({"instructions": {"unknown_field": "x" * 50000}})
    }
    proxy.list_genie_spaces_raw.side_effect = [
        {"spaces": [{"space_id": "space/1"}], "next_page_token": "next"},
        {"spaces": [{"space_id": "space-2"}]},
    ]
    proxy.get_genie_space_raw.side_effect = [
        detail,
        {"description": "", "serialized_space": "{}"},
    ]
    with caplog.at_level(logging.INFO):
        log_genie_spaces(proxy, UnityCatalogReport())
    assert proxy.list_genie_spaces_raw.call_args_list == [
        call(page_token=None),
        call(page_token="next"),
    ]
    chunks = [
        record.getMessage().split(" payload=", 1)[1]
        for record in caplog.records
        if record.getMessage().startswith("Genie diagnostic detail id=space/1 ")
    ]
    assert all(len(chunk) <= LOG_CHUNK_SIZE for chunk in chunks)
    assert json.loads("".join(chunks)) == detail
    formatter = logging.Formatter(BASE_LOGGING_FORMAT)
    assert all(
        len(formatter.format(record)) <= IN_MEMORY_LOG_BUFFER_MAX_LINE_LENGTH
        for record in caplog.records
    )
    assert "visible_spaces=2 details=2 failures=0 listing_complete=True" in caplog.text


@pytest.mark.parametrize(
    "error, status",
    [
        (PermissionDenied("private failure"), "permission_denied"),
        (Unauthenticated("private failure"), "unauthenticated"),
    ],
)
def test_list_permission_failure_is_not_empty_result(
    error: Exception, status: str, caplog: pytest.LogCaptureFixture
) -> None:
    proxy = Mock(spec=UnityCatalogApiProxy)
    proxy.list_genie_spaces_raw.side_effect = error
    report = UnityCatalogReport()
    with caplog.at_level(logging.INFO):
        log_genie_spaces(proxy, report)
    # No error_code in the body, so the exception class identifies the failure.
    assert f"status={status} error_code={type(error).__name__}" in caplog.text
    assert "failures=1 listing_complete=False" in caplog.text
    assert "private failure" not in caplog.text
    assert report.warnings


def test_list_failure_names_its_page_and_keeps_its_own_report_entry(
    caplog: pytest.LogCaptureFixture,
) -> None:
    proxy = Mock(spec=UnityCatalogApiProxy)
    proxy.list_genie_spaces_raw.side_effect = [
        {"spaces": [{"space_id": "denied"}], "next_page_token": "next"},
        {"spaces": "invalid"},
    ]
    proxy.get_genie_space_raw.side_effect = [PermissionDenied("denied")]
    report = UnityCatalogReport()
    with caplog.at_level(logging.INFO):
        log_genie_spaces(proxy, report)
    assert "Genie diagnostic list id=2 status=request_failed" in caplog.text
    assert "list id=3" not in caplog.text
    assert len(report.warnings) == 2


def test_detail_denied_preserves_list_metadata_and_continues(
    caplog: pytest.LogCaptureFixture,
) -> None:
    proxy = Mock(spec=UnityCatalogApiProxy)
    proxy.list_genie_spaces_raw.side_effect = [
        {
            "spaces": [
                {"space_id": "denied", "title": "Visible title"},
                {"space_id": "allowed"},
            ]
        },
    ]
    proxy.get_genie_space_raw.side_effect = [
        PermissionDenied("denied"),
        {"description": "Available description"},
    ]
    with caplog.at_level(logging.INFO):
        log_genie_spaces(proxy, UnityCatalogReport())
    assert "Visible title" in caplog.text
    assert "Available description" in caplog.text
    assert "visible_spaces=2 details=1 failures=1 listing_complete=True" in caplog.text


@pytest.mark.parametrize(
    "responses",
    [
        [
            {},
        ],
        [
            {"spaces": [], "next_page_token": "repeat"},
            {"spaces": [], "next_page_token": "repeat"},
        ],
        [{"spaces": "invalid"}],
        [RuntimeError("private failure")],
    ],
)
def test_empty_and_incomplete_scans_are_distinguished(
    responses: List[object], caplog: pytest.LogCaptureFixture
) -> None:
    proxy = Mock(spec=UnityCatalogApiProxy)
    proxy.list_genie_spaces_raw.side_effect = responses
    with caplog.at_level(logging.INFO):
        log_genie_spaces(proxy, UnityCatalogReport())
    assert (
        "listing_complete=True" in caplog.text
        if responses == [{}]
        else "listing_complete=False" in caplog.text
    )
    assert "private failure" not in caplog.text


@pytest.mark.parametrize("enabled", [False, True])
def test_source_opt_in_emits_no_genie_workunits(enabled: bool) -> None:
    with (
        patch("datahub.ingestion.source.unity.source.create_workspace_client"),
        patch("datahub.ingestion.source.unity.source.log_genie_spaces") as diagnostic,
    ):
        source = UnityCatalogSource(
            ctx=PipelineContext(run_id="test"),
            config=UnityCatalogSourceConfig(
                workspace_url="https://workspace.example.com",
                log_genie_spaces=enabled,
                include_hive_metastore=False,
                include_tags=False,
                include_usage_statistics=False,
                include_ml_models=False,
            ),
        )
        try:
            with (
                patch.object(source, "process_metastores", return_value=[]),
                patch.object(source, "get_view_lineage", return_value=[]),
            ):
                assert list(source.get_workunits_internal()) == []
            assert diagnostic.call_count == int(enabled)
        finally:
            source.close()
