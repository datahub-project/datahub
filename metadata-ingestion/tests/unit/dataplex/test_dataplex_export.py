"""Unit tests for the Dataplex export extraction method."""

import json
from typing import Any, Dict
from unittest.mock import MagicMock, Mock

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.dataplex.dataplex_config import (
    DataplexConfig,
    DataplexExportConfig,
)
from datahub.ingestion.source.dataplex.dataplex_export import (
    ExportTarget,
    _output_path,
    export_scope_entry_types,
    iter_exported_entries,
    run_exports,
)
from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport


def make_export_config(**overrides: Any) -> DataplexConfig:
    """Build a valid export-mode DataplexConfig."""
    config: Dict[str, Any] = {
        "project_ids": ["test-project"],
        "entries_locations": ["us"],
        "extraction_method": "export",
        "export_config": {
            "export_job_runner_project": "runner-project",
            "bucket_base_name": "my-export",
            "export_poll_seconds": 1,
        },
    }
    config.update(overrides)
    return DataplexConfig.model_validate(config)


class TestExportConfig:
    """Export-mode configuration validation."""

    def test_default_extraction_method_is_api(self):
        config = DataplexConfig(project_ids=["test-project"])
        assert config.extraction_method == "api"
        assert config.export_config is None

    def test_export_mode_requires_export_config(self):
        with pytest.raises(ValidationError, match="export_config must be set"):
            DataplexConfig.model_validate(
                {"project_ids": ["test-project"], "extraction_method": "export"}
            )

    def test_export_mode_requires_bucket_for_every_location(self):
        with pytest.raises(ValidationError, match="no export bucket configured"):
            DataplexConfig.model_validate(
                {
                    "project_ids": ["test-project"],
                    "extraction_method": "export",
                    "entries_locations": ["us", "eu"],
                    "export_config": {
                        "export_job_runner_project": "runner-project",
                        "export_bucket_config": {"us": "bucket-us"},
                    },
                }
            )

    def test_bucket_for_location_precedence(self):
        export_config = DataplexExportConfig(
            export_job_runner_project="runner-project",
            export_bucket_config={"us": "explicit-bucket"},
            bucket_base_name="base",
        )
        assert export_config.bucket_for_location("us") == "explicit-bucket"
        assert export_config.bucket_for_location("eu") == "base-eu"

    def test_bucket_for_location_missing_raises(self):
        export_config = DataplexExportConfig(
            export_job_runner_project="runner-project",
            export_bucket_config={"us": "bucket-us"},
        )
        with pytest.raises(ValueError, match="No bucket configured"):
            export_config.bucket_for_location("eu")

    def test_valid_export_config(self):
        config = make_export_config()
        assert config.extraction_method == "export"
        assert config.export_config is not None
        assert config.export_config.bucket_for_location("us") == "my-export-us"


class TestExportScope:
    """Export scope derivation from the mapper registry."""

    def test_scope_matches_supported_mappers(self):
        entry_types = export_scope_entry_types()
        assert (
            "projects/dataplex-types/locations/global/entryTypes/bigquery-table"
            in entry_types
        )
        assert (
            "projects/dataplex-types/locations/global/entryTypes/bigquery-dataset"
            in entry_types
        )
        # Every scope item is a dataplex-types system entry type.
        assert all(
            et.startswith("projects/dataplex-types/locations/global/entryTypes/")
            for et in entry_types
        )

    def test_output_path(self):
        assert _output_path("my-bucket", None) == "gs://my-bucket/"
        assert _output_path("my-bucket", "exports") == "gs://my-bucket/exports/"
        assert _output_path("my-bucket", "/exports/") == "gs://my-bucket/exports/"


def make_session(job_states):
    """Mock AuthorizedSession: POST accepts jobs, GET walks through job_states."""
    session = MagicMock()
    post_resp = Mock(status_code=200)
    post_resp.json.return_value = {"name": "projects/p/locations/us/metadataJobs/j"}
    session.post.return_value = post_resp

    states = iter(job_states)

    def get_side_effect(url):
        resp = Mock(status_code=200)
        resp.json.return_value = {"status": {"state": next(states)}}
        return resp

    session.get.side_effect = get_side_effect
    return session


class TestRunExports:
    """Export job submission and polling."""

    def test_successful_job_returns_target(self):
        config = make_export_config()
        report = DataplexReport()
        session = make_session(["RUNNING", "SUCCEEDED"])

        targets = run_exports(
            config=config,
            project_ids=["test-project"],
            session=session,
            report=report,
        )

        assert len(targets) == 1
        assert targets[0].location == "us"
        assert targets[0].bucket == "my-export-us"
        assert targets[0].output_path == "gs://my-export-us/"
        assert report.export_jobs_submitted == 1
        assert report.export_jobs_succeeded == 1
        assert report.export_jobs_failed == 0
        assert not report.is_export_partial()

        # Submitted body is scoped to configured projects and supported types.
        body = session.post.call_args.kwargs["json"]
        assert body["type"] == "EXPORT"
        assert body["export_spec"]["scope"]["projects"] == ["projects/test-project"]
        assert body["export_spec"]["output_path"] == "gs://my-export-us/"
        assert body["export_spec"]["scope"]["entry_types"] == (
            export_scope_entry_types()
        )

    def test_failed_job_reports_failure(self):
        config = make_export_config()
        report = DataplexReport()
        session = make_session(["FAILED"])

        targets = run_exports(
            config=config,
            project_ids=["test-project"],
            session=session,
            report=report,
        )

        assert targets == []
        assert report.export_jobs_failed == 1
        assert report.is_export_partial()
        assert len(report.failures) == 1

    def test_submission_error_reports_failure(self):
        config = make_export_config()
        report = DataplexReport()
        session = MagicMock()
        session.post.side_effect = Exception("boom")

        targets = run_exports(
            config=config,
            project_ids=["test-project"],
            session=session,
            report=report,
        )

        assert targets == []
        assert report.export_jobs_submitted == 0
        assert report.export_jobs_failed == 1
        assert report.is_export_partial()


def make_blob(name: str, lines: list) -> Mock:
    """Mock a GCS blob whose open() streams the given JSONL lines."""
    blob = Mock()
    blob.name = name
    text = "\n".join(lines) + "\n"
    handle = MagicMock()
    handle.__enter__.return_value = iter(text.splitlines(keepends=True))
    blob.open.return_value = handle
    return blob


def make_entry_line(fqn: str, entry_id: str) -> str:
    return json.dumps(
        {
            "entry": {
                "name": (
                    "projects/test-project/locations/us/entryGroups/@bigquery"
                    f"/entries/{entry_id}"
                ),
                "entryType": (
                    "projects/655216118709/locations/global/entryTypes/bigquery-table"
                ),
                "fullyQualifiedName": fqn,
                "entrySource": {"displayName": entry_id},
            }
        }
    )


class TestIterExportedEntries:
    """GCS JSONL streaming and proto parsing."""

    def make_target(self) -> ExportTarget:
        return ExportTarget(
            location="us",
            bucket="my-export-us",
            job_id="datahub-export-us-abc12345",
            output_path="gs://my-export-us/",
        )

    def test_parses_entries_and_filters_blobs_by_job_marker(self):
        config = make_export_config()
        report = DataplexReport()
        target = self.make_target()

        matching_blob = make_blob(
            "metadata/job=datahub-export-us-abc12345/entry_group=@bigquery/part-0.jsonl",
            [
                make_entry_line("bigquery:test-project.ds.t1", "t1"),
                "",  # blank lines are skipped
                make_entry_line("bigquery:test-project.ds.t2", "t2"),
            ],
        )
        other_job_blob = make_blob(
            "metadata/job=datahub-export-us-zzz99999/entry_group=@bigquery/part-0.jsonl",
            [make_entry_line("bigquery:test-project.ds.stale", "stale")],
        )
        non_jsonl_blob = make_blob(
            "metadata/job=datahub-export-us-abc12345/manifest.json", []
        )

        storage_client = Mock()
        storage_client.list_blobs.return_value = [
            matching_blob,
            other_job_blob,
            non_jsonl_blob,
        ]

        results = list(
            iter_exported_entries(
                storage_client=storage_client,
                target=target,
                config=config,
                report=report,
            )
        )

        assert len(results) == 2
        entry, location = results[0]
        assert location == "us"
        assert entry.fully_qualified_name == "bigquery:test-project.ds.t1"
        assert entry.entry_source.display_name == "t1"
        assert (
            entry.entry_type
            == "projects/655216118709/locations/global/entryTypes/bigquery-table"
        )
        assert report.export_blobs_read == 1
        assert report.export_entries_read == 2

    def test_malformed_line_is_skipped_and_counted(self):
        config = make_export_config()
        report = DataplexReport()
        target = self.make_target()

        blob = make_blob(
            "job=datahub-export-us-abc12345/part-0.jsonl",
            [
                "{not-valid-json",
                make_entry_line("bigquery:test-project.ds.t1", "t1"),
            ],
        )
        storage_client = Mock()
        storage_client.list_blobs.return_value = [blob]

        results = list(
            iter_exported_entries(
                storage_client=storage_client,
                target=target,
                config=config,
                report=report,
            )
        )

        assert len(results) == 1
        assert report.export_malformed_lines_skipped == 1
        assert not report.is_export_partial()

    def test_bucket_listing_failure_reports_failure(self):
        config = make_export_config()
        report = DataplexReport()
        target = self.make_target()

        storage_client = Mock()
        storage_client.list_blobs.side_effect = Exception("permission denied")

        results = list(
            iter_exported_entries(
                storage_client=storage_client,
                target=target,
                config=config,
                report=report,
            )
        )

        assert results == []
        assert report.export_blobs_read_failed == 1
        assert report.is_export_partial()
        assert len(report.failures) == 1

    def test_no_matching_blobs_warns(self):
        config = make_export_config()
        report = DataplexReport()
        target = self.make_target()

        storage_client = Mock()
        storage_client.list_blobs.return_value = []

        results = list(
            iter_exported_entries(
                storage_client=storage_client,
                target=target,
                config=config,
                report=report,
            )
        )

        assert results == []
        assert report.export_locations_with_no_output == 1
        assert len(report.warnings) == 1
