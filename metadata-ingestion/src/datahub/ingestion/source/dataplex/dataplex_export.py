"""Dataplex metadata EXPORT job submission and Cloud Storage reader.

Implements the ``extraction_method: export`` fetch path. One Dataplex
``metadataJobs.create`` EXPORT job is submitted per configured entries
location (all jobs submitted before any polling begins), scoped to the
configured projects and the entry types supported by ``ENTRY_MAPPERS``,
writing newline-delimited JSON to a per-location bucket resolved from
``export_config``. After the jobs finish, the exported objects are streamed
from GCS line-by-line and parsed back into ``dataplex_v1.Entry`` protos, which
feed the exact same mapping pipeline as the API-based fetch path.

This path exists because ``list_entries`` only returns entries physically
created in a project's entry groups. Central-catalog / federated architectures
surface tenant assets in a catalog project via Dataplex catalog linking; those
linked entries are invisible to ``list_entries`` but ARE included in a
metadata export scoped to the catalog project.

Reference: https://docs.cloud.google.com/dataplex/docs/export-metadata
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Tuple

import google.auth
import google.auth.transport.requests
from google.cloud import dataplex_v1, storage
from google.oauth2 import service_account

from datahub.ingestion.source.dataplex.dataplex_config import (
    DataplexConfig,
    DataplexExportConfig,
)
from datahub.ingestion.source.dataplex.dataplex_mappers import ENTRY_MAPPERS
from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport

logger = logging.getLogger(__name__)

DATAPLEX_API_ROOT = "https://dataplex.googleapis.com/v1"
GCP_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]

# System-managed entry types live under the Google-owned ``dataplex-types``
# project at ``locations/global``. The export scope is derived from the mapper
# registry so it automatically stays in sync as new entry types gain support.
DATAPLEX_TYPES_PREFIX = "projects/dataplex-types/locations/global/entryTypes"

TERMINAL_STATES = ("SUCCEEDED", "FAILED", "CANCELED")


def export_scope_entry_types() -> List[str]:
    """Entry-type resource names for the export scope, one per supported mapper."""
    return [
        f"{DATAPLEX_TYPES_PREFIX}/{short_name}" for short_name in sorted(ENTRY_MAPPERS)
    ]


@dataclass(frozen=True)
class ExportTarget:
    """A successfully finished export job for a single location."""

    location: str
    bucket: str
    job_id: str
    output_path: str


def build_authed_session(
    credentials: Optional[service_account.Credentials],
) -> google.auth.transport.requests.AuthorizedSession:
    """Build an AuthorizedSession for the Dataplex REST API.

    Service-account credentials created without scopes cannot mint tokens, so
    scopes are applied here when required.
    """
    if credentials is None:
        adc_credentials, _ = google.auth.default(scopes=GCP_SCOPES)
        return google.auth.transport.requests.AuthorizedSession(adc_credentials)
    if credentials.requires_scopes:
        credentials = credentials.with_scopes(GCP_SCOPES)
    return google.auth.transport.requests.AuthorizedSession(credentials)


def _output_path(bucket: str, prefix: Optional[str]) -> str:
    """Return the ``gs://BUCKET[/PREFIX]/`` value for ``export_spec.output_path``."""
    cleaned_prefix = (prefix or "").strip().strip("/")
    if not cleaned_prefix:
        return f"gs://{bucket}/"
    return f"gs://{bucket}/{cleaned_prefix}/"


def _submit_export_job(
    session: google.auth.transport.requests.AuthorizedSession,
    job_project: str,
    location: str,
    job_id: str,
    output_path: str,
    project_ids: List[str],
    entry_types: List[str],
) -> None:
    """POST ``metadataJobs.create`` (EXPORT) scoped to the given entry types."""
    url = (
        f"{DATAPLEX_API_ROOT}/projects/{job_project}/locations/{location}"
        f"/metadataJobs?metadataJobId={job_id}"
    )
    scoped_projects = [
        p if p.startswith("projects/") else f"projects/{p}" for p in project_ids
    ]
    body = {
        "type": "EXPORT",
        "export_spec": {
            "output_path": output_path,
            "scope": {
                "projects": scoped_projects,
                "entry_types": entry_types,
            },
        },
    }
    logger.info(
        "Submitting Dataplex EXPORT job '%s' (runner_project=%s, location=%s, "
        "projects=%s) -> %s",
        job_id,
        job_project,
        location,
        scoped_projects,
        output_path,
    )
    resp = session.post(
        url,
        json=body,
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    if resp.status_code >= 400:
        logger.error(
            "metadataJobs.create failed for job '%s': HTTP %s\n%s",
            job_id,
            resp.status_code,
            resp.text,
        )
        resp.raise_for_status()


def _get_job(
    session: google.auth.transport.requests.AuthorizedSession,
    job_project: str,
    location: str,
    job_id: str,
) -> dict:
    url = (
        f"{DATAPLEX_API_ROOT}/projects/{job_project}/locations/{location}"
        f"/metadataJobs/{job_id}"
    )
    resp = session.get(url)
    resp.raise_for_status()
    return resp.json()


@dataclass
class _PendingJob:
    """Tracks a submitted export job while waiting for it to finish."""

    location: str
    job_id: str
    bucket: str
    output_path: str
    started: float = field(default_factory=time.time)
    last_state: Optional[str] = None


def run_exports(
    config: DataplexConfig,
    project_ids: List[str],
    session: google.auth.transport.requests.AuthorizedSession,
    report: DataplexReport,
) -> List[ExportTarget]:
    """Submit one EXPORT job per entries location, then poll them jointly.

    Phase 1 – submit: fire off one ``metadataJobs.create`` per location without
    waiting; all jobs are queued on GCP simultaneously.
    Phase 2 – poll: loop over every still-pending job until it reaches a
    terminal state or the configured timeout elapses. Total wall-clock time is
    roughly max(individual job durations) instead of their sum.

    A failed / timed-out job is reported as a source failure (not a warning):
    the location's entities are missing from this run's stream, and the
    stale-entity removal handler must not soft-delete them.
    """
    export_config = config.export_config
    assert export_config is not None  # enforced by config validation
    entry_types = export_scope_entry_types()

    # Phase 1: submit all jobs.
    pending: List[_PendingJob] = []
    for location in config.entries_locations:
        bucket = export_config.bucket_for_location(location)
        output_path = _output_path(bucket, export_config.prefix)
        job_id = f"datahub-export-{location}-{uuid.uuid4().hex[:8]}"
        try:
            _submit_export_job(
                session=session,
                job_project=export_config.export_job_runner_project,
                location=location,
                job_id=job_id,
                output_path=output_path,
                project_ids=project_ids,
                entry_types=entry_types,
            )
            report.export_jobs_submitted += 1
            pending.append(
                _PendingJob(
                    location=location,
                    job_id=job_id,
                    bucket=bucket,
                    output_path=output_path,
                )
            )
        except Exception as exc:
            report.export_jobs_failed += 1
            report.failure(
                title="Dataplex export job submission failed",
                message="Could not submit metadata export job for a location. "
                "Entities in this location will be missing from this run.",
                context=f"location={location}, bucket={bucket}",
                exc=exc,
            )

    if not pending:
        logger.warning("No Dataplex export jobs were submitted successfully.")
        return []

    logger.info(
        "All %d export job(s) submitted; polling until each completes.",
        len(pending),
    )

    # Phase 2: poll all pending jobs jointly.
    targets: List[ExportTarget] = []
    deadline = time.time() + export_config.export_timeout_seconds

    while pending:
        still_pending: List[_PendingJob] = []
        for pj in pending:
            try:
                job = _get_job(
                    session,
                    export_config.export_job_runner_project,
                    pj.location,
                    pj.job_id,
                )
            except Exception as exc:
                logger.warning(
                    "Failed to poll export job '%s': %s — retrying next cycle.",
                    pj.job_id,
                    exc,
                )
                still_pending.append(pj)
                continue

            status = job.get("status") or {}
            state = status.get("state", "STATE_UNSPECIFIED")
            elapsed = int(time.time() - pj.started)

            if state != pj.last_state:
                logger.info(
                    "Export job '%s' [%s] state: %s -> %s (after %ds)",
                    pj.job_id,
                    pj.location,
                    pj.last_state or "<start>",
                    state,
                    elapsed,
                )
                pj.last_state = state

            if state not in TERMINAL_STATES:
                still_pending.append(pj)
                continue

            if state == "SUCCEEDED":
                report.export_jobs_succeeded += 1
                logger.info(
                    "Export job '%s' [%s] SUCCEEDED after %ds.",
                    pj.job_id,
                    pj.location,
                    elapsed,
                )
                targets.append(
                    ExportTarget(
                        location=pj.location,
                        bucket=pj.bucket,
                        job_id=pj.job_id,
                        output_path=pj.output_path,
                    )
                )
            else:
                report.export_jobs_failed += 1
                report.failure(
                    title="Dataplex export job did not succeed",
                    message=(
                        f"Export job ended in state={state}; its output will be "
                        "skipped and this location's entities will be missing "
                        "from this run. Inspect the metadata job in the Dataplex "
                        "console or with `gcloud dataplex metadata-jobs describe`."
                    ),
                    context=f"location={pj.location}, job_id={pj.job_id}",
                )

        pending = still_pending

        if pending:
            if time.time() > deadline:
                for pj in pending:
                    report.export_jobs_failed += 1
                    report.failure(
                        title="Dataplex export job timed out",
                        message=(
                            f"Gave up waiting after "
                            f"{export_config.export_timeout_seconds}s. Increase "
                            "export_config.export_timeout_seconds if the export "
                            "legitimately takes longer."
                        ),
                        context=(
                            f"location={pj.location}, job_id={pj.job_id}, "
                            f"last_state={pj.last_state}"
                        ),
                    )
                break
            time.sleep(export_config.export_poll_seconds)

    logger.info(
        "Export stage complete: submitted=%d, succeeded=%d, failed=%d",
        report.export_jobs_submitted,
        report.export_jobs_succeeded,
        report.export_jobs_failed,
    )
    return targets


def _list_matching_blobs(
    storage_client: storage.Client,
    target: ExportTarget,
    export_config: DataplexExportConfig,
    report: DataplexReport,
) -> List[storage.Blob]:
    """List this export job's ``.jsonl`` blobs under the configured prefix."""
    prefix = (export_config.prefix or "").strip().strip("/")
    list_prefix = f"{prefix}/" if prefix else None
    try:
        blobs = list(storage_client.list_blobs(target.bucket, prefix=list_prefix))
    except Exception as exc:
        # Reported as a failure so stale-entity removal is suppressed: this
        # location's entities are missing from the stream, not deleted.
        report.export_blobs_read_failed += 1
        report.failure(
            title="Failed to list Dataplex export bucket",
            message="Could not list export objects. Skipping this location.",
            context=f"bucket={target.bucket}, prefix={list_prefix}",
            exc=exc,
        )
        return []

    # Export output object names embed the metadata-job id, which scopes the
    # read to exactly this run's output even when the bucket holds older runs.
    job_marker = f"job={target.job_id}"
    matching = [b for b in blobs if b.name.endswith(".jsonl") and job_marker in b.name]
    logger.info(
        "Found %d object(s) under gs://%s/%s; %d belong to export job '%s'.",
        len(blobs),
        target.bucket,
        list_prefix or "",
        len(matching),
        target.job_id,
    )
    if not matching:
        report.export_locations_with_no_output += 1
        report.warning(
            title="No Dataplex export objects found",
            message=(
                "A SUCCEEDED export job produced no matching .jsonl objects "
                "under the configured bucket/prefix. The export scope may "
                "contain no supported entries for this location, or the "
                "bucket/prefix may be misconfigured."
            ),
            context=f"bucket={target.bucket}, job_id={target.job_id}",
        )
    return matching


def iter_exported_entries(
    storage_client: storage.Client,
    target: ExportTarget,
    config: DataplexConfig,
    report: DataplexReport,
) -> Iterable[Tuple[dataplex_v1.Entry, str]]:
    """Stream ``(entry, location)`` pairs from one export target's GCS output.

    Each JSONL line holds ``{"entry": {...}}`` — the JSON form of the
    ``dataplex_v1.Entry`` proto — so it parses back into a real proto via
    ``Entry.from_json`` and flows through the same mappers as the API path.

    Blobs are decoded as UTF-8 explicitly (JSON is UTF-8 per RFC 8259); the
    process-locale default could raise ``UnicodeDecodeError`` mid-iteration on
    the first non-ASCII description. Text IO buffers across network chunks and
    only yields complete lines, so a truncated tail raises ``JSONDecodeError``
    and is skipped rather than silently ingested.
    """
    export_config = config.export_config
    assert export_config is not None  # enforced by config validation
    matching = _list_matching_blobs(storage_client, target, export_config, report)

    for blob in matching:
        report.export_blobs_read += 1
        logger.info("Reading gs://%s/%s", target.bucket, blob.name)
        try:
            with blob.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError as exc:
                        report.export_malformed_lines_skipped += 1
                        report.warning(
                            title="Malformed Dataplex export line",
                            message="Skipping unparseable JSONL line.",
                            context=f"blob={blob.name}",
                            exc=exc,
                        )
                        continue
                    entry_dict = obj.get("entry")
                    if not isinstance(entry_dict, dict):
                        continue
                    try:
                        entry = dataplex_v1.Entry.from_json(
                            json.dumps(entry_dict), ignore_unknown_fields=True
                        )
                    except Exception as exc:
                        report.export_malformed_lines_skipped += 1
                        report.warning(
                            title="Unparseable Dataplex export entry",
                            message="Skipping entry that failed proto parsing.",
                            context=f"blob={blob.name}",
                            exc=exc,
                        )
                        continue
                    report.export_entries_read += 1
                    yield entry, target.location
        except Exception as exc:
            # The remainder of this blob is lost — reported as a failure so
            # stale-entity removal is suppressed (a half-read blob is an
            # incomplete entity set, not a set of deletions).
            report.export_blobs_read_failed += 1
            report.failure(
                title="Failed to read Dataplex export object",
                message="Aborted reading a single export object. Continuing.",
                context=f"blob={blob.name}",
                exc=exc,
            )
