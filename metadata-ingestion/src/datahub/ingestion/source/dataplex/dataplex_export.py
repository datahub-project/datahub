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

When ``export_config.existing_export_paths`` is set (read-only mode), job
submission is skipped entirely: the connector reads already-completed export
output from the configured ``gs://`` paths, needing only storage read access.

Reference: https://docs.cloud.google.com/dataplex/docs/export-metadata
"""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import google.auth
import google.auth.transport.requests
from google.cloud import dataplex_v1, storage
from google.oauth2 import service_account

from datahub.ingestion.source.dataplex.dataplex_config import (
    DataplexConfig,
    DataplexExportConfig,
)
from datahub.ingestion.source.dataplex.dataplex_helpers import (
    ExportedEntry,
    parse_gcs_path,
)
from datahub.ingestion.source.dataplex.dataplex_mappers import ENTRY_MAPPERS
from datahub.ingestion.source.dataplex.dataplex_report import (
    DataplexReport,
    ExportJobInfo,
)

logger = logging.getLogger(__name__)

DATAPLEX_API_ROOT = "https://dataplex.googleapis.com/v1"
GCP_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]

# System-managed entry types live under the Google-owned ``dataplex-types``
# project at ``locations/global``. The export scope is derived from the mapper
# registry so it automatically stays in sync as new entry types gain support.
DATAPLEX_TYPES_PREFIX = "projects/dataplex-types/locations/global/entryTypes"

# metadataJobs wire vocabulary.
EXPORT_JOB_TYPE = "EXPORT"
JOB_STATUS_KEY = "status"
JOB_STATE_KEY = "state"
JOB_MESSAGE_KEY = "message"
ENTRY_KEY = "entry"
STATE_UNSPECIFIED = "STATE_UNSPECIFIED"
STATE_SUCCEEDED = "SUCCEEDED"
TERMINAL_STATES = (STATE_SUCCEEDED, "FAILED", "CANCELED")

JSONL_SUFFIX = ".jsonl"

# A persistent poll error (revoked token, 403, deleted job) should surface as
# such instead of spinning until the deadline and reading as a timeout.
MAX_CONSECUTIVE_POLL_FAILURES = 5

# Export output object names embed the metadata-job id as a ``job=<id>/`` path
# segment, e.g. ``metadata/job=my-job/entry_group=@bigquery/part-0.jsonl``.
JOB_PARTITION_RE = re.compile(r"(?:^|/)(job=[^/]+)/")

# Recency fallback for partitions whose blobs carry no usable creation time
# (GCS timestamps are always timezone-aware, so compare against an aware epoch).
_EPOCH = datetime.min.replace(tzinfo=timezone.utc)


def export_scope_entry_types() -> List[str]:
    """Entry-type resource names for the export scope, one per supported mapper."""
    return [
        f"{DATAPLEX_TYPES_PREFIX}/{short_name}" for short_name in sorted(ENTRY_MAPPERS)
    ]


@dataclass(frozen=True)
class ExportTarget:
    """One location's export output to read.

    ``job_id`` is set when this run submitted the export job itself, and the
    read is scoped to that job's output. It is None for read-only targets
    built from ``existing_export_paths``, where the freshest ``job=<id>``
    partition under ``output_path`` is read instead.
    """

    location: str
    bucket: str
    job_id: Optional[str]
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


def build_storage_client(
    export_config: DataplexExportConfig,
    credentials: Optional[service_account.Credentials],
) -> storage.Client:
    """Storage client for the export buckets, bound to the job runner project.

    In read-only mode there is no runner project; the client project is then
    inferred from the credentials / environment, which is sufficient for reads.
    """
    return storage.Client(
        project=export_config.export_job_runner_project,
        credentials=credentials,
    )


def existing_export_targets(export_config: DataplexExportConfig) -> List[ExportTarget]:
    """Build read-only targets from ``existing_export_paths`` (no job submission)."""
    targets: List[ExportTarget] = []
    for location, path in sorted(export_config.existing_export_paths.items()):
        gcs = parse_gcs_path(path)  # validated at config time
        targets.append(
            ExportTarget(
                location=location,
                bucket=gcs.bucket,
                job_id=None,
                output_path=gcs.uri,
            )
        )
    logger.info(
        "Read-only export mode: reading pre-existing export output for %d "
        "location(s); no export jobs will be submitted.",
        len(targets),
    )
    return targets


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
        "type": EXPORT_JOB_TYPE,
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
    # No explicit Content-Type header: requests sets application/json for json=.
    resp = session.post(url, json=body)
    if resp.status_code >= 400:
        logger.error(
            "metadataJobs.create failed for job '%s': HTTP %s\n%s",
            job_id,
            resp.status_code,
            resp.text,
        )
        resp.raise_for_status()
    try:
        server_job_name = resp.json().get("name", "")
    except Exception:
        server_job_name = ""
    if server_job_name:
        logger.info(
            "metadataJobs.create accepted job '%s' as %s", job_id, server_job_name
        )


def _get_job(
    session: google.auth.transport.requests.AuthorizedSession,
    job_project: str,
    location: str,
    job_id: str,
) -> Dict[str, Any]:
    url = (
        f"{DATAPLEX_API_ROOT}/projects/{job_project}/locations/{location}"
        f"/metadataJobs/{job_id}"
    )
    resp = session.get(url)
    resp.raise_for_status()
    return resp.json()


def _job_state(job: Dict[str, Any]) -> str:
    status = job.get(JOB_STATUS_KEY) or {}
    return status.get(JOB_STATE_KEY, STATE_UNSPECIFIED)


def _job_error_message(job: Dict[str, Any]) -> str:
    status = job.get(JOB_STATUS_KEY) or {}
    return status.get(JOB_MESSAGE_KEY, "")


@dataclass
class _PendingJob:
    """Tracks a submitted export job while waiting for it to finish."""

    location: str
    job_id: str
    bucket: str
    output_path: str
    info: ExportJobInfo
    started: float = field(default_factory=time.time)
    last_state: Optional[str] = None
    consecutive_poll_failures: int = 0
    last_poll_exc: Optional[Exception] = None


def _submit_all_jobs(
    config: DataplexConfig,
    export_config: DataplexExportConfig,
    runner_project: str,
    project_ids: List[str],
    session: google.auth.transport.requests.AuthorizedSession,
    report: DataplexReport,
) -> List[_PendingJob]:
    """Submit one EXPORT job per entries location (no waiting)."""
    entry_types = export_scope_entry_types()
    pending: List[_PendingJob] = []
    for location in config.entries_locations:
        job_id = f"datahub-export-{location}-{uuid.uuid4().hex[:8]}"
        try:
            # Bucket resolution stays inside the try so a bad location config
            # fails just this location instead of aborting all of them.
            bucket = export_config.bucket_for_location(location)
            output_path = _output_path(bucket, export_config.prefix)
            _submit_export_job(
                session=session,
                job_project=runner_project,
                location=location,
                job_id=job_id,
                output_path=output_path,
                project_ids=project_ids,
                entry_types=entry_types,
            )
        except Exception as exc:
            report.export_jobs_failed += 1
            report.failure(
                title="Dataplex export job submission failed",
                message="Could not submit metadata export job for a location. "
                "Entities in this location will be missing from this run.",
                context=f"location={location}",
                exc=exc,
            )
            continue
        report.export_jobs_submitted += 1
        info = ExportJobInfo(
            location=location,
            job_id=job_id,
            output_path=output_path,
            state="SUBMITTED",
        )
        report.export_jobs.append(info)
        pending.append(
            _PendingJob(
                location=location,
                job_id=job_id,
                bucket=bucket,
                output_path=output_path,
                info=info,
            )
        )
    return pending


def _evaluate_job(
    pj: _PendingJob,
    job: Dict[str, Any],
    report: DataplexReport,
    targets: List[ExportTarget],
) -> bool:
    """Process one poll result; returns True when the job is still pending."""
    state = _job_state(job)
    elapsed = int(time.time() - pj.started)
    pj.info.state = state
    pj.info.elapsed_seconds = elapsed

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
        return True

    if state == STATE_SUCCEEDED:
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
        return False

    error_message = _job_error_message(job)
    report.export_jobs_failed += 1
    report.failure(
        title="Dataplex export job did not succeed",
        message=(
            f"Export job ended in state={state}"
            + (f": {error_message}" if error_message else "")
            + ". Its output will be skipped and this location's entities will "
            "be missing from this run. Inspect the metadata job in the "
            "Dataplex console or with `gcloud dataplex metadata-jobs describe`."
        ),
        context=f"location={pj.location}, job_id={pj.job_id}",
    )
    return False


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
    runner_project = export_config.export_job_runner_project
    assert runner_project is not None  # enforced by config validation (submit mode)

    pending = _submit_all_jobs(
        config=config,
        export_config=export_config,
        runner_project=runner_project,
        project_ids=project_ids,
        session=session,
        report=report,
    )

    if not pending:
        logger.warning("No Dataplex export jobs were submitted successfully.")
        return []

    logger.info(
        "All %d export job(s) submitted; polling until each completes.",
        len(pending),
    )

    targets: List[ExportTarget] = []
    deadline = time.time() + export_config.export_timeout_seconds

    while pending:
        still_pending: List[_PendingJob] = []
        for pj in pending:
            try:
                job = _get_job(session, runner_project, pj.location, pj.job_id)
            except Exception as exc:
                pj.consecutive_poll_failures += 1
                pj.last_poll_exc = exc
                if pj.consecutive_poll_failures >= MAX_CONSECUTIVE_POLL_FAILURES:
                    report.export_jobs_failed += 1
                    pj.info.state = "POLL_FAILED"
                    report.failure(
                        title="Dataplex export job polling kept failing",
                        message=(
                            f"Gave up after {MAX_CONSECUTIVE_POLL_FAILURES} "
                            "consecutive poll errors — likely an auth/permission "
                            "problem rather than a slow job. This location's "
                            "entities will be missing from this run."
                        ),
                        context=f"location={pj.location}, job_id={pj.job_id}",
                        exc=exc,
                    )
                    continue
                logger.warning(
                    "Failed to poll export job '%s' (%d consecutive): %s — "
                    "retrying next cycle.",
                    pj.job_id,
                    pj.consecutive_poll_failures,
                    exc,
                )
                still_pending.append(pj)
                continue

            pj.consecutive_poll_failures = 0
            pj.last_poll_exc = None
            if _evaluate_job(pj, job, report, targets):
                still_pending.append(pj)

        pending = still_pending

        if pending:
            now = time.time()
            if now > deadline:
                for pj in pending:
                    report.export_jobs_failed += 1
                    pj.info.state = "TIMED_OUT"
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
                        # A poll error that never resolved is the likelier root
                        # cause than slowness; surface it with the timeout.
                        exc=pj.last_poll_exc,
                    )
                break
            # Cap the sleep so a poll interval longer than the remaining budget
            # cannot overshoot the configured total timeout.
            time.sleep(min(export_config.export_poll_seconds, max(0.0, deadline - now)))

    logger.info(
        "Export stage complete: submitted=%d, succeeded=%d, failed=%d",
        report.export_jobs_submitted,
        report.export_jobs_succeeded,
        report.export_jobs_failed,
    )
    return targets


def _partition_recency(blobs: List[storage.Blob]) -> datetime:
    """Newest blob creation time in a partition (epoch when unavailable)."""
    times = [b.time_created for b in blobs if isinstance(b.time_created, datetime)]
    return max(times) if times else _EPOCH


def _select_latest_partition(
    jsonl_blobs: List[storage.Blob],
    target: ExportTarget,
    report: DataplexReport,
) -> List[storage.Blob]:
    """Pick the blobs to read from a pre-existing export path (read-only mode).

    Without a job id of our own, the freshest ``job=<id>`` partition under the
    path is read (by newest blob creation time, job segment as tie-breaker).
    Output without any ``job=`` segment means the path points directly at a
    single export's files, so everything is read.
    """
    if not jsonl_blobs:
        report.export_locations_with_no_output += 1
        # An explicitly configured path with no output is a misconfiguration
        # (the export it points at should already exist), so this is a failure
        # and stale-entity removal is suppressed.
        report.failure(
            title="No Dataplex export output at configured path",
            message=(
                "No .jsonl objects found under the configured "
                "existing_export_paths entry. Check that the path points at a "
                "completed metadata export's output."
            ),
            context=f"location={target.location}, path={target.output_path}",
        )
        return []

    partitions: Dict[str, List[storage.Blob]] = {}
    unpartitioned: List[storage.Blob] = []
    for blob in jsonl_blobs:
        match = JOB_PARTITION_RE.search(blob.name)
        if match:
            partitions.setdefault(match.group(1), []).append(blob)
        else:
            unpartitioned.append(blob)

    if not partitions:
        return jsonl_blobs

    if unpartitioned:
        report.warning(
            title="Unpartitioned objects skipped in export path",
            message=(
                ".jsonl objects without a job=<id> path segment were skipped "
                "because job partitions are present under the same path. Point "
                "existing_export_paths at those objects directly if they "
                "should be read."
            ),
            context=f"path={target.output_path}, skipped={len(unpartitioned)}",
        )

    selected = max(
        partitions,
        key=lambda segment: (_partition_recency(partitions[segment]), segment),
    )
    skipped = len(jsonl_blobs) - len(partitions[selected])
    logger.info(
        "Path %s holds %d export job partition(s); reading the most recent "
        "('%s', %d object(s), %d other object(s) skipped).",
        target.output_path,
        len(partitions),
        selected,
        len(partitions[selected]),
        skipped,
    )
    return partitions[selected]


def _list_matching_blobs(
    storage_client: storage.Client,
    target: ExportTarget,
    report: DataplexReport,
) -> List[storage.Blob]:
    """List the ``.jsonl`` blobs to read for one export target."""
    gcs = parse_gcs_path(target.output_path)
    try:
        blobs = list(storage_client.list_blobs(gcs.bucket, prefix=gcs.list_prefix))
    except Exception as exc:
        # Reported as a failure so stale-entity removal is suppressed: this
        # location's entities are missing from the stream, not deleted.
        report.export_blobs_read_failed += 1
        report.failure(
            title="Failed to list Dataplex export bucket",
            message="Could not list export objects. Skipping this location.",
            context=f"bucket={target.bucket}, prefix={gcs.list_prefix}",
            exc=exc,
        )
        return []

    jsonl_blobs = [b for b in blobs if b.name.endswith(JSONL_SUFFIX)]

    if target.job_id is None:
        return _select_latest_partition(jsonl_blobs, target, report)

    # Export output object names embed the metadata-job id, which scopes the
    # read to exactly this run's output even when the bucket holds older runs.
    job_marker = f"job={target.job_id}"
    matching = [b for b in jsonl_blobs if job_marker in b.name]
    logger.info(
        "Found %d object(s) under gs://%s/%s; %d belong to export job '%s'.",
        len(blobs),
        target.bucket,
        gcs.list_prefix or "",
        len(matching),
        target.job_id,
    )
    if not matching:
        report.export_locations_with_no_output += 1
        if blobs:
            # Objects exist under the prefix but none belong to this run's job:
            # almost certainly a bucket/prefix/job-id mismatch rather than a
            # legitimately empty location. Reported as a failure so stale-entity
            # removal is suppressed instead of tombstoning the whole location.
            report.failure(
                title="Dataplex export output missing for this run",
                message=(
                    "A SUCCEEDED export job produced no .jsonl objects carrying "
                    "this run's job id, although the bucket/prefix does contain "
                    "other objects. The configured bucket/prefix likely does not "
                    "match where the job wrote its output."
                ),
                context=(
                    f"bucket={target.bucket}, job_id={target.job_id}, "
                    f"objects_under_prefix={len(blobs)}"
                ),
            )
        else:
            report.warning(
                title="No Dataplex export objects found",
                message=(
                    "A SUCCEEDED export job produced no objects under the "
                    "configured bucket/prefix. The export scope may contain no "
                    "supported entries for this location, or the bucket/prefix "
                    "may be misconfigured."
                ),
                context=f"bucket={target.bucket}, job_id={target.job_id}",
            )
    return matching


def iter_exported_entries(
    storage_client: storage.Client,
    target: ExportTarget,
    report: DataplexReport,
) -> Iterable[ExportedEntry]:
    """Stream ``ExportedEntry`` items from one export target's GCS output.

    Each JSONL line holds ``{"entry": {...}}`` — the JSON form of the
    ``dataplex_v1.Entry`` proto — so it parses back into a real proto via
    ``Entry.from_json`` and flows through the same mappers as the API path.

    Blobs are decoded as UTF-8 explicitly (JSON is UTF-8 per RFC 8259); the
    process-locale default could raise ``UnicodeDecodeError`` mid-iteration on
    the first non-ASCII description. Text IO buffers across network chunks and
    only yields complete lines, so a truncated tail raises ``JSONDecodeError``
    and is skipped rather than silently ingested.
    """
    matching = _list_matching_blobs(storage_client, target, report)
    entries_yielded = 0

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
                    # Valid JSON that is not an ``{"entry": {...}}`` object is
                    # skipped like a malformed line; guarding here also keeps a
                    # stray non-object line (e.g. ``[]``) from raising and
                    # aborting the rest of the blob.
                    entry_dict = obj.get(ENTRY_KEY) if isinstance(obj, dict) else None
                    if not isinstance(entry_dict, dict):
                        report.export_malformed_lines_skipped += 1
                        report.warning(
                            title="Malformed Dataplex export line",
                            message=(
                                "Skipping JSONL line without an object "
                                f"'{ENTRY_KEY}' field."
                            ),
                            context=f"blob={blob.name}",
                        )
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
                    entries_yielded += 1
                    yield ExportedEntry(entry=entry, location=target.location)
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

    if target.job_id is not None:
        for job_info in report.export_jobs:
            if job_info.job_id == target.job_id:
                job_info.entries_read = entries_yielded
                break
