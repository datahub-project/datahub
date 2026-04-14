"""CLI commands for managing ingestion recordings.

This module provides helper commands for working with recording archives:
- info: Display information about a recording archive
- extract: Extract a recording archive to a directory
- list: List contents of a recording archive
"""

import json
import logging
import sys
from pathlib import Path
from typing import Optional

import click

from datahub.telemetry import telemetry
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)


@click.group()
def recording() -> None:
    """Manage ingestion recording archives."""
    pass


@recording.command()
@click.argument("archive_path", type=str)
@click.option(
    "--password",
    type=str,
    required=False,
    help="Password for the archive. Can also be set via DATAHUB_RECORDING_PASSWORD env var.",
)
@click.option(
    "--json",
    "output_json",
    is_flag=True,
    default=False,
    help="Output as JSON instead of formatted text.",
)
@telemetry.with_telemetry()
@upgrade.check_upgrade
def info(archive_path: str, password: Optional[str], output_json: bool) -> None:
    """Display information about a recording archive.

    archive_path can be a local file or S3 URL.
    """
    from datahub.ingestion.recording.archive import get_archive_info
    from datahub.ingestion.recording.config import (
        check_recording_dependencies,
        get_recording_password_from_env,
    )

    try:
        check_recording_dependencies()
    except ImportError as e:
        click.secho(str(e), fg="red", err=True)
        sys.exit(1)

    password = password or get_recording_password_from_env()
    if not password:
        click.secho(
            "Error: Password required. Provide via --password or "
            "DATAHUB_RECORDING_PASSWORD env var.",
            fg="red",
            err=True,
        )
        sys.exit(1)

    # Handle S3 URL if needed
    local_path = _ensure_local_archive(archive_path)

    try:
        archive_info = get_archive_info(local_path, password)

        if output_json:
            click.echo(json.dumps(archive_info, indent=2))
        else:
            click.echo(f"Recording Archive: {archive_path}")
            click.echo("-" * 50)
            click.echo(f"Run ID:          {archive_info.get('run_id', 'N/A')}")
            click.echo(f"Source Type:     {archive_info.get('source_type') or 'N/A'}")
            click.echo(f"Sink Type:       {archive_info.get('sink_type') or 'N/A'}")
            click.echo(
                f"DataHub Version: {archive_info.get('datahub_cli_version') or 'N/A'}"
            )
            click.echo(f"Created At:      {archive_info.get('created_at', 'N/A')}")
            click.echo(
                f"Recording Start: {archive_info.get('recording_start_time') or 'N/A'}"
            )
            click.echo(f"Format Version:  {archive_info.get('format_version', 'N/A')}")
            click.echo(f"File Count:      {archive_info.get('file_count', 0)}")

            # Show recording content status
            click.echo()
            click.echo("Recording Content:")
            has_http = archive_info.get("has_http_cassette", False)
            has_db = archive_info.get("has_db_queries", False)
            if has_http:
                click.secho("  ✓ HTTP traffic recorded", fg="green")
            else:
                click.secho(
                    "  ✗ No HTTP traffic (connection may have failed before requests)",
                    fg="yellow",
                )
            if has_db:
                click.secho("  ✓ Database queries recorded", fg="green")
            else:
                click.echo("  - No database queries (source may not use SQL)")

            # Show exception info if recording captured a failure
            if archive_info.get("has_exception"):
                click.echo()
                click.secho("⚠️  Recording includes an exception:", fg="yellow")
                click.secho(
                    f"   Type:    {archive_info.get('exception_type', 'Unknown')}",
                    fg="yellow",
                )
                click.secho(
                    f"   Message: {archive_info.get('exception_message', 'N/A')}",
                    fg="yellow",
                )
                traceback_str = archive_info.get("exception_traceback")
                if traceback_str:
                    click.echo()
                    click.secho("   Traceback (truncated):", fg="yellow")
                    for line in traceback_str.split("\n")[:10]:
                        click.echo(f"   {line}")

            click.echo()
            click.echo("Files:")
            for file_path in sorted(archive_info.get("files", [])):
                click.echo(f"  - {file_path}")

    except Exception as e:
        click.secho(f"Error reading archive: {e}", fg="red", err=True)
        sys.exit(1)


@recording.command()
@click.argument("archive_path", type=str)
@click.option(
    "--password",
    type=str,
    required=False,
    help="Password for the archive. Can also be set via DATAHUB_RECORDING_PASSWORD env var.",
)
@click.option(
    "-o",
    "--output-dir",
    type=click.Path(file_okay=False),
    required=True,
    help="Directory to extract the archive to.",
)
@click.option(
    "--verify/--no-verify",
    default=True,
    help="Verify checksums after extraction.",
)
@telemetry.with_telemetry()
@upgrade.check_upgrade
def extract(
    archive_path: str,
    password: Optional[str],
    output_dir: str,
    verify: bool,
) -> None:
    """Extract a recording archive to a directory.

    archive_path can be a local file or S3 URL.
    """
    from datahub.ingestion.recording.archive import RecordingArchive
    from datahub.ingestion.recording.config import (
        check_recording_dependencies,
        get_recording_password_from_env,
    )

    try:
        check_recording_dependencies()
    except ImportError as e:
        click.secho(str(e), fg="red", err=True)
        sys.exit(1)

    password = password or get_recording_password_from_env()
    if not password:
        click.secho(
            "Error: Password required. Provide via --password or "
            "DATAHUB_RECORDING_PASSWORD env var.",
            fg="red",
            err=True,
        )
        sys.exit(1)

    # Handle S3 URL if needed
    local_path = _ensure_local_archive(archive_path)
    output_path = Path(output_dir)

    try:
        archive = RecordingArchive(password)
        extracted_dir = archive.extract(local_path, output_path)

        if verify:
            manifest = archive.read_manifest(local_path)
            if archive.verify_checksums(extracted_dir, manifest):
                click.echo("✅ Checksum verification passed")
            else:
                click.secho("⚠️ Checksum verification failed", fg="yellow", err=True)

        click.echo(f"Extracted to: {extracted_dir}")

    except Exception as e:
        click.secho(f"Error extracting archive: {e}", fg="red", err=True)
        sys.exit(1)


@recording.command(name="list")
@click.argument("archive_path", type=str)
@click.option(
    "--password",
    type=str,
    required=False,
    help="Password for the archive. Can also be set via DATAHUB_RECORDING_PASSWORD env var.",
)
@telemetry.with_telemetry()
@upgrade.check_upgrade
def list_contents(archive_path: str, password: Optional[str]) -> None:
    """List contents of a recording archive.

    archive_path can be a local file or S3 URL.
    """
    from datahub.ingestion.recording.archive import list_archive_contents
    from datahub.ingestion.recording.config import (
        check_recording_dependencies,
        get_recording_password_from_env,
    )

    try:
        check_recording_dependencies()
    except ImportError as e:
        click.secho(str(e), fg="red", err=True)
        sys.exit(1)

    password = password or get_recording_password_from_env()
    if not password:
        click.secho(
            "Error: Password required. Provide via --password or "
            "DATAHUB_RECORDING_PASSWORD env var.",
            fg="red",
            err=True,
        )
        sys.exit(1)

    # Handle S3 URL if needed
    local_path = _ensure_local_archive(archive_path)

    try:
        contents = list_archive_contents(local_path, password)
        for file_path in sorted(contents):
            click.echo(file_path)

    except Exception as e:
        click.secho(f"Error listing archive: {e}", fg="red", err=True)
        sys.exit(1)


def _ensure_local_archive(archive_path: str) -> Path:
    """Ensure archive is available locally, downloading from S3 if needed.

    Note: For S3 downloads, creates a temp file that persists until manually deleted
    or system cleanup. The caller is responsible for cleanup after use.
    """
    if archive_path.startswith("s3://"):
        import tempfile
        from urllib.parse import urlparse

        import boto3

        logger.info(f"Downloading from S3: {archive_path}")

        parsed = urlparse(archive_path)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")

        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmpfile:
            local_path = Path(tmpfile.name)

        try:
            s3_client = boto3.client("s3")
            s3_client.download_file(bucket, key, str(local_path))
        except Exception as e:
            # Clean up temp file on download failure
            if local_path.exists():
                local_path.unlink()
            raise click.ClickException(f"Failed to download from S3: {e}") from e

        logger.info(f"Downloaded to: {local_path}")
        logger.debug(f"Note: Temp file at {local_path} should be cleaned up after use")
        return local_path

    return Path(archive_path)
