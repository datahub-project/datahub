import logging
import pathlib
import tempfile
from contextlib import ExitStack
from urllib.parse import urlparse

from datahub.ingestion.source.aws.s3_util import is_s3_uri
from datahub.ingestion.source.sqlmesh.sqlmesh_config import (
    SqlmeshSourceConfig,
    SqlmeshSourceReport,
)

# boto3 (via aws_common) only ships with the [sqlmesh]/aws extras. Guard the
# import so this module stays loadable with base deps; the S3 branch below only
# runs when project_path is an s3:// URI, which requires the extra to be present.
try:
    from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
except ImportError:
    pass

logger = logging.getLogger(__name__)


def resolve_project_location(
    config: SqlmeshSourceConfig,
    report: SqlmeshSourceReport,
    stack: ExitStack,
) -> str:
    """Resolve ``config.project_path`` to a local directory the SQLMesh Context can load.

    SQLMesh loads a whole project *tree* (config + models/ + audits/ + macros/),
    not a single file, so a remote location is materialised into a temp directory
    that lives for the entire ingestion via ``stack`` — the Context reads project
    files well after ``__init__`` (e.g. during rendering), so the checkout can't be
    torn down early.

    - ``git_info`` set: shallow-clone the repo (SSH deploy key) and treat
      ``project_path`` as a path relative to the checkout (``.`` == repo root).
    - ``project_path`` is an ``s3://`` prefix: download every object under it into
      a temp dir, preserving the key layout, and return that dir.
    - otherwise: return ``project_path`` unchanged (local filesystem).
    """
    project_path = config.project_path

    if config.git_info is not None:
        tmp_dir = stack.enter_context(
            tempfile.TemporaryDirectory(suffix="_sqlmesh_git")
        )
        checkout = config.git_info.clone(tmp_path=tmp_dir)
        report.git_checkout = str(checkout)
        resolved = _safe_join(pathlib.Path(checkout), project_path)
        logger.info("Loading SQLMesh project from git checkout: %s", resolved)
        return str(resolved)

    if is_s3_uri(project_path):
        if config.aws_connection is None:
            # The config validator normally blocks this; guard defensively so a
            # programmatic caller can't slip past it into a confusing boto error.
            raise ValueError(
                "aws_connection is required to load a SQLMesh project from an "
                "s3:// project_path."
            )
        tmp_dir = stack.enter_context(tempfile.TemporaryDirectory(suffix="_sqlmesh_s3"))
        count = _download_s3_tree(project_path, config.aws_connection, tmp_dir)
        report.num_project_files_downloaded = count
        logger.info(
            "Downloaded %d object(s) from %s into local project dir %s",
            count,
            project_path,
            tmp_dir,
        )
        return tmp_dir

    return project_path


def _safe_join(root: pathlib.Path, relative: str) -> pathlib.Path:
    """Join a relative path onto ``root``, rejecting anything that escapes it.

    Guards both the git-subdir case (``project_path: ../../secrets``) and
    malicious S3 keys (``../../etc/passwd``) from writing/reading outside the
    materialised project directory.
    """
    root = root.resolve()
    candidate = (root / relative).resolve()
    if candidate != root and root not in candidate.parents:
        raise ValueError(f"path {relative!r} escapes the project root {str(root)!r}.")
    return candidate


def _download_s3_tree(
    uri: str, aws_connection: "AwsConnectionConfig", dest_dir: str
) -> int:
    parsed = urlparse(uri)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    # Treat the prefix as a directory so "proj" and "proj/" behave the same and
    # relative keys are computed against a trailing slash.
    normalized_prefix = prefix.rstrip("/") + "/" if prefix else ""

    s3_client = aws_connection.get_s3_client()
    paginator = s3_client.get_paginator("list_objects_v2")
    dest_root = pathlib.Path(dest_dir)

    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=normalized_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Directory placeholder objects (created by some UIs) have no content.
            if key.endswith("/"):
                continue
            relative_key = key[len(normalized_prefix) :]
            if not relative_key:
                continue
            destination = _safe_join(dest_root, relative_key)
            destination.parent.mkdir(parents=True, exist_ok=True)
            # download_file streams to disk rather than buffering the object in
            # memory — a SQLMesh project can carry large seed CSVs.
            s3_client.download_file(bucket, key, str(destination))
            count += 1

    if count == 0:
        raise ValueError(
            f"No objects found under {uri}; check the bucket/prefix and credentials."
        )
    return count
