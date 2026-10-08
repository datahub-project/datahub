import logging
from typing import TYPE_CHECKING

# Conditional import for OpenLineage (may not be installed)
try:
    from openlineage.client.run import Dataset as OpenLineageDataset

    OPENLINEAGE_AVAILABLE = True
except ImportError:
    # Not available when openlineage packages aren't installed
    OpenLineageDataset = None  # type: ignore[assignment,misc]
    OPENLINEAGE_AVAILABLE = False

if TYPE_CHECKING:
    from openlineage.client.run import Dataset as OpenLineageDataset

import datahub.emitter.mce_builder as builder
from datahub_airflow_plugin._constants import OL_FS_SCHEME_TO_PLATFORM

logger = logging.getLogger(__name__)


OL_SCHEME_TWEAKS = {
    "sqlserver": "mssql",
    "awsathena": "athena",
}

# Fire the sanitiser warning at most once per worker process
_warning_logged: bool = False


def _warn_once(message: str, *args: object) -> None:
    global _warning_logged
    if _warning_logged:
        return
    _warning_logged = True
    logger.warning(message, *args)


def _sanitize_ol_dataset_name(name: str) -> str:
    """Drop literal ``"None"`` and empty segments from a dotted OL name.

    Some OpenLineage producers build dataset names by interpolating optional
    fields into an f-string (e.g. ``f"{database}.{schema}.{table}"``) without
    a ``None`` guard, so an unset field becomes the literal string ``"None"``
    and the resulting DataHub URN orphans from the canonical lineage graph.
    A well-known case is Airflow's ``S3ToRedshiftOperator`` when ``schema``
    is unset (ING-2018), but the bug class is generic.

    Returns the input unchanged on every path that doesn't strictly need
    rewriting (no-dot, dotted-but-clean). When every segment is junk we keep
    the original so the orphan stays *findable* rather than becoming an
    empty-name URN. Sanitising paths log a deduplicated WARNING.
    """
    if "." not in name:
        return name

    parts = name.split(".")
    cleaned = [p for p in parts if p and p != "None"]
    if len(cleaned) == len(parts):
        return name

    if not cleaned:
        _warn_once(
            "OpenLineage Dataset name %r had only 'None'/empty segments; kept "
            "original to avoid emitting an empty URN. The resulting DataHub URN "
            "will literally contain %r in its name field — search DataHub for "
            "that substring to find orphans and report the upstream producer.",
            name,
            name,
        )
        return name

    sanitized = ".".join(cleaned)
    _warn_once(
        "OpenLineage Dataset name %r contained 'None'/empty segments; "
        "sanitized to %r before producing DataHub URN. Likely upstream bug "
        "in the producer (unset field interpolated into an f-string).",
        name,
        sanitized,
    )
    return sanitized


def _fs_dataset_name(namespace: str, name: str) -> str:
    """Build a path dataset name the way the Java converter's HdfsPathDataset does.

    OpenLineage splits a path into namespace (`s3://<bucket>`) and name (the
    key). Rejoin them, then keep everything after the scheme: the bucket (or
    container/authority) followed by the key, with no trailing slash.
    """
    if namespace.endswith("/") or name.startswith("/"):
        uri = namespace + name
    else:
        uri = f"{namespace}/{name}"
    path = uri.rstrip("/").split("://", maxsplit=1)[1]
    # `file` keeps its leading slash so absolute paths stay absolute.
    if uri.startswith("file://"):
        return path
    return path[1:] if path.startswith("/") else path


def translate_ol_to_datahub_urn(
    ol_uri: "OpenLineageDataset",
    env: str = builder.DEFAULT_ENV,
    normalize_object_storage: bool = True,
) -> str:
    """Translate OpenLineage dataset URI to DataHub URN.

    Args:
        ol_uri: OpenLineage dataset with namespace and name
        env: DataHub environment (default: PROD for backward compatibility)
        normalize_object_storage: Map filesystem/object-store schemes (see
            OL_FS_SCHEME_TO_PLATFORM) to DataHub platforms and keep the bucket in
            the name, matching the Java converter and DataHub's S3/GCS/ABS
            sources. When false, the scheme is used as the platform and the
            bucket is dropped (the behaviour before this option existed).

    Returns:
        DataHub dataset URN string
    """
    namespace = ol_uri.namespace
    name = _sanitize_ol_dataset_name(ol_uri.name)

    scheme, *rest = namespace.split("://", maxsplit=1)

    # A bare namespace such as `file` has no `://` and keeps the name as is,
    # which is also what the Java converter does.
    if normalize_object_storage and rest and scheme in OL_FS_SCHEME_TO_PLATFORM:
        return builder.make_dataset_urn(
            platform=OL_FS_SCHEME_TO_PLATFORM[scheme],
            name=_fs_dataset_name(namespace, name),
            env=env,
        )

    platform = OL_SCHEME_TWEAKS.get(scheme, scheme)
    return builder.make_dataset_urn(platform=platform, name=name, env=env)
