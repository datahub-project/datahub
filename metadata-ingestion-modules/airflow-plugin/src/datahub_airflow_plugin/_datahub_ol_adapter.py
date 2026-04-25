import logging
from functools import lru_cache
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

logger = logging.getLogger(__name__)


OL_SCHEME_TWEAKS = {
    "sqlserver": "mssql",
    "awsathena": "athena",
}


# Cache size is generous enough to absorb a fleet of distinct buggy names
# without unbounded growth. 512 is well above any realistic per-process count.
_WARN_DEDUPE_MAXSIZE = 512


@lru_cache(maxsize=_WARN_DEDUPE_MAXSIZE)
def _warn_sanitized_once(original: str, sanitized: str) -> None:
    # One WARNING per distinct (original, sanitized) pair per process.
    logger.warning(
        "OpenLineage Dataset name %r contained 'None'/empty segments; "
        "sanitized to %r before producing DataHub URN. Likely upstream bug "
        "in the producer (unset field interpolated into an f-string).",
        original,
        sanitized,
    )


@lru_cache(maxsize=_WARN_DEDUPE_MAXSIZE)
def _warn_all_none_once(original: str) -> None:
    # One WARNING per distinct all-None name per process.
    logger.warning(
        "OpenLineage Dataset name %r had only 'None'/empty segments; "
        "kept original to avoid emitting an empty URN.",
        original,
    )


def _sanitize_ol_dataset_name(name: str) -> str:
    """Drop literal ``"None"`` and empty segments from a dotted OL name.

    Some OpenLineage producers build dataset names by interpolating optional
    fields into an f-string (e.g. ``f"{database}.{schema}.{table}"``) without
    a ``None`` guard, so an unset field becomes the literal string ``"None"``
    and the resulting DataHub URN orphans from the canonical lineage graph.
    A well-known case is Airflow's ``S3ToRedshiftOperator`` when ``schema``
    is unset (ING-2018), but the bug class is generic.

    Returns the input unchanged on every path that doesn't strictly need
    rewriting (no-dot, dotted-but-clean, non-string). When every segment is
    junk we keep the original so the orphan stays *findable* rather than
    becoming an empty-name URN. Sanitising paths log a deduplicated WARNING.
    """
    if not isinstance(name, str):
        return name  # type: ignore[unreachable]

    if "." not in name:
        return name

    parts = name.split(".")
    cleaned = [p for p in parts if p and p != "None"]
    if len(cleaned) == len(parts):
        return name

    if not cleaned:
        _warn_all_none_once(name)
        return name

    sanitized = ".".join(cleaned)
    _warn_sanitized_once(name, sanitized)
    return sanitized


def translate_ol_to_datahub_urn(
    ol_uri: "OpenLineageDataset", env: str = builder.DEFAULT_ENV
) -> str:
    """Translate OpenLineage dataset URI to DataHub URN.

    Args:
        ol_uri: OpenLineage dataset with namespace and name
        env: DataHub environment (default: PROD for backward compatibility)

    Returns:
        DataHub dataset URN string
    """
    namespace = ol_uri.namespace
    name = _sanitize_ol_dataset_name(ol_uri.name)

    scheme, *rest = namespace.split("://", maxsplit=1)

    platform = OL_SCHEME_TWEAKS.get(scheme, scheme)
    return builder.make_dataset_urn(platform=platform, name=name, env=env)
