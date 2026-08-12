"""Shared connection-mapping logic for the two writers that name datasets.

The Airflow Asset path and the OpenLineage facet path both turn a connection URI into a
DataHub URN, and they historically disagreed about how: the OL adapter dropped the URI
authority, the Asset adapter concatenated it into the dataset name. Neither could supply
a `platform_instance` or match a warehouse's lowercasing, so a table reachable by both
produced two URNs, neither matching the one that platform's own connector emits.

Both now resolve through here, so a single mapping entry makes them converge.
"""

import logging
from typing import Dict, List, Optional

import datahub.emitter.mce_builder as builder
from datahub_airflow_plugin._config import (
    AssetConnectionDetail,
    normalize_connection_key,
)

logger = logging.getLogger(__name__)

# Canonical URI-scheme -> DataHub-platform map, shared by both writers so one mapping
# entry serves both. This matters because the same connection is spelled differently by
# each: an Airflow Asset uses the SQLAlchemy-style `postgresql://`, while an OpenLineage
# namespace uses `postgres://`. Canonicalising the scheme before building the lookup key
# means the user configures `postgres://host:5432` once.
SCHEME_TO_PLATFORM = {
    # object stores / filesystems
    "s3": "s3",
    "s3a": "s3",
    "gs": "gcs",
    "gcs": "gcs",
    "file": "file",
    "hdfs": "hdfs",
    "abfs": "adls",
    "abfss": "adls",
    # warehouses / databases
    "postgresql": "postgres",
    "postgres": "postgres",
    "mysql": "mysql",
    "bigquery": "bigquery",
    "snowflake": "snowflake",
    "sqlserver": "mssql",
    "awsathena": "athena",
}

# Platforms whose datasets are named `<database>.<schema>.<table>` (or
# `<database>.<table>`) by their DataHub connector, so the URI authority is a connection
# identifier — an account or host — and must not appear in the dataset name. A URI alone
# can't tell us which platform_instance the warehouse recipe used, nor whether that
# platform lowercases, so these require an explicit mapping.
#
# Path-shaped platforms (s3, gcs, file, hdfs, adls) are deliberately absent: for those
# the URI form already IS the connector's naming (`s3://bucket/key` -> `bucket/key`), so
# they need no mapping and keep their existing behaviour. mssql/athena are also absent
# for now — they are table-shaped too, but no Asset URIs for them have been reported and
# adding them would widen the set of URIs that stop resolving without a mapping.
TABLE_SHAPED_PLATFORMS = frozenset(
    {
        "snowflake",
        "postgres",
        "mysql",
        "bigquery",
    }
)


def platform_for_scheme(scheme: str) -> str:
    return SCHEME_TO_PLATFORM.get(scheme.lower(), scheme.lower())


# Fire per-connection warnings at most once each, so a DAG with hundreds of Assets on one
# unmapped connection doesn't flood the task log.
_warned_connections: set = set()


def connection_key(scheme: str, authority: str) -> str:
    """Lookup key for a connection, canonical across both writers.

    The scheme is canonicalised to its DataHub platform so that an Asset's
    `postgresql://host:5432` and an OpenLineage namespace's `postgres://host:5432`
    resolve to the same entry.
    """
    return normalize_connection_key(f"{platform_for_scheme(scheme)}://{authority}")


def lookup(
    connections: Optional[Dict[str, AssetConnectionDetail]],
    scheme: str,
    authority: str,
) -> Optional[AssetConnectionDetail]:
    if not connections:
        return None
    return connections.get(connection_key(scheme, authority))


def is_table_shaped(scheme: str) -> bool:
    return platform_for_scheme(scheme) in TABLE_SHAPED_PLATFORMS


def warn_unmapped_once(scheme: str, authority: str, uri: str) -> None:
    key = connection_key(scheme, authority)
    if key in _warned_connections:
        return
    _warned_connections.add(key)
    logger.warning(
        "Airflow Asset %r has no [datahub] asset_connections entry for %r, so it was "
        "skipped. A URI alone does not say which platform_instance the %s recipe used, "
        "and guessing would create a dataset that looks real but matches nothing. Add "
        'asset_connections = {"%s": {"platform_instance": "..."}} to map it, or use a '
        "DataHub-native inlet/outlet, which needs no inference.",
        uri,
        key,
        scheme,
        key,
    )


def build_table_urn(
    *,
    platform: str,
    path_segments: List[str],
    detail: AssetConnectionDetail,
) -> Optional[str]:
    """Build a `<database>.<schema>.<table>`-style URN from a mapped connection."""
    segments = [s for s in path_segments if s]
    if detail.database:
        segments = [detail.database, *segments]
    if not segments:
        return None

    name = ".".join(segments)
    if detail.convert_urns_to_lowercase:
        name = name.lower()

    return builder.make_dataset_urn_with_platform_instance(
        platform=platform,
        name=name,
        platform_instance=detail.platform_instance,
        env=detail.env,
    )


def build_named_urn(
    *,
    platform: str,
    name: str,
    detail: Optional[AssetConnectionDetail],
    env: str,
    lowercase: bool,
) -> str:
    """Build a URN for a writer that already has the platform's own dataset name.

    Used by the OpenLineage path, where the producer supplies a dotted name, and for
    path-shaped Asset URIs, where the URI is already the connector's naming. `database`
    is deliberately not applied here — the name already carries it, so prepending would
    duplicate the segment.

    `lowercase` is the caller's decision rather than read from `detail`, because the
    field defaults to True for table-shaped warehouses while object-store keys are
    case-sensitive: a mapping added only to set `platform_instance` on an `s3://`
    connection must not silently re-case the object path.
    """
    if lowercase:
        name = name.lower()

    if detail is None:
        return builder.make_dataset_urn(platform=platform, name=name, env=env)

    return builder.make_dataset_urn_with_platform_instance(
        platform=platform,
        name=name,
        platform_instance=detail.platform_instance,
        env=detail.env,
    )
