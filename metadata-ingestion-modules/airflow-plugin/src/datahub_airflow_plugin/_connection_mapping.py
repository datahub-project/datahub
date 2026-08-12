"""Shared connection-mapping logic for the two writers that name datasets.

The Airflow Asset path and the OpenLineage facet path both turn a connection URI into a
DataHub URN, and they historically disagreed about how: the OL adapter dropped the URI
authority, the Asset adapter concatenated it into the dataset name. Neither could supply
a `platform_instance` or match a warehouse's lowercasing, so a table reachable by both
produced two URNs, neither matching the one that platform's own connector emits.

Both now resolve through here, so a single mapping entry makes them converge.
"""

import logging
from typing import Dict, List, NamedTuple, Optional

import datahub.emitter.mce_builder as builder
from datahub.sql_parsing.sql_parsing_common import PLATFORMS_WITH_CASE_SENSITIVE_TABLES
from datahub_airflow_plugin._config import (
    AssetConnectionDetail,
    normalize_connection_key,
)
from datahub_airflow_plugin._platform_schemes import (
    SCHEME_TO_PLATFORM,
    platform_for_scheme,
    scheme_of,
)

__all__ = ["SCHEME_TO_PLATFORM", "platform_for_scheme", "scheme_of"]

logger = logging.getLogger(__name__)


class TableNaming(NamedTuple):
    """How one platform's DataHub connector names its datasets.

    These are fixed conventions, not guesses, so a URI is enough to build a matching
    dataset name without any configuration. Only `platform_instance` — which the URI
    genuinely cannot carry — needs the connection mapping.
    """

    # Whether the URI authority is part of the dataset name. False for connection
    # identifiers (a Snowflake account, a Postgres host); True for BigQuery, where the
    # authority is the project and the connector's name starts with it.
    keep_authority: bool
    # How many dot-separated segments the connector's name has. Used only to warn when a
    # hand-authored URI doesn't look like the expected shape.
    expected_segments: int


# Platforms whose datasets are named `<database>.<schema>.<table>` (or
# `<database>.<table>`), where the URI needs restructuring rather than passing through.
#
# Path-shaped platforms (s3, gcs, file, hdfs, adls) are deliberately absent: for those
# the URI form already IS the connector's naming (`s3://bucket/key` -> `bucket/key`).
# mssql/athena are absent for now — table-shaped too, but no Asset URIs for them have
# been reported, and each needs its naming confirmed before being added.
TABLE_NAMING = {
    "snowflake": TableNaming(keep_authority=False, expected_segments=3),
    "postgres": TableNaming(keep_authority=False, expected_segments=3),
    "mysql": TableNaming(keep_authority=False, expected_segments=2),
    "bigquery": TableNaming(keep_authority=True, expected_segments=3),
}


# A DAG can declare hundreds of Assets sharing one connection and one defect. Warn once
# per (cause, connection) so the task log stays readable: the first occurrence carries the
# offending URI and stays actionable, and the rest would only repeat it.
_warned_keys: set = set()


def warn_once(dedup_key: str, message: str, *args: object) -> None:
    if dedup_key in _warned_keys:
        return
    _warned_keys.add(dedup_key)
    logger.warning(message, *args)


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
    return platform_for_scheme(scheme) in TABLE_NAMING


def resolve_lowercase(platform: str, detail: Optional[AssetConnectionDetail]) -> bool:
    """Whether to lowercase this platform's dataset name.

    An explicit mapping value always wins. Otherwise defer to
    PLATFORMS_WITH_CASE_SENSITIVE_TABLES — the same rule the SQL parser applies via
    SchemaResolver._prefers_urn_lower — so all three of the plugin's writers (Asset URIs,
    OpenLineage facets, SQL parsing) agree on casing. Deriving it from that shared
    constant rather than repeating a list here is what stops the two from drifting apart.

    Only applies to table-shaped platforms. Object stores and unrecognised schemes keep
    their case: their keys really are case-sensitive, and that constant is about SQL
    identifier folding, not object paths.
    """
    if detail is not None and detail.convert_urns_to_lowercase is not None:
        return detail.convert_urns_to_lowercase
    if platform not in TABLE_NAMING:
        return False
    return platform not in PLATFORMS_WITH_CASE_SENSITIVE_TABLES


def resolve_default_database(
    from_operator: Optional[str],
    from_connection: Optional[str],
    detail: Optional[AssetConnectionDetail],
) -> Optional[str]:
    """Default database for SQL parsing, most authoritative source first.

    The operator's own argument and the database the connection reports describe what the
    query actually ran against, so neither is ever overridden by config. The mapping's
    `database` is a last resort for connections that report none: without a database the
    parser cannot fully qualify table names, and a configured one beats nothing.
    """
    return from_operator or from_connection or (detail.database if detail else None)


def _warn_shape_once(uri: str, key: str, expected: int, got: int, urn: str) -> None:
    warn_once(
        f"shape:{key}",
        "Airflow Asset %r produced %d name segments where %s datasets have %d, so the "
        "URN %r may be wrong — a hand-written URI that omits the account or host is the "
        "usual cause, since the authority is dropped as a connection identifier. "
        'Set [datahub] asset_connections = {"%s": {"database": "..."}} to supply the '
        "missing segment, or use a DataHub-native inlet/outlet, which needs no inference.",
        uri,
        got,
        key.split("://")[0],
        expected,
        urn,
        key,
    )


def build_table_urn(
    *,
    platform: str,
    scheme_platform: str,
    authority: str,
    path_segments: List[str],
    detail: Optional[AssetConnectionDetail],
    env: str,
    uri: str,
    key: str,
) -> Optional[str]:
    """Build a `<database>.<schema>.<table>`-style URN for a table-shaped platform.

    Works with no mapping at all: the authority and separator rules come from the
    platform's own naming convention. A mapping only supplies what a URI cannot —
    `platform_instance` — plus optional overrides.
    """
    # A `platform` override can point outside TABLE_NAMING (is_table_shaped only inspects
    # the URI scheme). Fall back to the scheme's own naming rather than raising: a KeyError
    # here drops the asset's lineage, and inside an alias batch it can clear the rest.
    naming = TABLE_NAMING.get(platform) or TABLE_NAMING[scheme_platform]

    segments = [s for s in path_segments if s]
    authority_supplied_a_segment = bool(naming.keep_authority and authority)
    if authority_supplied_a_segment:
        segments = [authority, *segments]
    # `database` fills a segment the URI omitted, so skip it when the authority already
    # provided the leading one — otherwise a BigQuery entry naming its project yields
    # project.project.dataset.table.
    if detail is not None and detail.database and not authority_supplied_a_segment:
        segments = [detail.database, *segments]
    if not segments:
        return None

    name = ".".join(segments)
    if resolve_lowercase(platform, detail):
        name = name.lower()

    urn = builder.make_dataset_urn_with_platform_instance(
        platform=platform,
        name=name,
        platform_instance=detail.platform_instance if detail else None,
        # An entry that doesn't set env must not silently reset the dataset to PROD; fall
        # back to the plugin-wide cluster.
        env=(detail.env if detail is not None and detail.env else env),
    )

    if len(segments) != naming.expected_segments:
        _warn_shape_once(uri, key, naming.expected_segments, len(segments), urn)

    return urn


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
        # See build_table_urn: an entry without env keeps the plugin-wide cluster.
        env=detail.env or env,
    )
