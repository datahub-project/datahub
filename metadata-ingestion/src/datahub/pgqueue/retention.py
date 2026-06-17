"""pgQueue message retention helpers (aligned with SqlSetup ``*_apply_retention``)."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PGConnection


def sequence_anchor_exclusion_sql(
    *, message_alias: str, qualified_message_table: str
) -> str:
    """SQL fragment: do not delete the per-partition row with MAX(enqueue_seq)."""
    return (
        f" AND {message_alias}.enqueue_seq < ("
        f"SELECT MAX(m_anchor.enqueue_seq) FROM {qualified_message_table} m_anchor "
        f"WHERE m_anchor.topic_id = {message_alias}.topic_id "
        f"AND m_anchor.partition_id = {message_alias}.partition_id)"
    )


def apply_retention(
    conn: PGConnection,
    *,
    qualified_apply_retention: str,
) -> None:
    """Run server-side retention (age / caps / aggressive) including sequence-anchor rules."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT {qualified_apply_retention}()")


def qualified_apply_retention_function(schema: str, table_prefix: str) -> str:
    """Qualified name of the SqlSetup ``{prefix}_apply_retention`` function."""
    return f'"{schema}".{table_prefix}_apply_retention'
