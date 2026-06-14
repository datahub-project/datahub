"""Direct MySQL client for ZDU phases.

Connection-per-call (single-thread test usage; no pool). Defaults match the
local Docker Compose dev profile: ``localhost:3306`` / ``datahub`` /
``datahub`` / ``datahub``.

Reads parse JSON columns and return strongly-typed values; null/malformed
inputs return ``None`` rather than raising — callers treat absence as
"the row exists in a pre-ZDU shape".

Note: ``count_aspects_by_schema_version`` uses MySQL's ``JSON_EXTRACT``,
which surfaces values as strings via pymysql (integer JSON returns the
string ``"2"``, JSON null returns ``"null"``, missing keys return SQL
``NULL`` / Python ``None``). The method coerces all of these to
``int | None`` before returning so callers get the documented type.
"""

from __future__ import annotations

import csv
import io
import json
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator

import pymysql  # type: ignore[import-untyped]
import pymysql.cursors  # type: ignore[import-untyped]

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class EbeanAspectV2Row:
    """One row from ``metadata_aspect_v2`` (the canonical aspect table)."""

    urn: str
    aspect: str
    version: int
    metadata: str
    systemmetadata: str | None
    createdon: str
    createdby: str


class MySQLClient:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 3306,
        user: str = "datahub",
        password: str = "datahub",
        database: str = "datahub",
    ) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database

    @contextmanager
    def _conn(self) -> Iterator[pymysql.connections.Connection]:
        conn = pymysql.connect(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            database=self._database,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )
        try:
            yield conn
        finally:
            conn.close()

    def get_aspect_raw(self, urn: str, aspect: str) -> EbeanAspectV2Row | None:
        sql = (
            "SELECT urn, aspect, version, metadata, systemmetadata, "
            "createdon, createdby FROM metadata_aspect_v2 "
            "WHERE urn=%s AND aspect=%s AND version=0"
        )
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (urn, aspect))
            row = cur.fetchone()
        return EbeanAspectV2Row(**row) if row else None

    def upsert_aspect_raw(
        self,
        urn: str,
        aspect: str,
        metadata: str,
        systemmetadata: str = "{}",
        createdby: str = "urn:li:corpuser:datahub",
        version: int = 0,
    ) -> None:
        """Insert (or replace) a single ``metadata_aspect_v2`` row.

        Bypasses the GMS write-path entirely — no MutationHook chain fires.
        Used by the IO-pool seed path so entities land at ``schemaVersion=null``
        (v1) in MySQL regardless of ``ASPECT_MIGRATION_MUTATOR_ENABLED`` on
        the running GMS.

        ``createdon`` is set client-side. ``systemmetadata`` defaults to
        ``"{}"`` (no ``schemaVersion`` key) which ``get_schema_version`` and
        the production read path both treat as v1.
        """
        sql = (
            "INSERT INTO metadata_aspect_v2 "
            "(urn, aspect, version, metadata, systemmetadata, createdon, createdby) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE "
            "metadata=VALUES(metadata), systemmetadata=VALUES(systemmetadata), "
            "createdon=VALUES(createdon), createdby=VALUES(createdby)"
        )
        params = (
            urn,
            aspect,
            version,
            metadata,
            systemmetadata,
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            createdby,
        )
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, params)
            c.commit()

    def get_schema_version(self, urn: str, aspect: str) -> int | None:
        sql = (
            "SELECT systemmetadata FROM metadata_aspect_v2 "
            "WHERE urn=%s AND aspect=%s AND version=0"
        )
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (urn, aspect))
            row = cur.fetchone()
        if not row or not row.get("systemmetadata"):
            return None
        try:
            meta = json.loads(row["systemmetadata"])
        except json.JSONDecodeError:
            return None
        if not isinstance(meta, dict):
            return None
        v = meta.get("schemaVersion")
        return int(v) if v is not None else None

    def count_aspects_by_schema_version(self, aspect: str) -> dict[int | None, int]:
        sql = (
            "SELECT JSON_EXTRACT(systemmetadata, '$.schemaVersion') AS v, "
            "COUNT(*) AS n FROM metadata_aspect_v2 "
            "WHERE aspect=%s AND version=0 GROUP BY v"
        )
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (aspect,))
            rows = cur.fetchall()
        out: dict[int | None, int] = {}
        for row in rows:
            key = self._coerce_json_extract_int(row["v"])
            out[key] = out.get(key, 0) + row["n"]
        return out

    def count_aspects_at_schema_version_for_urn_prefix(
        self, urn_prefix: str, aspect: str, schema_version: int
    ) -> int:
        """Count aspect rows whose URN starts with ``urn_prefix`` and whose
        ``systemmetadata.schemaVersion`` equals ``schema_version``.

        Used by ``KillSwitchSweepPhase`` to poll the sweep's progress on a
        scoped set of test entities (e.g., ``urn:li:dashboard:(test,zdu-tc-324-``)
        without being affected by other entities in the database.
        """
        sql = (
            "SELECT COUNT(*) AS n FROM metadata_aspect_v2 "
            "WHERE urn LIKE %s "
            "AND aspect=%s "
            "AND version=0 "
            "AND JSON_EXTRACT(systemmetadata, '$.schemaVersion') = %s"
        )
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (f"{urn_prefix}%", aspect, schema_version))
            row = cur.fetchone()
        return int(row["n"]) if row else 0

    def bulk_seed_aspects(
        self,
        rows: list[tuple[str, str, str, str]],
    ) -> int:
        """Bulk-insert ``metadata_aspect_v2`` rows in a single multi-VALUES INSERT.

        Each tuple is ``(urn, aspect, metadata_json, systemmetadata_json)``.
        Always inserts at ``version=0`` with the ``datahub`` system user as
        creator. Returns the number of rows affected by the statement.

        Each row gets a ``createdon`` 1ms apart so that the
        ``MigrateAspectsStep`` per-batch cursor (``lastCreatedOnMs``)
        advances by a distinct value at every batch checkpoint — required
        for TC-325's batch-delay timing assertion and for TC-324's
        kill-switch cursor capture.
        """
        if not rows:
            return 0
        from datetime import timedelta

        base = datetime.utcnow()
        createdby = "urn:li:corpuser:datahub"
        placeholders = ",".join(["(%s, %s, 0, %s, %s, %s, %s)"] * len(rows))
        sql = (
            "INSERT INTO metadata_aspect_v2 "
            "(urn, aspect, version, metadata, systemmetadata, createdon, createdby) "
            f"VALUES {placeholders} "
            "ON DUPLICATE KEY UPDATE "
            "metadata=VALUES(metadata), systemmetadata=VALUES(systemmetadata), "
            "createdon=VALUES(createdon), createdby=VALUES(createdby)"
        )
        params: list[str] = []
        for i, (urn, aspect, metadata, systemmetadata) in enumerate(rows):
            ts = (base + timedelta(milliseconds=i)).strftime("%Y-%m-%d %H:%M:%S.%f")[
                :-3
            ]
            params.extend([urn, aspect, metadata, systemmetadata, ts, createdby])
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, params)
            n = cur.rowcount
            c.commit()
        return int(n) if n is not None else 0

    @staticmethod
    def _coerce_json_extract_int(v: object) -> int | None:
        """Coerce pymysql's JSON_EXTRACT return value to int | None.

        pymysql surfaces MySQL JSON values as strings: integer JSON returns
        '2', JSON null returns 'null', and a missing key (SQL NULL) returns
        Python None. Strings that aren't decimal integers also return None.
        """
        if v is None or v == "null":
            return None
        try:
            return int(v)  # type: ignore[call-overload]
        except (TypeError, ValueError):
            return None

    def delete_upgrade_result_by_urn_prefix(self, urn_prefix: str) -> int:
        """Delete every ``dataHubUpgradeResult`` row whose upgrade-id begins
        with ``urn_prefix`` (e.g., ``"migrate-aspects-"``). Returns the number
        of rows deleted.

        Used by ``SkipAlreadyMigratedSweepPhase`` (TC-326) to reset the
        migrate-aspects upgrade-state to PENDING so the sweep actually runs
        again instead of short-circuiting on ``state=SUCCEEDED``.
        """
        full_prefix = f"urn:li:dataHubUpgrade:{urn_prefix}"
        with self._conn() as c, c.cursor() as cur:
            cur.execute(
                "DELETE FROM metadata_aspect_v2 "
                "WHERE aspect='dataHubUpgradeResult' AND version=0 "
                "AND urn LIKE %s",
                (f"{full_prefix}%",),
            )
            n = cur.rowcount
            c.commit()
        return int(n) if n is not None else 0

    def get_upgrade_result(self, upgrade_id: str) -> dict | None:
        """Return the parsed ``DataHubUpgradeResult`` metadata for ``upgrade_id``, or None."""
        urn = f"urn:li:dataHubUpgrade:{upgrade_id}"
        sql = (
            "SELECT metadata FROM metadata_aspect_v2 "
            "WHERE urn=%s AND aspect='dataHubUpgradeResult' AND version=0"
        )
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (urn,))
            row = cur.fetchone()
        if not row or not row.get("metadata"):
            return None
        try:
            parsed = json.loads(row["metadata"])
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None

    def dump_zdu_aspects_csv(self) -> str:
        """Return CSV of all ZDU test aspect rows (zdu-tc-*, zdu-io-pool-*,
        zdu-gap-*, zdu-dual-*, zdu-rt-*) for failure-bundle inclusion.

        Header row first, then one row per ``metadata_aspect_v2`` row whose
        ``urn`` matches the ZDU test prefixes. Used by ``FailureBundleWriter``.
        """
        sql = (
            "SELECT urn, aspect, version, metadata, systemmetadata, "
            "createdon, createdby FROM metadata_aspect_v2 "
            "WHERE urn LIKE %s OR urn LIKE %s OR urn LIKE %s OR urn LIKE %s OR urn LIKE %s "
            "ORDER BY urn, aspect, version"
        )
        patterns = [
            "%zdu-tc-%",
            "%zdu-io-pool-%",
            "%zdu-gap-%",
            "%zdu-dual-%",
            "%zdu-rt-%",
        ]
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, tuple(patterns))
            rows = cur.fetchall()

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(
            [
                "urn",
                "aspect",
                "version",
                "metadata",
                "systemmetadata",
                "createdon",
                "createdby",
            ]
        )
        for r in rows:
            writer.writerow(
                [
                    r["urn"],
                    r["aspect"],
                    r["version"],
                    r["metadata"],
                    r["systemmetadata"],
                    str(r["createdon"]),
                    r["createdby"],
                ]
            )
        return buf.getvalue()

    def find_upgrade_result_with_field(
        self, field_name: str
    ) -> tuple[str | None, dict | None]:
        """Find any dataHubUpgradeResult whose parsed metadata contains ``field_name``.

        Returns ``(upgrade_id, parsed_dict)`` for the first match, or
        ``(None, None)`` if no match. ``upgrade_id`` is the URN suffix
        (e.g. ``"system-update-blocking"``). Used by ``UpgradeBlockingPhase``
        to discover the indicesState payload without hardcoding the URN.
        """
        with self._conn() as c, c.cursor() as cur:
            cur.execute(
                "SELECT urn, metadata FROM metadata_aspect_v2 "
                "WHERE aspect='dataHubUpgradeResult' AND version=0"
            )
            rows = cur.fetchall()
        for row in rows:
            md = row.get("metadata")
            if not md:
                continue
            try:
                parsed = json.loads(md)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict) and field_name in parsed:
                upgrade_id = row["urn"].removeprefix("urn:li:dataHubUpgrade:")
                return upgrade_id, parsed
        return None, None

    def find_upgrade_result_by_urn_prefix(
        self, urn_prefix: str
    ) -> tuple[str | None, dict | None]:
        """Find any dataHubUpgradeResult whose URN starts with ``urn_prefix``.

        Returns ``(upgrade_id, parsed_metadata_dict)`` for the first match,
        or ``(None, None)`` if no match. The parsed dict is the FULL metadata
        payload — callers extract what they need (e.g.,
        ``BuildIndicesIncremental_<version>`` rows have ``result`` as a
        flat-dotted-key dict that ``upgrade_blocking._parse_flat_indices_state``
        unflattens per alias).
        """
        full_prefix = f"urn:li:dataHubUpgrade:{urn_prefix}"
        with self._conn() as c, c.cursor() as cur:
            cur.execute(
                "SELECT urn, metadata FROM metadata_aspect_v2 "
                "WHERE aspect='dataHubUpgradeResult' AND version=0 "
                "AND urn LIKE %s",
                (f"{full_prefix}%",),
            )
            rows = cur.fetchall()
        for row in rows:
            md = row.get("metadata")
            if not md:
                continue
            try:
                parsed = json.loads(md)
            except json.JSONDecodeError:
                continue
            upgrade_id = row["urn"].removeprefix("urn:li:dataHubUpgrade:")
            return upgrade_id, parsed
        return None, None
