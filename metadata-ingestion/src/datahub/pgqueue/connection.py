"""psycopg2 connection factory with optional RDS IAM authentication.

This module intentionally avoids importing ``datahub.ingestion.source.*`` at import time.
RDS IAM support lazy-imports ``AwsConnectionConfig`` / ``RDSIAMTokenManager`` only when
``auth_mode`` is :class:`~datahub.pgqueue.config.PgQueueAuthMode.AWS_IAM`, mirroring the
Java-side isolation of pgQueue's DB access from general metadata ingestion.
"""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Optional

from datahub.pgqueue.config import PgQueueAuthMode, PgQueueConnectionConfig
from datahub.pgqueue.host_port import parse_host_port

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PGConnection

logger = logging.getLogger(__name__)


def create_pgqueue_connection(config: PgQueueConnectionConfig) -> "PGConnection":
    """Open a new PostgreSQL connection suitable for pgQueue operations."""
    import psycopg2

    kwargs = build_psycopg2_connect_kwargs(config)
    conn = psycopg2.connect(**kwargs)
    return conn


def flush_pg_connection(conn: "PGConnection") -> None:
    """Align connection transaction state at flush boundaries (Kafka sink parity).

    Repository helpers disable autocommit, commit, then restore the prior flag. If an
    error leaves the session *idle in failed transaction*, toggling autocommit can
    fail until rollback — callers should use :func:`restore_pg_connection_autocommit`
    after each block; this function commits an open transaction or rolls back a
    failed one at work-unit boundaries (see :meth:`DatahubPgQueueEmitter.flush`).
    """
    if getattr(conn, "closed", 0):
        return
    try:
        from psycopg2 import extensions
    except ImportError:
        return
    try:
        status = conn.get_transaction_status()
    except Exception:
        return
    if status == extensions.TRANSACTION_STATUS_INERROR:
        try:
            conn.rollback()
        except Exception:
            logger.debug("flush_pg_connection: rollback failed", exc_info=True)
        return
    if status == extensions.TRANSACTION_STATUS_INTRANS:
        try:
            conn.commit()
        except Exception:
            logger.warning("flush_pg_connection: commit failed", exc_info=True)
            try:
                conn.rollback()
            except Exception:
                logger.debug(
                    "flush_pg_connection: rollback after failed commit", exc_info=True
                )


def restore_pg_connection_autocommit(
    conn: "PGConnection", saved_autocommit: bool
) -> None:
    """Restore ``autocommit`` after a transactional block; rollback if the session aborted."""
    try:
        from psycopg2 import extensions

        if conn.get_transaction_status() == extensions.TRANSACTION_STATUS_INERROR:
            conn.rollback()
    except Exception:
        logger.debug(
            "restore_pg_connection_autocommit: pre-restore check failed",
            exc_info=True,
        )
    try:
        conn.autocommit = saved_autocommit
    except Exception:
        logger.debug("restore_pg_connection_autocommit: failed", exc_info=True)


def build_psycopg2_connect_kwargs(config: PgQueueConnectionConfig) -> dict:
    host, port = parse_host_port(config.host_port, default_port=5432)
    if port is None:
        raise ValueError(f"Could not parse port from host_port={config.host_port!r}")

    sslmode = config.sslmode
    password: Optional[str] = None

    if config.auth_mode == PgQueueAuthMode.PASSWORD:
        assert config.password is not None
        password = config.password.get_secret_value()
    elif config.auth_mode == PgQueueAuthMode.AWS_IAM:
        from datahub.ingestion.source.aws.aws_common import (
            AwsConnectionConfig,
            RDSIAMTokenManager,
        )

        aws_cfg = AwsConnectionConfig.model_validate(config.aws_config)
        mgr = RDSIAMTokenManager(
            endpoint=host,
            username=config.username,
            port=port,
            aws_config=aws_cfg,
        )
        password = mgr.get_token()
        if sslmode not in ("require", "verify-ca", "verify-full"):
            sslmode = "require"

    return {
        "host": host,
        "port": port,
        "dbname": config.database,
        "user": config.username,
        "password": password,
        "sslmode": sslmode,
        "connect_timeout": config.connect_timeout_seconds,
    }


def consumer_lock_owner(consumer_group: str) -> str:
    """Unique lock_owner string per consumer process.

    Matches the Java format in PgQueuePollWorker: ``{consumerGroupId}:{UUID}``.
    """
    return f"{consumer_group}:{uuid.uuid4()}"
