"""SQLAlchemy 2.0 compatibility shim for acryl-pyhive, with re-exports.

acryl-pyhive (0.6.18, the latest published release) is SQLAlchemy-1.4-era: its
``sqlalchemy_hive`` / ``sqlalchemy_presto`` dialects import names at module load time
that SQLAlchemy 2.0 moved or removed, so importing them on SA 2.0 raises
``ImportError`` / ``AttributeError`` and disables the hive and presto sources entirely.

This module restores the three names pyhive needs *before* importing it, then re-exports
the pyhive symbols the hive/presto sources use. Those sources import from here (a normal
first-party import) instead of from ``pyhive`` directly, which guarantees the shim runs
first without import-ordering gymnastics. Remove this module once acryl-pyhive ships a
SA-2.0-compatible release and import from ``pyhive`` again.
"""

import sys

import sqlalchemy
import sqlalchemy.dialects
from sqlalchemy.dialects import mysql

# `from sqlalchemy import processors`: relocated to sqlalchemy.engine.processors in 2.0.
if not hasattr(sqlalchemy, "processors"):
    from sqlalchemy.engine import processors

    sqlalchemy.processors = processors  # type: ignore[attr-defined]
    sys.modules.setdefault("sqlalchemy.processors", processors)

# `from sqlalchemy.databases import mysql`: the `databases` package was removed in 2.0;
# its dialects now live under sqlalchemy.dialects.
sys.modules.setdefault("sqlalchemy.databases", sqlalchemy.dialects)

# pyhive references the pre-1.0 MySQL type alias `MSTinyInteger` (renamed to `TINYINT`).
if not hasattr(mysql, "MSTinyInteger"):
    mysql.MSTinyInteger = mysql.TINYINT  # type: ignore[attr-defined]

# Imported only for its side effect of verifying the dependency is installed.
from pyhive import hive  # noqa: E402,F401
from pyhive.sqlalchemy_hive import (  # noqa: E402,F401
    HiveDate,
    HiveDecimal,
    HiveDialect,
    HiveTimestamp,
)
from pyhive.sqlalchemy_presto import PrestoDialect  # noqa: E402,F401
