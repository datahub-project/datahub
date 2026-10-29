"""Fixtures and helpers shared by the whole unit suite.

Only generic building blocks belong here: anything a test for any connector
could use without importing that connector. Connector-family builders live in
small helper modules beside the tests (e.g. ``tests/unit/sql_test_helpers.py``).
"""

import pathlib
import re
import sqlite3
from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Set,
    Type,
    TypeVar,
    Union,
    cast,
)
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.source import StructuredLogEntry
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    _Aspect,
)
from tests.test_helpers.masking_state_helpers import reset_masking_process_state

AspectT = TypeVar("AspectT", bound=_Aspect)


@pytest.fixture(autouse=True)
def isolated_masking_state():
    reset_masking_process_state()
    yield
    reset_masking_process_state()


# --- mocks ------------------------------------------------------------------


def as_mock(obj: object) -> MagicMock:
    """Narrow a patched collaborator back to ``MagicMock`` for call assertions.

    Sources type their collaborators (aggregators, inspectors, ...) by the real
    class, so reading ``call_args`` off them needs this cast; keeping it in one
    place stops every module from re-declaring it.
    """
    return cast(MagicMock, obj)


# --- aspects emitted by a source -------------------------------------------


def aspects_of_type(
    items: Iterable[Union[MetadataWorkUnit, _Aspect]], aspect_type: Type[AspectT]
) -> List[AspectT]:
    """Every aspect of ``aspect_type`` in ``items``.

    ``items`` may be workunits (MCP or MCE, as ``get_workunits_internal`` yields
    them) or already-unwrapped aspects (as ``aspects_by_urn`` returns them).
    """
    found: List[AspectT] = []
    for item in items:
        if isinstance(item, MetadataWorkUnit):
            aspect = item.get_aspect_of_type(aspect_type)
            if aspect is not None:
                found.append(aspect)
        elif isinstance(item, aspect_type):
            found.append(item)
    return found


def single_aspect(
    items: Iterable[Union[MetadataWorkUnit, _Aspect]], aspect_type: Type[AspectT]
) -> AspectT:
    """The one aspect of ``aspect_type`` in ``items``; fails if there is not exactly one."""
    found = aspects_of_type(items, aspect_type)
    assert len(found) == 1, f"expected one {aspect_type.__name__}, got {found}"
    return found[0]


def aspects_by_urn(workunits: Iterable[MetadataWorkUnit]) -> Dict[str, List[_Aspect]]:
    """Every aspect emitted per entity urn, whether it came as an MCP or
    inside an MCE dataset snapshot."""
    aspects: Dict[str, List[_Aspect]] = {}
    for wu in workunits:
        metadata = wu.metadata
        if isinstance(metadata, MetadataChangeProposalWrapper):
            assert metadata.entityUrn is not None
            if metadata.aspect is not None:
                aspects.setdefault(metadata.entityUrn, []).append(metadata.aspect)
        elif isinstance(metadata, MetadataChangeEventClass):
            snapshot = metadata.proposedSnapshot
            assert isinstance(snapshot, DatasetSnapshotClass)
            aspects.setdefault(snapshot.urn, []).extend(snapshot.aspects)
    return aspects


# --- structured report entries ---------------------------------------------


def log_titles(entries: Iterable[StructuredLogEntry]) -> Set[str]:
    """The titles of ``report.warnings`` / ``report.failures`` / ``report.infos``.

    Membership on a title (or on ``log_contexts``) pins a contract without
    coupling the test to how many other entries happen to be recorded, or to the
    order they were recorded in.
    """
    return {entry.title for entry in entries if entry.title is not None}


def log_messages(entries: Iterable[StructuredLogEntry]) -> Set[str]:
    return {entry.message for entry in entries}


_EXC_SUFFIX = re.compile(r" <class '[^']*'>: .*$", re.DOTALL)


def log_contexts(entries: Iterable[StructuredLogEntry]) -> Set[str]:
    """Every context string attached to any entry, flattened.

    When an entry was reported with ``exc=``, ``report_log`` appends the
    exception type and text to the context; that suffix is dropped here so the
    set holds exactly the keys the source passed as ``context``.
    """
    return {
        _EXC_SUFFIX.sub("", context) for entry in entries for context in entry.context
    }


# --- SQLite-backed SQLAlchemy engines --------------------------------------


@pytest.fixture
def sqlite_engine() -> Iterator[Engine]:
    """An empty in-memory SQLAlchemy engine, disposed after the test."""
    engine = create_engine("sqlite://")
    yield engine
    engine.dispose()


@pytest.fixture
def sqlite_file_db(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> Callable[..., str]:
    """Factory for file-backed SQLite databases, returning a *relative* URL.

    The test's cwd is moved to a temp dir first, so ``sqlite:///<name>`` names a
    file there. Relative URLs matter: they make the engine's ``url.database`` a
    bare name, which is what SQL sources read back as the database name
    (``get_db_name``), so three-tier identifiers come out as ``name.schema.table``.
    """
    monkeypatch.chdir(tmp_path)

    def create(name: str, schema_sql: str = "") -> str:
        conn = sqlite3.connect(name)
        if schema_sql:
            conn.executescript(schema_sql)
        conn.commit()
        conn.close()
        return f"sqlite:///{name}"

    return create
