import re
from typing import List, Optional

from pydantic import BaseModel

from datahub.ingestion.source.qlik_sense.constants import DATA_FILES_CONNECTION
from datahub.ingestion.source.qlik_sense.script_lineage import (
    LIB_PATH_RE,
    SPACE_SEPARATOR,
)


class _LibConnection(BaseModel):
    # "" when the `lib://` connection is not qualified by a space.
    space: str
    name: str


def _lib_connection(connection: str) -> _LibConnection:
    """Splits a `lib://` connection into its space and name. Qlik brackets a
    name that holds the separator, as in `[Team: Sales]:Lake`."""
    connection = connection.strip()
    end = connection.find("]") if connection.startswith("[") else -1
    if end > 0:
        rest = connection[end + 1 :].strip()
        if not rest:
            return _LibConnection(space="", name=connection[1:end])
        if rest.startswith(SPACE_SEPARATOR):
            return _LibConnection(
                space=connection[1:end],
                name=rest[len(SPACE_SEPARATOR) :].strip().strip("[]"),
            )
    space, qualified, name = connection.partition(SPACE_SEPARATOR)
    if not qualified:
        return _LibConnection(space="", name=space.strip("[]"))
    return _LibConnection(
        space=space.strip().strip("[]"), name=name.strip().strip("[]")
    )


def _connection_key(lib_connection: _LibConnection) -> str:
    # How Qlik Cloud's data connection listing keys a connection.
    if not lib_connection.space:
        return lib_connection.name
    return f"{lib_connection.space}{SPACE_SEPARATOR}{lib_connection.name}"


def _lib_name(name: str) -> str:
    # Bracketed only when it must be, so other names keep their URNs.
    return f"[{name}]" if SPACE_SEPARATOR in name else name


def _is_data_files(connection: str) -> bool:
    return _lib_connection(connection).name.lower() == DATA_FILES_CONNECTION


def _is_ambiguous_connection(connection: str) -> bool:
    # Qlik load records drop the brackets around a name holding the separator,
    # so `team: sales:lake` may be space `team: sales` or connection `sales:lake`.
    return re.sub(r"\[[^\]]*\]", "", connection).count(SPACE_SEPARATOR) > 1


def _named_space(connection: str) -> Optional[str]:
    """The space a `<space>:<connection>` name, or the connection part of a
    `lib://` path, names."""
    if _is_ambiguous_connection(connection):
        return None
    return _lib_connection(connection).space or None


def _is_ambiguous_lib_path(path: str) -> bool:
    match = LIB_PATH_RE.match(path)
    return match is not None and _is_ambiguous_connection(match.group("connection"))


def _path_space(path: str) -> Optional[str]:
    match = LIB_PATH_RE.match(path)
    return _named_space(match.group("connection")) if match else None


def _path_key(path: str) -> str:
    # Qlik records loads in lowercase, and brackets around a space name with
    # spaces are optional.
    return path.lower().replace("[", "").replace("]", "")


def _lib_path_segments(path: str) -> Optional[List[str]]:
    """The space, connection, folders and file of a `lib://` path. The space
    is empty when the path doesn't name one."""
    match = LIB_PATH_RE.match(path)
    if not match or "$(" in path or _is_ambiguous_connection(match.group("connection")):
        return None
    connection = _lib_connection(match.group("connection"))
    return [
        # Optional brackets are kept as the script spelled them.
        f"[{name}]" if name and f"[{name}]" in match.group("connection") else name
        for name in (connection.space, connection.name)
    ] + _lib_path_parts(match.group("path"))


def _lib_path_parts(path: str) -> List[str]:
    return [part for part in re.split(r"[/\\]", path) if part]


def _lib_path(segments: List[str]) -> str:
    space, connection, *rest = segments
    space, connection = (
        name if name.startswith("[") else _lib_name(name)
        for name in (space, connection)
    )
    qualified = f"{space}{SPACE_SEPARATOR}{connection}" if space else connection
    return "/".join([f"lib://{qualified}", *rest])


def _path_key_parts(path: str) -> List[str]:
    return [part for part in re.split(r"[/\\]", _path_key(path)) if part]


def _path_has_label(path: str, label: str) -> bool:
    """Whether Qlik's label for the file, which holds its folders under the
    connection, is the whole path under the connection."""
    match = LIB_PATH_RE.match(path)
    return match is not None and _path_key_parts(
        match.group("path")
    ) == _path_key_parts(label)
