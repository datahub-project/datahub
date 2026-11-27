import re
from collections import Counter
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict

from datahub.ingestion.source.qlik_sense.constants import DATA_FILES_CONNECTION
from datahub.ingestion.source.qlik_sense.script_lineage import (
    LIB_PATH_RE,
    SPACE_SEPARATOR,
)


class LibConnection(BaseModel):
    # "" when the `lib://` connection is not qualified by a space.
    space: str
    name: str


def parse_lib_connection(connection: str) -> LibConnection:
    """Splits a `lib://` connection into its space and name. Qlik brackets a
    name that holds the separator, as in `[Team: Sales]:Lake`."""
    connection = connection.strip()
    end = connection.find("]") if connection.startswith("[") else -1
    if end > 0:
        rest = connection[end + 1 :].strip()
        if not rest:
            return LibConnection(space="", name=connection[1:end])
        if rest.startswith(SPACE_SEPARATOR):
            return LibConnection(
                space=connection[1:end],
                name=rest[len(SPACE_SEPARATOR) :].strip().strip("[]"),
            )
    space, qualified, name = connection.partition(SPACE_SEPARATOR)
    if not qualified:
        return LibConnection(space="", name=space.strip("[]"))
    return LibConnection(space=space.strip().strip("[]"), name=name.strip().strip("[]"))


def connection_key(lib_connection: LibConnection) -> str:
    # How Qlik Cloud's data connection listing keys a connection.
    if not lib_connection.space:
        return lib_connection.name
    return f"{lib_connection.space}{SPACE_SEPARATOR}{lib_connection.name}"


def lib_name(name: str) -> str:
    # Bracketed only when it must be, so other names keep their URNs.
    return f"[{name}]" if SPACE_SEPARATOR in name else name


def is_data_files(connection: str) -> bool:
    return parse_lib_connection(connection).name.lower() == DATA_FILES_CONNECTION


def is_ambiguous_connection(connection: str) -> bool:
    # Qlik load records drop the brackets around a name holding the separator,
    # so `team: sales:lake` may be space `team: sales` or connection `sales:lake`.
    return re.sub(r"\[[^\]]*\]", "", connection).count(SPACE_SEPARATOR) > 1


def named_space(connection: str) -> Optional[str]:
    """The space a `<space>:<connection>` name, or the connection part of a
    `lib://` path, names."""
    if is_ambiguous_connection(connection):
        return None
    return parse_lib_connection(connection).space or None


def is_ambiguous_lib_path(path: str) -> bool:
    match = LIB_PATH_RE.match(path)
    return match is not None and is_ambiguous_connection(match.group("connection"))


def path_space(path: str) -> Optional[str]:
    match = LIB_PATH_RE.match(path)
    return named_space(match.group("connection")) if match else None


def path_key(path: str) -> str:
    # Qlik records loads in lowercase, and brackets around a space name with
    # spaces are optional.
    return path.lower().replace("[", "").replace("]", "")


class LibPathSegments(BaseModel):
    model_config = ConfigDict(frozen=True)

    # "" when the path doesn't name a space.
    space: str
    connection: str
    # Folders, then the file name.
    path: List[str]

    def parts(self) -> List[str]:
        return [self.space, self.connection, *self.path]


def lib_path_segments(path: str) -> Optional[LibPathSegments]:
    match = LIB_PATH_RE.match(path)
    if not match or "$(" in path or is_ambiguous_connection(match.group("connection")):
        return None
    connection = parse_lib_connection(match.group("connection"))
    # Optional brackets are kept as the script spelled them.
    space, name = (
        f"[{part}]" if part and f"[{part}]" in match.group("connection") else part
        for part in (connection.space, connection.name)
    )
    return LibPathSegments(
        space=space, connection=name, path=lib_path_parts(match.group("path"))
    )


def lib_path_parts(path: str) -> List[str]:
    return [part for part in re.split(r"[/\\]", path) if part]


def build_lib_path(segments: LibPathSegments) -> str:
    space, connection = (
        name if name.startswith("[") else lib_name(name)
        for name in (segments.space, segments.connection)
    )
    qualified = f"{space}{SPACE_SEPARATOR}{connection}" if space else connection
    return "/".join([f"lib://{qualified}", *segments.path])


def _path_key_parts(path: str) -> List[str]:
    return [part for part in re.split(r"[/\\]", path_key(path)) if part]


def path_has_label(path: str, label: str) -> bool:
    """Whether Qlik's label for the file, which holds its folders under the
    connection, is the whole path under the connection."""
    match = LIB_PATH_RE.match(path)
    return match is not None and _path_key_parts(
        match.group("path")
    ) == _path_key_parts(label)


def voted_segments(
    path: str, spellings: Dict[str, Counter[str]]
) -> Optional[LibPathSegments]:
    """The segments of a path whose space and connection can't be told
    apart, when exactly one way of splitting them is spelled elsewhere."""
    match = LIB_PATH_RE.match(path)
    if not match or "$(" in path:
        return None
    connection = match.group("connection")
    splits = [
        (connection[:i].strip(), connection[i + 1 :].strip())
        for i, char in enumerate(connection)
        if char == SPACE_SEPARATOR
    ]
    known = [split for split in splits if path_key("/".join(split)) in spellings]
    if len(known) != 1:
        return None
    space, name = known[0]
    return LibPathSegments(
        space=space, connection=name, path=lib_path_parts(match.group("path"))
    )
