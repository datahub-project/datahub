import hashlib
import re
from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Sequence, Set

from datahub.ingestion.source.repository_documents.client import RepositoryFileInfo

# Source ids join path segments with "." and the instance joins the platform
# with "-". Parentheses and commas would break the dataPlatformInstance URN.
_PLATFORM_INSTANCE_FORBIDDEN = frozenset(".,()")
INDEX_FILENAME = "index.md"


def validate_platform_instance(platform_instance: Optional[str]) -> Optional[str]:
    """The instance with surrounding whitespace removed, or None when unset.

    Raises ``ValueError`` for a blank instance or one containing ``.``, ``,``,
    ``(`` or ``)``: an instance containing ``.`` could produce the same source
    id as another instance plus a repository path.
    """
    if platform_instance is None:
        return None
    stripped = platform_instance.strip()
    if not stripped:
        raise ValueError("platform_instance must not be blank.")
    forbidden = sorted(_PLATFORM_INSTANCE_FORBIDDEN.intersection(stripped))
    if forbidden:
        raise ValueError(
            f"platform_instance {stripped!r} must not contain "
            f"{', '.join(repr(char) for char in forbidden)}; use '-' or '_'."
        )
    return stripped


def make_source_prefix(platform: str, platform_instance: Optional[str]) -> str:
    """The ``source_prefix`` of every source id an import mints: the platform,
    or ``<platform>-<platform_instance>`` when an instance is configured.

    A hyphen, not a dot, joins them: ids without an instance always continue
    ``<platform>.``, so the two can never produce the same id.
    """
    instance = validate_platform_instance(platform_instance)
    return f"{platform}-{instance}" if instance else platform


def property_key(platform: str, name: str) -> str:
    """The custom property key an import stamps, e.g. ``github_file_path``."""
    return f"{platform}_{name}"


@dataclass(frozen=True)
class RepositoryPropertyKeys:
    """The ``<platform>_*`` custom property keys an import stamps on documents."""

    platform: str

    @property
    def repo(self) -> str:
        return property_key(self.platform, "repo")

    @property
    def branch(self) -> str:
        return property_key(self.platform, "branch")

    @property
    def file_path(self) -> str:
        return property_key(self.platform, "file_path")

    @property
    def directory_path(self) -> str:
        return property_key(self.platform, "directory_path")

    @property
    def commit_sha(self) -> str:
        return property_key(self.platform, "commit_sha")

    @property
    def blob_sha(self) -> str:
        return property_key(self.platform, "blob_sha")


def make_file_source_id(owner_repo: str, file_path: str, *, source_prefix: str) -> str:
    repo_part = owner_repo.replace("/", ".")
    path_no_ext = file_path
    dot = path_no_ext.rfind(".")
    if dot > 0:
        path_no_ext = path_no_ext[:dot]
    path_part = path_no_ext.replace("/", ".")
    return f"{source_prefix}.{repo_part}.{path_part}"


def make_dir_source_id(owner_repo: str, dir_path: str, *, source_prefix: str) -> str:
    repo_part = owner_repo.replace("/", ".")
    path_part = dir_path.replace("/", ".")
    return f"{source_prefix}.{repo_part}.{path_part}._dir"


def make_repo_source_id(owner_repo: str, *, source_prefix: str) -> str:
    repo_part = owner_repo.replace("/", ".")
    return f"{source_prefix}.{repo_part}._repo"


def normalize_document_id(source_id: str) -> str:
    """Normalize a source id into a stable document entity id (matches GMS import service)."""
    hash_suffix = hashlib.sha256(source_id.encode("utf-8")).hexdigest()[:16]
    safe = re.sub(r"[^a-zA-Z0-9_.\-]", "-", source_id)
    safe = re.sub(r"-{2,}", "-", safe)
    safe = safe.strip("-").lower()
    max_base_length = 200 - len(hash_suffix) - 1
    if max_base_length < 1:
        return hash_suffix
    if len(safe) > max_base_length:
        safe = safe[:max_base_length]
    return f"{safe}-{hash_suffix}"


def matches_filters(file_path: str, path_prefix: str, extensions: Set[str]) -> bool:
    normalized_prefix = path_prefix.strip("/")
    if normalized_prefix:
        if file_path != normalized_prefix and not file_path.startswith(
            f"{normalized_prefix}/"
        ):
            return False
    dot = file_path.rfind(".")
    if dot < 0:
        return False
    ext = file_path[dot:].lower()
    return ext in extensions


@dataclass
class TreeSelection:
    """The files of a repository tree listing that an import reads.

    Every ``index.md`` is kept in ``index_paths`` even outside the filters,
    because OKF bundle discovery needs indexes above ``path_prefix``.
    """

    path_prefix: str
    extensions: Set[str]
    files: List[RepositoryFileInfo] = field(default_factory=list)
    index_paths: Set[str] = field(default_factory=set)

    @classmethod
    def for_filters(
        cls, path_prefix: str, extensions: Iterable[str]
    ) -> "TreeSelection":
        return cls(
            path_prefix=path_prefix.strip("/"),
            extensions={ext.lower() for ext in extensions},
        )

    def add(self, blob: RepositoryFileInfo) -> None:
        if blob.path == INDEX_FILENAME or blob.path.endswith(f"/{INDEX_FILENAME}"):
            self.index_paths.add(blob.path)
        if matches_filters(blob.path, self.path_prefix, self.extensions):
            self.files.append(blob)


def collect_intermediate_directories(
    files: Sequence[RepositoryFileInfo], path_prefix: str
) -> Set[str]:
    dirs: Set[str] = set()
    normalized_prefix = path_prefix.strip("/")

    for file in files:
        relative_path = _relativize(file.path, normalized_prefix)
        last_slash = relative_path.rfind("/")
        while last_slash > 0:
            relative_dir = relative_path[:last_slash]
            full_dir = _resolve_path(normalized_prefix, relative_dir)
            dirs.add(full_dir)
            last_slash = relative_dir.rfind("/")
    return dirs


def resolve_parent_dir_source_id(
    owner_repo: str,
    full_path: str,
    path_prefix: str,
    *,
    repo_root_source_id: Optional[str] = None,
    source_prefix: str,
) -> Optional[str]:
    relative_path = _relativize(full_path, path_prefix.strip("/"))
    parent_dir = _parent_directory(relative_path)
    if parent_dir is None:
        return repo_root_source_id
    return make_dir_source_id(
        owner_repo,
        _resolve_path(path_prefix.strip("/"), parent_dir),
        source_prefix=source_prefix,
    )


def _relativize(full_path: str, prefix: str) -> str:
    if not prefix:
        return full_path
    if full_path.startswith(prefix + "/"):
        return full_path[len(prefix) + 1 :]
    if full_path.startswith(prefix):
        return full_path[len(prefix) :]
    return full_path


def _parent_directory(relative_path: str) -> Optional[str]:
    last_slash = relative_path.rfind("/")
    if last_slash <= 0:
        return None
    return relative_path[:last_slash]


def _resolve_path(prefix: str, relative: str) -> str:
    if not prefix:
        return relative
    return f"{prefix}/{relative}"
