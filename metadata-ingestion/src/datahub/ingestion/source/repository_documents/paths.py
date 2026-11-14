import hashlib
import re
from typing import Optional, Sequence, Set

from datahub.ingestion.source.repository_documents.client import RepositoryFileInfo


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
