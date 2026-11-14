from dataclasses import dataclass
from typing import List, Optional, Protocol, Set, Tuple

MAX_FILE_SIZE_BYTES = 1_000_000


@dataclass(frozen=True)
class RepositoryFileInfo:
    path: str
    size: Optional[int]
    sha: Optional[str] = None


class RepositoryDocumentsClient(Protocol):
    """Read access a repository documents source needs from a git host's API."""

    @property
    def index_file_paths(self) -> Set[str]: ...

    @property
    def last_api_error(self) -> Optional[str]: ...

    def get_latest_commit_sha(self, owner_repo: str, branch: str) -> str: ...

    def list_matching_files(
        self,
        owner_repo: str,
        branch: str,
        path_prefix: str,
        extensions: List[str],
    ) -> Tuple[List[RepositoryFileInfo], bool]: ...

    def fetch_file_content(
        self, owner_repo: str, file_path: str, branch: str
    ) -> Optional[str]: ...
