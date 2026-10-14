from typing import Protocol, Sequence

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.verdicts import Verdict

FOLDER_PATTERN = "folder_pattern"
DOCUMENT_PATTERN = "document_pattern"
RECURSIVE = "recursive"
CREATE_ROOT_DOCUMENTS = "create_root_documents"
SKIP_EMPTY_DOCUMENTS = "skip_empty_documents"
MIN_TEXT_LENGTH = "min_text_length"
MAX_TEXT_LENGTH = "max_text_length"


class DriveSelectionConfig(Protocol):
    # Read-only properties, so the pydantic config satisfies the protocol.
    @property
    def recursive(self) -> bool: ...

    @property
    def create_root_documents(self) -> bool: ...

    @property
    def folder_pattern(self) -> AllowDenyPattern: ...

    @property
    def document_pattern(self) -> AllowDenyPattern: ...

    @property
    def skip_empty_documents(self) -> bool: ...

    @property
    def min_text_length(self) -> int: ...

    @property
    def max_text_length(self) -> int: ...


def subfolder_verdict(config: DriveSelectionConfig, path: str) -> Verdict:
    """Whether the walk enters a folder below a root, given its parent was entered.

    `path` is the folder path folder_pattern is matched against, starting with the
    root's name. Roots are always entered."""
    if not config.recursive:
        return Verdict.exclude(RECURSIVE)
    if not config.folder_pattern.allowed(path):
        return Verdict.exclude(FOLDER_PATTERN)
    return Verdict.include()


def document_title_verdict(config: DriveSelectionConfig, title: str) -> Verdict:
    if not config.document_pattern.allowed(title):
        return Verdict.exclude(DOCUMENT_PATTERN)
    return Verdict.include()


def folder_chain_verdict(config: DriveSelectionConfig, chain: Sequence[str]) -> Verdict:
    """Whether the walk reaches the last folder of `chain`, the folder paths from a
    root down, root first. A denied folder is never listed, so nothing beneath it is
    reached whatever its own path matches."""
    for path in chain[1:]:
        verdict = subfolder_verdict(config, path)
        if not verdict.included:
            assert verdict.excluded_by is not None
            return Verdict.exclude(verdict.excluded_by, matched_target=path)
    return Verdict.include()


def root_document_verdict(config: DriveSelectionConfig) -> Verdict:
    """Whether a configured root is emitted as a folder Document. Its contents are
    ingested either way."""
    if not config.create_root_documents:
        return Verdict.exclude(CREATE_ROOT_DOCUMENTS)
    return Verdict.include()


def folder_document_verdict(
    config: DriveSelectionConfig, chain: Sequence[str]
) -> Verdict:
    """Whether the last folder of `chain` (root first) is emitted as a folder
    Document."""
    if len(chain) == 1:
        return root_document_verdict(config)
    return folder_chain_verdict(config, chain)


def google_doc_verdict(
    config: DriveSelectionConfig, folder_chain: Sequence[str], title: str
) -> Verdict:
    """Whether a Google Doc titled `title`, listed in the last folder of
    `folder_chain` (root first), is ingested. Export and text-length rules are
    applied later, to the exported text (exported_text_verdict)."""
    verdict = folder_chain_verdict(config, folder_chain)
    if not verdict.included:
        return verdict
    return document_title_verdict(config, title)


def exported_text_verdict(config: DriveSelectionConfig, text_length: int) -> Verdict:
    """Whether a Doc whose cleaned export is `text_length` characters long is
    ingested."""
    if config.skip_empty_documents and text_length == 0:
        return Verdict.exclude(SKIP_EMPTY_DOCUMENTS)
    if text_length < config.min_text_length:
        return Verdict.exclude(MIN_TEXT_LENGTH)
    if config.max_text_length and text_length > config.max_text_length:
        return Verdict.exclude(MAX_TEXT_LENGTH)
    return Verdict.include()
