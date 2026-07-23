import json
import logging
import os
from typing import Iterable, List, Optional, Type, TypeVar

from pydantic import ValidationError

from datahub.ingestion.source.zipline.constants import (
    DEFAULT_PRODUCTION_DIR,
    GROUP_BYS_DIR,
    JOINS_DIR,
    STAGING_QUERIES_DIR,
)
from datahub.ingestion.source.zipline.models import GroupBy, Join, StagingQuery
from datahub.ingestion.source.zipline.report import ZiplineSourceReport

logger = logging.getLogger(__name__)

_ModelT = TypeVar("_ModelT", GroupBy, Join, StagingQuery)


class ZiplineRepositoryReader:
    """Discovers and parses compiled thrift-JSON files on disk.

    Deliberately depends on neither `chronon-ai` nor `zipline-ai`: it parses the
    compiled JSON directly, reporting and skipping malformed files.
    """

    def __init__(self, path: str, report: ZiplineSourceReport) -> None:
        self.report = report
        self.root = self._resolve_root(path)

    @staticmethod
    def _resolve_root(path: str) -> str:
        expanded = os.path.abspath(os.path.expanduser(path))
        # Accept either the compiled output dir or a repo root containing
        # `production/`.
        if not os.path.isdir(os.path.join(expanded, GROUP_BYS_DIR)):
            candidate = os.path.join(expanded, DEFAULT_PRODUCTION_DIR)
            if os.path.isdir(os.path.join(candidate, GROUP_BYS_DIR)):
                return candidate
        return expanded

    def is_valid(self) -> bool:
        return os.path.isdir(self.root)

    def read_group_bys(self) -> Iterable[GroupBy]:
        yield from self._read_dir(GROUP_BYS_DIR, GroupBy)

    def read_joins(self) -> Iterable[Join]:
        yield from self._read_dir(JOINS_DIR, Join)

    def read_staging_queries(self) -> Iterable[StagingQuery]:
        yield from self._read_dir(STAGING_QUERIES_DIR, StagingQuery)

    def _read_dir(self, subdir: str, model: Type[_ModelT]) -> Iterable[_ModelT]:
        directory = os.path.join(self.root, subdir)
        if not os.path.isdir(directory):
            logger.debug("Zipline %s directory not found at %s", subdir, directory)
            return

        for file_path in self._iter_files(directory):
            parsed = self._parse_file(file_path, model)
            if parsed is not None:
                yield parsed

    @staticmethod
    def _iter_files(directory: str) -> List[str]:
        files: List[str] = []
        for dirpath, _dirnames, filenames in os.walk(directory):
            for filename in filenames:
                if filename.startswith("."):
                    continue
                files.append(os.path.join(dirpath, filename))
        # Deterministic ordering keeps golden-file output stable across runs.
        return sorted(files)

    def _parse_file(self, file_path: str, model: Type[_ModelT]) -> Optional[_ModelT]:
        try:
            with open(file_path, encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            self.report.report_unparseable_file(file_path)
            self.report.warning(
                title="Unreadable compiled config",
                message="Skipped a file that could not be read as JSON",
                context=file_path,
                exc=exc,
            )
            return None

        try:
            return model.model_validate(payload)
        except ValidationError as exc:
            self.report.report_unparseable_file(file_path)
            self.report.warning(
                title="Unparseable compiled config",
                message=f"Skipped a file that did not match the {model.__name__} schema",
                context=file_path,
                exc=exc,
            )
            return None
