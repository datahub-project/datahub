import json
import logging
import os
from typing import IO, Any, Dict, Iterable, Optional

import smart_open

from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_util import is_s3_uri
from datahub.ingestion.source.sql_queries.query_entry import (
    QueryEntry,
    QueryEntryContext,
)
from datahub.ingestion.source.sql_queries.readers.base import (
    QueryLogReader,
    guard_read_errors,
    tolerant_query_entries,
)
from datahub.ingestion.source.sql_queries.report import SqlQueriesSourceReport

logger = logging.getLogger(__name__)


class FileQueryLogReader(QueryLogReader):
    """Reads newline-delimited JSON query records from a local path or S3 URI."""

    def __init__(
        self,
        *,
        query_file: str,
        aws_config: Optional[AwsConnectionConfig],
        entry_context: QueryEntryContext,
        report: SqlQueriesSourceReport,
    ) -> None:
        if is_s3_uri(query_file) and aws_config is None:
            raise ValueError(f"aws_config is required to read {query_file}")
        self.query_file = query_file
        self.aws_config = aws_config
        self.entry_context = entry_context
        self.report = report

    def get_query_entries(self) -> Iterable[QueryEntry]:
        logger.info(f"Parsing queries from {os.path.basename(self.query_file)}")
        with self._open() as stream:
            # Guarded: a truncated read or undecodable bytes surface partway
            # through iteration, well after the open succeeded.
            lines = guard_read_errors(stream, self._report_read_error)
            stripped = (line.strip() for line in lines)
            yield from tolerant_query_entries(
                (line for line in stripped if line),
                self._to_entry,
                self.report,
                context_of=lambda line: line,
            )

    def _open(self) -> IO[str]:
        transport_params: Optional[Dict[str, Any]] = None
        try:
            if self.aws_config is not None and is_s3_uri(self.query_file):
                transport_params = {"client": self.aws_config.get_s3_client()}
            return smart_open.open(
                self.query_file, mode="r", transport_params=transport_params
            )
        except Exception as e:
            self.report.failure(
                title="Query file open error",
                message="Failed to open query file",
                context=self.query_file,
                exc=e,
            )
            raise

    def _report_read_error(self, e: Exception) -> None:
        self.report.failure(
            title="Query file read error",
            message="Error reading query file mid-transfer",
            context=self.query_file,
            exc=e,
        )

    def _to_entry(self, line: str) -> QueryEntry:
        return QueryEntry.create(
            json.loads(line, strict=False), context=self.entry_context
        )
