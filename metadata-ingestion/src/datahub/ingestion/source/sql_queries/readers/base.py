import logging
from abc import ABC, abstractmethod
from typing import Callable, Generator, Iterable, Iterator, Optional, TypeVar

from datahub.ingestion.source.sql_queries.query_entry import QueryEntry
from datahub.ingestion.source.sql_queries.report import SqlQueriesSourceReport

logger = logging.getLogger(__name__)

PROGRESS_LOG_INTERVAL = 1000

RecordT = TypeVar("RecordT")


class QueryLogReader(ABC):
    """Yields QueryEntry records from one query-log backend.

    Separates where query records come from and how the source turns them into
    lineage and usage, so a backend can be swapped without touching the aggregator.
    """

    @abstractmethod
    def get_query_entries(self) -> Iterable[QueryEntry]:
        raise NotImplementedError


def guard_read_errors(
    records: Iterable[RecordT], on_error: Callable[[Exception], None]
) -> Generator[RecordT, None, None]:
    """Yield records, handing errors raised while reading them to on_error.

    Only advancing the iterator sits inside the try. Wrapping the yield as well
    would catch exceptions thrown into this generator by its consumer, which
    belong to the consumer, not to reading the backend.
    """
    iterator = iter(records)
    while True:
        try:
            record = next(iterator)
        except StopIteration:
            return
        except Exception as e:
            on_error(e)
            raise
        yield record


def tolerant_query_entries(
    records: Iterable[RecordT],
    to_entry: Callable[[RecordT], Optional[QueryEntry]],
    report: SqlQueriesSourceReport,
    context_of: Callable[[RecordT], Optional[str]],
) -> Iterator[QueryEntry]:
    """Convert records to QueryEntry, skipping and counting malformed ones.

    to_entry returns None for a record with no SQL, which is counted separately.
    """
    for record in records:
        try:
            entry = to_entry(record)
        except (MemoryError, SystemExit, KeyboardInterrupt):
            raise
        except Exception as e:
            # Deliberately broad: one bad record must never abort the run.
            # Narrowing it to ValueError misses InvalidUrnError (a plain
            # Exception), which a blank user value raises. Systematic failures
            # still surface: _report_run_health fails the run when most records
            # fail.
            report.num_entries_failed += 1
            report.warning(
                title="Error processing query entry",
                message="Query skipped due to parsing error",
                context=context_of(record),
                exc=e,
                log=False,
            )
            if report.num_entries_failed % PROGRESS_LOG_INTERVAL == 0:
                # Warnings are sampled, so without this the true volume of a
                # systematically-malformed source never reaches the log.
                logger.warning(
                    f"{report.num_entries_failed} query entries have failed to parse"
                )
            continue
        if entry is None:
            report.num_entries_skipped_no_query += 1
            continue
        report.num_entries_processed += 1
        if report.num_entries_processed % PROGRESS_LOG_INTERVAL == 0:
            logger.info(f"Processed {report.num_entries_processed} query entries")
        yield entry
