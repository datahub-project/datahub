import dataclasses
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, List, Optional

from datahub.ingestion.api.closeable import Closeable
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.sql_parsing_aggregator import (
    PreparsedQuery,
    UrnStr,
)
from datahub.sql_parsing.sqlglot_utils import get_query_fingerprint
from datahub.utilities.file_backed_collections import FileBackedDict


@dataclasses.dataclass
class StoredProcCall:
    snowflake_root_query_id: str

    # Query text will typically be something like:
    # "CALL SALES_FORECASTING.CUSTOMER_ANALYSIS_PROC();"
    query_text: str

    timestamp: datetime
    user: CorpUserUrn
    default_db: str
    default_schema: str


@dataclass
class StoredProcExecutionLineage:
    call: StoredProcCall

    inputs: List[UrnStr]
    outputs: List[UrnStr]


@dataclass
class StoredProcLineageReport:
    num_stored_proc_calls: int = 0
    num_related_queries: int = 0
    num_related_queries_without_proc_call: int = 0

    # Incremented at generation/build time.
    num_stored_proc_lineage_entries: int = 0
    num_stored_proc_calls_with_no_inputs: int = 0
    num_stored_proc_calls_with_no_outputs: int = 0


class StoredProcLineageTracker(Closeable):
    """
    Tracks table-level lineage for Snowflake stored procedures.

    Stored procedures in Snowflake trigger multiple SQL queries during execution.
    Snowflake assigns each stored procedure call a unique query_id and uses this as the
    root_query_id for all subsequent queries executed within that procedure. This allows
    us to trace which queries belong to a specific stored procedure execution and build
    table-level lineage by aggregating inputs/outputs from all related queries.
    """

    def __init__(self, platform: str, shared_connection: Optional[Any] = None):
        self.platform = platform
        self.report = StoredProcLineageReport()

        # { root_query_id -> StoredProcExecutionLineage }
        self._stored_proc_execution_lineage: FileBackedDict[
            StoredProcExecutionLineage
        ] = FileBackedDict(shared_connection, tablename="stored_proc_lineage")

    def add_stored_proc_call(self, call: StoredProcCall) -> None:
        """Add a stored procedure call to track."""
        self._stored_proc_execution_lineage[call.snowflake_root_query_id] = (
            StoredProcExecutionLineage(
                call=call,
                # Will be populated by subsequent queries.
                inputs=[],
                outputs=[],
            )
        )
        self.report.num_stored_proc_calls += 1

    def add_related_query(self, query: PreparsedQuery) -> bool:
        """Add a query that might be related to a stored procedure execution.

        Returns True if the query was added to a stored procedure execution, False otherwise.
        """
        snowflake_root_query_id = (query.extra_info or {}).get(
            "snowflake_root_query_id"
        )

        if snowflake_root_query_id:
            if snowflake_root_query_id not in self._stored_proc_execution_lineage:
                self.report.num_related_queries_without_proc_call += 1
                return False

            stored_proc_execution = self._stored_proc_execution_lineage.for_mutation(
                snowflake_root_query_id
            )
            stored_proc_execution.inputs.extend(query.upstreams)
            if query.downstream is not None:
                stored_proc_execution.outputs.append(query.downstream)
            self.report.num_related_queries += 1
            return True

        return False

    def build_merged_lineage_entries(self) -> Iterable[PreparsedQuery]:
        # For stored procedures, we can only get table-level lineage from the audit log.
        # We represent these as PreparsedQuery objects for now. Eventually we'll want to
        # create dataJobInputOutput lineage instead.

        for stored_proc_execution in self._stored_proc_execution_lineage.values():
            if not stored_proc_execution.inputs:
                self.report.num_stored_proc_calls_with_no_inputs += 1
                continue

            if not stored_proc_execution.outputs:
                self.report.num_stored_proc_calls_with_no_outputs += 1
                # Still continue to generate lineage for cases where we have inputs but no outputs

            for downstream in stored_proc_execution.outputs:
                stored_proc_query_id = get_query_fingerprint(
                    stored_proc_execution.call.query_text,
                    self.platform,
                    fast=True,
                    secondary_id=downstream,
                )

                lineage_entry = PreparsedQuery(
                    query_id=stored_proc_query_id,
                    query_text=stored_proc_execution.call.query_text,
                    upstreams=stored_proc_execution.inputs,
                    downstream=downstream,
                    query_count=0,
                    user=stored_proc_execution.call.user,
                    timestamp=stored_proc_execution.call.timestamp,
                )

                self.report.num_stored_proc_lineage_entries += 1
                yield lineage_entry

    def close(self) -> None:
        self._stored_proc_execution_lineage.close()
