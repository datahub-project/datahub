import re
from typing import Iterable

from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)


def split_procedure_into_queries(sql: str) -> Iterable[str]:
    # cleanup syntax
    sql = sql.replace("= @RunID", "= 'hardcoded-run-id'")
    sql = sql.replace(".Union.", ".[Union].")

    # split on statements
    sql = sql.replace(",\n\n", ",\n")
    sql = sql.replace("GO", "\n\n")
    sql = re.sub(r"\ndeclare", "\n\ndeclare", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\ncreate\s+index", "\n\ncreate index", sql, flags=re.IGNORECASE)

    sqls = sql.split("\n\n")

    for i, chunk in enumerate(sqls):
        chunk = chunk.strip().rstrip(";") + "\n;"

        # If there's an "IF" statement with a begin and end, just take the inner part.
        # if (
        #     i == 12
        #     and file.stem == "Portland.Tribeca.sprDW_LiquidityTermDeposits-sanitised"
        # ):
        #     breakpoint()
        chunk = re.sub(
            r"IF.*BEGIN(.*)END",
            r"\1",
            chunk,
            flags=re.DOTALL | re.MULTILINE | re.IGNORECASE,
        )

        # Remove all lines starting with `IF`, `WHILE`, `BEGIN`, or `END`.
        chunk = re.sub(r"^IF.*$", "", chunk, flags=re.MULTILINE | re.IGNORECASE)
        chunk = re.sub(r"^BEGIN.*$", "", chunk, flags=re.MULTILINE | re.IGNORECASE)
        chunk = re.sub(r"^WHILE.*$", "", chunk, flags=re.MULTILINE | re.IGNORECASE)
        chunk = re.sub(r"^END$", "", chunk, flags=re.MULTILINE | re.IGNORECASE)
        chunk = re.sub(r"^END.*--.*$", "", chunk, flags=re.MULTILINE)

        chunk = chunk.strip()
        if not chunk or chunk == ";":
            continue

        yield chunk


# Is procedure handling generic enough to be added to SqlParsingAggregator?
def add_procedure_to_aggregator(
    *,
    aggregator: SqlParsingAggregator,
    procedure_code: str,
    default_db: str,
    default_schema: str,
    procedure_job_urn: str,
) -> None:
    for query in split_procedure_into_queries(procedure_code):
        aggregator.add_observed_query(
            observed=ObservedQuery(
                default_db=default_db, default_schema=default_schema, query=query
            )
        )
    # TODO: finalize and use data job urn as required
