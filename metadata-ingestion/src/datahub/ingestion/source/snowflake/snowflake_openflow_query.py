from typing import Optional

ACCOUNT_USAGE = "SNOWFLAKE.ACCOUNT_USAGE"
DEPLOYMENT_HISTORY = f"{ACCOUNT_USAGE}.OPENFLOW_DEPLOYMENT_HISTORY"
RUNTIME_HISTORY = f"{ACCOUNT_USAGE}.OPENFLOW_RUNTIME_HISTORY"
CONNECTOR_HISTORY = f"{ACCOUNT_USAGE}.OPENFLOW_CONNECTOR_HISTORY"
PAGE_SIZE = 1000


def _history_query(view: str, cursor: Optional[str]) -> str:
    # Manual cursor on CREATED_ON. OFFSET is never used: these views grow while
    # being read, and OFFSET would skip and duplicate rows across pages.
    #
    # DELETED_ON is deliberately NOT filtered here. The deleted rows are the
    # input to deletion detection; filtering them in SQL would throw away the
    # only observable signal that an object is gone.
    # `>=`, not `>`: a strict cursor drops every row sharing the exact CREATED_ON
    # of a page boundary. The overlap row it re-fetches is free -- merge_show_and_history
    # is keyed on row.key and collapses it -- whereas a dropped row is invisible,
    # surfacing only as a deleted object that stays live in DataHub.
    predicate = f"WHERE CREATED_ON >= '{cursor}'" if cursor else ""
    return f"""
SELECT *
FROM {view}
{predicate}
ORDER BY CREATED_ON
LIMIT {PAGE_SIZE}
""".strip()


class SnowflakeOpenflowQuery:
    PAGE_SIZE = PAGE_SIZE

    @staticmethod
    def show_deployments() -> str:
        return "SHOW OPENFLOW DEPLOYMENTS"

    @staticmethod
    def show_runtimes() -> str:
        return "SHOW OPENFLOW RUNTIMES"

    @staticmethod
    def show_connectors() -> str:
        return "SHOW OPENFLOW CONNECTORS"

    @staticmethod
    def deployment_history(cursor: Optional[str]) -> str:
        return _history_query(DEPLOYMENT_HISTORY, cursor)

    @staticmethod
    def runtime_history(cursor: Optional[str]) -> str:
        return _history_query(RUNTIME_HISTORY, cursor)

    @staticmethod
    def connector_history(cursor: Optional[str]) -> str:
        return _history_query(CONNECTOR_HISTORY, cursor)

    @staticmethod
    def describe_connector(fqn: str) -> str:
        # DESCRIBE, not SHOW: CONNECTOR_URL is one of the columns SHOW does not
        # return (measured -- DESCRIBE gives 20 columns against SHOW's 16). It
        # is the deep link into the connector's NiFi canvas.
        #
        # IDENTIFIER() takes the quoted name as a *string literal*, so the
        # single quotes that delimit it are doubled; `fqn` has already doubled
        # any embedded double quote for the identifier itself.
        return f"DESCRIBE OPENFLOW CONNECTOR IDENTIFIER('{fqn.replace(chr(39), chr(39) * 2)}')"

    @staticmethod
    def get_stage_file_to_local(
        version_location_uri: str, filename: str, local_dir: str
    ) -> str:
        # GET, not `SELECT $1 FROM '<uri>'`: the SELECT form parses the file
        # under Snowflake's default CSV file format, so $1 is only the text up
        # to the first comma -- confirmed against a live connector's
        # config.json, where it silently returned 24 of 2921 bytes. An inline
        # `FILE_FORMAT=>(TYPE=JSON)` is rejected as a non-constant table
        # function argument, and a named file format is DDL a read-only
        # metadata role should not need. GET downloads the file whole with no
        # such assumption.
        #
        # The URI is used verbatim as reported by the connector row. Substituting
        # a guessed version segment fails with errno 99112.
        return f"GET '{version_location_uri}{filename}' 'file://{local_dir}'"
