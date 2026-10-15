# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pathlib
from typing import Any, List, Tuple

import sqlalchemy as sa

from datahub_actions.plugin.action.snowflake.snowflake_util import SnowflakeTagHelper


def _helper_on_sqlite(db_path: pathlib.Path) -> Tuple[SnowflakeTagHelper, List[str]]:
    engine = sa.create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE tags (value TEXT)")

    executed: List[str] = []

    # SQLite has no USE; record it and substitute a no-op so the rest of the
    # statement sequence runs against a real SQLAlchemy 2.0 engine.
    @sa.event.listens_for(engine, "before_cursor_execute", retval=True)
    def _rewrite_use(
        conn: Any, cursor: Any, statement: str, params: Any, context: Any, many: bool
    ) -> Tuple[str, Any]:
        executed.append(statement)
        if statement.startswith("USE "):
            return "SELECT 1", params
        return statement, params

    # Bypass __init__, which builds a Snowflake URL from a SnowflakeConfig.
    helper = SnowflakeTagHelper.__new__(SnowflakeTagHelper)
    helper.engine = engine
    return helper, executed


def test_run_query_executes_and_commits(tmp_path: pathlib.Path) -> None:
    helper, executed = _helper_on_sqlite(tmp_path / "tags.db")

    # The ":li" / ":tag" segments must reach the driver verbatim, not be parsed
    # as bind parameters.
    helper.run_query("my_db", "my_schema", "INSERT INTO tags VALUES ('urn:li:tag:pii')")

    assert executed[-2:] == [
        "USE my_db.my_schema;",
        "INSERT INTO tags VALUES ('urn:li:tag:pii')",
    ]
    # Read from a fresh connection: SQLAlchemy 2.0 does not autocommit, so the
    # write is only visible here if run_query committed it.
    with helper.engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT value FROM tags").scalars().all() == [
            "urn:li:tag:pii"
        ]
