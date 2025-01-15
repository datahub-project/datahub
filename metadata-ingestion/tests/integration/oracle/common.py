import pathlib
import re
from dataclasses import dataclass
from typing import Any, Optional, Tuple

import pytest
from sqlalchemy.sql.elements import TextClause

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.sql.oracle import OracleConfig
from tests.test_helpers import mce_helpers


@dataclass
class MockComment:
    comment: str = "Some mock comment here ..."

    def scalar(self):
        return self.comment


@dataclass
class MockViewDefinition:
    view_definition: str = """CREATE VIEW mock_view AS
                                  SELECT
                                      mock_column1,
                                      mock_column2
                                  FROM mock_table"""

    def scalar(self):
        return self.view_definition


@dataclass
class MockConstraints:
    constraint_name: str = "mock constraint name"
    constraint_type: str = "P"
    local_column: str = "mock column name"
    remote_table: str = "test1"
    remote_column: str = "mock column name 2"
    remote_owner: str = "schema1"
    loc_pos: int = 1
    rem_pos: int = 1
    search_condition: str = "mock search condition"
    delete_rule: str = "mock delete rule"

    def fetchall(self):
        return [
            (
                self.constraint_name,
                self.constraint_type,
                self.local_column,
                self.remote_table,
                self.remote_column,
                self.remote_owner,
                self.loc_pos,
                self.rem_pos,
                self.search_condition,
                self.delete_rule,
            )
        ]


@dataclass
class MockColumns:
    colname: str = "mock column name"
    coltype: str = "NUMBER"
    length: int = 0
    precision: Optional[str] = None
    scale: Optional[int] = None
    nullable: str = "Y"
    default: str = "mock default"
    comment: str = "mock comment for column"
    generated: str = "mock generated"
    default_on_nul: Optional[str] = None
    identity_options: Optional[str] = None

    def execute(self):
        return [
            [
                self.colname,
                self.coltype,
                self.length,
                self.precision,
                self.scale,
                self.nullable,
                self.default,
                self.comment,
                self.generated,
                self.default_on_nul,
                self.identity_options,
            ]
        ]


class OracleSourceMockDataBase:
    """
    Extend this class if needed to mock data in different way
    """

    MOCK_DATA = {
        "SELECT username": (["schema1"], ["schema2"]),
        "SELECT view_name": ([["view1"]]),
        "SELECT comments": MockComment(),
        "SELECT ac.constraint_name": MockConstraints(),
        "SELECT col.column_name": MockColumns().execute(),
        "SELECT text": MockViewDefinition(),
        "schema1": (["test1"], ["test2"]),
        "schema2": (["test3"], ["test4"]),
    }

    def get_data(self, *arg: Any, **kwargs: Any) -> Any:
        assert arg or kwargs
        key: Optional[str] = None

        if arg and isinstance(arg[0], str):
            key = arg[0]

        if arg and isinstance(arg[0], TextClause) and not kwargs:
            key = str(arg[0])

        if arg and isinstance(arg[0], TextClause) and kwargs:
            key = kwargs.get("owner")

        # key should present in MOCK_DATA
        assert key is not None
        key = re.sub(" +", " ", key.replace("\n", " ").replace("\r", " "))
        res = {mock_key: mock_key in key for mock_key in self.MOCK_DATA.keys()}

        assert any(res.values())

        return OracleSourceMockDataBase.MOCK_DATA[
            [mock_key for mock_key, mock_value in res.items() if mock_value][0]
        ]


class OracleTestCaseBase:
    """
    Extend this class if needed to create new a test-case for oracle source
    """

    def __init__(
        self,
        pytestconfig: pytest.Config,
        tmp_path: pathlib.Path,
        golden_file_name: str = "",
        output_file_name: str = "",
        add_database_name_to_urn: bool = False,
    ):
        self.pytestconfig = pytestconfig
        self.tmp_path = tmp_path
        self.golden_file_name = golden_file_name
        self.mces_output_file_name = output_file_name
        self.default_mock_data = OracleSourceMockDataBase()
        self.add_database_name_to_urn = add_database_name_to_urn

    def get_recipe_source(self) -> dict:
        return {
            "source": {
                "type": "oracle",
                "config": {
                    **self.get_default_recipe_config().dict(),
                },
            }
        }

    def get_username(self) -> str:
        return "foo"

    def get_password(self) -> str:
        return "bar"

    def get_oracle_host_port(self) -> str:
        return "fake:port"

    def get_database_name(self) -> str:
        return "OraDoc"

    def get_data_dictionary_mode(self) -> str:
        return "DBA"

    def get_server_version_info(self) -> Tuple[int]:
        return (13,)

    def get_add_database_name_to_urn_flag(self) -> bool:
        return self.add_database_name_to_urn

    def get_default_recipe_config(self) -> OracleConfig:
        return OracleConfig(
            host_port=self.get_oracle_host_port(),
            database=self.get_database_name(),
            username=self.get_username(),
            password=self.get_password(),
            data_dictionary_mode=self.get_data_dictionary_mode(),
            add_database_name_to_urn=self.add_database_name_to_urn,
        )

    def get_test_resource_dir(
        self,
    ) -> pathlib.Path:
        return self.pytestconfig.rootpath / "tests/integration/oracle"

    def get_recipe_sink(self, output_path: str) -> dict:
        return {
            "sink": {
                "type": "file",
                "config": {
                    "filename": output_path,
                },
            }
        }

    def get_output_mce_path(self):
        return f"{self.tmp_path}/{self.mces_output_file_name}"

    def get_mock_data_impl(self):
        return self.default_mock_data

    def get_mock_data(self, *arg: Any, **kwargs: Any) -> Any:
        return self.get_mock_data_impl().get_data(*arg, **kwargs)

    def apply(self):
        output_path = self.get_output_mce_path()
        source_recipe = {
            **self.get_recipe_source(),
            **self.get_recipe_sink(output_path),
        }
        pipeline = Pipeline.create(source_recipe)
        pipeline.run()
        pipeline.raise_from_status()
        mce_helpers.check_golden_file(
            self.pytestconfig,
            output_path=output_path,
            golden_path="{}/{}".format(
                self.get_test_resource_dir(), self.golden_file_name
            ),
        )
