import pathlib
from typing import Any, Optional

import pytest
from sqlalchemy.sql.elements import TextClause

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.sql.oracle import OracleConfig
from tests.test_helpers import mce_helpers


class OracleSourceMockDataBase:
    """
    Extend this class if needed to mock data in different way
    """

    MOCK_DATA = {
        "SELECT username FROM dba_users ORDER BY username": (["schema1"], ["schema2"]),
        "schema1": (["test1"], ["test2"]),
        "schema2": (["test3"], ["test4"]),
    }

    def get_data(self, *arg: Any, **kwargs: Any) -> Any:
        assert arg or kwargs
        key: Optional[str] = None

        if arg and isinstance(arg[0], str):
            key = arg[0]

        if arg and isinstance(arg[0], TextClause) and kwargs:
            key = kwargs.get("owner")
        # key should present in MOCK_DATA
        assert key in OracleSourceMockDataBase.MOCK_DATA

        return OracleSourceMockDataBase.MOCK_DATA[key]


class OracleTestCaseBase:
    """
    Extend this class if needed to create new a test-case for oracle source
    """

    def __init__(
        self,
        pytestconfig: pytest.Config,
        tmp_path: pathlib.Path,
        golden_file_name: str = "golden_test_ingest.json",
        output_file_name: str = "oracle_mce_output.json",
    ):
        self.pytestconfig = pytestconfig
        self.tmp_path = tmp_path
        self.golden_file_name = golden_file_name
        self.mces_output_file_name = output_file_name
        self.default_mock_data = OracleSourceMockDataBase()

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

    def get_default_recipe_config(self) -> OracleConfig:
        return OracleConfig(
            host_port=self.get_oracle_host_port(),
            database=self.get_database_name(),
            username=self.get_username(),
            password=self.get_password(),
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
        return "{}/{}".format(self.tmp_path, self.mces_output_file_name)

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
