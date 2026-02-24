from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Inspector

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.doris.doris_dialect import DorisDialect
from datahub.ingestion.source.sql.doris.doris_source import DorisConfig, DorisSource


def test_doris_uses_native_dialect():
    config = DorisConfig(host_port="localhost:9030", database="test")
    assert config.scheme == "doris+pymysql"

    source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)
    assert source.config.scheme == "doris+pymysql"

    url_str = config.get_sql_alchemy_url()
    assert url_str.startswith("doris+pymysql://")

    try:
        engine = create_engine(url_str)
        assert engine.dialect.name == "doris"
        assert isinstance(engine.dialect, DorisDialect)
    except Exception:
        pass


class TestDorisConfig:
    def test_scheme_validator_corrects_mysql_scheme(self):
        config_dict = {
            "host_port": "localhost:9030",
            "scheme": "mysql+pymysql",
        }
        config = DorisConfig.model_validate(config_dict)
        assert config.scheme == "doris+pymysql"


class TestDorisSourceMethods:
    def test_create_classmethod_uses_doris_config(self):
        config_dict = {
            "host_port": "localhost:9030",
            "database": "test",
        }
        ctx = PipelineContext(run_id="test")
        source = DorisSource.create(config_dict, ctx)

        assert isinstance(source.config, DorisConfig)
        assert source.config.scheme == "doris+pymysql"

    @pytest.mark.parametrize(
        "input_view_def,expected_output",
        [
            (
                "CREATE VIEW v AS SELECT `internal`.`dorisdb`.`customers`.`id` FROM `internal`.`dorisdb`.`customers`",
                "CREATE VIEW v AS SELECT `dorisdb`.`customers`.`id` FROM `dorisdb`.`customers`",
            ),
            (
                "CREATE VIEW v AS SELECT * FROM `internal`.`internal`.`table`",
                "CREATE VIEW v AS SELECT * FROM `internal`.`table`",
            ),
            (
                "SELECT * FROM `internal`.`db`.`internal_users`",
                "SELECT * FROM `db`.`internal_users`",
            ),
            (
                "FROM `internal`.`dorisdb`.`customers` JOIN `internal`.`dorisdb`.`orders`",
                "FROM `dorisdb`.`customers` JOIN `dorisdb`.`orders`",
            ),
            (
                "SELECT `internal`.`db`.`c`.`col` FROM `internal`.`db`.`customers` AS c",
                "SELECT `db`.`c`.`col` FROM `db`.`customers` AS c",
            ),
            (
                "SELECT COUNT(`internal`.`dorisdb`.`orders`.`id`), SUM(`internal`.`dorisdb`.`orders`.`amount`) FROM `internal`.`dorisdb`.`orders`",
                "SELECT COUNT(`dorisdb`.`orders`.`id`), SUM(`dorisdb`.`orders`.`amount`) FROM `dorisdb`.`orders`",
            ),
            (
                "WHERE EXISTS(SELECT 1 FROM `internal`.`dorisdb`.`products`)",
                "WHERE EXISTS(SELECT 1 FROM `dorisdb`.`products`)",
            ),
            (
                "FROM `internal`.`db`.`t1`,`internal`.`db`.`t2`,`internal`.`db`.`t3`",
                "FROM `db`.`t1`,`db`.`t2`,`db`.`t3`",
            ),
        ],
    )
    def test_view_definition_catalog_prefix_stripping(
        self, input_view_def, expected_output
    ):
        from datahub.ingestion.source.sql.doris.doris_source import (
            _DORIS_CATALOG_PREFIX_PATTERN,
        )

        result = _DORIS_CATALOG_PREFIX_PATTERN.sub("", input_view_def)
        assert result == expected_output

    def test_get_database_list_with_config_database(self):
        config = DorisConfig(host_port="localhost:9030", database="my_database")
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

        mock_inspector = MagicMock(spec=Inspector)
        mock_inspector.get_schema_names.return_value = ["db1", "db2", "db3"]

        result = source._get_database_list(mock_inspector)

        assert result == ["my_database"]
        mock_inspector.get_schema_names.assert_not_called()

    def test_get_database_list_from_inspector(self):
        config = DorisConfig(host_port="localhost:9030")
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

        mock_inspector = MagicMock(spec=Inspector)
        mock_inspector.get_schema_names.return_value = ["db1", "db2", "db3"]

        result = source._get_database_list(mock_inspector)

        assert result == ["db1", "db2", "db3"]
        mock_inspector.get_schema_names.assert_called_once()

    def test_get_inspectors_happy_path(self):
        config = DorisConfig(host_port="localhost:9030")
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

        from unittest.mock import patch

        with (
            patch(
                "datahub.ingestion.source.sql.doris.doris_source.create_engine"
            ) as mock_create,
            patch(
                "datahub.ingestion.source.sql.doris.doris_source.inspect"
            ) as mock_inspect,
        ):
            mock_engine = MagicMock()
            mock_create.return_value = mock_engine
            mock_conn = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_conn

            mock_main_inspector = MagicMock(spec=Inspector)
            mock_main_inspector.get_schema_names.return_value = ["db1", "db2"]
            mock_db_inspector = MagicMock(spec=Inspector)

            mock_inspect.side_effect = [
                mock_main_inspector,
                mock_db_inspector,
                mock_db_inspector,
            ]

            inspectors = list(source.get_inspectors())

            assert len(inspectors) == 2
            assert mock_engine.dispose.call_count == 2

    def test_get_inspectors_exception_handling(self):
        config = DorisConfig(host_port="localhost:9030")
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

        from unittest.mock import patch

        with (
            patch(
                "datahub.ingestion.source.sql.doris.doris_source.create_engine"
            ) as mock_create,
            patch(
                "datahub.ingestion.source.sql.doris.doris_source.inspect"
            ) as mock_inspect,
        ):
            mock_main_engine = MagicMock()
            mock_main_conn = MagicMock()
            mock_main_engine.connect.return_value.__enter__.return_value = (
                mock_main_conn
            )

            mock_main_inspector = MagicMock(spec=Inspector)
            mock_main_inspector.get_schema_names.return_value = ["db1"]

            mock_inspect.return_value = mock_main_inspector

            mock_create.side_effect = [
                mock_main_engine,
                Exception("Connection failed"),
            ]

            inspectors = list(source.get_inspectors())

            assert len(inspectors) == 0
            assert len(source.report.failures) > 0
