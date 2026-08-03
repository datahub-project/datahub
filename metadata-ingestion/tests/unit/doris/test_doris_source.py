from unittest.mock import MagicMock, patch

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


def _mock_list_conn(current_catalog: str = "internal") -> MagicMock:
    conn = MagicMock()

    def _execute(statement, *args, **kwargs):
        sql = str(statement)
        result = MagicMock()
        if "CURRENT_CATALOG" in sql:
            result.fetchone.return_value = (current_catalog,)
        else:
            result.fetchone.return_value = None
        return result

    conn.execute.side_effect = _execute
    return conn


class TestDorisConfig:
    def test_scheme_validator_corrects_mysql_scheme(self):
        config_dict = {
            "host_port": "localhost:9030",
            "scheme": "mysql+pymysql",
        }
        config = DorisConfig.model_validate(config_dict)
        assert config.scheme == "doris+pymysql"

    def test_catalog_and_database_split_from_qualified_database(self):
        config = DorisConfig.model_validate(
            {
                "host_port": "localhost:9030",
                "database": "iceberg_catalog.db_ods",
            }
        )
        assert config.catalog == "iceberg_catalog"
        assert config.database == "db_ods"

    def test_catalog_mismatch_with_qualified_database_raises(self):
        with pytest.raises(ValueError, match="does not match catalog"):
            DorisConfig.model_validate(
                {
                    "host_port": "localhost:9030",
                    "catalog": "hive_catalog",
                    "database": "iceberg_catalog.db_ods",
                }
            )

    def test_explicit_catalog_with_short_database(self):
        config = DorisConfig(
            host_port="localhost:9030",
            catalog="iceberg_catalog",
            database="db_ods",
        )
        assert config.catalog == "iceberg_catalog"
        assert config.database == "db_ods"


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
            mock_engine.connect.return_value.__enter__.return_value = _mock_list_conn(
                "internal"
            )

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
            db_urls = [c.args[0] for c in mock_create.call_args_list[1:]]
            assert all("/db1" in url or "/db2" in url for url in db_urls)
            assert all("internal." not in url for url in db_urls)

    def test_get_inspectors_external_catalog_uses_qualified_database(self):
        config = DorisConfig(
            host_port="localhost:9030",
            catalog="iceberg_catalog",
        )
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

        with (
            patch(
                "datahub.ingestion.source.sql.doris.doris_source.create_engine"
            ) as mock_create,
            patch(
                "datahub.ingestion.source.sql.doris.doris_source.inspect"
            ) as mock_inspect,
        ):
            list_engine = MagicMock()
            db_engine = MagicMock()
            mock_create.side_effect = [list_engine, db_engine, db_engine]

            list_conn = _mock_list_conn("iceberg_catalog")
            list_engine.connect.return_value.__enter__.return_value = list_conn
            db_engine.connect.return_value.__enter__.return_value = MagicMock()

            mock_main_inspector = MagicMock(spec=Inspector)
            mock_main_inspector.get_schema_names.return_value = ["db_ods", "db_dwd"]
            mock_db_inspector = MagicMock(spec=Inspector)
            mock_inspect.side_effect = [
                mock_main_inspector,
                mock_db_inspector,
                mock_db_inspector,
            ]

            inspectors = list(source.get_inspectors())

            assert len(inspectors) == 2
            switch_calls = [
                c
                for c in list_conn.execute.call_args_list
                if "SWITCH" in str(c.args[0])
            ]
            assert len(switch_calls) == 1
            assert "iceberg_catalog" in str(switch_calls[0].args[0])

            db_urls = [c.args[0] for c in mock_create.call_args_list[1:]]
            assert "iceberg_catalog.db_ods" in db_urls[0]
            assert "iceberg_catalog.db_dwd" in db_urls[1]

    def test_get_inspectors_preserves_detected_catalog_without_config(self):
        config = DorisConfig(host_port="localhost:9030")
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

        with (
            patch(
                "datahub.ingestion.source.sql.doris.doris_source.create_engine"
            ) as mock_create,
            patch(
                "datahub.ingestion.source.sql.doris.doris_source.inspect"
            ) as mock_inspect,
        ):
            list_engine = MagicMock()
            db_engine = MagicMock()
            mock_create.side_effect = [list_engine, db_engine]

            list_engine.connect.return_value.__enter__.return_value = _mock_list_conn(
                "iceberg_catalog"
            )
            db_engine.connect.return_value.__enter__.return_value = MagicMock()

            mock_main_inspector = MagicMock(spec=Inspector)
            mock_main_inspector.get_schema_names.return_value = ["db_ods"]
            mock_inspect.side_effect = [
                mock_main_inspector,
                MagicMock(spec=Inspector),
            ]

            inspectors = list(source.get_inspectors())

            assert len(inspectors) == 1
            assert "iceberg_catalog.db_ods" in mock_create.call_args_list[1].args[0]

    def test_get_db_name_strips_catalog_prefix(self):
        config = DorisConfig(
            host_port="localhost:9030",
            catalog="iceberg_catalog",
        )
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)
        source._session_catalog = "iceberg_catalog"

        inspector = MagicMock()
        inspector.engine.url.database = "iceberg_catalog.db_ods"

        assert source.get_db_name(inspector) == "db_ods"

    def test_get_inspectors_exception_handling(self):
        config = DorisConfig(host_port="localhost:9030")
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

        with (
            patch(
                "datahub.ingestion.source.sql.doris.doris_source.create_engine"
            ) as mock_create,
            patch(
                "datahub.ingestion.source.sql.doris.doris_source.inspect"
            ) as mock_inspect,
        ):
            mock_main_engine = MagicMock()
            mock_main_engine.connect.return_value.__enter__.return_value = (
                _mock_list_conn("internal")
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
            assert any("db1" in str(ctx) for ctx in source.report.failures[0].context)
