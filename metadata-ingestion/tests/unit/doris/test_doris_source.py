from typing import Any, Generator, Iterator, List, cast
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import INTEGER, create_engine
from sqlalchemy.engine import Connection, Inspector
from sqlalchemy.exc import OperationalError, PendingRollbackError, ProgrammingError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.doris.doris_dialect import (
    DorisDialect,
    ReflectionFallback,
)
from datahub.ingestion.source.sql.doris.doris_source import (
    DorisConfig,
    DorisSource,
)
from datahub.ingestion.source.sql.mysql import MySQLSource
from datahub.metadata.schema_classes import SchemaMetadataClass


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

    @pytest.mark.parametrize(
        "catalog",
        ["iceberg`; DROP TABLE t; --", "1_catalog", "cata log"],
    )
    def test_invalid_catalog_rejected(self, catalog):
        with pytest.raises(ValueError, match="not a valid Doris identifier"):
            DorisConfig(host_port="localhost:9030", catalog=catalog)

    def test_invalid_catalog_rejected_from_qualified_database(self):
        with pytest.raises(ValueError, match="not a valid Doris identifier"):
            DorisConfig(host_port="localhost:9030", database="bad catalog.db_ods")

    def test_database_that_is_not_a_catalog_pair_is_left_alone(self):
        config = DorisConfig(host_port="localhost:9030", database="a.b.c")
        assert config.catalog is None
        assert config.database == "a.b.c"

    def test_hyphenated_catalog_accepted(self):
        config = DorisConfig(host_port="localhost:9030", catalog="iceberg-catalog")
        assert config.catalog == "iceberg-catalog"


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

    @pytest.mark.parametrize(
        "catalog,view_definition,expected",
        [
            (
                None,
                "SELECT * FROM `internal`.`db_ods`.`orders`",
                "SELECT * FROM `db_ods`.`orders`",
            ),
            (
                "iceberg_catalog",
                "SELECT * FROM `iceberg_catalog`.`db_ods`.`orders`",
                "SELECT * FROM `db_ods`.`orders`",
            ),
        ],
    )
    def test_view_definition_strips_the_active_catalog(
        self, catalog, view_definition, expected
    ):
        config = DorisConfig(host_port="localhost:9030", catalog=catalog)
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

        with patch.object(
            MySQLSource, "_get_view_definition", return_value=view_definition
        ):
            result = source._get_view_definition(MagicMock(), "db_ods", "v_orders")

        assert result == expected
        assert source.report.cross_catalog_views_skipped == 0

    def test_view_definition_skips_lineage_across_catalogs(self):
        # `internal`.`db_ods`.`orders` names a table in another catalog; stripping
        # the prefix would resolve it against iceberg_catalog's db_ods instead.
        config = DorisConfig(host_port="localhost:9030", catalog="iceberg_catalog")
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

        with patch.object(
            MySQLSource,
            "_get_view_definition",
            return_value=(
                "SELECT * FROM `iceberg_catalog`.`db_ods`.`orders` "
                "JOIN `internal`.`db_ods`.`customers`"
            ),
        ):
            result = source._get_view_definition(MagicMock(), "db_ods", "v_orders")

        assert result == ""
        assert source.report.cross_catalog_views_skipped == 1
        assert any("internal" in str(warning) for warning in source.report.warnings)

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
            # The listing engine plus one per database, all disposed.
            assert mock_engine.dispose.call_count == 3
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

    def test_get_inspectors_reports_reflection_fallbacks(self):
        """A table reflected from DESCRIBE loses its keys, so it has to reach
        the report rather than only the logs."""
        config = DorisConfig(host_port="localhost:9030", database="db1")
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

        dialect = DorisDialect()
        dialect.reflection_fallbacks["`db1`.`my_async_mv`"] = ReflectionFallback(
            error="not support async materialized view", expected=True
        )

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

            list_engine.connect.return_value.__enter__.return_value = _mock_list_conn()
            db_conn = MagicMock()
            db_conn.dialect = dialect
            db_engine.connect.return_value = db_conn

            mock_inspect.return_value = MagicMock(spec=Inspector)

            list(source.get_inspectors())

        # Bound to the Connection, not the Engine: the source checks
        # inspector.bind.invalidated to tell a dropped connection from other errors.
        assert mock_inspect.call_args_list[-1].args[0] is db_conn
        assert source.report.tables_reflected_without_keys == 1
        warnings = [str(warning) for warning in source.report.warnings]
        assert any("Table reflected without keys" in w for w in warnings)
        assert any("my_async_mv" in w for w in warnings)
        # Drained, so a second database cannot re-report the first one's tables.
        assert dialect.reflection_fallbacks == {}

    def test_reflection_fallbacks_reported_on_early_generator_close(self):
        """A pipeline abort closes the generator at the yield, which must still report
        the degradations that database already recorded."""
        config = DorisConfig(host_port="localhost:9030", database="db1")
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

        dialect = DorisDialect()
        dialect.reflection_fallbacks["`db1`.`my_async_mv`"] = ReflectionFallback(
            error="not support async materialized view", expected=True
        )

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

            list_engine.connect.return_value.__enter__.return_value = _mock_list_conn()
            db_conn = MagicMock()
            db_conn.dialect = dialect
            db_engine.connect.return_value = db_conn

            mock_inspect.return_value = MagicMock(spec=Inspector)

            # get_inspectors is declared Iterable, but the drain-on-close behaviour
            # under test is specifically generator semantics.
            inspectors = cast(Generator[Inspector, None, None], source.get_inspectors())
            next(inspectors)
            inspectors.close()

        assert source.report.tables_reflected_without_keys == 1
        assert dialect.reflection_fallbacks == {}

    def test_each_database_reports_only_its_own_fallbacks(self):
        """Two databases must accumulate into the counter without either re-reporting
        the other's tables."""
        config = DorisConfig(host_port="localhost:9030")
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

        dialects = []
        for db in ("db1", "db2"):
            dialect = DorisDialect()
            dialect.reflection_fallbacks[f"`{db}`.`mv`"] = ReflectionFallback(
                error="not support async materialized view", expected=True
            )
            dialects.append(dialect)

        with (
            patch(
                "datahub.ingestion.source.sql.doris.doris_source.create_engine"
            ) as mock_create,
            patch(
                "datahub.ingestion.source.sql.doris.doris_source.inspect"
            ) as mock_inspect,
        ):
            list_engine = MagicMock()
            db_engines = [MagicMock(), MagicMock()]
            mock_create.side_effect = [list_engine, *db_engines]

            list_engine.connect.return_value.__enter__.return_value = _mock_list_conn()
            for db_engine, dialect in zip(db_engines, dialects, strict=True):
                db_conn = MagicMock()
                db_conn.dialect = dialect
                db_engine.connect.return_value = db_conn

            main_inspector = MagicMock(spec=Inspector)
            main_inspector.get_schema_names.return_value = ["db1", "db2"]
            mock_inspect.side_effect = [
                main_inspector,
                MagicMock(spec=Inspector),
                MagicMock(spec=Inspector),
            ]

            assert len(list(source.get_inspectors())) == 2

        assert source.report.tables_reflected_without_keys == 2
        contexts = " ".join(str(warning) for warning in source.report.warnings)
        assert contexts.count("`db1`.`mv`") == 1
        assert contexts.count("`db2`.`mv`") == 1

    def test_unexpected_reflection_error_gets_its_own_warning(self):
        """An unexpected failure must not be filed under the routine-degradation
        heading an operator learns to ignore."""
        config = DorisConfig(host_port="localhost:9030", database="db1")
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

        dialect = DorisDialect()
        dialect.reflection_fallbacks["`db1`.`t`"] = ReflectionFallback(
            error="SHOW command denied", expected=False
        )
        dialect.type_overlay_failures["`db1`.`u`"] = "Connection lost"

        conn = MagicMock()
        conn.dialect = dialect
        source._report_reflection_fallbacks(conn)

        titles = [w.title for w in source.report.warnings]
        assert "Table reflected without keys after an unexpected error" in titles
        assert "Doris column types unavailable" in titles
        assert source.report.tables_with_unreflected_types == 1

    def test_comment_lookup_failure_reported_per_database(self):
        """Descriptions missing from a whole database look like undocumented tables
        unless the report says the lookup failed."""
        config = DorisConfig(host_port="localhost:9030")
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

        dialects = []
        for db in ("db1", "db2"):
            dialect = DorisDialect()
            dialect.comment_lookup_failures[db] = "Access denied"
            conn = MagicMock()
            conn.dialect = dialect
            source._report_reflection_fallbacks(conn)
            dialects.append(dialect)

        assert source.report.databases_without_comments == 2
        assert len(source.report.warnings) == 1
        contexts = " ".join(str(warning) for warning in source.report.warnings)
        assert contexts.count("db1: Access denied") == 1
        assert contexts.count("db2: Access denied") == 1
        assert all(dialect.comment_lookup_failures == {} for dialect in dialects)

    def test_only_the_metadata_pass_reads_comments_and_reports(self):
        """Profiling reflects every table again but never uses comments, and its
        reflection degradations were already reported by the metadata pass."""
        config = DorisConfig(host_port="localhost:9030", database="db1")
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)
        engines: List[MagicMock] = []

        def make_engine(url: str, **kwargs: Any) -> MagicMock:
            engine = MagicMock()
            engine.dialect = DorisDialect()
            engine.dialect.reflection_fallbacks["`db1`.`my_async_mv`"] = (
                ReflectionFallback(error="async materialized view", expected=True)
            )
            engine.connect.return_value.dialect = engine.dialect
            engine.connect.return_value.__enter__.return_value = _mock_list_conn()
            engines.append(engine)
            return engine

        def profile_each_database(self: DorisSource) -> Iterator[Any]:
            for _ in self.get_inspectors():
                yield from ()

        with (
            patch(
                "datahub.ingestion.source.sql.doris.doris_source.create_engine",
                side_effect=make_engine,
            ),
            patch("datahub.ingestion.source.sql.doris.doris_source.inspect"),
            patch.object(MySQLSource, "get_profiling_internal", profile_each_database),
        ):
            list(source.get_inspectors())
            list(source.get_profiling_internal())
            list(source.get_inspectors())

        # Each pass opens a listing engine, then one per database.
        db_dialects = [engine.dialect for engine in engines[1::2]]
        assert [dialect.read_comments for dialect in db_dialects] == [
            True,
            False,
            True,
        ]
        assert source.report.tables_reflected_without_keys == 2

    @staticmethod
    def _inspector_losing_connection_on(method: str) -> MagicMock:
        inspector = MagicMock(spec=Inspector)
        inspector.bind = MagicMock(spec=Connection, invalidated=False)
        inspector.get_columns.return_value = [
            {"name": "col_a", "type": INTEGER(), "nullable": True, "comment": None}
        ]
        inspector.get_table_comment.return_value = {"text": None}
        inspector.get_pk_constraint.return_value = {}
        inspector.get_foreign_keys.return_value = []
        inspector.get_view_definition.return_value = "SELECT 1"

        def drop_connection(*args: Any, **kwargs: Any) -> None:
            inspector.bind.invalidated = True
            raise PendingRollbackError("Can't reconnect until invalid transaction")

        def rollback() -> None:
            inspector.bind.invalidated = False

        inspector.bind.rollback.side_effect = rollback

        getattr(inspector, method).side_effect = drop_connection
        return inspector

    @pytest.mark.parametrize(
        "method",
        [
            # The base class would emit the table with no columns, overwriting its
            # schema in DataHub.
            "get_columns",
            # Read after the columns, as a separate round trip.
            "get_table_comment",
        ],
    )
    def test_lost_connection_fails_the_table_rather_than_emptying_or_deleting_it(
        self, method
    ):
        config = DorisConfig(host_port="localhost:9030")
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)
        inspector = self._inspector_losing_connection_on(method)

        workunits = list(
            source._process_table(
                "db1.my_table", inspector, "db1", "my_table", source.config, None
            )
        )

        assert not any(wu.get_aspect_of_type(SchemaMetadataClass) for wu in workunits)
        # loop_tables only warns about a table that raises, which would leave
        # stale-entity removal free to delete it; the failure stops that.
        assert len(source.report.failures) == 1
        failure = str(source.report.failures)
        assert "db1.my_table" in failure
        assert "Can't reconnect" in failure
        # Rolled back so the next table can reconnect.
        assert inspector.bind.invalidated is False

    def test_lost_connection_fails_a_view_too(self):
        config = DorisConfig(host_port="localhost:9030")
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)
        inspector = self._inspector_losing_connection_on("get_columns")

        workunits = list(
            source._process_view(
                "db1.my_view", inspector, "db1", "my_view", source.config
            )
        )

        assert not any(wu.get_aspect_of_type(SchemaMetadataClass) for wu in workunits)
        assert len(source.report.failures) == 1
        assert inspector.bind.invalidated is False

    @pytest.mark.parametrize("kind", ["table", "view"])
    def test_errors_on_a_live_connection_are_not_failures(self, kind):
        """Only a dropped connection fails the run; any other error keeps the base
        class's per-table warning and stale-entity removal."""
        config = DorisConfig(host_port="localhost:9030")
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)
        inspector = self._inspector_losing_connection_on("get_columns")
        inspector.get_columns.side_effect = None
        error = ProgrammingError("SELECT", {}, Exception("Access denied"))
        inspector.get_pk_constraint.side_effect = error
        inspector.get_view_definition.side_effect = error

        with pytest.raises(ProgrammingError):
            if kind == "table":
                list(
                    source._process_table(
                        "db1.t", inspector, "db1", "t", source.config, None
                    )
                )
            else:
                list(
                    source._process_view("db1.t", inspector, "db1", "t", source.config)
                )

        assert len(source.report.failures) == 0
        inspector.bind.rollback.assert_not_called()

    def test_lost_connection_costs_only_the_table_it_happened_on(self):
        config = DorisConfig(host_port="localhost:9030")
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)
        inspector = self._inspector_losing_connection_on("get_columns")
        inspector.get_table_names.return_value = ["t1", "t2", "t3"]
        inspector.get_columns.side_effect = None
        columns = inspector.get_columns.return_value

        def get_columns(table: str, schema: str) -> List[Any]:
            if inspector.bind.invalidated:
                raise PendingRollbackError("Can't reconnect until rolled back")
            if table == "t2":
                inspector.bind.invalidated = True
                raise PendingRollbackError("Lost connection")
            return columns

        inspector.get_columns.side_effect = get_columns

        with (
            patch.object(
                source, "get_identifier", side_effect=lambda schema, entity, **_: entity
            ),
            patch.object(source, "get_db_name", return_value="db1"),
        ):
            workunits = list(source.loop_tables(inspector, "db1", source.config))

        field_counts = {}
        for wu in workunits:
            schema_metadata = wu.get_aspect_of_type(SchemaMetadataClass)
            if schema_metadata is not None:
                field_counts[wu.get_urn().split(",")[1]] = len(schema_metadata.fields)
        assert field_counts == {"t1": 1, "t3": 1}
        assert len(source.report.failures) == 1
        assert "t2" in str(source.report.failures)
        # Reported once, as the failure, not again as a loop_tables warning.
        assert "t2" not in str(source.report.warnings)

    def test_column_errors_on_a_live_connection_skip_the_table(self):
        """Unlike the base class, the table is not emitted with an empty schema,
        which would overwrite the good one."""
        config = DorisConfig(host_port="localhost:9030")
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)
        inspector = MagicMock(spec=Inspector)
        inspector.get_columns.side_effect = ProgrammingError(
            "DESCRIBE", {}, Exception("Access denied")
        )
        inspector.bind = MagicMock(spec=Connection, invalidated=False)

        with pytest.raises(ProgrammingError):
            source._get_columns("db1.my_table", inspector, "db1", "my_table")
        assert len(source.report.failures) == 0

    def test_table_without_columns_is_warned_about(self):
        config = DorisConfig(host_port="localhost:9030")
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)
        inspector = MagicMock(spec=Inspector)
        inspector.get_columns.return_value = []

        assert source._get_columns("db1.my_table", inspector, "db1", "my_table") == []
        assert "db1.my_table" in str(source.report.warnings)

    def test_failed_reconnect_skips_the_rest_of_the_database(self):
        """Each remaining table would otherwise wait out its own connect timeouts
        and fail the same way."""
        config = DorisConfig(host_port="localhost:9030")
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)
        inspector = self._inspector_losing_connection_on("get_columns")
        inspector.get_table_names.return_value = ["t1", "t2", "t3"]
        inspector.bind.execute.side_effect = OperationalError(
            "SELECT 1", {}, Exception("Can't connect to MySQL server")
        )

        with (
            patch.object(
                source, "get_identifier", side_effect=lambda schema, entity, **_: entity
            ),
            patch.object(source, "get_db_name", return_value="db1"),
        ):
            list(source.loop_tables(inspector, "db1", source.config))

        assert inspector.get_columns.call_count == 1
        assert len(source.report.failures) == 2
        assert "Can't connect" in str(source.report.failures)
        assert source.report.tables_skipped_after_connection_loss == 2

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
