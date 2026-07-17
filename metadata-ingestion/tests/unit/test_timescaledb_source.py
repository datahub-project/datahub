from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from unittest.mock import MagicMock, patch

from sqlalchemy.exc import DatabaseError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.postgres import PostgresSource
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource
from datahub.ingestion.source.sql.stored_procedures.base import BaseProcedure
from datahub.ingestion.source.sql.timescaledb import (
    ContinuousAggregate,
    Hypertable,
    HypertableDimension,
    JobExecution,
    RefreshPolicy,
    RetentionPolicy,
    TimescaleDBConfig,
    TimescaleDBEnvironment,
    TimescaleDBJob,
    TimescaleDBSource,
    _SchemaMetadata,
    format_timedelta_human_readable,
    safe_str_convert,
)
from datahub.metadata.schema_classes import (
    DataJobInfoClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRelationshipsClass,
    DataProcessInstanceRunEventClass,
    GlobalTagsClass,
    SubTypesClass,
)


def _base_config() -> Dict[str, Any]:
    return {
        "username": "user",
        "password": "password",
        "host_port": "localhost:5432",
        "database": "tsdb",
    }


def _aspects_of_type(workunits, aspect_class):
    return [
        wu
        for wu in workunits
        if hasattr(wu, "metadata")
        and hasattr(wu.metadata, "aspect")
        and isinstance(wu.metadata.aspect, aspect_class)
    ]


class TestTimescaleDBSource:
    @patch("datahub.ingestion.source.sql.postgres.source.create_engine")
    def test_timescaledb_extension_check(self, create_engine_mock):
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 1

        mock_conn.execute.return_value = mock_result
        mock_inspector.engine.connect.return_value.__enter__.return_value = mock_conn

        assert source._is_timescaledb_enabled(mock_inspector) is True

        # Cache should prevent a second query.
        assert source._is_timescaledb_enabled(mock_inspector) is True
        assert mock_conn.execute.call_count == 1

        source2 = TimescaleDBSource(config, PipelineContext(run_id="test2"))
        mock_result2 = MagicMock()
        mock_result2.rowcount = 0
        mock_conn2 = MagicMock()
        mock_conn2.execute.return_value = mock_result2
        mock_inspector2 = MagicMock()
        mock_inspector2.engine.connect.return_value.__enter__.return_value = mock_conn2

        assert source2._is_timescaledb_enabled(mock_inspector2) is False

    @patch("datahub.ingestion.source.sql.postgres.source.create_engine")
    def test_get_hypertables(self, create_engine_mock):
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        mock_conn = MagicMock()

        env_result = MagicMock()
        env_result.rowcount = 1

        hypertable_result = [
            {
                "hypertable_name": "sensor_data",
                "num_dimensions": 2,
                "num_chunks": 10,
                "compression_enabled": True,
                "dimensions": [
                    {
                        "column_name": "time",
                        "column_type": "TIMESTAMPTZ",
                        "time_interval": "7 days",
                    }
                ],
                "retention_policy": {"drop_after": "30 days"},
            }
        ]

        mock_conn.execute.side_effect = [env_result, hypertable_result]
        mock_inspector.engine.connect.return_value.__enter__.return_value = mock_conn

        hypertables = source._get_hypertables(mock_inspector, "public")

        assert "sensor_data" in hypertables
        assert hypertables["sensor_data"].num_dimensions == 2
        assert hypertables["sensor_data"].compression_enabled is True

    @patch("datahub.ingestion.source.sql.postgres.source.create_engine")
    def test_get_continuous_aggregates(self, create_engine_mock):
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        mock_conn = MagicMock()

        env_result = MagicMock()
        env_result.rowcount = 1

        cagg_result = [
            {
                "view_name": "hourly_metrics",
                "materialized_only": False,
                "compression_enabled": False,
                "hypertable_schema": "public",
                "hypertable_name": "sensor_data",
                "view_definition": "SELECT time_bucket('1 hour', time) AS hour...",
                "refresh_policy": {
                    "schedule_interval": "1 hour",
                    "config": {"start_offset": "2 hours", "end_offset": "1 hour"},
                },
            }
        ]

        mock_conn.execute.side_effect = [env_result, cagg_result]
        mock_inspector.engine.connect.return_value.__enter__.return_value = mock_conn

        caggs = source._get_continuous_aggregates(mock_inspector, "public")

        assert "hourly_metrics" in caggs
        assert caggs["hourly_metrics"].hypertable_name == "sensor_data"
        assert caggs["hourly_metrics"].refresh_policy is not None
        assert caggs["hourly_metrics"].refresh_policy.schedule_interval == "1 hour"

    @patch("datahub.ingestion.source.sql.postgres.source.create_engine")
    def test_get_jobs(self, create_engine_mock):
        config = TimescaleDBConfig.parse_obj(
            {**_base_config(), "include_background_jobs": True}
        )
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        mock_conn = MagicMock()

        env_result = MagicMock()
        env_result.rowcount = 1

        job_result = [
            {
                "job_id": 1001,
                "application_name": "Refresh Continuous Aggregate",
                "schedule_interval": "1 hour",
                "max_runtime": "5 minutes",
                "max_retries": 3,
                "retry_period": "5 minutes",
                "proc_schema": "_timescaledb_internal",
                "proc_name": "policy_refresh_continuous_aggregate",
                "scheduled": True,
                "fixed_schedule": False,
                "initial_start": None,
                "timezone": "UTC",
                "config": {"start_offset": "2 hours"},
                "hypertable_schema": "public",
                "hypertable_name": "hourly_metrics",
            }
        ]

        mock_conn.execute.side_effect = [env_result, job_result]
        mock_inspector.engine.connect.return_value.__enter__.return_value = mock_conn

        jobs = source._get_jobs(mock_inspector, "public")

        assert 1001 in jobs
        assert jobs[1001].proc_name == "policy_refresh_continuous_aggregate"
        assert jobs[1001].hypertable_name == "hourly_metrics"

    @patch("datahub.ingestion.source.sql.postgres.source.create_engine")
    def test_add_information_for_schema(self, create_engine_mock):
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        mock_conn = MagicMock()

        extension_result = MagicMock()
        extension_result.rowcount = 1
        env_detection_result = MagicMock()
        env_detection_result.rowcount = 1
        hypertables_result: list = []
        caggs_result: list = []

        mock_conn.execute.side_effect = [
            extension_result,
            env_detection_result,
            hypertables_result,
            caggs_result,
        ]

        mock_inspector.engine.connect.return_value.__enter__.return_value = mock_conn

        with patch.object(source, "get_db_name", return_value="tsdb"):
            source.add_information_for_schema(mock_inspector, "public")

        cache_key = "tsdb.public"
        assert cache_key in source._timescaledb_metadata_cache
        metadata = source._timescaledb_metadata_cache[cache_key]
        assert metadata.hypertables == {}
        assert metadata.continuous_aggregates == {}

    @patch("datahub.ingestion.source.sql.postgres.source.create_engine")
    def test_get_table_properties_for_hypertable(self, create_engine_mock):
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        hypertable = Hypertable(
            name="sensor_data",
            num_dimensions=2,
            num_chunks=10,
            compression_enabled=True,
            dimensions=[
                HypertableDimension(
                    column_name="time",
                    column_type="TIMESTAMPTZ",
                    time_interval="7 days",
                ),
                HypertableDimension(
                    column_name="device_id",
                    column_type="INTEGER",
                    num_partitions=4,
                ),
            ],
            retention_policy=RetentionPolicy(drop_after="30 days"),
        )

        source._timescaledb_metadata_cache["tsdb.public"] = _SchemaMetadata(
            hypertables={"sensor_data": hypertable},
        )

        mock_inspector = MagicMock()

        with (
            patch.object(
                PostgresSource,
                "get_table_properties",
                return_value=("Table description", {}, None),
            ),
            patch.object(source, "get_db_name", return_value="tsdb"),
        ):
            _, props, _ = source.get_table_properties(
                mock_inspector, "public", "sensor_data"
            )

        assert props["is_hypertable"] == "true"
        assert props["num_dimensions"] == "2"
        assert props["num_chunks"] == "10"
        assert props["compression_enabled"] == "True"
        assert props["dimension_0_column"] == "time"
        assert props["dimension_0_type"] == "TIMESTAMPTZ"
        assert props["dimension_0_interval"] == "7 days"
        assert props["dimension_1_column"] == "device_id"
        assert props["retention_period"] == "30 days"

    @patch("datahub.ingestion.source.sql.postgres.source.create_engine")
    def test_get_table_properties_for_continuous_aggregate(self, create_engine_mock):
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        cagg = ContinuousAggregate(
            name="hourly_metrics",
            materialized_only=False,
            compression_enabled=False,
            hypertable_schema="public",
            hypertable_name="sensor_data",
            view_definition="SELECT ...",
            refresh_policy=RefreshPolicy(
                schedule_interval="1 hour",
                config={
                    "start_offset": "2 hours",
                    "end_offset": "1 hour",
                },
            ),
        )

        source._timescaledb_metadata_cache["tsdb.public"] = _SchemaMetadata(
            continuous_aggregates={"hourly_metrics": cagg},
        )

        mock_inspector = MagicMock()

        with (
            patch.object(
                PostgresSource,
                "get_table_properties",
                return_value=("View description", {"is_view": "True"}, None),
            ),
            patch.object(source, "get_db_name", return_value="tsdb"),
        ):
            _, props, _ = source.get_table_properties(
                mock_inspector, "public", "hourly_metrics"
            )

        assert props["is_continuous_aggregate"] == "true"
        # The materialized + continuous_aggregate keys must be on the parent's
        # single DatasetPropertiesClass — emitting a second MCP would overwrite
        # the qualified name.
        assert props["materialized"] == "true"
        assert props["continuous_aggregate"] == "true"
        assert props["materialized_only"] == "False"
        assert props["source_hypertable"] == "public.sensor_data"
        assert props["refresh_interval"] == "1 hour"
        assert props["refresh_start_offset"] == "2 hours"
        assert props["refresh_end_offset"] == "1 hour"

    @patch("datahub.ingestion.source.sql.postgres.source.create_engine")
    def test_identifier_with_database(self, create_engine_mock):
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        identifier = source.get_identifier(
            schema="public", entity="sensor_data", inspector=mock_inspector
        )

        assert identifier == "tsdb.public.sensor_data"

    @patch("datahub.ingestion.source.sql.postgres.source.create_engine")
    def test_process_table_adds_hypertable_subtype(self, create_engine_mock):
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        hypertable = Hypertable(name="sensor_data")
        source._timescaledb_metadata_cache["tsdb.public"] = _SchemaMetadata(
            hypertables={"sensor_data": hypertable},
        )

        mock_inspector = MagicMock()

        with (
            patch.object(
                PostgresSource,
                "_process_table",
                return_value=iter([]),
            ),
            patch.object(source, "get_db_name", return_value="tsdb"),
        ):
            workunits = list(
                source._process_table(
                    "tsdb.public.sensor_data",
                    mock_inspector,
                    "public",
                    "sensor_data",
                    MagicMock(),
                    None,
                )
            )

        assert _aspects_of_type(workunits, SubTypesClass)
        if config.tag_hypertables:
            assert _aspects_of_type(workunits, GlobalTagsClass)

    @patch("datahub.ingestion.source.sql.postgres.source.create_engine")
    def test_process_view_adds_continuous_aggregate_subtype(self, create_engine_mock):
        # _process_view must NOT emit a second DatasetPropertiesClass — the
        # parent already emits one with the merged table properties.
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        cagg = ContinuousAggregate(name="hourly_metrics")
        source._timescaledb_metadata_cache["tsdb.public"] = _SchemaMetadata(
            continuous_aggregates={"hourly_metrics": cagg},
        )

        mock_inspector = MagicMock()

        with (
            patch.object(
                PostgresSource,
                "_process_view",
                return_value=iter([]),
            ),
            patch.object(
                PostgresSource,
                "get_table_properties",
                return_value=(None, {}, None),
            ),
            patch.object(source, "get_db_name", return_value="tsdb"),
        ):
            workunits = list(
                source._process_view(
                    "tsdb.public.hourly_metrics",
                    mock_inspector,
                    "public",
                    "hourly_metrics",
                    MagicMock(),
                )
            )

        assert _aspects_of_type(workunits, SubTypesClass)


class TestTimescaleDBJobProcessing:
    @patch("datahub.ingestion.source.sql.postgres.source.create_engine")
    def test_process_timescaledb_jobs(self, create_engine_mock):
        config = TimescaleDBConfig.parse_obj(
            {**_base_config(), "include_background_jobs": True}
        )
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        job = TimescaleDBJob(
            job_id=1001,
            application_name="Refresh Continuous Aggregate",
            schedule_interval="1 hour",
            max_runtime="5 minutes",
            max_retries=3,
            retry_period="5 minutes",
            proc_schema="_timescaledb_internal",
            proc_name="policy_refresh_continuous_aggregate",
            scheduled=True,
            fixed_schedule=False,
            initial_start=None,
            config={"start_offset": "2 hours"},
            hypertable_schema="public",
            hypertable_name="hourly_metrics",
        )

        source._timescaledb_metadata_cache["tsdb.public"] = _SchemaMetadata(
            jobs={1001: job},
        )

        mock_inspector = MagicMock()

        with (
            patch.object(source, "get_db_name", return_value="tsdb"),
            patch.object(
                source, "get_identifier", return_value="tsdb.public.hourly_metrics"
            ),
        ):
            workunits = list(
                source._process_timescaledb_jobs(mock_inspector, "public", "tsdb")
            )

        assert len(workunits) >= 3
        assert len(_aspects_of_type(workunits, DataJobInfoClass)) == 1

    def test_refresh_cagg_job_lineage_includes_source_hypertable(self):
        # Refresh CAgg policies read the source hypertable and write the CAgg
        # — both must appear on the DataJob's lineage.
        config = TimescaleDBConfig.parse_obj(
            {**_base_config(), "include_background_jobs": True}
        )
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        job = TimescaleDBJob(
            job_id=1001,
            proc_name="policy_refresh_continuous_aggregate",
            hypertable_schema="public",
            hypertable_name="sensor_hourly_avg",
        )
        cagg = ContinuousAggregate(
            name="sensor_hourly_avg",
            hypertable_schema="public",
            hypertable_name="sensor_data",
        )

        mock_inspector = MagicMock()

        def fake_get_identifier(schema, entity, inspector):
            return f"tsdb.{schema}.{entity}"

        with patch.object(source, "get_identifier", side_effect=fake_get_identifier):
            inputs, outputs = source._build_job_lineage(
                inspector=mock_inspector,
                job=job,
                continuous_aggregates={"sensor_hourly_avg": cagg},
            )

        assert any("sensor_data" in urn for urn in inputs)
        assert any("sensor_hourly_avg" in urn for urn in outputs)

    def test_emit_job_run_instances(self):
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()

        job = TimescaleDBJob(
            job_id=1001,
            application_name="Continuous Aggregate Policy [1001]",
            schedule_interval="1 hour",
            max_runtime="5 minutes",
            max_retries=3,
            retry_period="1 minute",
            proc_schema="public",
            proc_name="policy_refresh_continuous_aggregate",
            scheduled=True,
            fixed_schedule=False,
            initial_start="2023-01-01 00:00:00",
            config={"end_offset": "1 hour"},
            hypertable_schema="public",
            hypertable_name="sensor_data",
        )

        job_urn = "urn:li:dataJob:(urn:li:dataFlow:(timescaledb,tsdb.public.background_jobs,PROD),1001_sensor_data_policy_refresh_continuous_aggregate)"

        mock_executions = [
            JobExecution(
                job_id=1001,
                last_run_started_at=datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                last_successful_finish=datetime(
                    2023, 1, 1, 12, 1, 0, tzinfo=timezone.utc
                ),
                last_run_status="success",
                total_runs=100,
                total_successes=98,
                total_failures=2,
                consecutive_failures=0,
            )
        ]

        with patch.object(
            source, "_get_job_execution_history", return_value=mock_executions
        ):
            workunits = list(
                source._emit_job_run_instances(mock_inspector, job_urn, 1001, job)
            )

        properties_workunits = _aspects_of_type(
            workunits, DataProcessInstancePropertiesClass
        )
        assert len(properties_workunits) == 1

        run_event_workunits = _aspects_of_type(
            workunits, DataProcessInstanceRunEventClass
        )
        assert len(run_event_workunits) == 1

        relationship_workunits = _aspects_of_type(
            workunits, DataProcessInstanceRelationshipsClass
        )
        assert len(relationship_workunits) == 1
        assert relationship_workunits[0].metadata.aspect.parentTemplate == job_urn

        properties_aspect = properties_workunits[0].metadata.aspect
        assert properties_aspect.customProperties["job_id"] == "1001"
        assert properties_aspect.customProperties["total_runs"] == "100"
        assert properties_aspect.customProperties["hypertable"] == "public.sensor_data"

        # created.time must come from last_run_started_at, not ingestion-time
        # time.time(). datetime(2023, 1, 1, 12, 0, 0).timestamp() * 1000 =
        # 1672574400000.
        assert properties_aspect.created.time == 1672574400000


class TestTimescaleDBStoredProcedures:
    def test_exclude_background_job_procedures(self):
        # Policy procs are surfaced as DataJob entities instead, so they must
        # be filtered out of stored-procedure ingestion regardless of whether
        # include_background_jobs is enabled.
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        mock_procedures = [
            BaseProcedure(
                name="policy_refresh_continuous_aggregate",
                language="c",
                argument_signature="",
                return_type=None,
                procedure_definition="...",
                created=None,
                last_altered=None,
                comment="TimescaleDB background job procedure",
                extra_properties=None,
            ),
            BaseProcedure(
                name="user_custom_procedure",
                language="plpgsql",
                argument_signature="",
                return_type=None,
                procedure_definition="...",
                created=None,
                last_altered=None,
                comment="User-defined procedure",
                extra_properties=None,
            ),
        ]

        with (
            patch.object(source, "get_db_name", return_value="tsdb"),
            patch.object(source, "_is_timescaledb_enabled", return_value=True),
            patch.object(
                PostgresSource,
                "get_procedures_for_schema",
                return_value=mock_procedures,
            ),
        ):
            result = source.get_procedures_for_schema(mock_inspector, "public", "tsdb")

        assert len(result) == 1
        assert result[0].name == "user_custom_procedure"


class TestTimescaleDBLineage:
    def test_get_view_definition_override(self):
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()

        cagg = ContinuousAggregate(
            name="sensor_hourly_avg",
            materialized_only=False,
            compression_enabled=False,
            hypertable_schema="public",
            hypertable_name="sensor_data",
            view_definition="SELECT time_bucket('1 hour', time) as hour, device_id, avg(temperature) as avg_temp FROM sensor_data GROUP BY hour, device_id",
        )

        source._timescaledb_metadata_cache["tsdb.public"] = _SchemaMetadata(
            continuous_aggregates={"sensor_hourly_avg": cagg},
        )

        with patch.object(source, "get_db_name", return_value="tsdb"):
            view_definition = source._get_view_definition(
                mock_inspector, "public", "sensor_hourly_avg"
            )

        # Must return the user-defined SQL referencing the source hypertable,
        # not pg_views' rewritten form that points at the internal
        # _materialized_hypertable_N — that's what makes column-level lineage
        # back to the source hypertable possible.
        assert view_definition == cagg.view_definition
        assert "sensor_data" in view_definition
        assert "_timescaledb_internal" not in view_definition

    def test_get_view_definition_fallback(self):
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()

        source._timescaledb_metadata_cache["tsdb.public"] = _SchemaMetadata()

        with (
            patch.object(source, "get_db_name", return_value="tsdb"),
            patch.object(
                SQLAlchemySource,
                "_get_view_definition",
                return_value="SELECT * FROM regular_table",
            ) as mock_parent,
        ):
            view_definition = source._get_view_definition(
                mock_inspector, "public", "regular_view"
            )

        mock_parent.assert_called_once_with(mock_inspector, "public", "regular_view")
        assert view_definition == "SELECT * FROM regular_table"


class TestTimescaleDBErrorScenarios:
    @patch("datahub.ingestion.source.sql.postgres.source.create_engine")
    def test_missing_timescaledb_extension(self, create_engine_mock):
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 0

        mock_conn.execute.return_value = mock_result
        mock_inspector.engine.connect.return_value.__enter__.return_value = mock_conn

        assert source._is_timescaledb_enabled(mock_inspector) is False
        assert source._is_timescaledb_enabled(mock_inspector) is False
        assert mock_conn.execute.call_count == 1

    @patch("datahub.ingestion.source.sql.postgres.source.create_engine")
    def test_permission_denied_on_extension_check(self, create_engine_mock):
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = DatabaseError(
            "permission denied for table pg_extension", None, None
        )
        mock_inspector.engine.connect.return_value.__enter__.return_value = mock_conn

        assert source._is_timescaledb_enabled(mock_inspector) is False
        assert source.report.warnings

    @patch("datahub.ingestion.source.sql.postgres.source.create_engine")
    def test_environment_detection_unknown_schema(self, create_engine_mock):
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 0

        mock_conn.execute.return_value = mock_result
        mock_inspector.engine.connect.return_value.__enter__.return_value = mock_conn

        env = source._detect_timescaledb_environment(mock_inspector)
        assert env == TimescaleDBEnvironment.UNKNOWN

    @patch("datahub.ingestion.source.sql.postgres.source.create_engine")
    def test_environment_detection_permission_denied(self, create_engine_mock):
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = DatabaseError(
            "permission denied for schema information_schema", None, None
        )
        mock_inspector.engine.connect.return_value.__enter__.return_value = mock_conn

        env = source._detect_timescaledb_environment(mock_inspector)
        assert env == TimescaleDBEnvironment.UNKNOWN

    @patch("datahub.ingestion.source.sql.postgres.source.create_engine")
    def test_get_hypertables_permission_denied(self, create_engine_mock):
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        mock_conn = MagicMock()

        env_result = MagicMock()
        env_result.rowcount = 1
        mock_conn.execute.side_effect = [
            env_result,
            DatabaseError(
                "permission denied for schema timescaledb_information", None, None
            ),
        ]
        mock_inspector.engine.connect.return_value.__enter__.return_value = mock_conn

        with patch.object(source, "get_db_name", return_value="tsdb"):
            hypertables = source._get_hypertables(mock_inspector, "public")

        assert hypertables == {}
        assert source.report.warnings

    @patch("datahub.ingestion.source.sql.postgres.source.create_engine")
    def test_get_continuous_aggregates_malformed_data(self, create_engine_mock):
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        mock_conn = MagicMock()

        env_result = MagicMock()
        env_result.rowcount = 1

        malformed_row = {
            "view_name": None,
            "materialized_only": False,
            "compression_enabled": False,
        }

        mock_conn.execute.side_effect = [
            env_result,
            [malformed_row],
        ]
        mock_inspector.engine.connect.return_value.__enter__.return_value = mock_conn

        with patch.object(source, "get_db_name", return_value="tsdb"):
            caggs = source._get_continuous_aggregates(mock_inspector, "public")

        assert caggs == {}
        # Malformed rows must surface as report.warning, not logger.warning,
        # so users see them in the ingestion summary.
        assert source.report.warnings

    @patch("datahub.ingestion.source.sql.postgres.source.create_engine")
    def test_get_jobs_with_unknown_environment(self, create_engine_mock):
        config = TimescaleDBConfig.parse_obj(
            {**_base_config(), "include_background_jobs": True}
        )
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        source._timescaledb_environment = TimescaleDBEnvironment.UNKNOWN

        jobs = source._get_jobs(mock_inspector, "public")

        assert jobs == {}

    @patch("datahub.ingestion.source.sql.postgres.source.create_engine")
    def test_get_view_definition_missing_definition(self, create_engine_mock):
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()

        cagg = ContinuousAggregate(
            name="hourly_metrics",
            materialized_only=False,
            compression_enabled=False,
            hypertable_schema="public",
            hypertable_name="sensor_data",
            view_definition=None,
        )

        source._timescaledb_metadata_cache["tsdb.public"] = _SchemaMetadata(
            continuous_aggregates={"hourly_metrics": cagg},
        )

        with (
            patch.object(source, "get_db_name", return_value="tsdb"),
            patch.object(
                SQLAlchemySource,
                "_get_view_definition",
                return_value="SELECT * FROM fallback_view",
            ) as mock_parent,
        ):
            view_definition = source._get_view_definition(
                mock_inspector, "public", "hourly_metrics"
            )

        mock_parent.assert_called_once()
        assert view_definition == "SELECT * FROM fallback_view"

    @patch("datahub.ingestion.source.sql.postgres.source.create_engine")
    def test_job_execution_history_permission_denied(self, create_engine_mock):
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = DatabaseError(
            "permission denied for table timescaledb_information.job_stats", None, None
        )
        mock_inspector.engine.connect.return_value.__enter__.return_value = mock_conn

        executions = source._get_job_execution_history(mock_inspector, 1001, limit=10)

        assert executions == []


class TestTimescaleDBHelperFunctions:
    def test_format_timedelta_human_readable(self):
        assert format_timedelta_human_readable(timedelta(hours=1)) == "1 hour"
        assert format_timedelta_human_readable(timedelta(hours=2)) == "2 hours"
        assert format_timedelta_human_readable(timedelta(days=1)) == "1 day"
        assert format_timedelta_human_readable(timedelta(days=30)) == "30 days"
        assert (
            format_timedelta_human_readable(timedelta(days=1, hours=2))
            == "1 day 2 hours"
        )
        assert (
            format_timedelta_human_readable(timedelta(hours=1, minutes=30))
            == "1 hour 30 minutes"
        )
        assert format_timedelta_human_readable(timedelta(minutes=5)) == "5 minutes"
        assert format_timedelta_human_readable(timedelta(seconds=30)) == "30 seconds"
        assert format_timedelta_human_readable(timedelta(seconds=0)) == "0 seconds"

    def test_safe_str_convert(self):
        assert safe_str_convert(None) is None
        assert safe_str_convert(timedelta(hours=1)) == "1 hour"
        assert safe_str_convert(timedelta(days=7)) == "7 days"
        assert safe_str_convert("test") == "test"
        assert safe_str_convert(123) == "123"

    def test_job_display_name_known_policies(self):
        job = TimescaleDBJob(
            job_id=1001,
            proc_name="policy_refresh_continuous_aggregate",
            hypertable_name="hourly_metrics",
        )
        assert job.get_display_name() == "Refresh Continuous Aggregate - hourly_metrics"

        job = TimescaleDBJob(
            job_id=1002, proc_name="policy_retention", hypertable_name="sensor_data"
        )
        assert job.get_display_name() == "Data Retention - sensor_data"

        job = TimescaleDBJob(
            job_id=1003, proc_name="policy_compression", hypertable_name="metrics"
        )
        assert job.get_display_name() == "Compression Policy - metrics"

        job = TimescaleDBJob(
            job_id=1004, proc_name="policy_reorder", hypertable_name="events"
        )
        assert job.get_display_name() == "Reorder Policy - events"

    def test_job_display_name_unknown_procedure(self):
        job = TimescaleDBJob(
            job_id=1005,
            proc_name="custom_maintenance_job",
            hypertable_name="custom_table",
        )
        assert job.get_display_name() == "Custom Maintenance Job - custom_table"

        job = TimescaleDBJob(job_id=1006, proc_name="some_procedure")
        assert job.get_display_name() == "Some Procedure"

    def test_job_description_known_policies(self):
        job = TimescaleDBJob(
            job_id=1001,
            proc_name="policy_refresh_continuous_aggregate",
            hypertable_name="hourly_metrics",
            schedule_interval="1 hour",
        )
        description = job.get_description()
        assert "Refreshes continuous aggregate materialized data" in description
        assert "hourly_metrics" in description
        assert "1 hour" in description

        job = TimescaleDBJob(
            job_id=1002,
            proc_name="policy_compression",
            hypertable_name="sensor_data",
            schedule_interval="1 day",
        )
        description = job.get_description()
        assert "Compresses hypertable chunks to save storage" in description
        assert "sensor_data" in description

    def test_job_custom_properties_omits_empty_values(self):
        # Empty / None values should not pollute the UI as empty-string keys.
        job = TimescaleDBJob(
            job_id=42,
            proc_name="policy_compression",
            scheduled=True,
            fixed_schedule=False,
            max_retries=0,
        )
        props = job.get_custom_properties()

        assert "application_name" not in props
        assert "schedule_interval" not in props
        assert "max_runtime" not in props
        assert "initial_start" not in props
        assert props["job_id"] == "42"
        assert props["proc_name"] == "policy_compression"
