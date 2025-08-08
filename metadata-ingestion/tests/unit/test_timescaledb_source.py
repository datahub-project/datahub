"""
Unit tests for TimescaleDB DataHub connector
"""

from typing import Any, Dict
from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.timescaledb import (
    TimescaleDBConfig,
    TimescaleDBSource,
)
from datahub.metadata.schema_classes import (
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


class TestTimescaleDBConfig:
    """Test TimescaleDB configuration parsing"""

    def test_default_config(self):
        config = TimescaleDBConfig.parse_obj(_base_config())
        assert config.emit_timescaledb_metadata is True
        assert config.tag_hypertables is True
        assert config.tag_continuous_aggregates is True
        assert config.include_jobs is False

    def test_custom_config(self):
        custom_config = {
            **_base_config(),
            "include_jobs": True,
            "tag_hypertables": False,
        }
        config = TimescaleDBConfig.parse_obj(custom_config)
        assert config.include_jobs is True
        assert config.tag_hypertables is False


class TestTimescaleDBSource:
    """Test TimescaleDB source functionality"""

    @patch("datahub.ingestion.source.sql.postgres.create_engine")
    def test_platform_name(self, create_engine_mock):
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))
        assert source.get_platform() == "timescaledb"

    @patch("datahub.ingestion.source.sql.postgres.create_engine")
    def test_timescaledb_extension_check(self, create_engine_mock):
        """Test checking if TimescaleDB extension is installed"""
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        # Mock inspector
        mock_inspector = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 1

        mock_conn.execute.return_value = mock_result
        mock_inspector.engine.connect.return_value.__enter__.return_value = mock_conn

        # Test when extension is installed
        assert source._is_timescaledb_enabled(mock_inspector) is True

        # Test when extension is not installed
        mock_result.rowcount = 0
        assert source._is_timescaledb_enabled(mock_inspector) is False

    @patch("datahub.ingestion.source.sql.postgres.create_engine")
    def test_get_hypertables(self, create_engine_mock):
        """Test fetching hypertable metadata"""
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        mock_conn = MagicMock()
        mock_result = [
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

        mock_conn.execute.return_value = mock_result
        mock_inspector.engine.connect.return_value.__enter__.return_value = mock_conn

        hypertables = source._get_hypertables(mock_inspector, "public")

        assert "sensor_data" in hypertables
        assert hypertables["sensor_data"].num_dimensions == 2
        assert hypertables["sensor_data"].compression_enabled is True

    @patch("datahub.ingestion.source.sql.postgres.create_engine")
    def test_get_continuous_aggregates(self, create_engine_mock):
        """Test fetching continuous aggregate metadata"""
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        mock_conn = MagicMock()
        mock_result = [
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

        mock_conn.execute.return_value = mock_result
        mock_inspector.engine.connect.return_value.__enter__.return_value = mock_conn

        caggs = source._get_continuous_aggregates(mock_inspector, "public")

        assert "hourly_metrics" in caggs
        assert caggs["hourly_metrics"].hypertable_name == "sensor_data"
        assert caggs["hourly_metrics"].refresh_policy is not None
        assert caggs["hourly_metrics"].refresh_policy.schedule_interval == "1 hour"

    @patch("datahub.ingestion.source.sql.postgres.create_engine")
    def test_get_jobs(self, create_engine_mock):
        """Test fetching TimescaleDB background jobs"""
        config = TimescaleDBConfig.parse_obj({**_base_config(), "include_jobs": True})
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        mock_conn = MagicMock()
        mock_result = [
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

        mock_conn.execute.return_value = mock_result
        mock_inspector.engine.connect.return_value.__enter__.return_value = mock_conn

        jobs = source._get_jobs(mock_inspector, "public")

        assert 1001 in jobs
        assert jobs[1001].proc_name == "policy_refresh_continuous_aggregate"
        assert jobs[1001].hypertable_name == "hourly_metrics"

    @patch("datahub.ingestion.source.sql.postgres.create_engine")
    def test_add_information_for_schema(self, create_engine_mock):
        """Test caching TimescaleDB metadata for a schema"""
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        mock_conn = MagicMock()

        # Mock TimescaleDB extension check
        extension_result = MagicMock()
        extension_result.rowcount = 1

        # Mock hypertables query
        hypertables_result: list[Any] = []

        # Mock continuous aggregates query
        caggs_result: list[Any] = []

        mock_conn.execute.side_effect = [
            extension_result,
            hypertables_result,
            caggs_result,
        ]

        mock_inspector.engine.connect.return_value.__enter__.return_value = mock_conn

        with patch.object(source, "get_db_name", return_value="tsdb"):
            source.add_information_for_schema(mock_inspector, "public")

        cache_key = "tsdb.public"
        assert cache_key in source._timescaledb_metadata_cache
        assert "hypertables" in source._timescaledb_metadata_cache[cache_key]
        assert "continuous_aggregates" in source._timescaledb_metadata_cache[cache_key]

    @patch("datahub.ingestion.source.sql.postgres.create_engine")
    def test_get_table_properties_for_hypertable(self, create_engine_mock):
        """Test enriching table properties for hypertables"""
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        # Import the models for creating test data
        from datahub.ingestion.source.sql.timescaledb import (
            Hypertable,
            HypertableDimension,
            RetentionPolicy,
        )

        # Setup cache with proper models
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

        source._timescaledb_metadata_cache["tsdb.public"] = {
            "hypertables": {"sensor_data": hypertable},
            "continuous_aggregates": {},
        }

        mock_inspector = MagicMock()

        # Call parent method mock
        with (
            patch.object(
                source.__class__.__bases__[0],
                "get_table_properties",
                return_value=("Table description", {}, None),
            ),
            patch.object(source, "get_db_name", return_value="tsdb"),
        ):
            desc, props, location = source.get_table_properties(
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

    @patch("datahub.ingestion.source.sql.postgres.create_engine")
    def test_get_table_properties_for_continuous_aggregate(self, create_engine_mock):
        """Test enriching view properties for continuous aggregates"""
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        # Import the models for creating test data
        from datahub.ingestion.source.sql.timescaledb import (
            ContinuousAggregate,
            RefreshPolicy,
        )

        # Setup cache with proper models
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

        source._timescaledb_metadata_cache["tsdb.public"] = {
            "hypertables": {},
            "continuous_aggregates": {"hourly_metrics": cagg},
        }

        mock_inspector = MagicMock()

        # Call parent method mock
        with (
            patch.object(
                source.__class__.__bases__[0],
                "get_table_properties",
                return_value=("View description", {"is_view": "True"}, None),
            ),
            patch.object(source, "get_db_name", return_value="tsdb"),
        ):
            desc, props, location = source.get_table_properties(
                mock_inspector, "public", "hourly_metrics"
            )

        assert props["is_continuous_aggregate"] == "true"
        assert props["materialized_only"] == "False"
        assert props["source_hypertable"] == "public.sensor_data"
        assert props["refresh_interval"] == "1 hour"
        assert props["refresh_start_offset"] == "2 hours"
        assert props["refresh_end_offset"] == "1 hour"

    @patch("datahub.ingestion.source.sql.postgres.create_engine")
    def test_get_extra_tags_for_hypertable(self, create_engine_mock):
        """Test adding tags to hypertable columns"""
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        # Import the models for creating test data
        from datahub.ingestion.source.sql.timescaledb import Hypertable

        # Setup cache with proper models
        hypertable = Hypertable(name="sensor_data")

        source._timescaledb_metadata_cache["tsdb.public"] = {
            "hypertables": {"sensor_data": hypertable},
            "continuous_aggregates": {},
        }

        mock_inspector = MagicMock()
        mock_inspector.get_columns.return_value = [
            {"name": "time"},
            {"name": "device_id"},
            {"name": "temperature"},
        ]

        with patch.object(source, "get_db_name", return_value="tsdb"):
            tags = source.get_extra_tags(mock_inspector, "public", "sensor_data")

        assert tags is not None
        assert "hypertable" in tags["time"]
        assert "hypertable" in tags["device_id"]
        assert "hypertable" in tags["temperature"]

    @patch("datahub.ingestion.source.sql.postgres.create_engine")
    def test_identifier_with_database(self, create_engine_mock):
        """Test identifier generation with database name"""
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        identifier = source.get_identifier(
            schema="public", entity="sensor_data", inspector=mock_inspector
        )

        assert identifier == "tsdb.public.sensor_data"

    @patch("datahub.ingestion.source.sql.postgres.create_engine")
    def test_process_table_adds_hypertable_subtype(self, create_engine_mock):
        """Test that hypertables get proper subtype"""
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        # Import the models for creating test data
        from datahub.ingestion.source.sql.timescaledb import Hypertable

        # Setup cache with hypertable
        hypertable = Hypertable(name="sensor_data")

        source._timescaledb_metadata_cache["tsdb.public"] = {
            "hypertables": {"sensor_data": hypertable},
            "continuous_aggregates": {},
        }

        mock_inspector = MagicMock()

        # Mock parent _process_table to return empty generator
        with (
            patch.object(
                source.__class__.__bases__[0],
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

        # Check that SubTypesClass workunit was created
        subtype_workunits = [
            wu
            for wu in workunits
            if hasattr(wu, "metadata")
            and hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, SubTypesClass)
        ]
        assert len(subtype_workunits) > 0

        # Check that GlobalTagsClass workunit was created if configured
        if config.tag_hypertables:
            tag_workunits = [
                wu
                for wu in workunits
                if hasattr(wu, "metadata")
                and hasattr(wu.metadata, "aspect")
                and isinstance(wu.metadata.aspect, GlobalTagsClass)
            ]
            assert len(tag_workunits) > 0

    @patch("datahub.ingestion.source.sql.postgres.create_engine")
    def test_process_view_adds_continuous_aggregate_subtype(self, create_engine_mock):
        """Test that continuous aggregates get proper subtype"""
        config = TimescaleDBConfig.parse_obj(_base_config())
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        # Import the models for creating test data
        from datahub.ingestion.source.sql.timescaledb import ContinuousAggregate

        # Setup cache with continuous aggregate
        cagg = ContinuousAggregate(name="hourly_metrics")

        source._timescaledb_metadata_cache["tsdb.public"] = {
            "hypertables": {},
            "continuous_aggregates": {"hourly_metrics": cagg},
        }

        mock_inspector = MagicMock()

        # Mock parent _process_view to return empty generator
        with (
            patch.object(
                source.__class__.__bases__[0],
                "_process_view",
                return_value=iter([]),
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

        # Check that SubTypesClass workunit was created
        subtype_workunits = [
            wu
            for wu in workunits
            if hasattr(wu, "metadata")
            and hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, SubTypesClass)
        ]
        assert len(subtype_workunits) > 0


class TestTimescaleDBJobProcessing:
    """Test TimescaleDB job processing"""

    @patch("datahub.ingestion.source.sql.postgres.create_engine")
    def test_process_timescaledb_jobs(self, create_engine_mock):
        """Test processing TimescaleDB jobs into DataJob entities"""
        config = TimescaleDBConfig.parse_obj({**_base_config(), "include_jobs": True})
        source = TimescaleDBSource(config, PipelineContext(run_id="test"))

        # Import the models for creating test data
        from datahub.ingestion.source.sql.timescaledb import TimescaleDBJob

        # Setup cache with jobs
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

        source._timescaledb_metadata_cache["tsdb.public"] = {"jobs": {1001: job}}

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

        # Should have created DataJobInfo, Status, and DataJobInputOutput workunits
        assert len(workunits) >= 3

        # Check that job info was created correctly
        job_info_workunits = [
            wu
            for wu in workunits
            if hasattr(wu, "metadata")
            and hasattr(wu.metadata, "aspect")
            and wu.metadata.aspect.__class__.__name__ == "DataJobInfoClass"
        ]
        assert len(job_info_workunits) == 1
