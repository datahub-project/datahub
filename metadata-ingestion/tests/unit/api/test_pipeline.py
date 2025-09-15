import pathlib
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Iterable, List, Optional, cast
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time
from typing_extensions import Self

from datahub.configuration.common import DynamicTypedConfig
from datahub.ingestion.api.committable import CommitPolicy, Committable
from datahub.ingestion.api.common import RecordEnvelope
from datahub.ingestion.api.decorators import platform_name
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.transform import Transformer
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode, DatahubClientConfig
from datahub.ingestion.run.pipeline import Pipeline, PipelineContext
from datahub.ingestion.sink.datahub_rest import DatahubRestSink
from datahub.metadata.com.linkedin.pegasus2avro.mxe import SystemMetadata
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    StatusClass,
)
from datahub.utilities.server_config_util import RestServiceConfig
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.sink_helpers import RecordingSinkReport

FROZEN_TIME = "2020-04-14 07:00:00"

# TODO: It seems like one of these tests writes to ~/.datahubenv or otherwise sets
# some global config, which impacts other tests.
pytestmark = pytest.mark.random_order(disabled=True)


@pytest.fixture
def mock_server_config():
    # Create a mock RestServiceConfig with your desired raw_config
    config = RestServiceConfig(raw_config={"noCode": True})
    return config


class TestPipeline:
    @pytest.fixture(autouse=True)
    def clear_cache(self):
        yield
        get_default_graph.cache_clear()

    @patch("confluent_kafka.Consumer", autospec=True)
    @patch(
        "datahub.ingestion.source.kafka.kafka.KafkaSource.get_workunits", autospec=True
    )
    @patch("datahub.ingestion.sink.console.ConsoleSink.close", autospec=True)
    @freeze_time(FROZEN_TIME)
    def test_configure(self, mock_sink, mock_source, mock_consumer):
        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "kafka",
                    "config": {"connection": {"bootstrap": "fake-dns-name:9092"}},
                },
                "sink": {"type": "console"},
            }
        )
        pipeline.run()
        pipeline.raise_from_status()
        pipeline.pretty_print_summary()
        mock_source.assert_called_once()
        mock_sink.assert_called_once()

    @freeze_time(FROZEN_TIME)
    @patch("datahub.emitter.rest_emitter.DataHubRestEmitter.fetch_server_config")
    @patch(
        "datahub.cli.config_utils.load_client_config",
        return_value=DatahubClientConfig(server="http://fake-gms-server:8080"),
    )
    def test_configure_without_sink(
        self, mock_load_client_config, mock_fetch_config, mock_server_config
    ):
        mock_fetch_config.return_value = mock_server_config

        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "file",
                    "config": {"path": "test_file.json"},
                },
            }
        )
        # assert that the default sink is a DatahubRestSink
        assert isinstance(pipeline.sink, DatahubRestSink)
        assert pipeline.sink.config.server == "http://fake-gms-server:8080"
        assert pipeline.sink.config.token is None

    @freeze_time(FROZEN_TIME)
    @patch("datahub.emitter.rest_emitter.DataHubRestEmitter.fetch_server_config")
    @patch(
        "datahub.cli.config_utils.load_client_config",
        return_value=DatahubClientConfig(server="http://fake-internal-server:8080"),
    )
    @patch(
        "datahub.cli.config_utils.get_system_auth",
        return_value="Basic user:pass",
    )
    def test_configure_without_sink_use_system_auth(
        self,
        mock_get_system_auth,
        mock_load_client_config,
        mock_fetch_config,
        mock_server_config,
    ):
        mock_fetch_config.return_value = mock_server_config

        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "file",
                    "config": {"path": "test_file.json"},
                },
            }
        )
        # assert that the default sink is a DatahubRestSink
        assert isinstance(pipeline.sink, DatahubRestSink)
        assert pipeline.sink.config.server == "http://fake-internal-server:8080"
        assert pipeline.sink.config.token is None
        assert (
            pipeline.sink.emitter._session.headers["Authorization"] == "Basic user:pass"
        )

    @freeze_time(FROZEN_TIME)
    @patch("datahub.emitter.rest_emitter.DataHubRestEmitter.fetch_server_config")
    def test_configure_with_rest_sink_initializes_graph(
        self, mock_fetch_config, mock_server_config
    ):
        mock_fetch_config.return_value = mock_server_config

        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "file",
                    "config": {"path": "test_events.json"},
                },
                "sink": {
                    "type": "datahub-rest",
                    "config": {
                        "server": "http://somehost.someplace.some:8080",
                        "token": "foo",
                    },
                },
            },
            # We don't want to make actual API calls during this test.
            report_to=None,
        )
        # assert that the default sink config is for a DatahubRestSink
        assert isinstance(pipeline.config.sink, DynamicTypedConfig)
        assert pipeline.config.sink.type == "datahub-rest"
        assert pipeline.config.sink.config == {
            "server": "http://somehost.someplace.some:8080",
            "token": "foo",
        }
        assert pipeline.ctx.graph is not None, "DataHubGraph should be initialized"
        assert pipeline.ctx.graph.config.server == pipeline.config.sink.config["server"]
        assert pipeline.ctx.graph.config.token == pipeline.config.sink.config["token"]

    @freeze_time(FROZEN_TIME)
    @patch("datahub.emitter.rest_emitter.DataHubRestEmitter.fetch_server_config")
    def test_configure_with_rest_sink_with_additional_props_initializes_graph(
        self, mock_fetch_config, mock_server_config
    ):
        mock_fetch_config.return_value = mock_server_config

        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "file",
                    "config": {"path": "test_events.json"},
                },
                "sink": {
                    "type": "datahub-rest",
                    "config": {
                        "server": "http://somehost.someplace.some:8080",
                        "token": "foo",
                        "mode": "sync",
                    },
                },
            }
        )
        # assert that the default sink config is for a DatahubRestSink
        assert isinstance(pipeline.config.sink, DynamicTypedConfig)
        assert pipeline.config.sink.type == "datahub-rest"
        assert pipeline.config.sink.config == {
            "server": "http://somehost.someplace.some:8080",
            "token": "foo",
            "mode": "sync",
        }
        assert pipeline.ctx.graph is not None, "DataHubGraph should be initialized"
        assert pipeline.ctx.graph.config.server == pipeline.config.sink.config["server"]
        assert pipeline.ctx.graph.config.token == pipeline.config.sink.config["token"]

    @freeze_time(FROZEN_TIME)
    @patch(
        "datahub.ingestion.source.kafka.kafka.KafkaSource.get_workunits", autospec=True
    )
    def test_configure_with_file_sink_does_not_init_graph(self, mock_source, tmp_path):
        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "kafka",
                    "config": {"connection": {"bootstrap": "localhost:9092"}},
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": str(tmp_path / "test.json"),
                    },
                },
            }
        )
        # assert that the default sink config is for a DatahubRestSink
        assert isinstance(pipeline.config.sink, DynamicTypedConfig)
        assert pipeline.config.sink.type == "file"
        assert pipeline.config.sink.config == {"filename": str(tmp_path / "test.json")}
        assert pipeline.ctx.graph is None, "DataHubGraph should not be initialized"

    @freeze_time(FROZEN_TIME)
    def test_run_including_fake_transformation(self):
        pipeline = Pipeline.create(
            {
                "source": {"type": "tests.unit.api.test_pipeline.FakeSource"},
                "transformers": [
                    {"type": "tests.unit.api.test_pipeline.AddStatusRemovedTransformer"}
                ],
                "sink": {"type": "tests.test_helpers.sink_helpers.RecordingSink"},
                "run_id": "pipeline_test",
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        expected_mce = get_initial_mce()

        dataset_snapshot = cast(DatasetSnapshotClass, expected_mce.proposedSnapshot)
        dataset_snapshot.aspects.append(get_status_removed_aspect())

        sink_report: RecordingSinkReport = cast(
            RecordingSinkReport, pipeline.sink.get_report()
        )

        assert len(sink_report.received_records) == 1
        assert expected_mce == sink_report.received_records[0].record

    @freeze_time(FROZEN_TIME)
    def test_run_including_registered_transformation(self):
        # This is not testing functionality, but just the transformer registration system.

        pipeline = Pipeline.create(
            {
                "source": {"type": "tests.unit.api.test_pipeline.FakeSource"},
                "transformers": [
                    {
                        "type": "simple_add_dataset_ownership",
                        "config": {
                            "owner_urns": ["urn:li:corpuser:foo"],
                            "ownership_type": "urn:li:ownershipType:__system__technical_owner",
                        },
                    }
                ],
                "sink": {"type": "tests.test_helpers.sink_helpers.RecordingSink"},
            }
        )
        assert pipeline

    @pytest.mark.parametrize(
        "source,strict_warnings,exit_code",
        [
            pytest.param(
                "FakeSource",
                False,
                0,
            ),
            pytest.param(
                "FakeSourceWithWarnings",
                False,
                0,
            ),
            pytest.param(
                "FakeSourceWithWarnings",
                True,
                1,
            ),
        ],
    )
    @freeze_time(FROZEN_TIME)
    def test_pipeline_return_code(self, tmp_path, source, strict_warnings, exit_code):
        config_file: pathlib.Path = tmp_path / "test.yml"

        config_file.write_text(
            f"""
---
run_id: pipeline_test
source:
    type: tests.unit.api.test_pipeline.{source}
    config: {{}}
sink:
    type: console
"""
        )

        res = run_datahub_cmd(
            [
                "ingest",
                "-c",
                f"{config_file}",
                *(("--strict-warnings",) if strict_warnings else ()),
            ],
            tmp_path=tmp_path,
            check_result=False,
        )
        assert res.exit_code == exit_code, res.stdout

    @pytest.mark.parametrize(
        "commit_policy,source,should_commit",
        [
            pytest.param(
                CommitPolicy.ALWAYS,
                "FakeSource",
                True,
                id="ALWAYS-no-warnings-no-errors",
            ),
            pytest.param(
                CommitPolicy.ON_NO_ERRORS,
                "FakeSource",
                True,
                id="ON_NO_ERRORS-no-warnings-no-errors",
            ),
            pytest.param(
                CommitPolicy.ON_NO_ERRORS_AND_NO_WARNINGS,
                "FakeSource",
                True,
                id="ON_NO_ERRORS_AND_NO_WARNINGS-no-warnings-no-errors",
            ),
            pytest.param(
                CommitPolicy.ALWAYS,
                "FakeSourceWithWarnings",
                True,
                id="ALWAYS-with-warnings",
            ),
            pytest.param(
                CommitPolicy.ON_NO_ERRORS,
                "FakeSourceWithWarnings",
                True,
                id="ON_NO_ERRORS-with-warnings",
            ),
            pytest.param(
                CommitPolicy.ON_NO_ERRORS_AND_NO_WARNINGS,
                "FakeSourceWithWarnings",
                False,
                id="ON_NO_ERRORS_AND_NO_WARNINGS-with-warnings",
            ),
            pytest.param(
                CommitPolicy.ALWAYS,
                "FakeSourceWithFailures",
                True,
                id="ALWAYS-with-errors",
            ),
            pytest.param(
                CommitPolicy.ON_NO_ERRORS,
                "FakeSourceWithFailures",
                False,
                id="ON_NO_ERRORS-with-errors",
            ),
            pytest.param(
                CommitPolicy.ON_NO_ERRORS_AND_NO_WARNINGS,
                "FakeSourceWithFailures",
                False,
                id="ON_NO_ERRORS_AND_NO_WARNINGS-with-errors",
            ),
        ],
    )
    @freeze_time(FROZEN_TIME)
    def test_pipeline_process_commits(self, commit_policy, source, should_commit):
        pipeline = Pipeline.create(
            {
                "source": {"type": f"tests.unit.api.test_pipeline.{source}"},
                "sink": {"type": "console"},
                "run_id": "pipeline_test",
            }
        )

        class FakeCommittable(Committable):
            def __init__(self, commit_policy: CommitPolicy):
                self.name = "test_checkpointer"
                self.commit_policy = commit_policy

            def commit(self) -> None:
                pass

        fake_committable: Committable = FakeCommittable(commit_policy)

        with patch.object(
            FakeCommittable, "commit", wraps=fake_committable.commit
        ) as mock_commit:
            pipeline.ctx.register_checkpointer(fake_committable)

            pipeline.run()
            # check that we called the commit method once only if should_commit is True
            if should_commit:
                mock_commit.assert_called_once()
            else:
                mock_commit.assert_not_called()

    @freeze_time(FROZEN_TIME)
    @patch("datahub.emitter.rest_emitter.DataHubRestEmitter.fetch_server_config")
    def test_pipeline_graph_has_expected_client_mode_and_component(
        self, mock_fetch_config, mock_server_config
    ):
        mock_fetch_config.return_value = mock_server_config

        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "file",
                    "config": {"path": "test_events.json"},
                },
                "sink": {
                    "type": "datahub-rest",
                    "config": {
                        "server": "http://somehost.someplace.some:8080",
                        "token": "foo",
                    },
                },
            }
        )

        # Assert that the DataHubGraph is initialized with expected properties
        assert pipeline.ctx.graph is not None, "DataHubGraph should be initialized"

        # Check that graph has the expected client mode and datahub_component
        assert pipeline.ctx.graph.config.client_mode == ClientMode.INGESTION
        assert pipeline.ctx.graph.config.datahub_component is None

        # Check that the graph was configured with the expected server and token
        assert pipeline.ctx.graph.config.server == "http://somehost.someplace.some:8080"
        assert pipeline.ctx.graph.config.token == "foo"

    @freeze_time(FROZEN_TIME)
    @patch("datahub.emitter.rest_emitter.DataHubRestEmitter.fetch_server_config")
    @patch(
        "datahub.cli.config_utils.load_client_config",
        return_value=DatahubClientConfig(server="http://fake-gms-server:8080"),
    )
    def test_pipeline_graph_client_mode(
        self, mock_load_client_config, mock_fetch_config, mock_server_config
    ):
        """Test that the graph created in Pipeline has the correct client_mode."""
        mock_fetch_config.return_value = mock_server_config

        # Mock the DataHubGraph context manager and test_connection method
        mock_graph = MagicMock()
        mock_graph.__enter__.return_value = mock_graph

        # Create a patch that replaces the DataHubGraph constructor
        with patch(
            "datahub.ingestion.run.pipeline.DataHubGraph", return_value=mock_graph
        ) as mock_graph_class:
            # Create a pipeline with datahub_api config
            pipeline = Pipeline.create(
                {
                    "source": {
                        "type": "file",
                        "config": {"path": "test_events.json"},
                    },
                    "datahub_api": {
                        "server": "http://datahub-gms:8080",
                        "token": "test_token",
                    },
                }
            )

            # Verify DataHubGraph was called with the correct parameters
            assert mock_graph_class.call_count == 1

            # Get the arguments passed to DataHubGraph
            config_arg = mock_graph_class.call_args[0][0]

            # Assert the config is a DatahubClientConfig with the expected values
            assert isinstance(config_arg, DatahubClientConfig)
            assert config_arg.server == "http://datahub-gms:8080"
            assert config_arg.token == "test_token"
            assert config_arg.client_mode == ClientMode.INGESTION
            assert config_arg.datahub_component is None

            # Verify the graph has been stored in the pipeline context
            assert pipeline.ctx.graph is mock_graph


class AddStatusRemovedTransformer(Transformer):
    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Transformer":
        return cls()

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        for record_envelope in record_envelopes:
            if isinstance(record_envelope.record, MetadataChangeEventClass):
                assert isinstance(
                    record_envelope.record.proposedSnapshot, DatasetSnapshotClass
                )
                record_envelope.record.proposedSnapshot.aspects.append(
                    get_status_removed_aspect()
                )
            yield record_envelope


@platform_name("fake")
class FakeSource(Source):
    def __init__(self, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_report = SourceReport()
        self.work_units: List[MetadataWorkUnit] = [
            MetadataWorkUnit(id="workunit-1", mce=get_initial_mce())
        ]

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Self:
        assert not config_dict
        return cls(ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        return self.work_units

    def get_report(self) -> SourceReport:
        return self.source_report

    def close(self):
        pass


@platform_name("fake")
class FakeSourceWithWarnings(FakeSource):
    def __init__(self, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_report.report_warning("test_warning", "warning_text")

    def get_report(self) -> SourceReport:
        return self.source_report


@platform_name("fake")
class FakeSourceWithFailures(FakeSource):
    def __init__(self, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_report.report_failure("test_failure", "failure_text")

    def get_report(self) -> SourceReport:
        return self.source_report


def get_initial_mce() -> MetadataChangeEventClass:
    return MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn="urn:li:dataset:(urn:li:dataPlatform:test_platform,test,PROD)",
            aspects=[
                DatasetPropertiesClass(
                    description="test.description",
                )
            ],
        ),
        systemMetadata=SystemMetadata(
            lastObserved=1586847600000, runId="pipeline_test"
        ),
    )


def get_status_removed_aspect() -> StatusClass:
    return StatusClass(removed=False)


class RealisticSinkReport(SinkReport):
    """Sink report that simulates the real DatahubRestSinkReport behavior"""

    def __init__(self):
        super().__init__()
        self.pending_requests = (
            0  # Start with 0, will be incremented by async operations
        )


class RealisticDatahubRestSink(Sink):
    """
    Realistic simulation of DatahubRestSink that uses an executor like the real implementation.
    This simulates the async behavior that can cause timing issues.
    """

    def __init__(self):
        self.report = RealisticSinkReport()
        self.executor = ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="sink-worker"
        )
        self.close_called = False
        self.shutdown_complete = False

    def write_record_async(
        self,
        record_envelope: RecordEnvelope,
        write_callback: Optional[WriteCallback] = None,
    ) -> None:
        """Simulate async record writing like the real DatahubRestSink"""
        # Increment pending requests when starting async work
        self.report.pending_requests += 1
        print(
            f"📝 Starting async write - pending_requests now: {self.report.pending_requests}"
        )

        # Submit async work to executor
        future = self.executor.submit(self._process_record, record_envelope)

        # Add callback to decrement pending requests when done
        future.add_done_callback(self._on_request_complete)

    def _process_record(self, record_envelope: RecordEnvelope) -> None:
        """Simulate processing a record (like sending HTTP request)"""
        # Simulate network delay
        time.sleep(0.1)
        print(f"🌐 Processed record: {record_envelope.record}")

    def _on_request_complete(self, future: Future) -> None:
        """Callback when async request completes"""
        self.report.pending_requests -= 1
        print(
            f"✅ Request completed - pending_requests now: {self.report.pending_requests}"
        )

    def close(self):
        """Simulate the real DatahubRestSink close() method"""
        print("🔧 Starting sink close()...")
        self.close_called = True

        # Simulate the executor.shutdown() behavior
        print(
            f"⏳ Shutting down executor with {self.report.pending_requests} pending requests..."
        )
        self.executor.shutdown(wait=True)  # Wait for all pending requests to complete

        # After shutdown, pending_requests should be 0
        self.report.pending_requests = 0
        self.shutdown_complete = True
        print("✅ Sink close() completed - all pending requests processed")


class MockReporter:
    """Mock reporter that tracks when completion notification is called"""

    def __init__(self, sink=None):
        self.completion_called = False
        self.completion_pending_requests = None
        self.sink = sink
        self.start_called = False

    def on_start(self, ctx):
        """Mock on_start method"""
        self.start_called = True
        print("📊 MockReporter.on_start() called")

    def on_completion(self, status, report, ctx):
        self.completion_called = True
        # Check pending requests at the time of completion notification
        if (
            self.sink
            and hasattr(self.sink, "report")
            and hasattr(self.sink.report, "pending_requests")
        ):
            self.completion_pending_requests = self.sink.report.pending_requests
            print(
                f"📊 Completion notification sees {self.completion_pending_requests} pending requests"
            )


class TestSinkReportTimingOnClose:
    """Test class for validating sink report timing when sink.close() is called"""

    def _create_real_pipeline_with_realistic_sink(self):
        """Create a real Pipeline instance with a realistic sink using Pipeline.create()"""

        from datahub.ingestion.run.pipeline import Pipeline

        # Create realistic sink
        sink = RealisticDatahubRestSink()

        # Create pipeline using Pipeline.create() like the existing tests do
        # Use demo data source which is simpler and doesn't require external dependencies
        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "demo-data",
                    "config": {},
                },
                "sink": {"type": "console"},  # Use console sink to avoid network issues
            }
        )

        # Replace the sink with our realistic sink and register it with the exit_stack
        # This ensures the context manager calls close() on our realistic sink
        pipeline.sink = pipeline.exit_stack.enter_context(sink)

        return pipeline, sink

    def _add_pending_requests_to_sink(
        self, sink: RealisticDatahubRestSink, count: int = 3
    ) -> int:
        """Add some pending requests to the sink to simulate async work"""
        print(f"📝 Adding {count} pending requests to sink...")
        for i in range(count):
            sink.write_record_async(RecordEnvelope(f"record_{i}", metadata={}))

        # Give some time for work to start
        time.sleep(0.05)
        print(f"📊 Current pending requests: {sink.report.pending_requests}")
        return sink.report.pending_requests

    def test_sink_report_timing_on_close(self):
        """Test that validates completion notification runs with 0 pending requests after sink.close()"""
        print("\n🧪 Testing sink report timing on close...")

        # Create test pipeline with realistic sink
        pipeline, sink = self._create_real_pipeline_with_realistic_sink()

        # Add pending requests to simulate async work
        pending_count = self._add_pending_requests_to_sink(sink, 3)
        assert pending_count > 0, "❌ Expected pending requests to be added"

        # Create mock reporter to track completion
        reporter = MockReporter(sink=sink)
        pipeline.reporters = [reporter]

        # Create a wrapper that checks pending requests before calling the real method
        original_notify = pipeline._notify_reporters_on_ingestion_completion

        def notify_with_pending_check():
            print("🔔 Calling _notify_reporters_on_ingestion_completion()...")

            # Check pending requests before notifying reporters
            if hasattr(pipeline.sink, "report") and hasattr(
                pipeline.sink.report, "pending_requests"
            ):
                pending_requests = pipeline.sink.report.pending_requests
                print(
                    f"📊 Pending requests when _notify_reporters_on_ingestion_completion() runs: {pending_requests}"
                )

                # This is the key assertion - there should be no pending requests
                assert pending_requests == 0, (
                    f"❌ Expected 0 pending requests when _notify_reporters_on_ingestion_completion() runs, "
                    f"but found {pending_requests}. This indicates a timing issue."
                )
                print(
                    "✅ No pending requests when _notify_reporters_on_ingestion_completion() runs"
                )

            # Call original method
            original_notify()

        # Replace the method temporarily
        pipeline._notify_reporters_on_ingestion_completion = notify_with_pending_check

        # Run the REAL Pipeline.run() method with the CORRECT behavior (completion notification after context manager)
        print(
            "📢 Running REAL Pipeline.run() with correct timing (completion notification after context manager)..."
        )
        pipeline.run()

        # Verify the fix: sink.close() was called by context manager before _notify_reporters_on_ingestion_completion
        assert sink.close_called, "❌ Sink close() should be called by context manager"
        print("✅ Sink close() was called by context manager")

        assert reporter.completion_called, "❌ Completion notification should be called"
        print("✅ Completion notification was called")

        # Verify no pending requests at completion (the key fix)
        assert reporter.completion_pending_requests == 0, (
            f"❌ Expected 0 pending requests at completion, got {reporter.completion_pending_requests}. "
            "This indicates the timing fix is working correctly."
        )
        print("✅ No pending requests at completion notification")

        # Verify the sink's pending_requests is also 0
        assert sink.report.pending_requests == 0, (
            f"❌ Sink should have 0 pending requests after close() completes, got {sink.report.pending_requests}"
        )
        print("✅ Sink has 0 pending requests after close()")

        print(
            "🎉 Sink report timing fix test passed! The fix works with realistic async behavior."
        )
