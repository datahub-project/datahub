import time
from dataclasses import dataclass, field as dataclass_field
from typing import Any, Dict, Iterable, List, Optional, cast
from unittest import mock

import pydantic
import pytest
from freezegun import freeze_time
from pydantic import Field

from datahub.api.entities.dataprocess.dataprocess_instance import DataProcessInstance
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataprocess import (
    DataProcessInstanceProperties,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataPlatformInstanceClass,
    StatusClass,
)
from datahub.metadata.urns import DataPlatformUrn, QueryUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn
from tests.test_helpers import mce_helpers
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2020-04-14 07:00:00"

dummy_datasets: List = ["dummy_dataset1", "dummy_dataset2", "dummy_dataset3"]


@dataclass
class DummySourceReport(StaleEntityRemovalSourceReport):
    datasets_scanned: int = 0
    filtered_datasets: List[str] = dataclass_field(default_factory=list)

    def report_datasets_scanned(self, count: int = 1) -> None:
        self.datasets_scanned += count

    def report_datasets_dropped(self, model: str) -> None:
        self.filtered_datasets.append(model)


class DummySourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    dataset_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for datasets to filter in ingestion.",
    )
    # Configuration for stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = pydantic.Field(
        default=None, description="Dummy source Ingestion Config."
    )
    report_failure: bool = Field(
        default=False,
        description="Should this dummy source report a failure.",
    )
    dpi_id_to_ingest: Optional[str] = Field(
        default=None,
        description="Data process instance id to ingest.",
    )
    query_id_to_ingest: Optional[str] = Field(
        default=None, description="Query id to ingest"
    )


class DummySource(StatefulIngestionSourceBase):
    """
    This is dummy source which only extract dummy datasets
    """

    source_config: DummySourceConfig
    reporter: DummySourceReport

    def __init__(self, config: DummySourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self.reporter = DummySourceReport()
        # Create and register the stateful ingestion use-case handler.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, self.source_config, self.ctx
        )

    @classmethod
    def create(cls, config_dict, ctx):
        config = DummySourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            self.stale_entity_removal_handler.workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        for dataset in dummy_datasets:
            if not self.source_config.dataset_patterns.allowed(dataset):
                self.reporter.report_datasets_dropped(dataset)
                continue
            else:
                self.reporter.report_datasets_scanned()
            dataset_urn = DatasetUrn.create_from_ids(
                platform_id="postgres",
                table_name=dataset,
                env=DEFAULT_ENV,
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=str(dataset_urn),
                aspect=StatusClass(removed=False),
            ).as_workunit()

        if self.source_config.dpi_id_to_ingest:
            dpi = DataProcessInstance(
                id=self.source_config.dpi_id_to_ingest,
                orchestrator="dummy",
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=str(dpi.urn),
                aspect=DataProcessInstanceProperties(
                    name=dpi.id,
                    created=AuditStampClass(
                        time=int(time.time() * 1000),
                        actor="urn:li:corpuser:datahub",
                    ),
                    type=dpi.type,
                ),
            ).as_workunit()

        if self.source_config.query_id_to_ingest:
            yield MetadataChangeProposalWrapper(
                entityUrn=QueryUrn(self.source_config.query_id_to_ingest).urn(),
                aspect=DataPlatformInstanceClass(
                    platform=DataPlatformUrn("bigquery").urn()
                ),
            ).as_workunit()

        if self.source_config.report_failure:
            self.reporter.report_failure("Dummy error", "Error")

    def get_report(self) -> SourceReport:
        return self.reporter


@pytest.fixture(scope="module")
def mock_generic_checkpoint_state():
    with mock.patch(
        "datahub.ingestion.source.state.entity_removal_state.GenericCheckpointState"
    ) as mock_checkpoint_state:
        checkpoint_state = mock_checkpoint_state.return_value
        checkpoint_state.serde.return_value = "utf-8"
        yield mock_checkpoint_state


@freeze_time(FROZEN_TIME)
def test_stateful_ingestion(pytestconfig, tmp_path, mock_time):
    # test stateful ingestion using dummy source
    state_file_name: str = "checkpoint_state_mces.json"
    golden_state_file_name: str = "golden_test_checkpoint_state.json"
    golden_state_file_name_after_deleted: str = (
        "golden_test_checkpoint_state_after_deleted.json"
    )
    output_file_name: str = "dummy_mces.json"
    golden_file_name: str = "golden_test_stateful_ingestion.json"
    output_file_name_after_deleted: str = "dummy_mces_stateful_after_deleted.json"
    golden_file_name_after_deleted: str = (
        "golden_test_stateful_ingestion_after_deleted.json"
    )

    test_resources_dir = pytestconfig.rootpath / "tests/unit/stateful_ingestion/state"

    base_pipeline_config = {
        "run_id": "dummy-test-stateful-ingestion",
        "pipeline_name": "dummy_stateful",
        "source": {
            "type": "tests.unit.stateful_ingestion.state.test_stateful_ingestion.DummySource",
            "config": {
                "stateful_ingestion": {
                    "enabled": True,
                    "remove_stale_metadata": True,
                    "fail_safe_threshold": 100,
                    "state_provider": {
                        "type": "file",
                        "config": {
                            "filename": f"{tmp_path}/{state_file_name}",
                        },
                    },
                },
                "dpi_id_to_ingest": "job1",
                "query_id_to_ingest": "query1",
            },
        },
        "sink": {
            "type": "file",
            "config": {},
        },
    }

    with mock.patch(
        "datahub.ingestion.source.state.stale_entity_removal_handler.StaleEntityRemovalHandler._get_state_obj"
    ) as mock_state, mock.patch(
        "datahub.ingestion.source.state.stale_entity_removal_handler.STATEFUL_INGESTION_IGNORED_ENTITY_TYPES",
        {},
        # Second mock is to imitate earlier behavior where entity type check was not present when adding entity to state
    ):
        mock_state.return_value = GenericCheckpointState(serde="utf-8")
        pipeline_run1 = None
        pipeline_run1_config: Dict[str, Dict[str, Dict[str, Any]]] = dict(  # type: ignore
            base_pipeline_config  # type: ignore
        )
        pipeline_run1_config["sink"]["config"]["filename"] = (
            f"{tmp_path}/{output_file_name}"
        )
        pipeline_run1 = Pipeline.create(pipeline_run1_config)
        pipeline_run1.run()
        pipeline_run1.raise_from_status()
        pipeline_run1.pretty_print_summary()

        # validate both dummy source mces and checkpoint state mces files
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / output_file_name,
            golden_path=f"{test_resources_dir}/{golden_file_name}",
        )
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / state_file_name,
            golden_path=f"{test_resources_dir}/{golden_state_file_name}",
        )
        checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)
        assert checkpoint1
        assert checkpoint1.state

    with mock.patch(
        "datahub.ingestion.source.state.stale_entity_removal_handler.StaleEntityRemovalHandler._get_state_obj"
    ) as mock_state:
        mock_state.return_value = GenericCheckpointState(serde="utf-8")
        pipeline_run2 = None
        pipeline_run2_config: Dict[str, Dict[str, Dict[str, Any]]] = dict(
            base_pipeline_config  # type: ignore
        )
        pipeline_run2_config["source"]["config"]["dataset_patterns"] = {
            "allow": ["dummy_dataset1", "dummy_dataset2"],
        }
        pipeline_run2_config["source"]["config"]["dpi_id_to_ingest"] = "job2"
        pipeline_run2_config["source"]["config"]["query_id_to_ingest"] = "query2"

        pipeline_run2_config["sink"]["config"]["filename"] = (
            f"{tmp_path}/{output_file_name_after_deleted}"
        )
        pipeline_run2 = Pipeline.create(pipeline_run2_config)
        pipeline_run2.run()
        pipeline_run2.raise_from_status()
        pipeline_run2.pretty_print_summary()

        # validate both updated dummy source mces and checkpoint state mces files after deleting dataset
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / output_file_name_after_deleted,
            golden_path=f"{test_resources_dir}/{golden_file_name_after_deleted}",
        )
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / state_file_name,
            golden_path=f"{test_resources_dir}/{golden_state_file_name_after_deleted}",
        )
        checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)
        assert checkpoint2
        assert checkpoint2.state

        # Validate that all providers have committed successfully.
        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run1, expected_providers=1
        )
        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run2, expected_providers=1
        )

        # Perform all assertions on the states. The deleted table should not be
        # part of the second state
        state1 = cast(GenericCheckpointState, checkpoint1.state)
        state2 = cast(GenericCheckpointState, checkpoint2.state)

        difference_dataset_urns = list(
            state1.get_urns_not_in(type="dataset", other_checkpoint_state=state2)
        )
        # the difference in dataset urns is the dataset which is not allowed to ingest
        assert len(difference_dataset_urns) == 1
        deleted_dataset_urns: List[str] = [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,dummy_dataset3,PROD)",
        ]
        assert sorted(deleted_dataset_urns) == sorted(difference_dataset_urns)

        report = pipeline_run2.source.get_report()
        assert isinstance(report, StaleEntityRemovalSourceReport)
        # assert report last ingestion state non_deletable entity urns
        non_deletable_urns: List[str] = [
            "urn:li:dataProcessInstance:478810e859f870a54f72c681f41af619",
            "urn:li:query:query1",
        ]
        assert sorted(non_deletable_urns) == sorted(
            report.last_state_non_deletable_entities
        )


@freeze_time(FROZEN_TIME)
def test_stateful_ingestion_failure(pytestconfig, tmp_path, mock_time):
    # test stateful ingestion using dummy source with pipeline execution failed in second ingestion
    state_file_name: str = "checkpoint_state_mces_failure.json"
    golden_state_file_name: str = "golden_test_checkpoint_state_failure.json"
    golden_state_file_name_after_deleted: str = (
        "golden_test_checkpoint_state_after_deleted_failure.json"
    )
    output_file_name: str = "dummy_mces_failure.json"
    golden_file_name: str = "golden_test_stateful_ingestion_failure.json"
    output_file_name_after_deleted: str = (
        "dummy_mces_stateful_after_deleted_failure.json"
    )
    golden_file_name_after_deleted: str = (
        "golden_test_stateful_ingestion_after_deleted_failure.json"
    )

    test_resources_dir = pytestconfig.rootpath / "tests/unit/stateful_ingestion/state"

    base_pipeline_config = {
        "run_id": "dummy-test-stateful-ingestion",
        "pipeline_name": "dummy_stateful",
        "source": {
            "type": "tests.unit.stateful_ingestion.state.test_stateful_ingestion.DummySource",
            "config": {
                "stateful_ingestion": {
                    "enabled": True,
                    "remove_stale_metadata": True,
                    "state_provider": {
                        "type": "file",
                        "config": {
                            "filename": f"{tmp_path}/{state_file_name}",
                        },
                    },
                },
            },
        },
        "sink": {
            "type": "file",
            "config": {},
        },
    }

    with mock.patch(
        "datahub.ingestion.source.state.stale_entity_removal_handler.StaleEntityRemovalHandler._get_state_obj"
    ) as mock_state:
        mock_state.return_value = GenericCheckpointState(serde="utf-8")
        pipeline_run1 = None
        pipeline_run1_config: Dict[str, Dict[str, Dict[str, Any]]] = dict(  # type: ignore
            base_pipeline_config  # type: ignore
        )
        pipeline_run1_config["sink"]["config"]["filename"] = (
            f"{tmp_path}/{output_file_name}"
        )
        pipeline_run1 = Pipeline.create(pipeline_run1_config)
        pipeline_run1.run()
        pipeline_run1.raise_from_status()
        pipeline_run1.pretty_print_summary()

        # validate both dummy source mces and checkpoint state mces files
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / output_file_name,
            golden_path=f"{test_resources_dir}/{golden_file_name}",
        )
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / state_file_name,
            golden_path=f"{test_resources_dir}/{golden_state_file_name}",
        )
        checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)
        assert checkpoint1
        assert checkpoint1.state

    with mock.patch(
        "datahub.ingestion.source.state.stale_entity_removal_handler.StaleEntityRemovalHandler._get_state_obj"
    ) as mock_state:
        mock_state.return_value = GenericCheckpointState(serde="utf-8")
        pipeline_run2 = None
        pipeline_run2_config: Dict[str, Dict[str, Dict[str, Any]]] = dict(
            base_pipeline_config  # type: ignore
        )
        pipeline_run2_config["source"]["config"]["dataset_patterns"] = {
            "allow": ["dummy_dataset1", "dummy_dataset2"],
        }
        pipeline_run2_config["source"]["config"]["report_failure"] = True
        pipeline_run2_config["sink"]["config"]["filename"] = (
            f"{tmp_path}/{output_file_name_after_deleted}"
        )
        pipeline_run2 = Pipeline.create(pipeline_run2_config)
        pipeline_run2.run()
        pipeline_run2.pretty_print_summary()

        # validate both updated dummy source mces and checkpoint state mces files after deleting dataset
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / output_file_name_after_deleted,
            golden_path=f"{test_resources_dir}/{golden_file_name_after_deleted}",
        )
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / state_file_name,
            golden_path=f"{test_resources_dir}/{golden_state_file_name_after_deleted}",
        )
        checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)
        assert checkpoint2
        assert checkpoint2.state

        # Validate that all providers have committed successfully.
        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run1, expected_providers=1
        )
        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run2, expected_providers=1
        )

        # Perform assertions on the states. The deleted table should be
        # still part of the second state as pipeline run failed
        state1 = cast(GenericCheckpointState, checkpoint1.state)
        state2 = cast(GenericCheckpointState, checkpoint2.state)
        assert state1 == state2
