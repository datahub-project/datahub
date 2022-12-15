from pathlib import PosixPath
from typing import Any, Dict, Optional, Union, cast
from unittest.mock import patch

import pytest
from freezegun import freeze_time
from iceberg.core.filesystem.file_status import FileStatus
from iceberg.core.filesystem.local_filesystem import LocalFileSystem

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.iceberg.iceberg import IcebergSource
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from tests.test_helpers import mce_helpers
from tests.test_helpers.state_helpers import (
    run_and_get_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2020-04-14 07:00:00"
GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"


def get_current_checkpoint_from_pipeline(
    pipeline: Pipeline,
) -> Optional[Checkpoint[GenericCheckpointState]]:
    iceberg_source = cast(IcebergSource, pipeline.source)
    return iceberg_source.get_current_checkpoint(
        iceberg_source.stale_entity_removal_handler.job_id
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_iceberg_ingest(pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/iceberg/"

    # Run the metadata ingestion pipeline.
    pipeline = Pipeline.create(
        {
            "run_id": "iceberg-test",
            "source": {
                "type": "iceberg",
                "config": {
                    "localfs": str(test_resources_dir / "test_data/ingest_test"),
                    "user_ownership_property": "owner",
                    "group_ownership_property": "owner",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/iceberg_mces.json",
                },
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "iceberg_mces.json",
        golden_path=test_resources_dir
        / "test_data/ingest_test/iceberg_mces_golden.json",
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_iceberg_stateful_ingest(pytestconfig, tmp_path, mock_time, mock_datahub_graph):
    test_resources_dir = (
        pytestconfig.rootpath / "tests/integration/iceberg/test_data/stateful_test"
    )
    platform_instance = "test_platform_instance"

    scd_before_deletion: Dict[str, Any] = {
        "localfs": str(test_resources_dir / "run1"),
        "user_ownership_property": "owner",
        "group_ownership_property": "owner",
        "platform_instance": f"{platform_instance}",
        # enable stateful ingestion
        "stateful_ingestion": {
            "enabled": True,
            "remove_stale_metadata": True,
            "fail_safe_threshold": 100.0,
            "state_provider": {
                "type": "datahub",
                "config": {"datahub_api": {"server": GMS_SERVER}},
            },
        },
    }

    scd_after_deletion: Dict[str, Any] = {
        "localfs": str(test_resources_dir / "run2"),
        "user_ownership_property": "owner",
        "group_ownership_property": "owner",
        "platform_instance": f"{platform_instance}",
        # enable stateful ingestion
        "stateful_ingestion": {
            "enabled": True,
            "remove_stale_metadata": True,
            "fail_safe_threshold": 100.0,
            "state_provider": {
                "type": "datahub",
                "config": {"datahub_api": {"server": GMS_SERVER}},
            },
        },
    }

    pipeline_config_dict: Dict[str, Any] = {
        "source": {
            "type": "iceberg",
            "config": scd_before_deletion,
        },
        "sink": {
            # we are not really interested in the resulting events for this test
            "type": "console"
        },
        "pipeline_name": "test_pipeline",
    }

    with patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:

        # Both checkpoint and reporting will use the same mocked graph instance.
        mock_checkpoint.return_value = mock_datahub_graph

        # Do the first run of the pipeline and get the default job's checkpoint.
        pipeline_run1 = run_and_get_pipeline(pipeline_config_dict)
        checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)

        assert checkpoint1
        assert checkpoint1.state

        # Set iceberg config where a table is deleted.
        pipeline_config_dict["source"]["config"] = scd_after_deletion
        # Capture MCEs of second run to validate Status(removed=true)
        deleted_mces_path = f"{tmp_path}/iceberg_deleted_mces.json"
        pipeline_config_dict["sink"]["type"] = "file"
        pipeline_config_dict["sink"]["config"] = {"filename": deleted_mces_path}

        # Do the second run of the pipeline.
        pipeline_run2 = run_and_get_pipeline(pipeline_config_dict)
        checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)

        assert checkpoint2
        assert checkpoint2.state

        # Perform all assertions on the states. The deleted table should not be
        # part of the second state
        state1 = checkpoint1.state
        state2 = checkpoint2.state
        difference_urns = list(
            state1.get_urns_not_in(type="dataset", other_checkpoint_state=state2)
        )

        assert len(difference_urns) == 1

        urn1 = "urn:li:dataset:(urn:li:dataPlatform:iceberg,test_platform_instance.namespace.iceberg_test_2,PROD)"

        assert urn1 in difference_urns

        # Validate that all providers have committed successfully.
        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run1, expected_providers=1
        )
        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run2, expected_providers=1
        )

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=deleted_mces_path,
            golden_path=test_resources_dir / "iceberg_deleted_table_mces_golden.json",
        )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_iceberg_profiling(pytestconfig, tmp_path, mock_time):
    """
    This test is using a table created using https://github.com/tabular-io/docker-spark-iceberg.
    Here are the DDL statements that you can execute with `spark-sql`:
    ```SQL
    CREATE TABLE datahub.integration.profiling (
        field_int bigint COMMENT 'An integer field',
        field_str string COMMENT 'A string field',
        field_timestamp timestamp COMMENT 'A timestamp field')
        USING iceberg;

    INSERT INTO datahub.integration.profiling VALUES (1, 'row1', current_timestamp()), (2, 'row2', null);
    INSERT INTO datahub.integration.profiling VALUES (3, 'row3', current_timestamp()), (4, 'row4', null);
    ```

    When importing the metadata files into this test, we need to create a `version-hint.text` with a value that
    reflects the version of the table, and then change the code in `TestLocalFileSystem._replace_path()` accordingly.
    """
    test_resources_dir = (
        pytestconfig.rootpath / "tests/integration/iceberg/test_data/profiling_test"
    )

    # Run the metadata ingestion pipeline.
    pipeline = Pipeline.create(
        {
            "run_id": "iceberg-test",
            "source": {
                "type": "iceberg",
                "config": {
                    "localfs": str(test_resources_dir),
                    "user_ownership_property": "owner",
                    "group_ownership_property": "owner",
                    "max_path_depth": 3,
                    "profiling": {
                        "enabled": True,
                    },
                    "table_pattern": {"allow": ["datahub.integration.profiling"]},
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/iceberg_mces.json",
                },
            },
        }
    )

    class TestLocalFileSystem(LocalFileSystem):
        # This class acts as a wrapper on LocalFileSystem to intercept calls using a path location.
        # The wrapper will normalize those paths to be usable by the test.
        fs: LocalFileSystem

        @staticmethod
        def _replace_path(path: Union[str, PosixPath]) -> str:
            # When the Iceberg table was created, its warehouse folder was '/home/iceberg/warehouse'.  Iceberg tables
            # are not portable, so we need to replace the warehouse folder by the test location at runtime.
            normalized_path: str = str(path).replace(
                "/home/iceberg/warehouse", str(test_resources_dir)
            )

            # When the Iceberg table was created, a postgres catalog was used instead of a HadoopCatalog.  The HadoopCatalog
            # expects a file named 'v{}.metadata.json' where {} is the version number from 'version-hint.text'.  Since
            # 'v2.metadata.json' does not exist, we will redirect the call to '00002-02782173-8364-4caf-a3c4-9567c1d6608f.metadata.json'.
            if normalized_path.endswith("v2.metadata.json"):
                return normalized_path.replace(
                    "v2.metadata.json",
                    "00002-cc241948-4c12-46d0-9a75-ce3578ec03d4.metadata.json",
                )
            return normalized_path

        def __init__(self, fs: LocalFileSystem) -> None:
            self.fs = fs

        def open(self, path: str, mode: str = "rb") -> object:
            return self.fs.open(TestLocalFileSystem._replace_path(path), mode)

        def delete(self, path: str) -> None:
            self.fs.delete(TestLocalFileSystem._replace_path(path))

        def stat(self, path: str) -> FileStatus:
            return self.fs.stat(TestLocalFileSystem._replace_path(path))

        @staticmethod
        def fix_path(path: str) -> str:
            return TestLocalFileSystem.fs.fix_path(
                TestLocalFileSystem._replace_path(path)
            )

        def create(self, path: str, overwrite: bool = False) -> object:
            return self.fs.create(TestLocalFileSystem._replace_path(path), overwrite)

        def rename(self, src: str, dest: str) -> bool:
            return self.fs.rename(
                TestLocalFileSystem._replace_path(src),
                TestLocalFileSystem._replace_path(dest),
            )

        def exists(self, path: str) -> bool:
            return self.fs.exists(TestLocalFileSystem._replace_path(path))

    local_fs_wrapper: TestLocalFileSystem = TestLocalFileSystem(
        LocalFileSystem.get_instance()
    )
    with patch.object(LocalFileSystem, "get_instance", return_value=local_fs_wrapper):
        pipeline.run()
        pipeline.raise_from_status()

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "iceberg_mces.json",
        golden_path=test_resources_dir / "iceberg_mces_golden.json",
    )
