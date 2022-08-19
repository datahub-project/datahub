from pathlib import PosixPath
from typing import Union
from unittest.mock import patch

import pytest
from freezegun import freeze_time
from iceberg.core.filesystem.file_status import FileStatus
from iceberg.core.filesystem.local_filesystem import LocalFileSystem

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2020-04-14 07:00:00"


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
                    "localfs": str(test_resources_dir / "test_data"),
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
        golden_path=test_resources_dir / "iceberg_mces_golden.json",
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
    test_resources_dir = pytestconfig.rootpath / "tests/integration/iceberg/test_data"

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
        golden_path=test_resources_dir
        / "datahub/integration/profiling/iceberg_mces_golden.json",
    )
