import pathlib
import site
from typing import Optional

import pytest

from datahub.metadata import schema_classes as models
from datahub.metadata.urns import DataPlatformUrn, DatasetUrn
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity_client import EntityClient
from datahub.testing.pytest_hooks import (  # noqa: F401
    load_golden_flags,
    pytest_addoption,
)

# See https://coverage.readthedocs.io/en/latest/subprocess.html#configuring-python-for-sub-process-measurement
coverage_startup_code = "import coverage; coverage.process_startup()"
site_packages_dir = pathlib.Path(site.getsitepackages()[0])
pth_file_path = site_packages_dir / "datahub_coverage_startup.pth"
pth_file_path.write_text(coverage_startup_code)


class DummyDataHubClient:
    def __init__(self) -> None:
        pass


class StubEntityClient(EntityClient):
    def __init__(self) -> None:
        # Initialize with a dummy client since we're just stubbing the behavior
        super().__init__(client=DummyDataHubClient())  # type: ignore[arg-type]  # DummyDataHubClient is a stub

    def get(self, urn: DatasetUrn) -> Optional[Dataset]:  # type: ignore[override]  # Stubbed
        if str(urn) == "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)":
            return Dataset(
                platform=str(DataPlatformUrn("urn:li:dataPlatform:snowflake")),
                name=urn.name,
                schema=[
                    models.SchemaFieldClass(
                        fieldPath="last_modified",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.DateTypeClass()
                        ),
                        nativeDataType="DATE",
                    ),
                    models.SchemaFieldClass(
                        fieldPath="last_modified_wrong_type",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.NumberTypeClass()
                        ),
                        nativeDataType="NUMBER",
                    ),
                    models.SchemaFieldClass(
                        fieldPath="wrong_type_int_as_a_string",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.StringTypeClass()
                        ),
                        nativeDataType="VARCHAR",
                    ),
                ],
            )
        return None


@pytest.fixture
def stub_entity_client() -> StubEntityClient:
    return StubEntityClient()


class StubDataHubClient:
    def __init__(self) -> None:
        self.entities = StubEntityClient()


@pytest.fixture
def stub_datahub_client() -> StubDataHubClient:
    return StubDataHubClient()
