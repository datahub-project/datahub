import pytest
import warnings


@pytest.fixture(autouse=True)
def handle_base_path_warning():
    warnings.filterwarnings(
        "ignore",
        message="The 'base_path' option is deprecated",
        module="datahub.ingestion.source.delta_lake.config",
    )
