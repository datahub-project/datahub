from importlib.machinery import SourceFileLoader
import sys

import pytest
from feast import FeatureStore
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2020-04-14 07:00:00"


@freeze_time(FROZEN_TIME)
@pytest.mark.skipif(sys.version_info < (3, 7), reason="requires Python 3.7 or higher")
@pytest.mark.integration
def test_feast_repository_ingest(pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/feast-repository/"
    repository_path = test_resources_dir / "feature_store"
    output_path = tmp_path / "feast_repository_mces.json"

    features = SourceFileLoader(
        "features", str(repository_path / "features.py")
    ).load_module()
    feature_store = FeatureStore(repo_path=repository_path)

    feature_store.apply(
        [
            features.driver_entity,
            features.driver_hourly_stats_view,
            features.transformed_conv_rate,
        ]
    )

    pipeline = Pipeline.create(
        {
            "run_id": "feast-repository-test",
            "source": {
                "type": "feast-repository",
                "config": {
                    "path": str(repository_path),
                    "environment": "PROD",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_path),
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "feast_repository_mces_golden.json",
    )
