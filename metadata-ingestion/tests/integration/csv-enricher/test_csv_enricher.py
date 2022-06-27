import pathlib

from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.csv_enricher import CSVEnricherConfig
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2022-02-03 07:00:00"


def test_csv_enricher_config():
    config = CSVEnricherConfig.parse_obj(
        dict(
            filename="../integration/csv_enricher/csv_enricher_test_data.csv",
            should_overwrite=True,
            delimiter=",",
            array_delimiter="|",
        )
    )
    assert config


@freeze_time(FROZEN_TIME)
def test_csv_enricher_source(pytestconfig, tmp_path):
    test_resources_dir: pathlib.Path = (
        pytestconfig.rootpath / "tests/integration/csv-enricher"
    )

    pipeline = Pipeline.create(
        {
            "run_id": "test-csv-enricher",
            "source": {
                "type": "csv-enricher",
                "config": {
                    "filename": f"{test_resources_dir}/csv_enricher_test_data.csv",
                    "should_overwrite": True,
                    "delimiter": ",",
                    "array_delimiter": "|",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/csv_enricher.json",
                },
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "csv_enricher.json",
        golden_path=test_resources_dir / "csv_enricher_golden.json",
    )
