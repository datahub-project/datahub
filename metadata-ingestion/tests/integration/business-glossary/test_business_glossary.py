import shutil
from typing import List

import pytest
from freezegun import freeze_time

from datahub.ingestion.source.metadata import business_glossary
from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd

FROZEN_TIME = "2020-04-14 07:00:00"


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_glossary_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/business-glossary"

    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / "glossary_to_file.yml").resolve()
    shutil.copy(test_resources_dir / "business_glossary.yml", tmp_path)
    run_datahub_cmd(
        ["ingest", "--strict-warnings", "-c", f"{config_file}"], tmp_path=tmp_path
    )
    # These paths change from one instance run of the clickhouse docker to the other, and the FROZEN_TIME does not apply to these.
    ignore_paths: List[str] = [
        r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['customProperties'\]\['metadata_modification_time'\]",
        r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['customProperties'\]\['data_paths'\]",
        r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['customProperties'\]\['metadata_path'\]",
    ]
    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        ignore_paths=ignore_paths,
        output_path=tmp_path / "glossary_events.json",
        golden_path=test_resources_dir / "glossary_events_golden.json",
    )


@freeze_time(FROZEN_TIME)
def test_auto_id_creation_on_reserved_char():
    id_: str = business_glossary.create_id(["pii", "secure % password"], None, False)
    assert id_ == "24baf9389cc05c162c7148c96314d733"
