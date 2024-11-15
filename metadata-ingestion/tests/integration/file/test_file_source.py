import pathlib
from unittest import mock

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from tests.test_helpers import mce_helpers


def test_stateful_ingestion(tmp_path, pytestconfig):
    state_file_name = "state.json"
    golden_state_file_name = "state_golden.json"

    test_resources_dir: pathlib.Path = pytestconfig.rootpath / "tests/integration/file"
    pipeline_config = {
        "run_id": "test-run",
        "pipeline_name": "dummy_stateful",
        "source": {
            "type": "file",
            "config": {
                "filename": str(test_resources_dir / "metadata_file.json"),
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
            "type": "blackhole",
            "config": {},
        },
    }
    with mock.patch(
        "datahub.ingestion.source.state.stale_entity_removal_handler.StaleEntityRemovalHandler._get_state_obj"
    ) as mock_state:
        mock_state.return_value = GenericCheckpointState(serde="utf-8")
        pipeline = Pipeline.create(pipeline_config)
        pipeline.run()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / state_file_name,
        golden_path=f"{test_resources_dir}/{golden_state_file_name}",
    )
