from typing import Any, Dict, List, cast

from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from tests.test_helpers import mce_helpers
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2020-04-14 07:00:00"


@freeze_time(FROZEN_TIME)
def test_stateful_ingestion(pytestconfig, tmp_path, mock_time):
    # test stateful ingestion using dummy source
    output_file_name: str = "dummy_mces.json"
    golden_file_name: str = "golden_test_stateful_ingestion.json"
    output_file_deleted_name: str = "dummy_mces_deleted_stateful.json"
    golden_file_deleted_name: str = "golden_test_deleted_stateful_ingestion.json"

    test_resources_dir = pytestconfig.rootpath / "tests/unit/stateful_ingestion/state"

    base_pipeline_config = {
        "run_id": "dummy-test-stateful-ingestion",
        "pipeline_name": "dummy_stateful",
        "source": {
            "type": "dummy",
            "config": {
                "stateful_ingestion": {
                    "enabled": True,
                    "remove_stale_metadata": True,
                    "state_provider": {
                        "type": "file",
                        "config": {
                            "filename": f"{tmp_path}/checkpoint_mces.json",
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

    pipeline_run1 = None
    pipeline_run1_config: Dict[str, Dict[str, Dict[str, Any]]] = dict(  # type: ignore
        base_pipeline_config  # type: ignore
    )
    pipeline_run1_config["sink"]["config"][
        "filename"
    ] = f"{tmp_path}/{output_file_name}"
    pipeline_run1 = Pipeline.create(pipeline_run1_config)
    pipeline_run1.run()
    pipeline_run1.raise_from_status()
    pipeline_run1.pretty_print_summary()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / output_file_name,
        golden_path=f"{test_resources_dir}/{golden_file_name}",
    )

    checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)
    assert checkpoint1
    assert checkpoint1.state

    pipeline_run2 = None
    pipeline_run2_config: Dict[str, Dict[str, Dict[str, Any]]] = dict(base_pipeline_config)  # type: ignore
    pipeline_run2_config["source"]["config"]["dataset_patterns"] = {
        "allow": ["dummy_dataset1", "dummy_dataset2"],
    }
    pipeline_run2_config["sink"]["config"][
        "filename"
    ] = f"{tmp_path}/{output_file_deleted_name}"
    pipeline_run2 = Pipeline.create(pipeline_run2_config)
    pipeline_run2.run()
    pipeline_run2.raise_from_status()
    pipeline_run2.pretty_print_summary()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / output_file_deleted_name,
        golden_path=f"{test_resources_dir}/{golden_file_deleted_name}",
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
    # the difference in dataset urns are all the views that are not reachable from the model file
    assert len(difference_dataset_urns) == 1
    deleted_dataset_urns: List[str] = [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,dummy_dataset3,PROD)",
    ]
    assert sorted(deleted_dataset_urns) == sorted(difference_dataset_urns)
