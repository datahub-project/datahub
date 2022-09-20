import pytest
import test_tableau_common
from freezegun import freeze_time

FROZEN_TIME = "2021-12-07 07:00:00"


@freeze_time(FROZEN_TIME)
@pytest.mark.slow_unit
def test_tableau_usage_stat(pytestconfig, tmp_path):
    output_file_name: str = "tableau_stat_mces.json"
    golden_file_name: str = "tableau_state_mces_golden.json"
    side_effect_query_metadata = test_tableau_common.define_query_metadata_func(
        "workbooksConnection_0.json", "workbooksConnection_state_all.json"
    )
    test_tableau_common.tableau_ingest_common(
        pytestconfig,
        tmp_path,
        side_effect_query_metadata,
        golden_file_name,
        output_file_name,
    )
