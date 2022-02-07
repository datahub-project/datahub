from datahub.emitter.mce_builder import make_container_urn, make_dataset_urn
from datahub.ingestion.source.state.sql_common_state import (
    BaseSQLAlchemyCheckpointState,
)


def test_sql_common_state() -> None:
    state1 = BaseSQLAlchemyCheckpointState()
    test_table_urn = make_dataset_urn("test_platform", "db1.test_table1", "test")
    state1.add_table_urn(test_table_urn)
    test_view_urn = make_dataset_urn("test_platform", "db1.test_view1", "test")
    state1.add_view_urn(test_view_urn)

    test_container_urn = make_container_urn("test_container")
    state1.add_container_guid(test_container_urn)

    state2 = BaseSQLAlchemyCheckpointState()

    table_urns_diff = list(state1.get_table_urns_not_in(state2))
    assert len(table_urns_diff) == 1 and table_urns_diff[0] == test_table_urn

    view_urns_diff = list(state1.get_view_urns_not_in(state2))
    assert len(view_urns_diff) == 1 and view_urns_diff[0] == test_view_urn

    container_urns_diff = list(state1.get_container_urns_not_in(state2))
    assert (
        len(container_urns_diff) == 1 and container_urns_diff[0] == test_container_urn
    )
