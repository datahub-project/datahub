from datahub.emitter.mce_builder import make_container_urn, make_dataset_urn
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.sql_common_state import (
    BaseSQLAlchemyCheckpointState,
)


def test_sql_common_state() -> None:
    state1 = BaseSQLAlchemyCheckpointState()
    test_table_urn = make_dataset_urn("test_platform", "db1.test_table1", "test")
    state1.add_checkpoint_urn(type="table", urn=test_table_urn)
    test_view_urn = make_dataset_urn("test_platform", "db1.test_view1", "test")
    state1.add_checkpoint_urn(type="view", urn=test_view_urn)

    test_container_urn = make_container_urn("test_container")
    state1.add_checkpoint_urn(type="container", urn=test_container_urn)

    state2 = BaseSQLAlchemyCheckpointState()

    dataset_urns_diff = list(
        state1.get_urns_not_in(type="dataset", other_checkpoint_state=state2)
    )
    assert len(dataset_urns_diff) == 2 and sorted(dataset_urns_diff) == sorted(
        [
            test_table_urn,
            test_view_urn,
        ]
    )

    container_urns_diff = list(
        state1.get_urns_not_in(type="container", other_checkpoint_state=state2)
    )
    assert (
        len(container_urns_diff) == 1 and container_urns_diff[0] == test_container_urn
    )


def test_state_backward_compat() -> None:
    state = BaseSQLAlchemyCheckpointState.parse_obj(
        dict(
            encoded_table_urns=["mysql||db1.t1||PROD"],
            encoded_view_urns=["mysql||db1.v1||PROD"],
            encoded_container_urns=["1154d1da73a95376c9f33f47694cf1de"],
            encoded_assertion_urns=["815963e1332b46a203504ba46ebfab24"],
        )
    )
    assert state == BaseSQLAlchemyCheckpointState(
        urns=[
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db1.t1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db1.v1,PROD)",
            "urn:li:container:1154d1da73a95376c9f33f47694cf1de",
            "urn:li:assertion:815963e1332b46a203504ba46ebfab24",
        ]
    )


def test_deduplication_and_order_preservation() -> None:
    state = GenericCheckpointState(
        urns=[
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db1.t1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db1.v1,PROD)",
            "urn:li:container:1154d1da73a95376c9f33f47694cf1de",
            "urn:li:assertion:815963e1332b46a203504ba46ebfab24",
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db1.v1,PROD)",  # duplicate
        ]
    )
    assert len(state.urns) == 4

    state.add_checkpoint_urn(
        type="dataset",
        urn="urn:li:dataset:(urn:li:dataPlatform:mysql,db1.v1,PROD)",  # duplicate
    )
    assert len(state.urns) == 4

    state.add_checkpoint_urn(
        type="dataset",
        urn="urn:li:dataset:(urn:li:dataPlatform:mysql,this_one_is_new,PROD)",
    )
    assert len(state.urns) == 5

    assert list(state.urns) == [
        "urn:li:dataset:(urn:li:dataPlatform:mysql,db1.t1,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:mysql,db1.v1,PROD)",
        "urn:li:container:1154d1da73a95376c9f33f47694cf1de",
        "urn:li:assertion:815963e1332b46a203504ba46ebfab24",
        "urn:li:dataset:(urn:li:dataPlatform:mysql,this_one_is_new,PROD)",
    ]

    # verifies that the state can be serialized without raising an error
    json = state.json()
    assert json
