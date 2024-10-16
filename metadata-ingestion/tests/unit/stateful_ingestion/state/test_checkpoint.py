from datetime import datetime, timezone
from typing import Dict, List

import pydantic
import pytest

from datahub.ingestion.source.state.checkpoint import Checkpoint, CheckpointStateBase
from datahub.ingestion.source.state.sql_common_state import (
    BaseSQLAlchemyCheckpointState,
)
from datahub.ingestion.source.state.usage_common_state import (
    BaseTimeWindowCheckpointState,
)
from datahub.metadata.schema_classes import (
    DatahubIngestionCheckpointClass,
    IngestionCheckpointStateClass,
)

# 1. Setup common test param values.
test_pipeline_name: str = "test_pipeline"
test_job_name: str = "test_job_1"
test_run_id: str = "test_run_1"


def _assert_checkpoint_deserialization(
    serialized_checkpoint_state: IngestionCheckpointStateClass,
    expected_checkpoint_state: CheckpointStateBase,
) -> Checkpoint:
    # Serialize a checkpoint aspect with the previous state.
    checkpoint_aspect = DatahubIngestionCheckpointClass(
        timestampMillis=int(datetime.now(tz=timezone.utc).timestamp() * 1000),
        pipelineName=test_pipeline_name,
        platformInstanceId="this-can-be-anything-and-will-be-ignored",
        config="this-is-also-ignored",
        state=serialized_checkpoint_state,
        runId=test_run_id,
    )

    # 2. Create the checkpoint from the raw checkpoint aspect and validate.
    checkpoint_obj = Checkpoint.create_from_checkpoint_aspect(
        job_name=test_job_name,
        checkpoint_aspect=checkpoint_aspect,
        state_class=type(expected_checkpoint_state),
    )

    expected_checkpoint_obj = Checkpoint(
        job_name=test_job_name,
        pipeline_name=test_pipeline_name,
        run_id=test_run_id,
        state=expected_checkpoint_state,
    )
    assert checkpoint_obj == expected_checkpoint_obj

    return checkpoint_obj


# 2. Create the params for parametrized tests.


def _make_sql_alchemy_checkpoint_state() -> BaseSQLAlchemyCheckpointState:
    # Note that the urns here purposely use a lowercase env, even though it's
    # technically incorrect. This is purely for backwards compatibility testing, but
    # all existing code uses correctly formed envs.
    base_sql_alchemy_checkpoint_state_obj = BaseSQLAlchemyCheckpointState()
    base_sql_alchemy_checkpoint_state_obj.add_checkpoint_urn(
        type="table", urn="urn:li:dataset:(urn:li:dataPlatform:mysql,db1.t1,prod)"
    )
    base_sql_alchemy_checkpoint_state_obj.add_checkpoint_urn(
        type="view", urn="urn:li:dataset:(urn:li:dataPlatform:mysql,db1.v1,prod)"
    )
    return base_sql_alchemy_checkpoint_state_obj


def _make_usage_checkpoint_state() -> BaseTimeWindowCheckpointState:
    base_usage_checkpoint_state_obj = BaseTimeWindowCheckpointState(
        version="2.0", begin_timestamp_millis=1, end_timestamp_millis=100
    )
    return base_usage_checkpoint_state_obj


_checkpoint_aspect_test_cases: Dict[str, CheckpointStateBase] = {
    # An instance of BaseSQLAlchemyCheckpointState.
    "BaseSQLAlchemyCheckpointState": _make_sql_alchemy_checkpoint_state(),
    # An instance of BaseTimeWindowCheckpointState.
    "BaseTimeWindowCheckpointState": _make_usage_checkpoint_state(),
}


# 3. Define the test with the params


@pytest.mark.parametrize(
    "state_obj",
    _checkpoint_aspect_test_cases.values(),
    ids=_checkpoint_aspect_test_cases.keys(),
)
def test_checkpoint_serde(state_obj: CheckpointStateBase) -> None:
    """
    Tests CheckpointStateBase.to_bytes() and Checkpoint.create_from_checkpoint_aspect().
    """

    # 1. Construct the raw aspect object with the state
    checkpoint_state = IngestionCheckpointStateClass(
        formatVersion=state_obj.version,
        serde=state_obj.serde,
        payload=state_obj.to_bytes(),
    )

    _assert_checkpoint_deserialization(checkpoint_state, state_obj)


@pytest.mark.parametrize(
    "state_obj",
    _checkpoint_aspect_test_cases.values(),
    ids=_checkpoint_aspect_test_cases.keys(),
)
def test_serde_idempotence(state_obj):
    """
    Verifies that Serialization + Deserialization reconstructs the original object fully.
    """
    # 1. Construct the initial checkpoint object
    orig_checkpoint_obj = Checkpoint(
        job_name=test_job_name,
        pipeline_name=test_pipeline_name,
        run_id=test_run_id,
        state=state_obj,
    )

    # 2. Convert it to the aspect form.
    checkpoint_aspect = orig_checkpoint_obj.to_checkpoint_aspect(
        max_allowed_state_size=2**20
    )
    assert checkpoint_aspect is not None

    # 3. Reconstruct from the aspect form and verify that it matches the original.
    serde_checkpoint_obj = Checkpoint.create_from_checkpoint_aspect(
        job_name=test_job_name,
        checkpoint_aspect=checkpoint_aspect,
        state_class=type(state_obj),
    )
    assert orig_checkpoint_obj == serde_checkpoint_obj


def test_supported_encodings():
    """
    Tests utf-8 and base85-bz2-json encodings
    """
    test_state = BaseTimeWindowCheckpointState(
        version="1.0", begin_timestamp_millis=1, end_timestamp_millis=100
    )

    # 1. Test UTF-8 encoding
    test_state.serde = "utf-8"
    test_serde_idempotence(test_state)

    # 2. Test Base85 encoding
    test_state.serde = "base85-bz2-json"
    test_serde_idempotence(test_state)


def test_base85_upgrade_pickle_to_json():
    """Verify that base85 (pickle) encoding is transitioned to base85-bz2-json."""

    base85_payload = b"LRx4!F+o`-Q&~9zyaE6Km;c~@!8ry1Vd6kI1ULe}@BgM?1daeO0O_j`RP>&v5Eub8X^>>mqalb7C^byc8UsjrKmgDKAR1|q0#p(YC>k_rkk9}C0g>tf5XN6Ukbt0I-PV9G8w@zi7T+Sfbo$@HCtElKF-WJ9s~2<3(ryuxT}MN0DW*v>5|o${#bF{|bU_>|0pOAXZ$h9H+K5Hnfao<V0t4|A&l|ECl%3a~3snn}%ap>6Y<yIr$4eZIcxS2Ig`q(J&`QRF$0_OwQfa!>g3#ELVd4P5nvyX?j>N&ZHgqcR1Zc?#LWa^1m=n<!NpoAI5xrS(_*3yB*fiuZ44Funf%Sq?N|V|85WFwtbQE8kLB%FHC-}RPDZ+$-$Q9ra"
    checkpoint_state = IngestionCheckpointStateClass(
        formatVersion="1.0", serde="base85", payload=base85_payload
    )

    checkpoint = _assert_checkpoint_deserialization(
        checkpoint_state, _checkpoint_aspect_test_cases["BaseSQLAlchemyCheckpointState"]
    )
    assert checkpoint.state.serde == "base85-bz2-json"
    assert len(checkpoint.state.to_bytes()) < len(base85_payload)


@pytest.mark.parametrize(
    "serde",
    ["utf-8", "base85-bz2-json"],
)
def test_state_forward_compatibility(serde: str) -> None:
    class PrevState(CheckpointStateBase):
        list_a: List[str]
        list_b: List[str]

    class NextState(CheckpointStateBase):
        list_stuff: List[str]

        @pydantic.root_validator(pre=True, allow_reuse=True)
        def _migrate(cls, values: dict) -> dict:
            values.setdefault("list_stuff", [])
            values["list_stuff"] += values.pop("list_a", [])
            values["list_stuff"] += values.pop("list_b", [])
            return values

    prev_state = PrevState(list_a=["a", "b"], list_b=["c", "d"], serde=serde)
    expected_next_state = NextState(list_stuff=["a", "b", "c", "d"], serde=serde)

    checkpoint_state = IngestionCheckpointStateClass(
        formatVersion=prev_state.version,
        serde=prev_state.serde,
        payload=prev_state.to_bytes(),
    )

    _assert_checkpoint_deserialization(checkpoint_state, expected_next_state)
