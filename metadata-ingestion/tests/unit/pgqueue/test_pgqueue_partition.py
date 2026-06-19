from datahub.pgqueue.repository import advisory_lock_key, stable_partition_id


def test_stable_partition_id_deterministic() -> None:
    assert stable_partition_id(
        "urn:li:dataset:(foo,bar,PROD)", 4
    ) == stable_partition_id("urn:li:dataset:(foo,bar,PROD)", 4)


def test_stable_partition_id_range() -> None:
    p = stable_partition_id("k", 8)
    assert 0 <= p < 8


def test_advisory_lock_key_stable() -> None:
    assert advisory_lock_key(1, 0) == advisory_lock_key(1, 0)
    assert advisory_lock_key(1, 0) != advisory_lock_key(1, 1)
