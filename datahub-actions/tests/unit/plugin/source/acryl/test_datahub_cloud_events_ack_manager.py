# test_ack_manager.py

from typing import Any, Dict

from datahub_actions.plugin.source.acryl.datahub_cloud_events_ack_manager import (
    AckManager,
)


def test_new_batch() -> None:
    ack_manager = AckManager()
    assert ack_manager.batch_id == 0
    assert ack_manager.msg_id == 0
    assert ack_manager.outstanding_acks() == 0

    ack_manager.new_batch()
    assert ack_manager.batch_id == 1
    assert ack_manager.msg_id == 0
    assert ack_manager.outstanding_acks() == 0


def test_get_meta() -> None:
    ack_manager = AckManager()
    ack_manager.new_batch()

    meta: Dict[str, Any] = ack_manager.get_meta("some event")
    assert meta["batch_id"] == 1
    assert meta["msg_id"] == 1
    assert ack_manager.outstanding_acks() == 1


def test_ack_processed() -> None:
    ack_manager = AckManager()
    ack_manager.new_batch()

    meta: Dict[str, Any] = ack_manager.get_meta("some event")
    assert ack_manager.outstanding_acks() == 1

    ack_manager.ack(meta, processed=True)
    assert ack_manager.outstanding_acks() == 0


def test_ack_not_processed() -> None:
    ack_manager = AckManager()
    ack_manager.new_batch()

    meta: Dict[str, Any] = ack_manager.get_meta("some event")
    assert ack_manager.outstanding_acks() == 1

    ack_manager.ack(meta, processed=False)
    # The ack should remain since processed=False
    assert ack_manager.outstanding_acks() == 1


def test_multiple_acks() -> None:
    ack_manager = AckManager()
    ack_manager.new_batch()

    meta1: Dict[str, Any] = ack_manager.get_meta("event1")
    meta2: Dict[str, Any] = ack_manager.get_meta("event2")
    assert ack_manager.outstanding_acks() == 2

    ack_manager.ack(meta1, processed=True)
    assert ack_manager.outstanding_acks() == 1

    ack_manager.ack(meta2, processed=True)
    assert ack_manager.outstanding_acks() == 0
