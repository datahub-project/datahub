from datahub.pgqueue.contiguous_offset import advance_watermark


def test_advance_watermark_empty() -> None:
    assert advance_watermark(5, []) == 5


def test_advance_watermark_contiguous_run() -> None:
    assert advance_watermark(0, [1, 2, 3]) == 3


def test_advance_watermark_stops_at_gap() -> None:
    assert advance_watermark(0, [2, 3]) == 0
    assert advance_watermark(0, [1, 3]) == 1


def test_advance_watermark_out_of_order_acks() -> None:
    assert advance_watermark(0, [3, 1, 2]) == 3
