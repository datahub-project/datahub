from typing import Any, cast

from datahub.ingestion.source.external_dq.state import (
    ExternalDQCheckpointState,
    ExternalDQStateHandler,
    LoadedState,
    advance,
    plan_window,
    run_key,
)
from datahub.ingestion.source.state.stateful_ingestion_base import StateProviderWrapper
from tests.unit.external_dq._fixtures import FakeStateProvider

HOUR = 3_600_000


def handler_for(provider: FakeStateProvider, **kwargs: Any) -> ExternalDQStateHandler:
    return ExternalDQStateHandler(
        state_provider=cast(StateProviderWrapper, provider),
        pipeline_name="p",
        run_id="run",
        **kwargs,
    )


def test_plan_window_first_and_subsequent_runs() -> None:
    first = plan_window(
        last_watermark=None,
        last_next_start=None,
        last_recent={},
        now_millis=10 * HOUR,
        initial_lookback_ms=5 * HOUR,
        overlap_ms=HOUR,
    )
    assert first.start_millis == 5 * HOUR and first.seen == frozenset()
    later = plan_window(
        last_watermark=8 * HOUR,
        last_next_start=7 * HOUR,
        last_recent={"k": 8 * HOUR},
        now_millis=10 * HOUR,
        initial_lookback_ms=5 * HOUR,
        overlap_ms=HOUR,
    )
    assert later.start_millis == 7 * HOUR and later.seen == frozenset({"k"})


def test_plan_window_never_starts_before_the_stored_next_start() -> None:
    # `recent` only covers rows from the stored next start on, so reading earlier
    # (e.g. after raising late_arrival_minutes) would re-emit published results.
    widened = plan_window(
        last_watermark=8 * HOUR,
        last_next_start=7 * HOUR,
        last_recent={},
        now_millis=10 * HOUR,
        initial_lookback_ms=5 * HOUR,
        overlap_ms=4 * HOUR,
    )
    assert widened.start_millis == 7 * HOUR


def test_advance_keeps_boundary_keys() -> None:
    watermark, recent = advance(
        last_watermark=8 * HOUR,
        last_recent={"old": 6 * HOUR, "edge": 8 * HOUR},
        observed={"late": 7 * HOUR + 1, "new": 9 * HOUR},
        overlap_ms=HOUR,
        now_millis=10 * HOUR,
    )
    assert watermark == 9 * HOUR
    # Everything at or after watermark - overlap is re-read next run, so it must be kept.
    assert recent == {"edge": 8 * HOUR, "new": 9 * HOUR}
    assert advance(
        last_watermark=None,
        last_recent={},
        observed={},
        overlap_ms=HOUR,
        now_millis=10 * HOUR,
    ) == (None, {})


def test_advance_caps_the_watermark_at_now() -> None:
    watermark, recent = advance(
        last_watermark=8 * HOUR,
        last_recent={},
        observed={"ahead": 12 * HOUR},
        overlap_ms=HOUR,
        now_millis=10 * HOUR,
    )
    assert watermark == 10 * HOUR
    assert recent == {"ahead": 12 * HOUR}


def test_advance_holds_the_watermark_for_unresolved_rows() -> None:
    # The next window starts at watermark - overlap, so holding at the oldest
    # unresolved row + overlap makes that row the next window start.
    watermark, recent = advance(
        last_watermark=8 * HOUR,
        last_recent={},
        observed={"new": 12 * HOUR},
        overlap_ms=HOUR,
        now_millis=20 * HOUR,
        hold_millis=9 * HOUR,
    )
    assert watermark == 10 * HOUR
    assert recent == {"new": 12 * HOUR}


def test_run_key_does_not_collide_on_separator_characters() -> None:
    assert run_key("a:b", "c") != run_key("a", "b:c")


def test_save_then_load_round_trips() -> None:
    first = FakeStateProvider()
    handler_for(first).save("t", 5, {"k": 5}, [4, 2], next_start=4)
    second = FakeStateProvider(last=first.current)
    assert handler_for(second).load("t") == LoadedState(5, {"k": 5}, [4, 2], 4)


def test_load_carries_forward() -> None:
    first = FakeStateProvider()
    handler_for(first).save("t", 5, {"k": 5}, [4, 2], next_start=4)
    second = FakeStateProvider(last=first.current)
    handler_for(second).load("t")  # run fails before save()
    assert second.current is not None
    state = cast(ExternalDQCheckpointState, second.current.state)
    assert state.watermarks == {"t": 5}
    assert state.recent_keys == {"t": {"k": 5}}
    assert state.late_baselines == {"t": [4, 2]}
    assert state.next_starts == {"t": 4}


def test_ignore_new_state_creates_no_checkpoint() -> None:
    assert (
        handler_for(FakeStateProvider(), ignore_new_state=True).create_checkpoint()
        is None
    )
