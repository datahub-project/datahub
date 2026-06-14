from tests.zdu.framework.log_monitor import (
    NonBlockingState,
    Phase1State,
    SweepState,
    _parse_dual_write_line,
    _parse_line,
    _parse_nonblocking_line,
    _parse_phase1_line,
)


def test_parse_started():
    line = "2026-04-24 10:00:00 INFO  Starting migration sweep across 5 aspects"
    event = _parse_line("datahub-upgrade", line)
    assert event is not None
    assert event.state == SweepState.STARTED
    assert event.source == "datahub-upgrade"
    assert event.batch_count is None
    assert event.total_migrated is None


def test_parse_completed():
    line = "2026-04-24 10:05:00 INFO  Sweep complete. Total migrated: 42"
    event = _parse_line("datahub-upgrade", line)
    assert event is not None
    assert event.state == SweepState.COMPLETED
    assert event.total_migrated == 42


def test_parse_skipped():
    line = "Previous result was SUCCEEDED; skipping migrate-aspects-zdu-test-123"
    event = _parse_line("datahub-upgrade", line)
    assert event is not None
    assert event.state == SweepState.SKIPPED


def test_parse_failed():
    line = "ERROR  Failed upgrade step migrate-aspects-zdu-test-123"
    event = _parse_line("datahub-upgrade", line)
    assert event is not None
    assert event.state == SweepState.FAILED


def test_parse_batch_gms():
    line = "INFO  Migrated 25 aspect(s) in batch"
    event = _parse_line("datahub-gms", line)
    assert event is not None
    assert event.state == SweepState.BATCH
    assert event.batch_count == 25
    assert event.source == "datahub-gms"


def test_parse_cursor_heartbeat():
    line = "INFO  Saving state. Cursor: 1714000000000"
    event = _parse_line("datahub-gms", line)
    assert event is not None
    assert event.state == SweepState.BATCH
    assert event.batch_count is None


def test_no_match_returns_none():
    line = "INFO  Some unrelated log line about something else"
    assert _parse_line("datahub-upgrade", line) is None
    assert _parse_line("datahub-gms", line) is None


def test_upgrade_pattern_not_matched_by_gms():
    line = "Sweep complete. Total migrated: 10"
    assert _parse_line("datahub-gms", line) is None


def test_parse_phase1_no_indices_require_reindex():
    line = "INFO  No indices require incremental reindex"
    e = _parse_phase1_line(line)
    assert e is not None
    assert e.state == Phase1State.NO_REINDEX_NEEDED


def test_parse_phase1_already_completed():
    line = "INFO  Index datasetindex_v2 already COMPLETED in previous run, skipping"
    e = _parse_phase1_line(line)
    assert e is not None
    assert e.state == Phase1State.SKIP_ALREADY_DONE
    assert e.index_name == "datasetindex_v2"


def test_parse_phase1_already_dual_write_disabled():
    line = "INFO  Index datasetindex_v2 already DUAL_WRITE_DISABLED in previous run, skipping"
    e = _parse_phase1_line(line)
    assert e is not None
    assert e.state == Phase1State.SKIP_ALREADY_DONE
    assert e.index_name == "datasetindex_v2"


def test_parse_phase1_resuming_polling():
    line = (
        "INFO  Resuming polling for index datasetindex_v2 -> datasetindex_v2_1714000000"
    )
    e = _parse_phase1_line(line)
    assert e is not None
    assert e.state == Phase1State.RESUMING_POLLING
    assert e.index_name == "datasetindex_v2"
    assert e.next_index_name == "datasetindex_v2_1714000000"


def test_parse_phase1_alias_swapped():
    line = "INFO  Alias swapped: datasetindex_v2 -> datasetindex_v2_1714000000"
    e = _parse_phase1_line(line)
    assert e is not None
    assert e.state == Phase1State.ALIAS_SWAPPED
    assert e.index_name == "datasetindex_v2"
    assert e.next_index_name == "datasetindex_v2_1714000000"


def test_parse_phase1_empty_source_swap():
    line = "INFO  Index dashboardindex_v2 had 0 docs, next index created as empty, alias swapped"
    e = _parse_phase1_line(line)
    assert e is not None
    assert e.state == Phase1State.ALIAS_SWAPPED
    assert e.index_name == "dashboardindex_v2"


def test_parse_phase1_alias_swap_failed():
    line = "ERROR  Alias swap failed for datasetindex_v2 -> datasetindex_v2_1714: doc count mismatch"
    e = _parse_phase1_line(line)
    assert e is not None
    assert e.state == Phase1State.REINDEX_FAILED
    assert e.index_name == "datasetindex_v2"


def test_parse_phase1_unrelated_returns_none():
    assert _parse_phase1_line("INFO  Some unrelated GMS chatter") is None


def test_parse_dual_write_started():
    line = (
        "INFO  Recorded dual-write start time for index "
        "'datasetindex_v2' (entity 'dataset'): 1714000000000"
    )
    e = _parse_dual_write_line(line)
    assert e is not None
    assert e.index_name == "datasetindex_v2"
    assert e.entity_name == "dataset"
    assert e.timestamp_ms == 1714000000000


def test_parse_dual_write_with_namespaced_entity():
    line = (
        "INFO  Recorded dual-write start time for index "
        "'dashboardindex_v2' (entity 'dashboard'): 1714000005000"
    )
    e = _parse_dual_write_line(line)
    assert e is not None
    assert e.index_name == "dashboardindex_v2"
    assert e.entity_name == "dashboard"
    assert e.timestamp_ms == 1714000005000


def test_parse_dual_write_unrelated_returns_none():
    assert _parse_dual_write_line("INFO  Some unrelated GMS chatter") is None
    assert _parse_dual_write_line("Recorded dual-write — but malformed") is None


# ---------- _parse_nonblocking_line tests ----------


class TestParseNonBlockingLine:
    def test_dual_write_disabled_line(self) -> None:
        line = "Marked index dashboardindex_v2_old as DUAL_WRITE_DISABLED"
        ev = _parse_nonblocking_line(line)
        assert ev is not None
        assert ev.state == NonBlockingState.DUAL_WRITE_DISABLED
        assert ev.index_name == "dashboardindex_v2_old"
        assert ev.window is None

    def test_catch_up_window_line(self) -> None:
        line = (
            "Catch-up for entity index dashboardindex_v2_new: "
            "window [1700000000, 1700000300]"
        )
        ev = _parse_nonblocking_line(line)
        assert ev is not None
        assert ev.state == NonBlockingState.CATCH_UP_WINDOW
        assert ev.index_name == "dashboardindex_v2_new"
        assert ev.window == (1700000000, 1700000300)

    def test_unrelated_line_returns_none(self) -> None:
        assert _parse_nonblocking_line("Sweep complete. Total migrated: 42") is None
        assert _parse_nonblocking_line("Alias swapped: x -> y") is None
        assert _parse_nonblocking_line("") is None

    def test_malformed_window_returns_none(self) -> None:
        # Missing comma between epochs
        line = "Catch-up for entity index foo: window [1700000000 1700000300]"
        assert _parse_nonblocking_line(line) is None
