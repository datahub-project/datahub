from datetime import timedelta
from time import sleep

from datahub.utilities.progress_timer import ProgressTimer


def test_progress_timer_basic():
    timer = ProgressTimer(report_every=timedelta(milliseconds=100))

    # First call should not report since report_0=False by default
    assert not timer.should_report()

    # Call before interval elapsed should not report
    sleep(0.05)  # 50ms
    assert not timer.should_report()

    # Call after interval elapsed should report
    sleep(0.1)  # Additional 100ms
    assert timer.should_report()

    # Next immediate call should not report
    assert not timer.should_report()


def test_progress_timer_with_report_0():
    timer = ProgressTimer(report_every=timedelta(milliseconds=100), report_0=True)

    # First call should report since report_0=True
    assert timer.should_report()

    # Next immediate call should not report
    assert not timer.should_report()

    # Call after interval elapsed should report
    sleep(0.1)  # 100ms
    assert timer.should_report()


def test_progress_timer_multiple_intervals():
    timer = ProgressTimer(report_every=timedelta(milliseconds=50))

    # First call should not report
    assert not timer.should_report()

    # Check multiple intervals
    sleep(0.06)  # 60ms - should report
    assert timer.should_report()

    sleep(0.02)  # 20ms - should not report
    assert not timer.should_report()

    sleep(0.05)  # 50ms - should report
    assert timer.should_report()
