from datetime import datetime, timedelta, timezone


class ProgressTimer:
    def __init__(self, report_every: timedelta, report_0: bool = False):
        """A helper for reporting progress at a given time interval.

        Should be used for long-running processes that iterate over a large number of items,
        but each iteration is fast.

        Args:
            report_every: The time interval between progress reports.
            report_0: Whether to report progress on the first iteration.
        """

        self._report_every = report_every

        if report_0:
            # Use the earliest possible time to force reporting on the first iteration.
            self._last_report_time = datetime.min.replace(tzinfo=timezone.utc)
        else:
            self._last_report_time = self._now()

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def should_report(self) -> bool:
        current_time = self._now()

        should_report = (self._last_report_time + self._report_every) <= current_time
        if should_report:
            self._last_report_time = current_time

        return should_report
