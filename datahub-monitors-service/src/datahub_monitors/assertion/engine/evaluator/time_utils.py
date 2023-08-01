from datetime import datetime

from croniter import croniter
from pytz import timezone

from datahub_monitors.types import CalendarInterval, CronSchedule, FixedIntervalSchedule

MIN_PREV_CRON_INTERVAL_MS = 30000  # 30 seconds minimum
SECONDS_TO_MILLISECONDS = 1000
MINUTE_TO_MILLISECONDS = SECONDS_TO_MILLISECONDS * 60
HOUR_TO_MILLISECONDS = MINUTE_TO_MILLISECONDS * 60


def get_next_cron_schedule_time(schedule: CronSchedule) -> int:
    """
    Returns the next CRON schedule start time in milliseconds since epoch.
    """
    now = datetime.now(timezone(schedule.timezone))
    cron = croniter(schedule.cron, now)
    next_date = cron.get_next(ret_type=datetime)
    return next_date.timestamp() * 1000


def get_prev_cron_schedule_time(schedule: CronSchedule) -> int:
    """
    Returns the prev CRON schedule start time in milliseconds since epoch.
    """
    now = datetime.now(timezone(schedule.timezone))
    cron = croniter(schedule.cron, now)
    prev_date = cron.get_prev(ret_type=datetime)
    now_ms = int(datetime.now().timestamp() * 1000)
    prev_date_ms = prev_date.timestamp() * 1000
    if (now_ms - prev_date_ms) < MIN_PREV_CRON_INTERVAL_MS:
        prev_date = cron.get_prev(ret_type=datetime)
    return prev_date.timestamp() * 1000


def get_milliseconds_for_unit(unit: CalendarInterval) -> int:
    """
    Retrieves the number of milliseconds for a given Calendar Interval unit.
    """
    if unit == CalendarInterval.HOUR:
        return HOUR_TO_MILLISECONDS
    elif unit == CalendarInterval.MINUTE:
        return MINUTE_TO_MILLISECONDS
    else:
        raise Exception(
            f"Failed to retrieve milliseconds for unit {unit}. Unrecognized unit received!"
        )


def get_fixed_interval_start(end_time: int, schedule: FixedIntervalSchedule) -> int:
    """
    Returns the start of a fixed interval window using the end time of the window, plus a description of the window size.
    """
    return end_time - schedule.multiple * get_milliseconds_for_unit(schedule.unit)
