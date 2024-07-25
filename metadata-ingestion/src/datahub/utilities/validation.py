import pytz
from croniter import croniter


def validate_timezone(time_zone: str) -> None:
    if time_zone not in pytz.all_timezones:
        raise ValueError(f"Invalid timezone `{time_zone}`")


def validate_cron_schedule(cron_schedule: str) -> None:
    try:
        croniter(cron_schedule)
    except Exception as e:
        raise ValueError(f"Invalid cron schedule `{cron_schedule}`: {e}")
