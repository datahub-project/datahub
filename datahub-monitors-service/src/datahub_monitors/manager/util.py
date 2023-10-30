from datahub_monitors.types import Monitor, MonitorMode


def is_dry_run_mode(monitor: Monitor) -> bool:
    """
    If a monitor is operating in passive we always use dry_run.
    """
    status = getattr(monitor, "status", None)
    if status is not None:
        mode = getattr(status, "mode", None)
        return mode == MonitorMode.PASSIVE
    return False
