from datahub_executor.common.monitor.executor import MonitorExecutor


def handle_monitor_training_signal_requests(
    monitor_executor: MonitorExecutor,
) -> bool:
    return monitor_executor.get_active_thread_count() > 0
