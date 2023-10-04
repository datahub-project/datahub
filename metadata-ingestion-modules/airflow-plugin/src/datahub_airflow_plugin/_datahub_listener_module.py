from datahub_airflow_plugin.datahub_listener import get_airflow_plugin_listener

_listener = get_airflow_plugin_listener()
if _listener:
    on_task_instance_running = _listener.on_task_instance_running
    on_task_instance_success = _listener.on_task_instance_success
    on_task_instance_failed = _listener.on_task_instance_failed
