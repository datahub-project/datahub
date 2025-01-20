from datahub_airflow_plugin.datahub_listener import (
    get_airflow_plugin_listeners,
    hookimpl,
)

_listeners = get_airflow_plugin_listeners()
if _listeners:
    # The run_in_thread decorator messes with pluggy's interface discovery,
    # which causes the hooks to be called with no arguments and results in TypeErrors.
    # This is only an issue with Pluggy <= 1.0.0.
    # See https://github.com/pytest-dev/pluggy/issues/358
    # Note that pluggy 1.0.0 is in the constraints file for Airflow 2.4 and 2.5.

    @hookimpl
    def on_task_instance_running(previous_state, task_instance, session):
        assert _listeners
        for listener in _listeners:
            listener.on_task_instance_running(previous_state, task_instance, session)

    @hookimpl
    def on_task_instance_success(previous_state, task_instance, session):
        assert _listeners
        for listener in _listeners:
            listener.on_task_instance_success(previous_state, task_instance, session)

    @hookimpl
    def on_task_instance_failed(previous_state, task_instance, session):
        assert _listeners
        for listener in _listeners:
            listener.on_task_instance_failed(previous_state, task_instance, session)

    # We assume that all listeners have the same set of methods.
    if hasattr(_listeners[0], "on_dag_run_running"):

        @hookimpl
        def on_dag_run_running(dag_run, msg):
            assert _listeners
            for listener in _listeners:
                listener.on_dag_run_running(dag_run, msg)
