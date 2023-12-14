from datahub_airflow_plugin.datahub_listener import (
    get_airflow_plugin_listener,
    hookimpl,
)

_listener = get_airflow_plugin_listener()
if _listener:
    # The run_in_thread decorator messes with pluggy's interface discovery,
    # which causes the hooks to be called with no arguments and results in TypeErrors.
    # This is only an issue with Pluggy <= 1.0.0.
    # See https://github.com/pytest-dev/pluggy/issues/358
    # Note that pluggy 1.0.0 is in the constraints file for Airflow 2.4 and 2.5.

    @hookimpl
    def on_task_instance_running(previous_state, task_instance, session):
        assert _listener
        _listener.on_task_instance_running(previous_state, task_instance, session)

    @hookimpl
    def on_task_instance_success(previous_state, task_instance, session):
        assert _listener
        _listener.on_task_instance_success(previous_state, task_instance, session)

    @hookimpl
    def on_task_instance_failed(previous_state, task_instance, session):
        assert _listener
        _listener.on_task_instance_failed(previous_state, task_instance, session)

    if hasattr(_listener, "on_dag_run_running"):

        @hookimpl
        def on_dag_run_running(dag_run, session):
            assert _listener
            _listener.on_dag_run_running(dag_run, session)
