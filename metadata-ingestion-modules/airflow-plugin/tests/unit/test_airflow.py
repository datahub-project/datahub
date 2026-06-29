import contextlib
import json
from contextlib import contextmanager
from typing import Iterator
from unittest import mock

import airflow.configuration
import pytest
from airflow.models import Connection, DagBag

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.graph.config import ClientMode
from datahub_airflow_plugin import get_provider_info
from datahub_airflow_plugin._config import DatahubLineageConfig, get_lineage_config
from datahub_airflow_plugin.entities import Dataset, Urn
from datahub_airflow_plugin.hooks.datahub import DatahubKafkaHook, DatahubRestHook
from datahub_airflow_plugin.operators.datahub import DatahubEmitterOperator

lineage_mce = builder.make_lineage_mce(
    [
        builder.make_dataset_urn("bigquery", "upstream1"),
        builder.make_dataset_urn("bigquery", "upstream2"),
    ],
    builder.make_dataset_urn("bigquery", "downstream1"),
)

datahub_rest_connection_config = Connection(
    conn_id="datahub_rest_test",
    conn_type="datahub_rest",
    host="http://test_host:8080",
    extra=None,
)
datahub_rest_connection_config_with_timeout = Connection(
    conn_id="datahub_rest_test",
    conn_type="datahub_rest",
    host="http://test_host:8080",
    extra=json.dumps({"timeout_sec": 5}),
)

datahub_kafka_connection_config = Connection(
    conn_id="datahub_kafka_test",
    conn_type="datahub_kafka",
    host="test_broker:9092",
    extra=json.dumps(
        {
            "connection": {
                "producer_config": {},
                "schema_registry_url": "http://localhost:8081",
            }
        }
    ),
)


def setup_module(module):
    airflow.configuration.conf.load_test_config()


def test_airflow_provider_info():
    assert get_provider_info()


@pytest.mark.filterwarnings("ignore:.*is deprecated.*")
def test_dags_load_with_no_errors(pytestconfig: pytest.Config) -> None:
    airflow_examples_folder = (
        pytestconfig.rootpath / "src/datahub_airflow_plugin/example_dags"
    )

    # Note: the .airflowignore file skips the snowflake DAG.
    dag_bag = DagBag(dag_folder=str(airflow_examples_folder), include_examples=False)

    import_errors = dag_bag.import_errors

    assert len(import_errors) == 0
    assert dag_bag.size() > 0


@contextmanager
def patch_airflow_connection(conn: Connection) -> Iterator[Connection]:
    # The return type should really by ContextManager, but mypy doesn't like that.
    # See https://stackoverflow.com/questions/49733699/python-type-hints-and-context-managers#comment106444758_58349659.
    with mock.patch(
        "datahub_provider.hooks.datahub.BaseHook.get_connection", return_value=conn
    ):
        yield conn


@mock.patch("datahub.emitter.rest_emitter.DataHubRestEmitter", autospec=True)
def test_datahub_rest_hook(mock_emitter):
    with patch_airflow_connection(datahub_rest_connection_config) as config:
        assert config.conn_id
        hook = DatahubRestHook(config.conn_id)
        hook.emit_mces([lineage_mce])

        mock_emitter.assert_called_once_with(
            config.host,
            None,
            client_mode=ClientMode.INGESTION,
            datahub_component="airflow-plugin",
        )
        instance = mock_emitter.return_value
        instance.emit.assert_called_with(lineage_mce)


@mock.patch("datahub.emitter.rest_emitter.DataHubRestEmitter", autospec=True)
def test_datahub_rest_hook_with_timeout(mock_emitter):
    with patch_airflow_connection(
        datahub_rest_connection_config_with_timeout
    ) as config:
        assert config.conn_id
        hook = DatahubRestHook(config.conn_id)
        hook.emit_mces([lineage_mce])

        mock_emitter.assert_called_once_with(
            config.host,
            None,
            timeout_sec=5,
            client_mode=ClientMode.INGESTION,
            datahub_component="airflow-plugin",
        )
        instance = mock_emitter.return_value
        instance.emit.assert_called_with(lineage_mce)


@mock.patch("datahub.emitter.kafka_emitter.DatahubKafkaEmitter", autospec=True)
def test_datahub_kafka_hook(mock_emitter):
    with patch_airflow_connection(datahub_kafka_connection_config) as config:
        assert config.conn_id
        hook = DatahubKafkaHook(config.conn_id)
        hook.emit_mces([lineage_mce])

        mock_emitter.assert_called_once()
        instance = mock_emitter.return_value
        instance.emit.assert_called()
        instance.flush.assert_called_once()


@mock.patch("datahub_provider.hooks.datahub.DatahubRestHook.emit")
def test_datahub_lineage_operator(mock_emit):
    with patch_airflow_connection(datahub_rest_connection_config) as config:
        assert config.conn_id
        task = DatahubEmitterOperator(
            task_id="emit_lineage",
            datahub_conn_id=config.conn_id,
            mces=[
                builder.make_lineage_mce(
                    [
                        builder.make_dataset_urn("snowflake", "mydb.schema.tableA"),
                        builder.make_dataset_urn("snowflake", "mydb.schema.tableB"),
                    ],
                    builder.make_dataset_urn("snowflake", "mydb.schema.tableC"),
                )
            ],
        )
        task.execute({})

        mock_emit.assert_called()


@pytest.mark.parametrize(
    "hook",
    [
        DatahubRestHook,
        DatahubKafkaHook,
    ],
)
def test_hook_airflow_ui(hook):
    # Simply ensure that these run without issue. These will also show up
    # in the Airflow UI, where it will be even more clear if something
    # is wrong.
    hook.get_connection_form_widgets()
    hook.get_ui_field_behaviour()


def test_datajob_url_link_taskinstance_rejected_with_migration_message():
    """Users upgrading from Airflow 2 may still have `datajob_url_link=taskinstance`
    in airflow.cfg — the removed Airflow 2 URL format. Confirm the plugin fails fast
    with a migration-friendly error rather than an opaque pydantic enum error."""
    from datahub_airflow_plugin._config import get_lineage_config

    with mock.patch(
        "datahub_airflow_plugin._config.conf.get",
        side_effect=lambda section, key, fallback=None: (
            "taskinstance" if key == "datajob_url_link" else fallback
        ),
    ):
        with pytest.raises(ValueError, match="taskinstance"):
            get_lineage_config()


def test_basehook_falls_back_to_legacy_location_on_airflow_30(monkeypatch):
    """BaseHook moved into the Task SDK (airflow.sdk.bases.hook) in Airflow 3.1.
    On Airflow 3.0.x it is only importable from airflow.hooks.base, so
    hooks/datahub.py imports it via a try/except. Simulate the 3.0 surface
    (airflow.sdk without BaseHook) and re-import the module to prove the
    fallback branch keeps DatahubRestHook working — this guards 3.0 support
    even though the unit suite itself runs on a newer Airflow."""
    import importlib
    import sys
    import types

    # airflow.hooks.base exposes BaseHook via a lazy shim mypy can't see on 3.1+.
    # Keep this on one line so the inline ignore lands on the line mypy flags.
    from airflow.hooks.base import BaseHook  # type: ignore[attr-defined]

    import datahub_airflow_plugin.hooks.datahub as hook_module

    # Replace airflow.sdk with a stub that lacks BaseHook so
    # `from airflow.sdk import BaseHook` raises ImportError, as on Airflow 3.0.x.
    stub_sdk = types.ModuleType("airflow.sdk")
    monkeypatch.setitem(sys.modules, "airflow.sdk", stub_sdk)
    try:
        importlib.reload(hook_module)
        assert hook_module.BaseHook is BaseHook
        assert issubclass(hook_module.DatahubRestHook, BaseHook)
    finally:
        # Restore the real (3.1+) import surface for the rest of the suite.
        monkeypatch.undo()
        importlib.reload(hook_module)


def test_get_base_url_prefers_api_over_webserver():
    """_get_base_url should prefer Airflow 3's `[api] base_url` over the legacy
    `[webserver] base_url`, then fall back to localhost. The integration suite
    only sets `[api]`, so the precedence and the webserver fallback are unit-tested
    here."""
    from datahub_airflow_plugin.client.airflow_generator import _get_base_url

    def conf_get(values):
        return lambda section, key, fallback=None: values.get((section, key), fallback)

    cases = [
        # both set -> api wins
        (
            {
                ("api", "base_url"): "http://api:8080",
                ("webserver", "base_url"): "http://web:8080",
            },
            "http://api:8080",
        ),
        # only webserver set -> webserver fallback
        ({("webserver", "base_url"): "http://web:8080"}, "http://web:8080"),
        # neither set -> localhost default
        ({}, "http://localhost:8080"),
    ]
    for values, expected in cases:
        with mock.patch(
            "datahub_airflow_plugin.client.airflow_generator.conf.get",
            side_effect=conf_get(values),
        ):
            assert _get_base_url() == expected


def test_entities():
    assert (
        Dataset("snowflake", "mydb.schema.tableConsumed").urn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableConsumed,PROD)"
    )

    assert (
        Urn(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableConsumed,PROD)"
        ).urn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableConsumed,PROD)"
    )

    assert (
        Urn("urn:li:dataJob:(urn:li:dataFlow:(airflow,testDag,PROD),testTask)").urn
        == "urn:li:dataJob:(urn:li:dataFlow:(airflow,testDag,PROD),testTask)"
    )

    assert (
        Urn(
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,platform.testDag,PROD),testTask)"
        ).urn
        == "urn:li:dataJob:(urn:li:dataFlow:(airflow,platform.testDag,PROD),testTask)"
    )

    with pytest.raises(ValueError, match="invalid"):
        Urn("not a URN")

    with pytest.raises(
        ValueError, match="only supports datasets and upstream datajobs"
    ):
        Urn("urn:li:mlModel:(urn:li:dataPlatform:science,scienceModel,PROD)")


def _make_lineage_config(
    enable_datajob_lineage: bool = True,
    filter_str: str = '{"allow": [".*"]}',
) -> DatahubLineageConfig:
    # model_construct skips validation; we only need the fields read by the code under test.
    return DatahubLineageConfig.model_construct(  # type: ignore[call-arg]
        enable_datajob_lineage=enable_datajob_lineage,
        datajob_lineage_dag_filter_pattern=AllowDenyPattern.model_validate_json(
            filter_str
        ),
    )


def test_should_emit_datajob_lineage_default_allows_all():
    cfg = _make_lineage_config()
    assert cfg.should_emit_datajob_lineage("any_dag") is True


def test_should_emit_datajob_lineage_global_disable_overrides_filter():
    cfg = _make_lineage_config(enable_datajob_lineage=False)
    assert cfg.should_emit_datajob_lineage("any_dag") is False


def test_should_emit_datajob_lineage_per_dag_deny():
    cfg = _make_lineage_config(filter_str='{"deny": ["cosmos_dbt_dag"]}')
    assert cfg.should_emit_datajob_lineage("cosmos_dbt_dag") is False
    assert cfg.should_emit_datajob_lineage("etl_pipeline") is True


def test_should_emit_datajob_lineage_per_dag_allow():
    cfg = _make_lineage_config(filter_str='{"allow": ["analytics_.*"]}')
    assert cfg.should_emit_datajob_lineage("analytics_users") is True
    assert cfg.should_emit_datajob_lineage("billing_pipeline") is False


def test_get_lineage_config_reads_per_dag_filter_str():
    fake_conf = {
        ("datahub", "datajob_lineage_dag_filter_str"): '{"deny": ["my_dag"]}',
    }

    def fake_get(section, key, fallback=None):
        return fake_conf.get((section, key), fallback)

    with mock.patch("datahub_airflow_plugin._config.conf.get", side_effect=fake_get):
        cfg = get_lineage_config()

    assert cfg.should_emit_datajob_lineage("my_dag") is False
    assert cfg.should_emit_datajob_lineage("other_dag") is True


def _import_listener_module():
    """Import the listener module. Skip if Airflow runtime not available."""
    try:
        from datahub_airflow_plugin import datahub_listener as listener_module
    except Exception as e:
        pytest.skip(f"Airflow listener module unavailable: {e}")
    return listener_module


def test_listener_extract_lineage_skips_when_dag_denied():
    listener_module = _import_listener_module()

    listener = listener_module.DataHubListener.__new__(listener_module.DataHubListener)
    listener.config = _make_lineage_config(filter_str='{"deny": ["denied_dag"]}')

    datajob = mock.MagicMock()
    task = mock.MagicMock(dag_id="denied_dag", task_id="t1")

    listener._extract_lineage(
        datajob, mock.MagicMock(), task, mock.MagicMock(), complete=False
    )

    # Early return means no inlets/outlets/FGLs are touched on the datajob.
    datajob.inlets.append.assert_not_called()
    datajob.outlets.append.assert_not_called()


def test_listener_extract_lineage_skips_when_global_disabled():
    listener_module = _import_listener_module()

    listener = listener_module.DataHubListener.__new__(listener_module.DataHubListener)
    listener.config = _make_lineage_config(enable_datajob_lineage=False)

    datajob = mock.MagicMock()
    task = mock.MagicMock(dag_id="any_dag", task_id="t1")

    listener._extract_lineage(
        datajob, mock.MagicMock(), task, mock.MagicMock(), complete=False
    )

    datajob.inlets.append.assert_not_called()
    datajob.outlets.append.assert_not_called()


@pytest.mark.parametrize(
    ("dag_id", "expected_generate_lineage"),
    [
        ("allowed_dag", True),
        ("denied_dag", False),
    ],
)
def test_listener_generate_and_emit_datajob_passes_filter_to_generate_mcp(
    dag_id, expected_generate_lineage
):
    """Verify generate_mcp is called with generate_lineage gated on the per-DAG filter."""
    listener_module = _import_listener_module()

    listener = listener_module.DataHubListener.__new__(listener_module.DataHubListener)
    listener.config = _make_lineage_config(filter_str='{"deny": ["denied_dag"]}')
    listener.config.cluster = "PROD"
    listener.config.capture_tags_info = False
    listener.config.capture_ownership_info = False
    listener.config.materialize_iolets = False

    emitter = mock.MagicMock()
    listener._emitter = emitter
    listener._make_emit_callback = mock.MagicMock(return_value=None)

    datajob = mock.MagicMock()
    datajob.generate_mcp.return_value = []

    dag = mock.MagicMock(dag_id=dag_id)
    task = mock.MagicMock(dag_id=dag_id, task_id="t1")
    task_instance = mock.MagicMock(task=task)
    dagrun = mock.MagicMock()

    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(listener, "_extract_lineage"))
        stack.enter_context(
            mock.patch.object(
                listener_module.AirflowGenerator,
                "generate_datajob",
                return_value=datajob,
            )
        )
        stack.enter_context(
            mock.patch.object(listener, "_get_emitter", return_value=emitter)
        )
        listener._generate_and_emit_datajob(
            dagrun=dagrun,
            task=task,
            dag=dag,
            task_instance=task_instance,
            complete=True,
        )

    datajob.generate_mcp.assert_called_once()
    assert (
        datajob.generate_mcp.call_args.kwargs["generate_lineage"]
        is expected_generate_lineage
    )
