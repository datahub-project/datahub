from datetime import datetime, timezone
from types import SimpleNamespace
from unittest import mock

import packaging.version
import pytest

try:
    from great_expectations import __version__ as GX_VERSION  # type: ignore

    _gx_major = packaging.version.parse(GX_VERSION).major
except Exception:
    _gx_major = 0

pytestmark = pytest.mark.skipif(
    _gx_major < 1,
    reason="action_v1 requires great-expectations>=1.0.0",
)


@pytest.fixture
def mock_validation_result():
    run_id = SimpleNamespace(
        run_time=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    )
    return SimpleNamespace(
        suite_name="test_suite",
        meta={
            "run_id": run_id,
            "batch_spec": {
                "schema_name": "public",
                "table_name": "foo",
                "data_asset_name": "foo",
                "datasource_name": "my_postgres",
                "url": "postgresql://localhost:5432/testdb",
            },
        },
        evaluation_parameters=None,
        results=[
            {
                "success": True,
                "expectation_config": {
                    "type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "id"},
                },
                "result": {
                    "element_count": 10,
                    "unexpected_count": 0,
                    "missing_count": 0,
                },
            }
        ],
    )


def test_action_v1_emits_assertions(mock_validation_result):
    from datahub_gx_plugin.action_v1 import DataHubValidationAction

    action = DataHubValidationAction(
        name="datahub",
        server_url="http://localhost:8080",
        graceful_exceptions=False,
        platform_instance_map={"my_postgres": "postgres1"},
        convert_urns_to_lowercase=True,
    )

    checkpoint_result = SimpleNamespace(
        run_id=mock_validation_result.meta["run_id"],
        run_results={"validation-1": mock_validation_result},
        name="orders_checkpoint",
        checkpoint_config=SimpleNamespace(
            name="orders_checkpoint",
            id="cp-123",
            validation_definitions=[
                SimpleNamespace(id="val-456", name="orders_validation")
            ],
        ),
    )
    mock_validation_result.meta["validation_id"] = "val-456"

    with (
        mock.patch(
            "datahub_gx_plugin.action_v1.DatahubRestEmitter"
        ) as mock_emitter_cls,
        mock.patch(
            "datahub_gx_plugin.common.build_assertion_info_mcp"
        ) as mock_build_info,
    ):
        emitter = mock.Mock()
        mock_emitter_cls.return_value = emitter
        mock_build_info.side_effect = lambda urn, info: mock.Mock(
            entityUrn=urn, aspect=info
        )

        result = action.run(checkpoint_result, action_context=None)

    assert result["datahub_notification_result"] == "DataHub notification succeeded"
    assert emitter.emit_mcp.call_count >= 3
    # assertionInfo MCP should carry checkpoint custom properties
    info_calls = [call for call in mock_build_info.call_args_list]
    assert info_calls
    assertion_info = info_calls[0].args[1]
    assert assertion_info.customProperties["checkpoint_name"] == "orders_checkpoint"
    assert assertion_info.customProperties["checkpoint_id"] == "cp-123"
    assert assertion_info.customProperties["validation_id"] == "val-456"
    assert (
        assertion_info.customProperties["validation_definition_name"]
        == "orders_validation"
    )


def test_action_v1_resolves_config_str_token(mock_validation_result):
    from great_expectations.datasource.fluent.config_str import ConfigStr

    from datahub_gx_plugin.action_v1 import DataHubValidationAction

    # Plain "${VAR}" strings are coerced to ConfigStr by the pydantic field.
    action = DataHubValidationAction(
        name="datahub",
        server_url="http://localhost:8080",
        token="${DATAHUB_TOKEN}",
        graceful_exceptions=False,
        platform="pandas",
        dataset_name="orders",
    )
    assert isinstance(action.token, ConfigStr)

    checkpoint_result = SimpleNamespace(
        run_id=mock_validation_result.meta["run_id"],
        run_results={"validation-1": mock_validation_result},
        name="cp",
        checkpoint_config=None,
    )
    with (
        mock.patch(
            "datahub_gx_plugin.action_v1.DatahubRestEmitter"
        ) as mock_emitter_cls,
        mock.patch.object(
            DataHubValidationAction,
            "_substitute_config_str_if_needed",
            return_value="resolved-secret",
        ) as mock_sub,
        mock.patch(
            "datahub_gx_plugin.common.build_assertion_info_mcp",
            side_effect=lambda urn, info: mock.Mock(entityUrn=urn, aspect=info),
        ),
    ):
        emitter = mock.Mock()
        mock_emitter_cls.return_value = emitter

        action.run(checkpoint_result, action_context=None)

    mock_sub.assert_called_once()
    assert isinstance(mock_sub.call_args.args[0], ConfigStr)
    assert mock_emitter_cls.call_args.kwargs["token"] == "resolved-secret"


def test_action_0x_import_redirects_under_gx1():
    """The 0.x module must raise our redirect before crashing on removed GX APIs."""
    import importlib
    import sys

    sys.modules.pop("datahub_gx_plugin.action", None)
    with pytest.raises(ImportError, match="action_v1"):
        importlib.import_module("datahub_gx_plugin.action")


def test_action_v1_prefers_asset_name_over_weak_batch_spec():
    from datahub_gx_plugin.action_v1 import DataHubValidationAction

    action = DataHubValidationAction(
        name="datahub",
        server_url="http://localhost:8080",
        platform="pandas",
    )
    validation_result = SimpleNamespace(
        asset_name="orders_tbl",
        meta={
            "batch_spec": {"batch_data": "PandasDataFrame"},
            "active_batch_definition": {"datasource_name": "pd_src"},
        },
    )
    datasets = action._datasets_from_validation_result(validation_result)
    assert len(datasets) == 1
    assert "orders_tbl" in datasets[0]["dataset_urn"]
    assert datasets[0]["batchSpec"].customProperties["data_asset_name"] == "orders_tbl"


def test_action_v1_explicit_platform_dataset():
    from datahub_gx_plugin.action_v1 import DataHubValidationAction

    action = DataHubValidationAction(
        name="datahub",
        server_url="http://localhost:8080",
        platform="postgres",
        dataset_name="public.orders",
        platform_instance="prod",
    )
    urn = action._resolve_dataset_urn(
        batch_spec={},
        data_asset_name=None,
        datasource_name="",
    )
    assert urn is not None
    assert "postgres" in urn
    assert "public.orders" in urn


def test_action_v1_skips_when_no_dataset():
    from datahub_gx_plugin.action_v1 import DataHubValidationAction

    action = DataHubValidationAction(
        name="datahub",
        server_url="http://localhost:8080",
        graceful_exceptions=False,
    )
    empty_result = SimpleNamespace(
        suite_name="suite",
        meta={},
        evaluation_parameters=None,
        results=[],
    )
    checkpoint_result = SimpleNamespace(
        run_id=SimpleNamespace(
            run_time=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        ),
        run_results={"v1": empty_result},
    )

    with mock.patch("datahub_gx_plugin.action_v1.DatahubRestEmitter"):
        result = action.run(checkpoint_result, action_context=None)

    assert result["datahub_notification_result"] == "none required"
