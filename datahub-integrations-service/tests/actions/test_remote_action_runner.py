from unittest.mock import Mock, patch

import pytest

from datahub_integrations.actions.remote_action_runner import (
    DEFAULT_EXECUTOR_ID,
    RemoteActionRunner,
)


@pytest.fixture
def mock_graph() -> Mock:
    return Mock()


@pytest.fixture
def remote_action_runner(mock_graph: Mock) -> RemoteActionRunner:
    return RemoteActionRunner(graph=mock_graph, base_url="http://example.com")


def test_run_action(remote_action_runner: RemoteActionRunner, mock_graph: Mock) -> None:
    # Mock necessary methods
    remote_action_runner.try_create_remote_action_ingestion_source = Mock(  # type: ignore
        return_value="test_ingestion_urn"
    )
    remote_action_runner.start_running_ingestion_source = Mock()  # type: ignore

    # Run the action
    remote_action_runner.run_action("test_urn", {}, DEFAULT_EXECUTOR_ID)

    # Assert the correct methods were called
    remote_action_runner.try_create_remote_action_ingestion_source.assert_called_once_with(
        "test_urn", {}, DEFAULT_EXECUTOR_ID
    )
    remote_action_runner.start_running_ingestion_source.assert_called_once_with(
        "test_ingestion_urn"
    )


def test_stop_action(remote_action_runner: RemoteActionRunner) -> None:
    # Mock necessary methods
    remote_action_runner.clean_up_remote_action_ingestion_source = Mock()  # type: ignore
    remote_action_runner.create_remote_ingestion_source_urn = Mock(  # type: ignore
        return_value="test_ingestion_urn"
    )

    # Ensure event is set initially
    remote_action_runner.should_continue_running.set()

    # Run the stop action
    remote_action_runner.stop_action("test_urn")

    # Assert the event is cleared and cleanup was called
    assert not remote_action_runner.should_continue_running.is_set()
    remote_action_runner.clean_up_remote_action_ingestion_source.assert_called_once_with(
        "test_ingestion_urn"
    )


def test_try_create_remote_action_ingestion_source(
    remote_action_runner: RemoteActionRunner, mock_graph: Mock
) -> None:
    # Mock necessary methods
    remote_action_runner.create_remote_ingestion_source_urn = Mock(  # type: ignore
        return_value="test_ingestion_urn"
    )
    remote_action_runner.build_remote_ingestion_source_recipe_json = Mock(  # type: ignore
        return_value="test_recipe"
    )
    remote_action_runner.upsert_remote_ingestion_source = Mock()  # type: ignore

    # Run the method
    urn = remote_action_runner.try_create_remote_action_ingestion_source(
        "test_urn", {}, DEFAULT_EXECUTOR_ID
    )

    # Assert the URN is returned and the necessary methods were called
    assert urn == "test_ingestion_urn"
    remote_action_runner.create_remote_ingestion_source_urn.assert_called_once_with(
        "test_urn"
    )
    remote_action_runner.build_remote_ingestion_source_recipe_json.assert_called_once_with(
        "test_urn", {}
    )
    remote_action_runner.upsert_remote_ingestion_source.assert_called_once_with(
        "test_ingestion_urn", "test_recipe", DEFAULT_EXECUTOR_ID
    )


class IsRunningSideEffect:
    def __init__(self) -> None:
        self.counter = 0

    def __call__(self, *args: str, **kwargs: dict) -> bool:
        if self.counter == 0:
            self.counter += 1
            return False
        return True


def test_start_running_ingestion_source(
    remote_action_runner: RemoteActionRunner,
) -> None:
    # Mock methods
    is_running_side_effect = IsRunningSideEffect()

    remote_action_runner.is_running = Mock(side_effect=is_running_side_effect)  # type: ignore
    remote_action_runner.start_ingestion = Mock()  # type: ignore

    # Ensure event is set
    remote_action_runner.should_continue_running.set()

    # Patch time.sleep to avoid real delays
    with patch("time.sleep", return_value=None):
        # We use a side effect to stop the event after the second iteration
        with patch.object(
            remote_action_runner.should_continue_running,
            "is_set",
            side_effect=[True, True, False],
        ):
            remote_action_runner.start_running_ingestion_source("test_ingestion_urn")

    # Assert the ingestion was started and the check was called
    remote_action_runner.start_ingestion.assert_called_once_with("test_ingestion_urn")
    remote_action_runner.is_running.assert_called_with("test_ingestion_urn")


def test_execute_graphql_with_retry(
    remote_action_runner: RemoteActionRunner, mock_graph: Mock
) -> None:
    # Mock the execute_graphql method on the graph
    mock_graph.execute_graphql = Mock(
        side_effect=[Exception("Temporary failure"), {"data": "success"}]
    )

    # Run the method
    result = remote_action_runner.execute_graphql("some_query", {"key": "value"})

    # Assert that it retried and returned the correct result
    assert result == {"data": "success"}
    assert mock_graph.execute_graphql.call_count == 2


def test_cleanup_action_ingestion_source(
    remote_action_runner: RemoteActionRunner,
) -> None:
    # Mock methods
    remote_action_runner.get_running_execution_request_urns = Mock(  # type: ignore
        return_value=["urn1", "urn2"]
    )
    remote_action_runner.cancel_remote_action_ingestion_source = Mock()  # type: ignore
    remote_action_runner.delete_remote_action_ingestion_source = Mock()  # type: ignore

    # Run the method
    remote_action_runner.clean_up_remote_action_ingestion_source("test_ingestion_urn")

    # Assert the cleanup methods were called
    remote_action_runner.get_running_execution_request_urns.assert_called_once_with(
        "test_ingestion_urn"
    )
    assert remote_action_runner.cancel_remote_action_ingestion_source.call_count == 2
    remote_action_runner.delete_remote_action_ingestion_source.assert_called_once_with(
        "test_ingestion_urn"
    )
