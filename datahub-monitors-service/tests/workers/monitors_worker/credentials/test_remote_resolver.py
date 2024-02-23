import datetime
from unittest.mock import Mock

from datahub_monitors.common.graph import DataHubAssertionGraph
from datahub_monitors.workers.resolvers.executor_config_resolver import (
    ExecutorConfigResolver,
)


class TestRemoteResolver:
    def setup_method(self) -> None:
        self.graph = Mock(spec=DataHubAssertionGraph)

        self.resolver = ExecutorConfigResolver(self.graph)
        self.resolver.executor_configs = []

        self.graph.execute_graphql.return_value = {
            "listExecutorConfigs": {
                "total": 1,
                "executorConfigs": [
                    {
                        "region": "us-west-2",
                        "executorId": "default",
                        "queueUrl": "https://queue.amazon.com/something/123456",
                        "accessKeyId": "some-access-key",
                        "secretKeyId": "some-secret-key",
                        "expiration": datetime.datetime.now(
                            datetime.timezone.utc
                        ).isoformat(),
                        "sessionToken": "some-session-token",
                    },
                    {
                        "region": "us-west-1",
                    },
                ],
            }
        }

    def test_get_executor_configs(self) -> None:
        executor_configs = self.resolver.get_executor_configs()
        assert len(executor_configs) == 1
        assert executor_configs[0].executor_id == "default"

    def test_fetch_executor_configs_error(self) -> None:
        self.resolver.graph.execute_graphql.return_value = {"error": "something bad happened"}  # type: ignore
        executor_configs = self.resolver.fetch_executor_configs()
        assert len(executor_configs) == 0

    def test_fetch_executor_configs_missing_list(self) -> None:
        self.resolver.graph.execute_graphql.return_value = {}  # type: ignore
        executor_configs = self.resolver.fetch_executor_configs()
        assert len(executor_configs) == 0

    def test_fetch_executor_configs_missing_executor_configs(self) -> None:
        self.resolver.graph.execute_graphql.return_value = {"listAwsSqsCredentials": {}}  # type: ignore
        executor_configs = self.resolver.fetch_executor_configs()
        assert len(executor_configs) == 0

    def test_refresh_executor_configs_not_expired(self) -> None:
        self.resolver.graph.execute_graphql.return_value = {  # type: ignore
            "listExecutorConfigs": {
                "total": 1,
                "executorConfigs": [
                    {
                        "region": "us-west-2",
                        "executorId": "default",
                        "queueUrl": "https://queue.amazon.com/something/123456",
                        "accessKeyId": "some-access-key",
                        "secretKeyId": "some-secret-key",
                        "expiration": (
                            datetime.datetime.now(datetime.timezone.utc)
                            + datetime.timedelta(minutes=30)
                        ).isoformat(),
                        "sessionToken": "some-session-token",
                    },
                    {
                        "region": "us-west-1",
                    },
                ],
            }
        }
        self.resolver.get_executor_configs()
        expiring, executor_configs = self.resolver.refresh_executor_configs()

        assert expiring is False
