import datetime
from unittest.mock import Mock

from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.client.config.resolver import ExecutorConfigResolver
from datahub_executor.common.constants import DATAHUB_EXECUTOR_EMBEDDED_POOL_ID


class TestRemoteResolver:
    def setup_method(self) -> None:
        self.graph = Mock(spec=DataHubGraph)

        self.resolver = ExecutorConfigResolver(self.graph)
        self.resolver.executor_configs = []

        self.graph.execute_graphql.return_value = {
            "listExecutorConfigs": {
                "total": 1,
                "executorConfigs": [
                    {
                        "region": "us-west-2",
                        "executorId": DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
                        "queueUrl": "https://queue.amazon.com/something/123456",
                        "accessKeyId": "some-access-key",
                        "secretKeyId": "some-secret-key",
                        "expiration": datetime.datetime.now(
                            datetime.timezone.utc
                        ).isoformat(),
                        "sessionToken": "some-session-token",
                    },
                ],
            }
        }

    def test_get_executor_configs(self) -> None:
        executor_configs = self.resolver.get_executor_configs()
        assert len(executor_configs) == 1
        assert executor_configs[0].executor_id == DATAHUB_EXECUTOR_EMBEDDED_POOL_ID

    def test_fetch_executor_configs_empty_list(self) -> None:
        self.resolver.graph.execute_graphql.return_value = {  # type: ignore
            "listExecutorConfigs": {
                "total": 0,
                "executorConfigs": [],
            }
        }
        executor_configs = self.resolver.fetch_executor_configs()
        assert len(executor_configs) == 0

    def test_refresh_executor_configs_not_expired(self) -> None:
        self.resolver.graph.execute_graphql.return_value = {  # type: ignore
            "listExecutorConfigs": {
                "total": 1,
                "executorConfigs": [
                    {
                        "region": "us-west-2",
                        "executorId": DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
                        "queueUrl": "https://queue.amazon.com/something/123456",
                        "accessKeyId": "some-access-key",
                        "secretKeyId": "some-secret-key",
                        "expiration": (
                            datetime.datetime.now(datetime.timezone.utc)
                            + datetime.timedelta(minutes=30)
                        ).isoformat(),
                        "sessionToken": "some-session-token",
                    },
                ],
            }
        }
        self.resolver.get_executor_configs()
        expiring, executor_configs = self.resolver.refresh_executor_configs()

        assert expiring is False
