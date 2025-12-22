from locust import HttpUser, constant, task

from test_utils.datahub_sessions import DataHubSessions
from test_utils.mock_data_helper import default_mock_data


datahub_instances = DataHubSessions()


class GetEntityUser(HttpUser):
    wait_time = constant(1)

    @task
    def entities(self):
        # Get a random URN from mock data that matches actual ingested tables
        urn = default_mock_data.get_random_urn()

        session = datahub_instances.get_session(self.host)
        self.client.request_name = "/entities?[urn]"
        self.client.get(
            f"/api/gms/entities/{urn}",
            cookies=session.get_cookies()
        )
