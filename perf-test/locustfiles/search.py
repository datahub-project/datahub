import json

from locust import HttpUser, between, task

from test_utils.datahub_sessions import DataHubSessions


datahub_instances = DataHubSessions()


class SearchUser(HttpUser):
    wait_time = between(1, 5)

    @task
    def search(self):
        session = datahub_instances.get_session(self.host)
        self.client.post(
            "/api/gms/entities?action=search",
            json.dumps({"input": "test", "entity": "dataset", "start": 0, "count": 10}),
            cookies=session.get_cookies()
        )
