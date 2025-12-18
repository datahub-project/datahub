import json

from locust import HttpUser, between, task

from test_utils.datahub_sessions import DataHubSessions


datahub_instances = DataHubSessions()


class BrowseUser(HttpUser):
    wait_time = between(1, 5)

    @task
    def browse(self):
        session = datahub_instances.get_session(self.host)
        self.client.post(
            "/api/gms/entities?action=browse",
            json.dumps(
                {
                    "path": "/perf/test",
                    "entity": "dataset",
                    "start": 0,
                    "limit": 10,
                }
            ),
            cookies=session.get_cookies()
        )
