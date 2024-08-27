import json

from locust import HttpUser, between, task


class BrowseUser(HttpUser):
    wait_time = between(1, 5)

    @task
    def browse(self):
        self.client.post(
            "/entities?action=browse",
            json.dumps(
                {
                    "path": "/perf/test",
                    "entity": "dataset",
                    "start": 0,
                    "limit": 10,
                }
            ),
        )
