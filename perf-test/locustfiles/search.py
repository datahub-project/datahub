import json

from locust import HttpUser, between, task


class SearchUser(HttpUser):
    wait_time = between(1, 5)

    @task
    def search(self):
        self.client.post(
            "/entities?action=search",
            json.dumps({"input": "test", "entity": "dataset", "start": 0, "count": 10}),
        )
