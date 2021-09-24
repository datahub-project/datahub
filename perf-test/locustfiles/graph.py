import json

from locust import HttpUser, between, task


class GraphUser(HttpUser):
    wait_time = between(1, 5)

    @task
    def relationships(self):
        self.client.get(
            "/relationships?direction=INCOMING&urn=urn:li:corpuser:common&types=OwnedBy''"
        )
