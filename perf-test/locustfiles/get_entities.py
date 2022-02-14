import random

from locust import HttpUser, constant, task


class GetEntityUser(HttpUser):
    wait_time = constant(1)

    @task
    def entities(self):
        id = random.randint(1, 100000)
        self.client.request_name = "/entities?[urn]"
        self.client.get(
            f"/entities/urn:li:dataset:(urn:li:dataPlatform:bigquery,test_dataset_{id},PROD)"
        )
