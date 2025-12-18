import random

from locust import HttpUser, constant, task

from test_utils.datahub_sessions import DataHubSessions


datahub_instances = DataHubSessions()


class GetEntityUser(HttpUser):
    wait_time = constant(1)

    @task
    def entities(self):
        id = random.randint(1, 100000)
        session = datahub_instances.get_session(self.host)
        self.client.request_name = "/entities?[urn]"
        self.client.get(
            f"/api/gms/entities/urn:li:dataset:(urn:li:dataPlatform:bigquery,test_dataset_{id},PROD)",
            cookies=session.get_cookies()
        )
