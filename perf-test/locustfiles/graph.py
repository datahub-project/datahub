import json

from locust import HttpUser, between, task

from test_utils.datahub_sessions import DataHubSessions


datahub_instances = DataHubSessions()


class GraphUser(HttpUser):
    wait_time = between(1, 5)

    @task
    def relationships(self):
        session = datahub_instances.get_session(self.host)
        self.client.get(
            "/api/gms/relationships?direction=INCOMING&urn=urn:li:corpuser:common&types=OwnedBy''",
            cookies=session.get_cookies()
        )
