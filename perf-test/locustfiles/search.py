# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
