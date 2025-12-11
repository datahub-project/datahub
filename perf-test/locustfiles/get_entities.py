# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
