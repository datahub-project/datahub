# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create an emitter to the GMS REST API.

emitter = DatahubRestEmitter("http://localhost:8080")

jobFlow = DataFlow(env="prod", orchestrator="airflow", id="flow_new_api")
jobFlow.emit(emitter)

dataJob = DataJob(flow_urn=jobFlow.urn, id="job1")
dataJob.emit(emitter)
