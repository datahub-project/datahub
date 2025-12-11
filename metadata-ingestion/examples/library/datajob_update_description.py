# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# metadata-ingestion/examples/library/datajob_update_description.py
from datahub.sdk import DataFlowUrn, DataHubClient, DataJobUrn

client = DataHubClient.from_env()

dataflow_urn = DataFlowUrn(
    orchestrator="airflow", flow_id="daily_etl_pipeline", cluster="prod"
)
datajob_urn = DataJobUrn(flow=dataflow_urn, job_id="transform_customer_data")

datajob = client.entities.get(datajob_urn)
datajob.set_description(
    "This job performs critical customer data transformation. "
    "It joins raw customer records with address information and applies "
    "data quality rules before loading into the analytics warehouse."
)

client.entities.update(datajob)

print(f"Updated description for {datajob_urn}")
