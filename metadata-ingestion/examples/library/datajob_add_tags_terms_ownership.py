# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# metadata-ingestion/examples/library/datajob_add_tags_terms_ownership.py
from datahub.metadata.urns import (
    CorpUserUrn,
    DataFlowUrn,
    DataJobUrn,
    GlossaryTermUrn,
    TagUrn,
)
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

datajob_urn = DataJobUrn(
    job_id="transform_customer_data",
    flow=DataFlowUrn(
        orchestrator="airflow", flow_id="daily_etl_pipeline", cluster="prod"
    ),
)

datajob = client.entities.get(datajob_urn)

datajob.add_tag(TagUrn("Critical"))
datajob.add_tag(TagUrn("ETL"))

datajob.add_term(GlossaryTermUrn("CustomerData"))
datajob.add_term(GlossaryTermUrn("DataTransformation"))

datajob.add_owner(CorpUserUrn("data_engineering_team"))
datajob.add_owner(CorpUserUrn("john.doe"))

client.entities.update(datajob)

print(f"Added tags, terms, and ownership to {datajob_urn}")
