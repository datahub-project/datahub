# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import logging

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.urns import FormUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

graph = DataHubGraph(
    config=DatahubClientConfig(
        server="http://localhost:8080",
    )
)

form_urn = FormUrn("metadata_initiative_1")

# Hard delete the form
graph.delete_entity(urn=str(form_urn), hard=True)
# Delete references to this form (must do)
graph.delete_references_to_urn(urn=str(form_urn), dry_run=False)

log.info(f"Deleted form {form_urn}")
