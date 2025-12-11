# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import DataJobInputOutputClass

# Get the current lineage for a datajob
gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

urn = "urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)"
result = graph.get_aspect(entity_urn=urn, aspect_type=DataJobInputOutputClass)

print(result)
