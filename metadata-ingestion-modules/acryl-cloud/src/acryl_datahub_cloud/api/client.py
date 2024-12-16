from acryl_datahub_cloud.api.entity_versioning import EntityVersioningAPI
from datahub.ingestion.graph.client import DataHubGraph


class AcrylGraph(EntityVersioningAPI, DataHubGraph):
    pass
