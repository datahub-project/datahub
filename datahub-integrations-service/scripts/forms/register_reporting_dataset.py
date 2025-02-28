from datahub.ingestion.graph.client import get_default_graph
import sys
from datahub.metadata.schema_classes import DatasetPropertiesClass
from datahub.emitter.mcp import MetadataChangeProposalWrapper

s3_uri = sys.argv[1]

with get_default_graph() as graph:
    result = graph.execute_graphql(
        """
                          query {
                            formAnalyticsConfig {
                                enabled
                                datasetUrn
                            }
                          }
                          """
    )
    print(result)
    dataset_urn = result["formAnalyticsConfig"]["datasetUrn"]
    dataset_properties = graph.get_aspect(dataset_urn, DatasetPropertiesClass)
    if not dataset_properties:
        dataset_properties = DatasetPropertiesClass()
    dataset_properties.customProperties["physical_uri"] = s3_uri
    graph.emit(
        MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=dataset_properties)
    )
    print(f"Registered dataset {s3_uri} with dataset urn {dataset_urn}")
