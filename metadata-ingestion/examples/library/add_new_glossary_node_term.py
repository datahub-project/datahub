from datahub.emitter.mcp import MetadataChangeProposalWrapper

# read-modify-write requires access to the DataHubGraph (RestEmitter is not enough)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    GlossaryNodeInfoClass,
    GlossaryTermInfoClass,
)

nodeUrn="urn:li:glossaryNode:NodeTest"
termUrn="urn:li:glossaryNode:TermTest"

mcp = MetadataChangeProposalWrapper(
    entityType="glossaryNode",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=nodeUrn,
    aspectName="glossaryNodeInfo",
    aspect=GlossaryNodeInfoClass(
        definition="node description123",
        name="nice NodeTest []"
    ),
)
gms_endpoint = "http://localhost:8080"
token = "eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRlbW8iLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMiIsImV4cCI6MTY1ODA0NjQ3MCwianRpIjoiM2Q1YzcwZWUtYWRmYS00M2MwLWI4ZmMtZDhiNzhkMTkwYTk3Iiwic3ViIjoiZGVtbyIsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.Lb-FrVBqse6aFdVQF2b7RPLPyhjso21qAxatKsGCCew"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint, token=token))

graph.emit(mcp)
