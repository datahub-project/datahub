from datahub.emitter.mcp import MetadataChangeProposalWrapper

# read-modify-write requires access to the DataHubGraph (RestEmitter is not enough)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    GlossaryNodeInfoClass,
    GlossaryTermInfoClass,
)

nodeUrn="urn:li:glossaryNode:Metadata.Dataset.Frequency"

all_mcps = []
mcp = MetadataChangeProposalWrapper(
    entityType="glossaryNode",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=nodeUrn,
    aspectName="glossaryNodeInfo",
    aspect=GlossaryNodeInfoClass(
        definition="Term that describes the frequency of data upload",
        name="Dataset Frequency"
    ),
)
all_mcps.append(mcp)
for term in ["Adhoc","Onetime","Periodic","Unknown","Test"]:
    termUrn = f"urn:li:glossaryTerm:Metadata.Dataset.Frequency.{term}"
    mcp1 = MetadataChangeProposalWrapper(
        entityType="glossaryTerm",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=termUrn,
        aspectName="glossaryTermInfo",
        aspect=GlossaryTermInfoClass(
            definition=f"Upload for this dataset is {term}",
            name=f"{term} Upload",
            parentNode=nodeUrn,
            termSource="INTERNAL"
        )
    )
    # mcp2 = MetadataChangeProposalWrapper(
    #     entityType="glossaryTerm",
    #     changeType=ChangeTypeClass.UPSERT,
    #     entityUrn=termUrn,
    #     aspectName="browsePaths",
    #     aspect=BrowsePathsClass(paths=[f"/Metadata.Dataset.Frequency/{term}"])
    # )
    all_mcps.append(mcp1)
    # all_mcps.append(mcp2)
gms_endpoint = "http://localhost:8080"
token = "eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRlbW8iLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMiIsImV4cCI6MTY1ODA0NjQ3MCwianRpIjoiM2Q1YzcwZWUtYWRmYS00M2MwLWI4ZmMtZDhiNzhkMTkwYTk3Iiwic3ViIjoiZGVtbyIsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.Lb-FrVBqse6aFdVQF2b7RPLPyhjso21qAxatKsGCCew"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint, token=token))

for item in all_mcps:
    graph.emit(item)
