import logging

from datahub.api.entities.corpgroup.corpgroup import (
    CorpGroup,
    CorpGroupGenerationConfig,
)
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.urns import CorpUserUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

group_email = "foogroup@acryl.io"
group = CorpGroup(
    id=group_email,
    owners=[str(CorpUserUrn("datahub"))],
    members=[
        str(CorpUserUrn("bar@acryl.io")),
        str(CorpUserUrn("joe@acryl.io")),
    ],
    display_name="Foo Group",
    email=group_email,
    description="Software engineering team",
    slack="@foogroup",
)

# Create graph client
datahub_graph = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

for event in group.generate_mcp(
    generation_config=CorpGroupGenerationConfig(
        override_editable=False, datahub_graph=datahub_graph
    )
):
    datahub_graph.emit(event)
log.info(f"Upserted group {group.urn}")
