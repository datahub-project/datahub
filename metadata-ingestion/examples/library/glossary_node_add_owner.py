from datahub.metadata.urns import CorpUserUrn
from datahub.sdk import DataHubClient, GlossaryNodeUrn

client = DataHubClient.from_env()

node = client.entities.get(GlossaryNodeUrn("7f3d2c1a"))
node.add_owner(CorpUserUrn("jdoe"))
client.entities.upsert(node)
