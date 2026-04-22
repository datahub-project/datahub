import warnings

from datahub.errors import ExperimentalWarning

warnings.filterwarnings("ignore", category=ExperimentalWarning)

from datahub.metadata.urns import CorpUserUrn  # noqa: E402
from datahub.sdk import DataHubClient, GlossaryNodeUrn  # noqa: E402

client = DataHubClient.from_env()

node = client.entities.get(GlossaryNodeUrn("Finance"))
node.add_owner(CorpUserUrn("jdoe"))
client.entities.upsert(node)
