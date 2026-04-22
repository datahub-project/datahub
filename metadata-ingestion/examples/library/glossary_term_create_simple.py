import warnings

from datahub.errors import ExperimentalWarning

warnings.filterwarnings("ignore", category=ExperimentalWarning)

from datahub.sdk import DataHubClient  # noqa: E402
from datahub.sdk.glossary_term import GlossaryTerm  # noqa: E402

client = DataHubClient.from_env()

term = GlossaryTerm(
    name="rateofreturn",
    display_name="Rate of Return",
    definition="A rate of return (RoR) is the net gain or loss of an investment over a specified time period.",
)

client.entities.upsert(term)
