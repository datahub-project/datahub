"""Which Superset (and Preset) datasets ingestion keeps, as one pure predicate.

SupersetSource._process_dataset and SupersetConfig.probe_verdict_override both
call dataset_verdict, so `probe filter --kind Dataset` cannot drift from the
run. No I/O here: the caller fetches the facts.
"""

from dataclasses import dataclass
from typing import Optional, Protocol

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.verdicts import Verdict

# Superset datasets carry no subTypes aspect, so the probe's kind for them is the
# plain word rather than a DatasetSubTypes constant.
SUPERSET_DATASET_KIND = "Dataset"


class DatasetFilterConfig(Protocol):
    @property
    def dataset_pattern(self) -> AllowDenyPattern: ...

    @property
    def database_pattern(self) -> AllowDenyPattern: ...


@dataclass(frozen=True)
class DatasetFacts:
    # The dataset's `table_name`, which is all dataset_pattern is matched against.
    table_name: str
    # The database the dataset reads from. None (or empty) skips database_pattern:
    # ingestion does not drop a dataset whose database it could not learn.
    database_name: Optional[str] = None


def dataset_verdict(config: DatasetFilterConfig, facts: DatasetFacts) -> Verdict:
    if not config.dataset_pattern.allowed(facts.table_name):
        return Verdict.exclude("dataset_pattern")
    if facts.database_name and not config.database_pattern.allowed(facts.database_name):
        return Verdict.exclude("database_pattern", matched_target=facts.database_name)
    return Verdict.include()
