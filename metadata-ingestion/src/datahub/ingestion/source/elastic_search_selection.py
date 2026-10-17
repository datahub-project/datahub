"""Which Elasticsearch indices and index templates ingestion emits, as pure
functions.

ElasticsearchSource and the probe's verdict hook both call these, so `probe
filter` cannot drift from what ingestion picks up. No I/O and no client here.
"""

from typing import Optional, Protocol

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.verdicts import Verdict

# Ingestion emits nothing for an index or template whose mappings yield no
# schema field, so that is a rule of its own rather than a pattern's.
NO_MAPPED_FIELDS_RULE = "no_mapped_fields"


class ElasticsearchFilterConfig(Protocol):
    @property
    def index_pattern(self) -> AllowDenyPattern: ...

    @property
    def index_template_pattern(self) -> AllowDenyPattern: ...


def _verdict(
    pattern: AllowDenyPattern, field: str, name: str, mapped_fields: Optional[int]
) -> Verdict:
    if not pattern.allowed(name):
        return Verdict.exclude(field)
    if mapped_fields == 0:
        return Verdict.exclude(NO_MAPPED_FIELDS_RULE)
    return Verdict.include()


def index_verdict(
    config: ElasticsearchFilterConfig, name: str, mapped_fields: Optional[int]
) -> Verdict:
    """`name` is the concrete index name, a data stream's backing index
    included. `mapped_fields` is the number of schema fields its mappings
    yield; None (not known yet, or not given to the probe) skips that rule."""
    return _verdict(config.index_pattern, "index_pattern", name, mapped_fields)


def index_template_verdict(
    config: ElasticsearchFilterConfig, name: str, mapped_fields: Optional[int]
) -> Verdict:
    """As index_verdict, for a legacy or composable index template. Whether
    templates are ingested at all is ingest_index_templates, checked first."""
    return _verdict(
        config.index_template_pattern, "index_template_pattern", name, mapped_fields
    )
