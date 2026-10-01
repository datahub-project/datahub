"""A probe provider base class, and an ingestion-Source mixin, defined apart
from the test module that subclasses them: the framework must vouch for a
provider base's file, and must not vouch for a Source's."""

from typing import List

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.api.source import Source, SourceReport

SOURCE_SENTINEL = "PLANTED-source-mixin-text"


class BaseThingsProvider:
    @classmethod
    def for_config(cls, config: object) -> "BaseThingsProvider":
        return cls()

    def __enter__(self) -> "BaseThingsProvider":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    @probe_method(name="things")
    def things(self, name: str = "") -> List[str]:
        """List things."""
        raise ValueError(f"no thing named '{name}'")


class SourceWithProbeMethod(Source):
    """Stands in for an ingestion Source carrying @probe_method directly (as
    ModeSource does): its file is reused ingestion code, not provider code."""

    def get_report(self) -> SourceReport:
        return SourceReport()

    @probe_method(name="widgets")
    def widgets(self) -> List[str]:
        """List widgets."""
        raise ValueError(f"widget lookup quoted {SOURCE_SENTINEL}")
