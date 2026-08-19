"""
Product-domain taxonomy for the smoke-test suite.

Every smoke test declares the domain that owns it, so CI can select a subset of
the suite per pull request and route a failure to the owning team:

    @pytest.mark.domain(Domain.CATALOG)
    @pytest.mark.domain(Domain.CATALOG, Domain.INGESTION)   # spans two domains

The helpers here back the `--domain` command-line option wired up in conftest.py.
"""

from enum import Enum
from typing import Optional, Sequence, Set

from _pytest.mark.structures import Mark


class Domain(str, Enum):
    """Product domains that own smoke tests."""

    PLATFORM = "platform"
    OBSERVE = "observe"
    INGESTION = "ingestion"
    AI = "ai"
    CATALOG = "catalog"


ALL_DOMAINS: Set[str] = {domain.value for domain in Domain}


def parse_requested_domains(values: Sequence[str]) -> Set[str]:
    """Normalise `--domain` values, rejecting anything outside the enum.

    Unknown values are rejected rather than silently matching nothing, which
    would otherwise produce a green run over zero tests.
    """
    requested = {value.strip().lower() for value in values if value.strip()}
    unknown = sorted(requested - ALL_DOMAINS)
    if unknown:
        raise ValueError(
            f"Unknown --domain value(s): {', '.join(unknown)}. "
            f"Valid domains: {', '.join(sorted(ALL_DOMAINS))}"
        )
    return requested


def domains_of(marker: Optional[Mark]) -> Set[str]:
    """The domain values a test declares via its `domain(...)` marker."""
    if marker is None:
        return set()
    return {
        arg.value if isinstance(arg, Domain) else str(arg).lower()
        for arg in marker.args
    }


def is_selected(declared: Set[str], requested: Set[str]) -> bool:
    """Whether a test declaring `declared` runs when `requested` was asked for.

    An empty request selects everything. Otherwise a test runs when it declares
    at least one requested domain, so a test spanning domains is picked up by
    any of them, and an untagged test is not picked up at all.
    """
    if not requested:
        return True
    return bool(declared & requested)
