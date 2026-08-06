from typing import Dict, List, Optional

from datahub.metadata.urns import DatasetUrn
from datahub.utilities.urns.error import InvalidUrnError

# Whether a bulk load should fill its URN index. Off by default: the index holds a whole
# platform's URNs in memory, which most consumers of a bulk-loaded catalog never read.
# Process-wide rather than a per-call argument so that consumers wanting the index and
# consumers that don't still share one cached catalog and one fetch. Set once per
# ingestion, before any source exists — see PipelineContext.
_LOAD_URN_ALIASES = False


def set_urn_alias_loading(value: bool) -> None:
    global _LOAD_URN_ALIASES
    _LOAD_URN_ALIASES = value


def urn_alias_loading_enabled() -> bool:
    return _LOAD_URN_ALIASES


def _has_lowercased_name(urn: str) -> bool:
    # Only the dataset name can be judged: a URN's scaffolding is mixed case whatever the
    # entity is called (`dataPlatform`, the env), so `urn == urn.lower()` is never true.
    try:
        name = DatasetUrn.from_string(urn).name
    except InvalidUrnError:
        return False
    return name == name.lower()


class UrnAliasCache:
    """Stores URNs by their lowercased form, for case-insensitive lookup."""

    def __init__(self) -> None:
        # A list per key: two entities differing only by case can both exist, and the
        # caller needs to see that rather than be handed one of them.
        self._by_lower: Dict[str, List[str]] = {}
        self._count = 0

    def add(self, urn: str) -> None:
        entry = self._by_lower.setdefault(urn.lower(), [])
        if urn in entry:
            return
        entry.append(urn)
        self._count += 1

    def get(self, urn: str) -> Optional[List[str]]:
        """URNs stored for `urn` ignoring case; None if nothing is stored for it.

        None means unknown, an empty list means known not to exist.
        """
        entry = self._by_lower.get(urn.lower())
        # Copied so callers cannot mutate the store.
        return list(entry) if entry is not None else None

    def count(self) -> int:
        return self._count


class UrnAliasResolver:
    """Resolves a URN to the URNs DataHub stores for it, ignoring case."""

    def __init__(self) -> None:
        self._cache = UrnAliasCache()

    def add(self, urn: str) -> None:
        self._cache.add(urn)

    def lookup(self, urn: str) -> List[str]:
        """Stored URNs matching `urn` ignoring case; more than one means ambiguous."""
        return self._cache.get(urn) or []

    def resolve(self, urn: str, prefer_lowercased: bool = False) -> Optional[str]:
        """The URN DataHub stores for `urn`, or None if there is no single match.

        `prefer_lowercased` resolves a collision between several casings of the same name
        to the lowercase-named one instead of declining it.
        """
        matches = self.lookup(urn)
        if len(matches) == 1:
            return matches[0]
        if urn in matches:
            return urn
        if prefer_lowercased:
            return next((m for m in matches if _has_lowercased_name(m)), None)
        return None

    def cache_count(self) -> int:
        return self._cache.count()
