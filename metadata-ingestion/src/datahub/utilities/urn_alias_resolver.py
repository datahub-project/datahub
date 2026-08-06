from typing import Dict, List, Optional


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

    def count(self) -> int:
        return self._cache.count()
