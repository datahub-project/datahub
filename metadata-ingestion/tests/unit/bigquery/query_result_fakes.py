from typing import Any, Iterator, List


class FakeResultPage:
    """Stand-in for a BigQuery result page, which exposes num_items without copying rows."""

    def __init__(self, items: List[Any]) -> None:
        self.num_items = len(items)
        self._items = items

    def __iter__(self) -> Iterator[Any]:
        return iter(self._items)
