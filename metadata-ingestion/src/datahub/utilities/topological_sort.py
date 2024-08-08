from collections import deque
from typing import Dict, Iterable, List, Tuple, TypeVar

_K = TypeVar("_K")


def topological_sort(nodes: List[_K], edges: List[Tuple[_K, _K]]) -> Iterable[_K]:
    """Topological sort of a directed acyclic graph or forest.

    This is an implementation of Kahn's algorithm.

    Args:
        nodes: List of nodes.
        edges: List of edges, as tuples of (source, target).

    Returns:
        List of nodes in topological order.
    """

    # Build adjacency list.
    adj_list: Dict[_K, List[_K]] = {node: [] for node in nodes}
    for source, target in edges:
        adj_list[source].append(target)

    # Build in-degree map.
    in_degrees: Dict[_K, int] = {node: 0 for node in nodes}
    for _source, target in edges:
        in_degrees[target] += 1

    # Initialize queue with nodes with in-degree 0.
    queue = deque(node for node in nodes if in_degrees[node] == 0)

    results = 0
    while queue:
        node = queue.popleft()

        results += 1
        yield node

        # Decrement in-degree of each neighbor.
        for neighbor in adj_list[node]:
            in_degrees[neighbor] -= 1

            # If in-degree is 0, add to queue.
            if in_degrees[neighbor] == 0:
                queue.append(neighbor)

    if results != len(nodes):
        raise ValueError("Graph contains cycles.")
