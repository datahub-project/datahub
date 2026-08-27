import dataclasses
from collections import defaultdict
from typing import List


@dataclasses.dataclass
class PrefixGroup:
    prefix: str
    names: List[str]  # every name in the list has the same prefix
    exact_match: bool = False


def build_prefix_batches(
    names: List[str], max_batch_size: int, max_groups_in_batch: int
) -> List[List[PrefixGroup]]:
    """Split the names into a list of batches, where each batch is a list of groups and each group is a list of names with a common prefix."""

    groups = _build_prefix_groups(names, max_batch_size=max_batch_size)
    batches = _batch_prefix_groups(
        groups, max_batch_size=max_batch_size, max_groups_in_batch=max_groups_in_batch
    )
    return batches


def _build_prefix_groups(names: List[str], max_batch_size: int) -> List[PrefixGroup]:
    """Given a list of names, group them by shared prefixes such that no group is larger than `max_batch_size`."""

    def split_group(group: PrefixGroup) -> List[PrefixGroup]:
        if len(group.names) <= max_batch_size:
            return [group]

        result = []

        # Split into subgroups by the next character.
        prefix_length = len(group.prefix) + 1
        subgroups = defaultdict(list)
        for name in group.names:
            if len(name) < prefix_length:
                # Handle cases where a single name is also the prefix for a large number of names.
                # For example, if NAME and NAME_{1..10000} are both in the list.
                result.append(PrefixGroup(prefix=name, names=[name], exact_match=True))
                continue

            prefix = name[:prefix_length]
            subgroups[prefix].append(name)

        for prefix, names in subgroups.items():
            result.extend(split_group(PrefixGroup(prefix=prefix, names=names)))

        return result

    return split_group(PrefixGroup(prefix="", names=sorted(names)))


def _batch_prefix_groups(
    groups: List[PrefixGroup], max_batch_size: int, max_groups_in_batch: int
) -> List[List[PrefixGroup]]:
    """Batch the groups together, so that no batch holds more than
    `max_groups_in_batch` groups and the total size of names in a batch
    does not exceed `max_batch_size` (a single oversized group is allowed
    to exceed `max_batch_size` on its own, since groups are atomic)."""

    # A batch is a set of groups.

    # This is a variant of the 1D bin packing problem, which is actually NP-hard.
    # However, we'll just use a greedy algorithm for simplicity.

    batches = []
    current_batch_size = 0
    batch: List[PrefixGroup] = []
    for group in groups:
        # Both the name-count check and the group-count check ask the same
        # question: "would adding this group push us over the cap?" — i.e.
        # (current state) + (incoming delta) > cap. If either trips, close
        # the current batch before appending. The `+ 1` is the incoming
        # group itself.
        if (
            current_batch_size + len(group.names) > max_batch_size
            or len(batch) + 1 > max_groups_in_batch
        ):
            batches.append(batch)
            batch = []
            current_batch_size = 0
        batch.append(group)
        current_batch_size += len(group.names)
    if batch:
        batches.append(batch)
    return batches
