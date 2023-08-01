import dataclasses
import json
import re
from collections import defaultdict
from typing import Any, Dict, List, Sequence, Set, Tuple, Union

import deepdiff.serialization
import yaml
from deepdiff import DeepDiff
from deepdiff.model import DiffLevel
from deepdiff.operator import BaseOperator
from typing_extensions import Literal

ReportType = Literal[
    "type_changes",
    "dictionary_item_added",
    "dictionary_item_removed",
    "values_changed",
    "unprocessed",
    "iterable_item_added",
    "iterable_item_removed",
    "iterable_item_moved",
    "attribute_added",
    "attribute_removed",
    "set_item_added",
    "set_item_removed",
    "repetition_change",
]


@dataclasses.dataclass(frozen=True)
class AspectForDiff:
    urn: str
    change_type: str
    aspect_name: str
    aspect: Dict[str, Any] = dataclasses.field(hash=False)
    delta_info: "DeltaInfo" = dataclasses.field(hash=False, repr=False)

    @classmethod
    def create_from_mcp(cls, idx: int, obj: Dict[str, Any]) -> "AspectForDiff":
        aspect = obj["aspect"]
        return cls(
            urn=obj["entityUrn"],
            change_type=obj["changeType"],
            aspect_name=obj["aspectName"],
            aspect=aspect.get("json", aspect),
            delta_info=DeltaInfo(idx=idx, original=obj),
        )

    def __repr__(self):
        d = {
            field.name: getattr(self, field.name)
            for field in dataclasses.fields(self)
            if field.repr
        }
        d["aspect"] = "<aspect>"
        return "\n" + "\t" * 2 + str(json.dumps(d))


@dataclasses.dataclass
class DeltaInfo:
    """Information about an MCP used to construct a diff delta.

    In a separate class so it can be ignored by DeepDiff via MCPDeltaInfoOperator.
    """

    idx: int  # Location in list of MCEs in golden file
    original: Dict[str, Any]  # Original json-serialized MCP


class DeltaInfoOperator(BaseOperator):
    """Warning: Doesn't seem to be working right now.
    Ignored via an ignore path as an extra layer of defense.
    """

    def __init__(self):
        super().__init__(types=[DeltaInfo])

    def give_up_diffing(self, *args: Any, **kwargs: Any) -> bool:
        return True


AspectsByUrn = Dict[str, Dict[str, List[AspectForDiff]]]


class CannotCompareMCPs(Exception):
    pass


def get_aspects_by_urn(obj: object) -> AspectsByUrn:
    """Restructure a list of serialized MCPs by urn and aspect.
    Retains information like the original dict and index to facilitate `apply_delta` later.

    Raises:
        AssertionError: If the input is not purely a list of MCPs.
    """
    d: AspectsByUrn = defaultdict(dict)
    assert isinstance(obj, list), obj
    for i, entry in enumerate(obj):
        assert isinstance(entry, dict), entry
        if "proposedSnapshot" in entry:
            raise CannotCompareMCPs("Found MCEs")
        elif "entityUrn" in entry and "aspectName" in entry and "aspect" in entry:
            urn = entry["entityUrn"]
            aspect_name = entry["aspectName"]
            aspect = AspectForDiff.create_from_mcp(i, entry)
            d[urn].setdefault(aspect_name, []).append(aspect)
        else:
            raise AssertionError(f"Unrecognized MCE: {entry}")

    return d


@dataclasses.dataclass
class MCPAspectDiff:
    aspects_added: Dict[int, AspectForDiff]
    aspects_removed: Dict[int, AspectForDiff]
    aspects_changed: Dict[Tuple[int, AspectForDiff, AspectForDiff], List[DiffLevel]]

    @classmethod
    def create(cls, diff: DeepDiff) -> "MCPAspectDiff":
        # Parse DeepDiff to distinguish between aspects that were added, removed, or changed
        aspects_added = {}
        aspects_removed = {}
        aspects_changed = defaultdict(list)
        for key, diff_levels in diff.tree.items():
            for diff_level in diff_levels:
                path = diff_level.path(output_format="list")
                idx = int(path[0])
                if len(path) == 1 and key == "iterable_item_added":
                    aspects_added[idx] = diff_level.t2
                elif len(path) == 1 and key == "iterable_item_removed":
                    aspects_removed[idx] = diff_level.t1
                else:
                    level = diff_level
                    while not isinstance(level.t1, AspectForDiff):
                        level = level.up
                    aspects_changed[(idx, level.t1, level.t2)].append(diff_level)

        return cls(
            aspects_added=aspects_added,
            aspects_removed=aspects_removed,
            aspects_changed=aspects_changed,
        )


@dataclasses.dataclass
class MCPDiff:
    aspect_changes: Dict[str, Dict[str, MCPAspectDiff]]  # urn -> aspect -> diff
    urns_added: Set[str]
    urns_removed: Set[str]

    def __bool__(self) -> bool:
        return bool(self.aspect_changes)

    @classmethod
    def create(
        cls,
        golden: AspectsByUrn,
        output: AspectsByUrn,
        ignore_paths: Sequence[str],
    ) -> "MCPDiff":
        ignore_paths = [cls.convert_path(path) for path in ignore_paths]

        aspect_changes: Dict[str, Dict[str, MCPAspectDiff]] = defaultdict(dict)
        for urn in golden.keys() | output.keys():
            golden_map = golden.get(urn, {})
            output_map = output.get(urn, {})
            for aspect_name in golden_map.keys() | output_map.keys():
                diff = DeepDiff(
                    t1=golden_map.get(aspect_name, []),
                    t2=output_map.get(aspect_name, []),
                    exclude_regex_paths=ignore_paths,
                    ignore_order=True,
                    custom_operators=[DeltaInfoOperator()],
                )
                if diff:
                    aspect_changes[urn][aspect_name] = MCPAspectDiff.create(diff)

        return cls(
            urns_added=output.keys() - golden.keys(),
            urns_removed=golden.keys() - output.keys(),
            aspect_changes=aspect_changes,
        )

    @staticmethod
    def convert_path(path: str) -> str:
        # Attempt to use paths intended for the root golden... sorry for the regex
        return re.sub(
            r"^root\\?\[([0-9]+|\\d\+)\\?]\\?\['aspect'\\?](\\?\['(json|value)'\\?])?",
            r"root\[\\d+].aspect",
            path,
        )

    def apply_delta(self, golden: List[Dict[str, Any]]) -> None:
        aspect_diffs = [v for d in self.aspect_changes.values() for v in d.values()]
        for aspect_diff in aspect_diffs:
            for (_, old, new), diffs in aspect_diff.aspects_changed.items():
                golden[old.delta_info.idx] = new.delta_info.original

        indices_to_remove = set()
        for aspect_diff in aspect_diffs:
            for ga in aspect_diff.aspects_removed.values():
                indices_to_remove.add(ga.delta_info.idx)
        for idx in sorted(indices_to_remove, reverse=True):
            del golden[idx]

        for aspect_diff in aspect_diffs:  # Ideally would have smarter way to do this
            for ga in aspect_diff.aspects_added.values():
                golden.insert(ga.delta_info.idx, ga.delta_info.original)

    def pretty(self, verbose: bool = False) -> str:
        """The pretty human-readable string output of the diff between golden and output."""
        s = []
        for urn in self.urns_added:
            s.append(f"Urn added, {urn}{' with aspects:' if verbose else ''}")
            if verbose:
                for aspect_diff in self.aspect_changes[urn].values():
                    for i, ga in aspect_diff.aspects_added.items():
                        s.append(self.report_aspect(ga, i))
                        s.append(serialize_aspect(ga.aspect))
        if self.urns_added:
            s.append("")

        for urn in self.urns_removed:
            s.append(f"Urn removed, {urn}{' with aspects:' if verbose else ''}")
            if verbose:
                for aspect_diff in self.aspect_changes[urn].values():
                    for i, ga in aspect_diff.aspects_removed.items():
                        s.append(self.report_aspect(ga, i))
                        s.append(serialize_aspect(ga.aspect))
        if self.urns_removed:
            s.append("")

        for urn in self.aspect_changes.keys() - self.urns_added - self.urns_removed:
            aspect_map = self.aspect_changes[urn]
            s.append(f"Urn changed, {urn}:")
            for aspect_name, aspect_diffs in aspect_map.items():
                for i, ga in aspect_diffs.aspects_added.items():
                    s.append(self.report_aspect(ga, i, "added"))
                    if verbose:
                        s.append(serialize_aspect(ga.aspect))
                for i, ga in aspect_diffs.aspects_removed.items():
                    s.append(self.report_aspect(ga, i, "removed"))
                    if verbose:
                        s.append(serialize_aspect(ga.aspect))
                for (i, old, new), diffs in aspect_diffs.aspects_changed.items():
                    s.append(self.report_aspect(old, i, "changed") + ":")
                    for diff_level in diffs:
                        s.append(self.report_diff_level(diff_level, i))
                    if verbose:
                        s.append(f"Old aspect:\n{serialize_aspect(old.aspect)}")
                        s.append(f"New aspect:\n{serialize_aspect(new.aspect)}")

            s.append("")

        return "\n".join(s)

    @staticmethod
    def report_aspect(ga: AspectForDiff, idx: int, msg: str = "") -> str:
        # Describe as "nth <aspect>" if n > 1
        base = (idx + 1) % 10
        if base == 1:
            suffix = "st"
        elif base == 2:
            suffix = "nd"
        elif base == 3:
            suffix = "rd"
        else:
            suffix = "th"
        ordinal = f"{(idx+1)}{suffix} " if idx else ""
        return f"{ordinal}<{ga.aspect_name}> {msg}"

    @staticmethod
    def report_diff_level(diff: DiffLevel, idx: int) -> str:
        return "\t" + deepdiff.serialization.pretty_print_diff(diff).replace(
            f"root[{idx}].", ""
        )


def serialize_aspect(aspect: Union[AspectForDiff, Dict[str, Any]]) -> str:
    if isinstance(aspect, AspectForDiff):  # Unpack aspect
        aspect = aspect.aspect
    return "    " + yaml.dump(aspect, sort_keys=False).replace("\n", "\n    ").strip()
