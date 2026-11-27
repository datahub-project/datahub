"""Compile a GraphQL document for the exact GMS it will run on.

A selection newer than the oldest supported GMS carries a marker comment on the
line where it starts::

    query getDataset($urn: String!) {
      dataset(urn: $urn) {
        urn
        newField #[since oss=1.9.0 cloud=0.3.16]
      }
    }

The compiler drops each marked selection, with its subtree, that the target GMS
doesn't support. A side left out of a marker means the selection never runs
there, like ``@min_version``. Fragments and variables that only dropped
selections used are removed too. A selection set left empty is a compile error:
silently returning less than the caller asked for would hide a bug.

When GMS doesn't know its own version (unreleased builds), every version-gated
selection is kept and the caller should let the SDK strip what the server
lacks (``strip_unsupported_fields``).
"""

import dataclasses
import functools
import re
from typing import Any, Optional

from graphql import (
    DocumentNode,
    FieldNode,
    FragmentDefinitionNode,
    FragmentSpreadNode,
    GraphQLError,
    InlineFragmentNode,
    OperationDefinitionNode,
    SelectionSetNode,
    VariableDefinitionNode,
    VariableNode,
    Visitor,
    parse,
    print_ast,
    visit,
)
from graphql.language.visitor import REMOVE

from .gms_server_info import GmsServerInfo
from .version_requirements import VersionRequirement, parse_version

MARKER_TEXT = "#[since"
_MARKER = re.compile(r"#\[since\b([^\]]*)\]")
_MARKER_ARG = re.compile(r"^(oss|cloud)=(\S+)$")

Version = tuple[int, int, int, int]


class GqlCompileError(ValueError):
    """The document or its markers are malformed, or compiling emptied it."""


@dataclasses.dataclass(frozen=True)
class CompiledQuery:
    query: str
    strip_unsupported_fields: bool
    dropped_variables: frozenset[str] = frozenset()
    """Variables the compiled query no longer declares; don't send them."""


def compile_gql(document: str, info: Optional[GmsServerInfo]) -> CompiledQuery:
    """Compile ``document`` for ``info``; None means nothing is known about GMS."""
    if info is None:
        return _compile(document, is_cloud=None, version=None)
    return _compile(document, is_cloud=info.is_cloud, version=info.parsed_version)


def _parse_markers(document: str) -> dict[int, VersionRequirement]:
    markers: dict[int, VersionRequirement] = {}
    for line_number, line in enumerate(document.splitlines(), start=1):
        match = _MARKER.search(line)
        if match is None:
            if MARKER_TEXT in line:
                raise GqlCompileError(f"Line {line_number}: unterminated marker")
            continue
        sides: dict[str, Version] = {}
        for arg in match.group(1).split():
            arg_match = _MARKER_ARG.match(arg)
            if arg_match is None or arg_match.group(1) in sides:
                raise GqlCompileError(
                    f"Line {line_number}: bad marker argument {arg!r}"
                )
            try:
                sides[arg_match.group(1)] = parse_version(arg_match.group(2))
            except ValueError as e:
                raise GqlCompileError(f"Line {line_number}: {e}") from None
        if not sides:
            raise GqlCompileError(f"Line {line_number}: marker names no version")
        markers[line_number] = VersionRequirement(
            oss_min=sides.get("oss"), cloud_min=sides.get("cloud")
        )
    return markers


def _supported(
    requirement: VersionRequirement,
    is_cloud: Optional[bool],
    version: Optional[Version],
) -> bool:
    if is_cloud is None:
        return True
    minimum = requirement.cloud_min if is_cloud else requirement.oss_min
    if minimum is None:
        return False
    return version is None or version >= minimum


_SELECTIONS = (FieldNode, InlineFragmentNode, FragmentSpreadNode)


def _start_line(node: Any) -> int:
    return node.loc.start_token.line


class _DropUnsupported(Visitor):
    def __init__(self, dropped_lines: set[int]) -> None:
        super().__init__()
        self._dropped_lines = dropped_lines
        # Fragments left with no selections. Fine if nothing still spreads them
        # (pruning removes them); an error otherwise, checked after pruning.
        self.emptied_fragments: set[str] = set()

    def enter(self, node: Any, *_: Any) -> Any:
        if isinstance(node, _SELECTIONS) and _start_line(node) in self._dropped_lines:
            return REMOVE
        return None

    def leave(self, node: Any, key: Any, parent: Any, *_: Any) -> Any:
        if isinstance(node, SelectionSetNode) and not node.selections:
            if isinstance(parent, FragmentDefinitionNode):
                self.emptied_fragments.add(parent.name.value)
                return None
            raise GqlCompileError(
                f"Line {_start_line(node)}: every selection in this set was dropped"
            )
        return None


def _fragment_spreads(node: Any) -> set[str]:
    names: set[str] = set()

    class Collect(Visitor):
        def enter_fragment_spread(self, spread: FragmentSpreadNode, *_: Any) -> None:
            names.add(spread.name.value)

    visit(node, Collect())
    return names


def _variables_used(node: Any) -> set[str]:
    names: set[str] = set()

    class Collect(Visitor):
        def enter_variable_definition(self, *_: Any) -> Any:
            return self.SKIP  # declarations aren't uses

        def enter_variable(self, variable: VariableNode, *_: Any) -> None:
            names.add(variable.name.value)

    visit(node, Collect())
    return names


def _reachable_fragments(
    roots: list[Any], fragments: dict[str, FragmentDefinitionNode]
) -> set[str]:
    reached: set[str] = set()
    pending = [name for root in roots for name in _fragment_spreads(root)]
    while pending:
        name = pending.pop()
        if name in reached or name not in fragments:
            continue
        reached.add(name)
        pending.extend(_fragment_spreads(fragments[name]))
    return reached


def _prune(document: DocumentNode) -> tuple[DocumentNode, frozenset[str]]:
    """Drop fragments no operation reaches and variables no operation uses."""
    fragments = {
        d.name.value: d
        for d in document.definitions
        if isinstance(d, FragmentDefinitionNode)
    }
    operations = [
        d for d in document.definitions if isinstance(d, OperationDefinitionNode)
    ]
    reachable = _reachable_fragments(operations, fragments)

    unused_variables: set[int] = set()
    unused_names: set[str] = set()
    for operation in operations:
        used = _variables_used(operation)
        for name in _reachable_fragments([operation], fragments):
            used |= _variables_used(fragments[name])
        for definition in operation.variable_definitions or ():
            if definition.variable.name.value not in used:
                unused_variables.add(id(definition))
                unused_names.add(definition.variable.name.value)

    class Prune(Visitor):
        def enter_fragment_definition(
            self, node: FragmentDefinitionNode, *_: Any
        ) -> Any:
            return None if node.name.value in reachable else REMOVE

        def enter_variable_definition(
            self, node: VariableDefinitionNode, *_: Any
        ) -> Any:
            return REMOVE if id(node) in unused_variables else None

    return visit(document, Prune()), frozenset(unused_names)


@functools.lru_cache(maxsize=512)
def _compile(
    document: str, *, is_cloud: Optional[bool], version: Optional[Version]
) -> CompiledQuery:
    strip = version is None
    if MARKER_TEXT not in document:
        return CompiledQuery(query=document, strip_unsupported_fields=strip)

    markers = _parse_markers(document)
    try:
        ast = parse(document)
    except GraphQLError as e:
        raise GqlCompileError(f"Invalid GraphQL: {e.message}") from e

    starts: dict[int, int] = {}

    class CountStarts(Visitor):
        def enter(self, node: Any, *_: Any) -> None:
            if isinstance(node, _SELECTIONS):
                line = _start_line(node)
                starts[line] = starts.get(line, 0) + 1

    visit(ast, CountStarts())
    for line in markers:
        if starts.get(line, 0) != 1:
            raise GqlCompileError(
                f"Line {line}: a marker must share its line with exactly one "
                f"selection, found {starts.get(line, 0)}"
            )

    dropped = {
        line
        for line, requirement in markers.items()
        if not _supported(requirement, is_cloud, version)
    }
    drop = _DropUnsupported(dropped)
    if dropped:
        ast = visit(ast, drop)
    pruned, dropped_variables = _prune(ast)
    for definition in pruned.definitions:
        if (
            isinstance(definition, FragmentDefinitionNode)
            and definition.name.value in drop.emptied_fragments
        ):
            raise GqlCompileError(
                f"Fragment {definition.name.value}: every selection in it was "
                "dropped, but it is still used"
            )
    return CompiledQuery(
        query=print_ast(pruned),
        strip_unsupported_fields=strip,
        dropped_variables=dropped_variables,
    )
