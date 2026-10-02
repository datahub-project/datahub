"""An AllowDenyPattern addressed by a dotted path from the top-level config.

A Filters(...) declaration may sit on a nested ConfigModel's field, so the
field filtering a kind can be `filter_config.entries.pattern`. A path without
a dot is an ordinary top-level field. Free of other agent imports, so the leaf
module verdicts.py can use it.
"""

from typing import Optional

from pydantic import BaseModel

from datahub.configuration.common import AllowDenyPattern


def pattern_at(config: object, path: str) -> Optional[AllowDenyPattern]:
    """The pattern at `path`, or None when a segment is missing or unset."""
    node: object = config
    for segment in path.split("."):
        node = getattr(node, segment, None)
        if node is None:
            return None
    return node if isinstance(node, AllowDenyPattern) else None


def unset_block_on(config: object, path: str) -> Optional[str]:
    """The dotted prefix of the first block on the way to `path` that is None.

    An Optional block a recipe leaves out filters nothing, so callers read it as
    allow-all. None when every block is set, and when a block is not a field at
    all: that is a resolution bug, for require_pattern_at to raise.
    """
    node: object = config
    parents = path.split(".")[:-1]
    for depth, segment in enumerate(parents, start=1):
        if not hasattr(node, segment):
            return None
        node = getattr(node, segment)
        if node is None:
            return ".".join(parents[:depth])
    return None


def require_pattern_at(config: object, path: str) -> AllowDenyPattern:
    """As pattern_at, for callers that already resolved `path` as a pattern
    field: anything else is a resolution bug, so it raises rather than
    reading as allow-all."""
    pattern = pattern_at(config, path)
    if pattern is None:
        raise TypeError(
            f"'{path}' does not name an AllowDenyPattern on {type(config).__name__}"
        )
    return pattern


def copy_with_pattern_at(
    config: BaseModel, path: str, pattern: AllowDenyPattern
) -> BaseModel:
    """A shallow copy of `config` with the pattern at `path` replaced. Every
    block on the path is copied, so validating the leaf in place cannot rewrite
    the original; shallow, since a deep copy clones cached credentials."""
    head, _, rest = path.partition(".")
    if not rest:
        return config.model_copy(update={head: pattern})
    child = getattr(config, head)
    if not isinstance(child, BaseModel):
        raise TypeError(
            f"'{head}' on {type(config).__name__} is not a config block, so "
            f"'{path}' cannot address a pattern inside it"
        )
    return config.model_copy(update={head: copy_with_pattern_at(child, rest, pattern)})


def validate_pattern_at(
    config: BaseModel, path: str, pattern: AllowDenyPattern
) -> None:
    """Rerun the after-validators of the block owning the leaf field, not its
    parents': a nested pattern is normalized in its own block."""
    *parents, leaf = path.split(".")
    owner: BaseModel = config
    for segment in parents:
        child = getattr(owner, segment)
        if not isinstance(child, BaseModel):
            raise TypeError(f"'{segment}' in '{path}' is not a config block")
        owner = child
    type(owner).__pydantic_validator__.validate_assignment(owner, leaf, pattern)
