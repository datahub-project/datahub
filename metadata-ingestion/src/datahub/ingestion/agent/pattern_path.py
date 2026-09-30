"""An AllowDenyPattern addressed by a dotted path from the top-level config.

A Filters(...) declaration may sit on a field of a nested ConfigModel --
Dataplex keeps its entry filters under `filter_config.entries` -- so the field
that filters a kind is named `filter_config.entries.pattern`. A path with no
dot is an ordinary top-level field, and each helper here then does exactly
what getattr / model_copy / validate_assignment did before.

Kept free of other agent imports so verdicts.py (a leaf module) can use it.
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

    An Optional block left out of a recipe (`block: Optional[Inner] = None`)
    still resolves its nested Filters(...) field, because introspection
    unwraps the Optional. That recipe is valid and filters nothing there, so
    callers read it as allow-all -- unlike a leaf that is not a pattern, which
    is a resolution bug. None when every block along the path is set, and
    also when a block is absent altogether: a path through a field the model
    does not have is a resolution bug too, left for require_pattern_at to
    raise on.
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
    """A shallow copy of `config` with the pattern at `path` replaced.

    Every block along the path is copied, so the recipe's parsed config is not
    reached through a shared sub-model -- a plain model_copy of the top level
    would share `filter_config` and let a later in-place validation of the leaf
    rewrite the original. Shallow on purpose, as filter_check's own comment
    explains: a deep copy clones cached token managers.
    """
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
    """Rerun the validators of the block that owns the leaf field.

    Only that block's: validate_assignment reruns the owning model's
    after-validators, not its parents'. No connector normalizes a nested
    pattern from a parent validator today; one that does would need this
    widened.
    """
    *parents, leaf = path.split(".")
    owner: BaseModel = config
    for segment in parents:
        child = getattr(owner, segment)
        if not isinstance(child, BaseModel):
            raise TypeError(f"'{segment}' in '{path}' is not a config block")
        owner = child
    type(owner).__pydantic_validator__.validate_assignment(owner, leaf, pattern)
