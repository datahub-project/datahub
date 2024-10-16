import pathlib
from typing import Union

import lkml
import lkml.simple
import lkml.tree

# Patch lkml to support the manifest.lkml files.
# We have to patch both locations because lkml uses a immutable tuple
# instead of a list for this type.
lkml.simple.PLURAL_KEYS = (
    *lkml.simple.PLURAL_KEYS,
    "local_dependency",
    "remote_dependency",
    "constant",
    "override_constant",
)
lkml.tree.PLURAL_KEYS = lkml.simple.PLURAL_KEYS


def load_lkml(path: Union[str, pathlib.Path]) -> dict:
    """Loads a LookML file from disk and returns a dictionary."""

    # Using this method instead of lkml.load directly ensures
    # that our patches to lkml are applied.

    with open(path) as file:
        return lkml.load(file)
