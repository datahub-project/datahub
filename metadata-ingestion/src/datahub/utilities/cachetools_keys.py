from typing import Any

import cachetools.keys


def self_methodkey(self: Any, *args: Any, **kwargs: Any) -> Any:
    # Keeps the id of self around
    return cachetools.keys.hashkey(id(self), *args, **kwargs)
