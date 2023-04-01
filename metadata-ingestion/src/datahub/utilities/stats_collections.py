from typing import Any, Callable, DefaultDict, Dict, Optional, TypeVar, Union

_KT = TypeVar("_KT")
_VT = TypeVar("_VT")


class TopKDict(DefaultDict[_KT, _VT]):
    """A structure that only prints the top K items from the dictionary. Not lossy."""

    def __init__(
        self,
        default_factory: Optional[Callable[[], _VT]] = None,
        *args,
        top_k: int = 10,
        **kwargs: Any,
    ) -> None:
        super().__init__(default_factory, *args, **kwargs)
        self.top_k = top_k

    def __repr__(self) -> str:
        return repr(self.as_obj())

    def __str__(self) -> str:
        return self.__repr__()

    def as_obj(self) -> Dict[_KT, _VT]:
        if len(self) <= self.top_k:
            return dict(self)
        else:
            trimmed_dict = dict(sorted(self.items(), key=lambda x: -x[1])[: self.top_k])
            trimmed_dict[f"... top {self.top_k} of total {len(self)} entries"] = ""
            return trimmed_dict


def int_top_k_dict() -> TopKDict[str, int]:
    return TopKDict(int)
