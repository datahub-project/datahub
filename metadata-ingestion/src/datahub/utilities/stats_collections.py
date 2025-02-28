from typing import Any, Callable, DefaultDict, Dict, Optional, TypeVar, Union

from typing_extensions import Protocol

_CT = TypeVar("_CT")


class Comparable(Protocol):
    def __lt__(self: _CT, other: _CT) -> bool:
        pass


_KT = TypeVar("_KT")
_VT = TypeVar("_VT", bound=Comparable)


class TopKDict(DefaultDict[_KT, _VT]):
    """A structure that only prints the top K items from the dictionary. Not lossy."""

    def __init__(
        self,
        default_factory: Optional[Callable[[], _VT]] = None,
        *args: Any,
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
            try:
                trimmed_dict = dict(
                    sorted(self.items(), key=lambda x: x[1], reverse=True)[: self.top_k]
                )
            except TypeError:
                trimmed_dict = dict(list(self.items())[: self.top_k])

            try:
                total_value: Union[_VT, str] = sum(trimmed_dict.values())  # type: ignore
            except Exception:
                total_value = ""
            trimmed_dict[f"... top {self.top_k} of total {len(self)} entries"] = (  # type: ignore
                total_value  # type: ignore
            )
            return trimmed_dict


def int_top_k_dict() -> TopKDict[str, int]:
    return TopKDict(int)
