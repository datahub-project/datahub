from typing import Any, Callable, DefaultDict, Dict, Optional, TypeVar, Union

T = TypeVar("T")
_KT = TypeVar("_KT")
_VT = TypeVar("_VT")


class TopKDict(DefaultDict[_KT, _VT]):
    """A structure that only prints the top K items from the dictionary. Not lossy."""

    def __init__(
        self,
        default_factory: Optional[Callable[[], _VT]] = None,
        *,
        top_k: int = 10,
        **kwargs: Any,
    ) -> None:
        super().__init__(default_factory, **kwargs)
        self.top_k = top_k

    def __repr__(self) -> str:
        return repr(self.as_obj())

    def __str__(self) -> str:
        return self.__repr__()

    @staticmethod
    def _trim_dictionary(big_dict: Dict[str, Any]) -> Dict[str, Any]:
        if big_dict is not None and len(big_dict) > 10:
            dict_as_tuples = [(k, v) for k, v in big_dict.items()]
            sorted_tuples = sorted(dict_as_tuples, key=lambda x: x[1], reverse=True)
            dict_as_tuples = sorted_tuples[:10]
            trimmed_dict = {k: v for k, v in dict_as_tuples}
            trimmed_dict[f"... top(10) of total {len(big_dict)} entries"] = ""
            return trimmed_dict

        return big_dict

    def as_obj(self) -> Dict[Union[_KT, str], Union[_VT, str]]:
        base_dict: Dict[Union[_KT, str], Union[_VT, str]] = super().copy()  # type: ignore
        return self._trim_dictionary(base_dict)  # type: ignore


def int_top_k_dict() -> TopKDict[str, int]:
    return TopKDict(int)
