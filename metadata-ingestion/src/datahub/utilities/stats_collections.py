from typing import Any, Dict, TypeVar, Union

T = TypeVar("T")
_KT = TypeVar("_KT")
_VT = TypeVar("_VT")


class TopKDict(Dict[_KT, _VT]):
    """A structure that only prints the top K items from the dictionary. Not lossy."""

    def __init__(self, top_k: int = 10) -> None:
        super().__init__()
        self.top_k = 10

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
            print(f"Dropping entries {sorted_tuples[11:]}")
            return trimmed_dict

        return big_dict

    def as_obj(self) -> Dict[Union[_KT, str], Union[_VT, str]]:
        base_dict: Dict[Union[_KT, str], Union[_VT, str]] = super().copy()  # type: ignore
        return self._trim_dictionary(base_dict)  # type: ignore
