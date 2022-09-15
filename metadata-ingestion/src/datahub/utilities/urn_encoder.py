import urllib.parse
from typing import List

RESERVED_CHARS = [",", "(", ")"]


class UrnEncoder:
    @staticmethod
    def encode_string_array(arr: List[str]) -> List[str]:
        return [UrnEncoder.encode_string(s) for s in arr]

    @staticmethod
    def encode_string(s: str) -> str:
        return "".join([UrnEncoder.encode_char(c) for c in s])

    @staticmethod
    def encode_char(c: str) -> str:
        assert len(c) == 1, "Invalid input, Expected single character"
        return urllib.parse.quote(c) if c in RESERVED_CHARS else c
