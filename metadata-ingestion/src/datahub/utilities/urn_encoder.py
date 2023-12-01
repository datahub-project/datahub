import urllib.parse
from typing import List

# NOTE: Frontend relies on encoding these three characters. Specifically, we decode and encode schema fields for column level lineage.
# If this changes, make appropriate changes to datahub-web-react/src/app/lineage/utils/columnLineageUtils.ts
RESERVED_CHARS = {",", "(", ")"}
RESERVED_CHARS_EXTENDED = RESERVED_CHARS.union({"%"})


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

    @staticmethod
    def contains_reserved_char(value: str) -> bool:
        return bool(set(value).intersection(RESERVED_CHARS))

    @staticmethod
    def contains_extended_reserved_char(value: str) -> bool:
        return bool(set(value).intersection(RESERVED_CHARS_EXTENDED))
