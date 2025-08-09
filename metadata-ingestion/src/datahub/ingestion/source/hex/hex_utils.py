import re

from datahub.utilities.base62 import b62encode

HEX_BASE62_ALPHABET = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"


def hex_uuid_to_base62(uuid: str) -> str:
    if not re.match(
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
        uuid,
    ):
        raise ValueError(f"Not a valid uuid: {repr(uuid)}")

    return b62encode(
        bytes.fromhex(uuid.replace("-", "")), alphabet=HEX_BASE62_ALPHABET
    ).decode("ascii")
