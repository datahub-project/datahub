import math


def b62encode(v: bytes, *, alphabet: bytes) -> bytes:
    # Encodes the given bytes using Base62 and return the encoded bytes.
    # Since Base62 does not have a standard alphabet, require the caller
    # to pass in an alphabet of exactly 62 characters.
    # This function deals solely with bytes objects in order to more
    # closely match the standard library's base64.b64encode function.

    assert len(alphabet) == 62

    acc = 0
    for i, c in enumerate(v):
        acc += pow(256, len(v) - i - 1) * c

    result = b""
    while acc:
        acc, i = divmod(acc, len(alphabet))
        result = alphabet[i : i + 1] + result

    pad = math.ceil(len(v) * math.log(256, 62)) - len(result)
    return alphabet[0:1] * pad + result
