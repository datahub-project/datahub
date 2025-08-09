from datahub.utilities.base62 import b62encode


def test_base62():
    numbers_upper_lower = (
        b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    )
    numbers_lower_upper = (
        b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    )

    known_ids_and_alphabets = [
        (
            "019814d7-a5f1-7008-b413-872974a7a308",
            b"030UbtEpvL5rit3JLHGzSS",
            numbers_upper_lower,
        ),
        (
            "01983d47-f48e-7006-b39a-4a68b169adf1",
            b"030Z5O2bh7IIj0JGcCOiMj",
            numbers_upper_lower,
        ),
        (
            "01985538-504f-7001-a0cd-6e89ba762eb5",
            b"030bjewJpNKllTc1HdoGMX",
            numbers_upper_lower,
        ),
        (
            "bb3713aa-ba61-41a6-bb0c-1c7723fa4abf",
            b"5hGjaacY9tC1moArcFQpLz",
            numbers_upper_lower,
        ),
        (
            "d68f06ed-2f6c-4018-a364-232fdd3555a7",
            b"6WrijX4GInuHtbEMCShIvf",
            numbers_upper_lower,
        ),
        (
            "e2bf1f10-3d64-4b6c-b3ed-8d4dd080e833",
            b"6TRBJrvQgDM7kouANhuBrl",
            numbers_lower_upper,
        ),
        (
            "bfd8109a-4110-41ab-997a-39c6c29c3a36",
            b"5Q0h8mn6tocobC9GjGNixU",
            numbers_lower_upper,
        ),
        (
            "063b784e-2c5b-4d3a-bcfd-632385d1fd30",
            b"0bL8aE2ZIB70Dh1FX2OXCw",
            numbers_lower_upper,
        ),
    ]
    for uuid, base62, alphabet in known_ids_and_alphabets:
        uuid_as_bytes = bytes.fromhex(uuid.replace("-", ""))
        assert b62encode(uuid_as_bytes, alphabet=alphabet) == base62
