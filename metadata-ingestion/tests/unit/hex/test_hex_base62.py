import unittest

from datahub.ingestion.source.hex.hex_utils import hex_uuid_to_base62


class TestHexBase62(unittest.TestCase):
    def test_matches_known_ids(self):
        known_ids = [
            ("019814d7-a5f1-7008-b413-872974a7a308", "030UbtEpvL5rit3JLHGzSS"),
            ("01983d47-f48e-7006-b39a-4a68b169adf1", "030Z5O2bh7IIj0JGcCOiMj"),
            ("01985538-504f-7001-a0cd-6e89ba762eb5", "030bjewJpNKllTc1HdoGMX"),
            ("bb3713aa-ba61-41a6-bb0c-1c7723fa4abf", "5hGjaacY9tC1moArcFQpLz"),
            ("d68f06ed-2f6c-4018-a364-232fdd3555a7", "6WrijX4GInuHtbEMCShIvf"),
        ]
        for uuid, base62 in known_ids:
            assert hex_uuid_to_base62(uuid) == base62
