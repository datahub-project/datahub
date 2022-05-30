import unittest

from datahub.utilities.urns.error import InvalidUrnError
from datahub.utilities.urns.notebook_urn import NotebookUrn


class TestNotebookUrn(unittest.TestCase):
    def test_parse_urn(self) -> None:
        notebook_urn_str = "urn:li:notebook:(querybook,123)"
        notebook_urn = NotebookUrn.create_from_string(notebook_urn_str)
        assert notebook_urn.get_platform_id() == "querybook"
        assert notebook_urn.get_notebook_id() == "123"
        assert str(notebook_urn) == notebook_urn_str

        assert notebook_urn == NotebookUrn("notebook", ["querybook", "123"])

    def test_invalid_urn(self) -> None:
        with self.assertRaises(InvalidUrnError):
            NotebookUrn.create_from_string(
                "urn:li:abc:(urn:li:dataPlatform:abc,def,prod)"
            )

        with self.assertRaises(InvalidUrnError):
            NotebookUrn.create_from_string("urn:li:notebook:(part1,part2,part3)")
