import unittest

import pytest

from datahub.utilities.urns.notebook_urn import NotebookUrn


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
class TestNotebookUrn(unittest.TestCase):
    def test_parse_urn(self) -> None:
        notebook_urn_str = "urn:li:notebook:(querybook,123)"
        notebook_urn = NotebookUrn.create_from_string(notebook_urn_str)
        assert notebook_urn.get_platform_id() == "querybook"
        assert notebook_urn.get_notebook_id() == "123"
        assert str(notebook_urn) == notebook_urn_str

        assert notebook_urn == NotebookUrn("querybook", "123")
