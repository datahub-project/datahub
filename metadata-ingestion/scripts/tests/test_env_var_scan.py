"""Tests for the scanner that backs the "nothing undocumented ships" guarantee.

The completeness test runs the scanner over the real source tree, so a read form it does not
recognise shows up as nothing at all -- the test still passes and the variable ships
undocumented. These check the recognition itself, against a sample holding every form.
"""

import pathlib
import sys
import textwrap

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from env_var_scan import ScanResult, scan_tree

SAMPLE = """
import os
from os import environ, getenv

from datahub.cli.env_utils import get_boolean_env_variable

NAME_IN_CONST = "VIA_MODULE_CONSTANT"


def reads() -> None:
    os.getenv("PLAIN_GETENV")
    os.environ.get("ENVIRON_GET")
    os.environ["ENVIRON_SUBSCRIPT"]
    getenv("BARE_GETENV")
    environ.get("BARE_ENVIRON_GET")
    environ["BARE_ENVIRON_SUBSCRIPT"]
    os.getenv(NAME_IN_CONST)
    get_boolean_env_variable("VIA_HELPER", False)
    os.environ.setdefault("VIA_SETDEFAULT", "x")
    os.environ.pop("VIA_POP", None)
    if "VIA_MEMBERSHIP" in os.environ:
        pass


def writes() -> None:
    os.environ["WRITE_ONLY"] = "x"
"""

EXPECTED_READS = {
    "PLAIN_GETENV",
    "ENVIRON_GET",
    "ENVIRON_SUBSCRIPT",
    "BARE_GETENV",
    "BARE_ENVIRON_GET",
    "BARE_ENVIRON_SUBSCRIPT",
    "VIA_MODULE_CONSTANT",
    "VIA_HELPER",
    "VIA_SETDEFAULT",
    "VIA_POP",
    "VIA_MEMBERSHIP",
}


@pytest.fixture(scope="module")
def sample_scan(tmp_path_factory: pytest.TempPathFactory) -> ScanResult:
    root = tmp_path_factory.mktemp("pkg") / "datahub"
    root.mkdir()
    (root / "sample.py").write_text(textwrap.dedent(SAMPLE))
    return scan_tree(root)


@pytest.mark.parametrize("name", sorted(EXPECTED_READS))
def test_every_read_form_is_recognised(sample_scan: ScanResult, name: str) -> None:
    """None of these names carries a DataHub prefix, so the prefix fallback cannot save them."""
    assert name in sample_scan.reads, (
        f"{name} is read in the sample but the scanner did not see it. A variable read this "
        "way would ship undeclared with the completeness test still passing."
    )


def test_a_write_is_not_counted_as_a_read(sample_scan: ScanResult) -> None:
    """Setting a variable for a subprocess is not the SDK reading its own configuration."""
    assert "WRITE_ONLY" not in sample_scan.reads
    assert "WRITE_ONLY" in sample_scan.writes


def test_unparseable_file_is_not_skipped_silently(tmp_path: pathlib.Path) -> None:
    """A file the scanner cannot read is a file whose reads nobody is checking."""
    root = tmp_path / "datahub"
    root.mkdir()
    (root / "broken.py").write_text("def (:\n")
    with pytest.raises(RuntimeError, match="Could not parse"):
        scan_tree(root)
