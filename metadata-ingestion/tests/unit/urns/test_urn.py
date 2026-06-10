import logging
import pathlib
from typing import List

import pytest

import datahub.utilities.urns._urn_base
from datahub.metadata.urns import (
    CorpUserUrn,
    DataPlatformUrn,
    DatasetUrn,
    SchemaFieldUrn,
    TagUrn,
    Urn,
)
from datahub.testing.doctest import assert_doctest
from datahub.utilities.urns.error import InvalidUrnError

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")

_CURRENT_DIR = pathlib.Path(__file__).parent
logger = logging.getLogger(__name__)


def test_parse_urn() -> None:
    simple_urn_str = "urn:li:dataPlatform:abc"
    urn = Urn.create_from_string(simple_urn_str)
    assert urn.get_entity_id_as_string() == "abc"
    assert urn.get_entity_id() == ["abc"]
    assert urn.get_type() == "dataPlatform"
    assert urn.get_domain() == "li"
    assert urn.__str__() == simple_urn_str
    assert urn == Urn("dataPlatform", ["abc"])

    complex_urn_str = "urn:li:dataset:(urn:li:dataPlatform:abc,def,prod)"
    urn = Urn.create_from_string(complex_urn_str)
    assert urn.get_entity_id_as_string() == "(urn:li:dataPlatform:abc,def,prod)"
    assert urn.get_entity_id() == ["urn:li:dataPlatform:abc", "def", "prod"]
    assert urn.get_type() == "dataset"
    assert urn.__str__() == "urn:li:dataset:(urn:li:dataPlatform:abc,def,prod)"


def test_url_encode_urn() -> None:
    urn_with_slash: Urn = Urn.create_from_string(
        "urn:li:dataset:(urn:li:dataPlatform:abc,def/ghi,prod)"
    )
    assert (
        Urn.url_encode(str(urn_with_slash))
        == "urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Aabc%2Cdef%2Fghi%2Cprod%29"
    )


def test_urn_colon() -> None:
    # There's a bunch of other, simpler tests for special characters in the valid_urns test.

    # This test ensures that the type dispatch and fields work fine here.
    # I'm not sure why you'd ever want this, but technically it's a valid urn.

    urn = Urn.from_string("urn:li:corpuser::")
    assert isinstance(urn, CorpUserUrn)
    assert urn.username == ":"
    assert urn == CorpUserUrn(":")


def test_urn_coercion() -> None:
    urn = CorpUserUrn("foo␟bar")
    assert urn.urn() == "urn:li:corpuser:foo%E2%90%9Fbar"

    assert urn == Urn.from_string(urn.urn())


def test_urns_in_init() -> None:
    platform = DataPlatformUrn("abc")
    assert platform.urn() == "urn:li:dataPlatform:abc"
    assert platform == DataPlatformUrn(platform)
    assert platform == DataPlatformUrn(platform.urn())

    dataset_urn = DatasetUrn(platform, "def", "PROD")
    assert dataset_urn.urn() == "urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)"
    assert dataset_urn == DatasetUrn(platform.urn(), "def", "PROD")
    assert dataset_urn == DatasetUrn(platform.platform_name, "def", "PROD")

    with pytest.raises(
        InvalidUrnError, match="Expecting a DataPlatformUrn but got .*dataset.*"
    ):
        assert dataset_urn == DatasetUrn(dataset_urn, "def", "PROD")  # type: ignore

    schema_field = SchemaFieldUrn(dataset_urn, "foo")
    assert (
        schema_field.urn()
        == "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD),foo)"
    )


def test_urn_type_dispatch_1() -> None:
    urn = Urn.from_string("urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)")
    assert isinstance(urn, DatasetUrn)

    with pytest.raises(InvalidUrnError, match="Passed an urn of type corpuser"):
        DatasetUrn.from_string("urn:li:corpuser:foo")

    urn2 = DatasetUrn.from_string(urn)
    assert isinstance(urn2, DatasetUrn)
    assert urn2 == urn


def test_urn_type_dispatch_2() -> None:
    urn = "urn:li:dataJob:(urn:li:dataFlow:(airflow,flow_id,prod),job_id)"
    assert Urn.from_string(urn).urn() == urn

    with pytest.raises(InvalidUrnError, match="Passed an urn of type dataJob"):
        CorpUserUrn.from_string(urn)

    with pytest.raises(
        InvalidUrnError, match="Expecting a CorpUserUrn but got .*dataJob.*"
    ):
        CorpUserUrn(urn)  # type: ignore


def test_urn_type_dispatch_3() -> None:
    # Creating a "generic" Urn.
    urn = Urn("dataset", ["urn:li:dataPlatform:abc", "def", "PROD"])
    assert isinstance(urn, Urn)

    urn2 = DatasetUrn.from_string(urn)
    assert isinstance(urn2, DatasetUrn)
    assert urn2 == urn

    with pytest.raises(
        InvalidUrnError,
        match="Passed an urn of type dataset to the from_string method of CorpUserUrn",
    ):
        CorpUserUrn.from_string(urn)


def test_urn_type_dispatch_4() -> None:
    # A generic urn of a new entity type.
    urn_str = "urn:li:new_entity_type:(abc,def)"

    urn = Urn.from_string(urn_str)
    assert type(urn) is Urn
    assert urn == Urn("new_entity_type", ["abc", "def"])
    assert urn.urn() == urn_str

    urn2 = Urn.from_string(urn)
    assert type(urn2) is Urn
    assert urn2 == urn
    assert urn2.urn() == urn_str


def test_urn_from_urn_simple() -> None:
    # This capability is also tested by a bunch of other tests above.

    tag_str = "urn:li:tag:legacy"
    tag = TagUrn.from_string(tag_str)
    assert tag_str == tag.urn()
    assert tag.name == "legacy"
    assert tag == TagUrn(tag)
    assert tag == TagUrn(tag.urn())


def test_urn_from_urn_tricky() -> None:
    tag_str = "urn:li:tag:urn:li:tag:legacy"
    tag = TagUrn(tag_str)
    assert tag.urn() == tag_str
    assert tag.name == "urn:li:tag:legacy"


def test_urn_doctest() -> None:
    assert_doctest(datahub.utilities.urns._urn_base)


def _load_urns(file_name: pathlib.Path) -> List[str]:
    urns = [
        line.strip()
        for line in file_name.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]
    assert len(urns) > 0, f"No urns found in {file_name}"
    return urns


def test_valid_urns() -> None:
    valid_urns_file = _CURRENT_DIR / "valid_urns.txt"
    valid_urns = _load_urns(valid_urns_file)

    for valid_urn in valid_urns:
        logger.info(f"Testing valid URN: {valid_urn}")
        parsed_urn = Urn.from_string(valid_urn)
        assert parsed_urn.urn() == valid_urn


def test_invalid_urns() -> None:
    invalid_urns_file = _CURRENT_DIR / "invalid_urns.txt"
    invalid_urns = _load_urns(invalid_urns_file)

    # Test each invalid URN
    for invalid_urn in invalid_urns:
        with pytest.raises(InvalidUrnError):
            logger.info(f"Testing invalid URN: {invalid_urn}")
            Urn.from_string(invalid_urn)
