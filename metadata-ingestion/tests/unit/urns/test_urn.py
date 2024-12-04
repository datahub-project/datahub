import pytest

from datahub.metadata.urns import (
    CorpUserUrn,
    DashboardUrn,
    DataPlatformUrn,
    DatasetUrn,
    Urn,
)
from datahub.utilities.urns.error import InvalidUrnError

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


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


def test_invalid_urn() -> None:
    # Load invalid URNs from file
    with open("tests/unit/urns/invalid_urns.txt") as f:
        invalid_urns = [
            line.strip()
            for line in f.readlines()
            if line.strip() and not line.startswith("#")
        ]

    # Test each invalid URN
    for invalid_urn in invalid_urns:
        with pytest.raises(InvalidUrnError):
            print(f"Testing invalid URN: {invalid_urn}")
            Urn.from_string(invalid_urn)


def test_urn_colon() -> None:
    # Colon characters are valid in urns, and should not mess up parsing.

    urn = Urn.from_string(
        "urn:li:dashboard:(looker,dashboards.thelook::customer_lookup)"
    )
    assert isinstance(urn, DashboardUrn)

    assert DataPlatformUrn.from_string("urn:li:dataPlatform:abc:def")
    assert DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:abc:def,table_name,PROD)"
    )
    assert Urn.from_string("urn:li:corpuser:foo:bar@example.com")

    # I'm not sure why you'd ever want this, but technically it's a valid urn.
    urn = Urn.from_string("urn:li:corpuser::")
    assert isinstance(urn, CorpUserUrn)
    assert urn.username == ":"
    assert urn == CorpUserUrn(":")


def test_urn_coercion() -> None:
    urn = CorpUserUrn("foo␟bar")
    assert urn.urn() == "urn:li:corpuser:foo%E2%90%9Fbar"

    assert urn == Urn.from_string(urn.urn())


def test_incorrect_urn_type() -> None:
    urn = "urn:li:dataJob:(urn:li:dataFlow:(airflow,flow_id,prod),job_id)"

    assert Urn.from_string(urn).urn() == urn

    with pytest.raises(InvalidUrnError, match="Passed an urn of type dataJob"):
        CorpUserUrn.from_string(urn)


def test_urn_type_dispatch() -> None:
    urn = Urn.from_string("urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)")
    assert isinstance(urn, DatasetUrn)

    with pytest.raises(InvalidUrnError, match="Passed an urn of type corpuser"):
        DatasetUrn.from_string("urn:li:corpuser:foo")
