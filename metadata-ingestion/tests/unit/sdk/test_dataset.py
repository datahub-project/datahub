import pytest

from datahub.sdk._shared import HasUrn
from datahub.sdk.dataset import Dataset


def test_dataset() -> None:
    d = Dataset(
        platform="bigquery",
        name="test",
        subtype="Table",
        schema=[("field1", "string"), ("field2", "int64")],
    )

    assert isinstance(d, HasUrn)
    assert d.urn.urn() == "urn:li:dataset:(urn:li:dataPlatform:bigquery,test,PROD)"

    assert d.subtype == "Table"

    print(d.schema)

    with pytest.raises(AttributeError):
        # TODO: make this throw a nicer error e.g. reference set_owners
        d.owners = []  # type: ignore

    d.set_owners(["my_user", "other_user", ("third_user", "BUSINESS_OWNER")])
    print(d.owners)

    with pytest.raises(AttributeError):
        # ensure that all the slots are set, and so no extra attributes are allowed
        d._extra_field = {}  # type: ignore
