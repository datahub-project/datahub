import pytest

from datahub.utilities.urn_encoder import UrnEncoder


@pytest.mark.parametrize(
    "name",
    [
        "test-database.test-schema.test-table",
        "test_database.test$schema.test+table",
        "test&database.%testschema.test*table",
    ],
)
def test_encode_string_without_reserved_chars_no_change(name):
    assert UrnEncoder.encode_string(name) == name


@pytest.mark.parametrize(
    "name",
    [
        "test-database,test-schema,test-table",
        "test_database,(test$schema),test+table",
        "test&database.test(schema.test*table",
    ],
)
def test_encode_string_with_reserved_chars(name):
    assert UrnEncoder.encode_string(name) == name.replace(",", "%2C").replace(
        "(", "%28"
    ).replace(")", "%29")
