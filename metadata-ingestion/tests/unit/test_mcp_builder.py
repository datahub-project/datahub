import datahub.emitter.mcp_builder as builder
from datahub.emitter.mce_builder import datahub_guid


def test_guid_generator():
    key = builder.SchemaKey(
        database="test", schema="Test", platform="mysql", instance="some_mysql"
    )

    guid = key.guid()
    assert guid == "cc05baee5809b42df29c8e550c12b153"


def test_guid_generator_with_empty_instance():
    key = builder.SchemaKey(
        database="test", schema="Test", platform="mysql", instance=None
    )

    guid = key.guid()
    assert guid == "693ed953c7192bcf46f8b9db36d71c2b"


def test_guid_generators():
    key = builder.SchemaKey(
        database="test", schema="Test", platform="mysql", instance="some_mysql"
    )
    guid_datahub = datahub_guid(key.__dict__)

    guid = key.guid()
    assert guid == guid_datahub
