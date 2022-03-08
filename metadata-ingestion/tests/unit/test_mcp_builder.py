import datahub.emitter.mcp_builder as builder
from datahub.emitter.mce_builder import datahub_guid


def test_guid_generator():
    key = builder.SchemaKey(
        database="test", schema="Test", platform="mysql", instance="PROD"
    )

    guid = key.guid()
    assert guid == "f5268c71373b9100d50c1299861cfb3f"


def test_guid_generator_with_empty_instance():
    key = builder.SchemaKey(
        database="test", schema="Test", platform="mysql", instance=None
    )

    guid = key.guid()
    assert guid == "693ed953c7192bcf46f8b9db36d71c2b"


def test_guid_generators():
    key = builder.SchemaKey(
        database="test", schema="Test", platform="mysql", instance="PROD"
    )
    guid_datahub = datahub_guid(key.__dict__)

    guid = key.guid()
    assert guid == guid_datahub
