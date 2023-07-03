import datahub.emitter.mcp_builder as builder
from datahub.emitter.mce_builder import datahub_guid


def test_guid_generator():
    key = builder.SchemaKey(
        database="test", schema="Test", platform="mysql", instance="TestInstance"
    )

    guid = key.guid()
    assert guid == "f096b3799fc86a3e5d5d0c083eb1f2a4"


def test_guid_generator_with_empty_instance():
    key = builder.SchemaKey(
        database="test",
        schema="Test",
        platform="mysql",
        instance=None,
    )

    guid = key.guid()
    assert guid == "693ed953c7192bcf46f8b9db36d71c2b"


def test_guid_generator_with_instance():
    key = builder.SchemaKey(
        database="test",
        schema="Test",
        platform="mysql",
        instance="TestInstance",
        backcompat_env_as_instance=True,
    )
    guid = key.guid()
    assert guid == "f096b3799fc86a3e5d5d0c083eb1f2a4"


def test_guid_generator_with_instance_and_env():
    key = builder.SchemaKey(
        database="test",
        schema="Test",
        platform="mysql",
        instance="TestInstance",
        env="PROD",
        backcompat_env_as_instance=True,
    )
    guid = key.guid()
    assert guid == "f096b3799fc86a3e5d5d0c083eb1f2a4"

    assert key.property_dict() == {
        "database": "test",
        "schema": "Test",
        "platform": "mysql",
        "instance": "TestInstance",
        "env": "PROD",
    }


def test_guid_generator_with_env():
    key = builder.SchemaKey(
        database="test",
        schema="Test",
        platform="mysql",
        instance=None,
        env="TestInstance",
        backcompat_env_as_instance=True,
    )
    guid = key.guid()
    assert guid == "f096b3799fc86a3e5d5d0c083eb1f2a4"

    assert key.property_dict() == {
        "database": "test",
        "schema": "Test",
        "platform": "mysql",
        "env": "TestInstance",
    }


def test_guid_generators():
    key = builder.SchemaKey(
        database="test", schema="Test", platform="mysql", instance="TestInstance"
    )
    guid_datahub = datahub_guid(key.dict(by_alias=True))

    guid = key.guid()
    assert guid == guid_datahub
