import datahub.emitter.mcp_builder as builder
from datahub.metadata.schema_classes import StatusClass, TelemetryClientIdClass


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
    guid_datahub = key.guid()

    guid = key.guid()
    assert guid == guid_datahub


def _assert_eq(a: builder.ContainerKey, b: builder.ContainerKey) -> None:
    assert a == b
    assert a.__class__ is b.__class__
    assert a.guid() == b.guid()
    assert a.property_dict() == b.property_dict()


def test_parent_key() -> None:
    schema_key = builder.SchemaKey(
        database="test",
        schema="Test",
        platform="mysql",
        instance="TestInstance",
        env="DEV",
    )
    db_key = schema_key.parent_key()
    assert db_key is not None
    _assert_eq(
        db_key,
        builder.DatabaseKey(
            database="test",
            platform="mysql",
            instance="TestInstance",
            env="DEV",
        ),
    )

    assert db_key.parent_key() is None


def test_parent_key_with_backcompat_env_as_instance() -> None:
    schema_key = builder.SchemaKey(
        database="test",
        schema="Test",
        platform="mysql",
        instance=None,
        env="PROD",
        backcompat_env_as_instance=True,
    )
    db_key = schema_key.parent_key()
    assert db_key is not None
    _assert_eq(
        db_key,
        builder.DatabaseKey(
            database="test",
            platform="mysql",
            instance=None,
            env="PROD",
            backcompat_env_as_instance=True,
        ),
    )


def test_parent_key_on_container_key() -> None:
    # In general, people shouldn't be calling parent_key() on ContainerKey directly.
    # But just in case, we should make sure it works.
    container_key = builder.ContainerKey(
        platform="bigquery",
        name="test",
        env="DEV",
    )
    assert container_key.parent_key() is None


def test_entity_supports_aspect():
    assert builder.entity_supports_aspect("dataset", StatusClass)
    assert not builder.entity_supports_aspect("telemetry", StatusClass)

    assert not builder.entity_supports_aspect("dataset", TelemetryClientIdClass)
    assert builder.entity_supports_aspect("telemetry", TelemetryClientIdClass)
