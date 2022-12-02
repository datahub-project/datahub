# Copied from confluent_kafka.schema_registry.avro.py
from json import loads

from confluent_kafka.schema_registry.schema_registry_client import (
    RegisteredSchema,
    Schema,
)


def _schema_loads(schema_str: str) -> Schema:
    """
    Instantiates a Schema instance from a declaration string

    Args:
        schema_str (str): Avro Schema declaration.

    .. _Schema declaration:
        https://avro.apache.org/docs/current/spec.html#schemas

    Returns:
        Schema: Schema instance

    """
    schema_str = schema_str.strip()

    # canonical form primitive declarations are not supported
    if schema_str[0] != "{" and schema_str[0] != "[":
        schema_str = '{"type":' + schema_str + "}"

    return Schema(schema_str, schema_type="AVRO")


class InternalSchemaRegistryClient(object):

    # TODO: Create a mapping of the schemas
    # schemas: dict = {
    #    "": _schema_loads(getMetadataChangeEventSchema()),
    # }

    def register_schema(self, subject_name: str, schema: Schema) -> None:
        raise NotImplementedError

    def lookup_schema(self, subject_name: str, schema: Schema) -> RegisteredSchema:
        """
        Returns ``schema`` registration information for ``subject``.

        Args:
            subject_name (str): Subject name the schema is registered under

            schema (Schema): Schema instance.

        Returns:
            RegisteredSchema: Subject registration information for this schema.
        """

        schema_id: int = int(
            subject_name.removeprefix(loads(schema.schema_str)["name"])
            .removeprefix("_v")
            .removesuffix("-value")
        )

        return RegisteredSchema(schema_id, schema, subject_name, schema_id)

    def get_latest_version(self, subject_name: str) -> None:
        raise NotImplementedError
