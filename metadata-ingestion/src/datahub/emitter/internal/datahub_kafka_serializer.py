# Copied from confluent_kafka.schema_registry.avro.py
from io import BytesIO
from json import loads
from struct import pack

from fastavro import (parse_schema, schemaless_writer)
from confluent_kafka.serialization import (Serializer)
from confluent_kafka.schema_registry import (Schema,
                                             topic_subject_name_strategy)

# From confluent_kafka.schema_registry protected field
_MAGIC_BYTE = 0


class _ContextStringIO(BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.

    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


def _schema_loads(schema_str):
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
        schema_str = '{"type":' + schema_str + '}'

    return Schema(schema_str, schema_type='AVRO')


class DataHubAvroSerializer(Serializer):
    """
    AvroSerializer serializes objects in the Confluent Schema Registry binary
    format for Avro.


    AvroSerializer configuration properties:

    +---------------------------+----------+--------------------------------------------------+
    | Property Name             | Type     | Description                                      |
    +===========================+==========+==================================================+
    |                           |          | Registers schemas automatically if not           |
    | ``auto.register.schemas`` | bool     | previously associated with a particular subject. |
    |                           |          | Defaults to True.                                |
    +---------------------------+----------+--------------------------------------------------+
    |                           |          | Whether to use the latest subject version for    |
    | ``use.latest.version``    | bool     | serialization.                                   |
    |                           |          | WARNING: There is no check that the latest       |
    |                           |          | schema is backwards compatible with the object   |
    |                           |          | being serialized.                                |
    |                           |          | Defaults to False.                               |
    +-------------------------------------+----------+----------------------------------------+
    |                           |          | Callable(SerializationContext, str) -> str       |
    |                           |          |                                                  |
    | ``subject.name.strategy`` | callable | Instructs the AvroSerializer on how to construct |
    |                           |          | Schema Registry subject names.                   |
    |                           |          | Defaults to topic_subject_name_strategy.         |
    +---------------------------+----------+--------------------------------------------------+

    Schemas are registered to namespaces known as Subjects which define how a
    schema may evolve over time. By default the subject name is formed by
    concatenating the topic name with the message field separated by a hyphen.

    i.e. {topic name}-{message field}

    Alternative naming strategies may be configured with the property
    ``subject.name.strategy``.

    Supported subject name strategies:

    +--------------------------------------+------------------------------+
    | Subject Name Strategy                | Output Format                |
    +======================================+==============================+
    | topic_subject_name_strategy(default) | {topic name}-{message field} |
    +--------------------------------------+------------------------------+
    | topic_record_subject_name_strategy   | {topic name}-{record name}   |
    +--------------------------------------+------------------------------+
    | record_subject_name_strategy         | {record name}                |
    +--------------------------------------+------------------------------+

    See `Subject name strategy <https://docs.confluent.io/current/schema-registry/serializer-formatter.html#subject-name-strategy>`_ for additional details.

    Note:
        Prior to serialization all ``Complex Types`` must first be converted to
        a dict instance. This may handled manually prior to calling
        :py:func:`SerializingProducer.produce()` or by registering a `to_dict`
        callable with the AvroSerializer.

        See ``avro_producer.py`` in the examples directory for example usage.

    Note:
       Tuple notation can be used to determine which branch of an ambiguous union to take.

       See `fastavro notation <https://fastavro.readthedocs.io/en/latest/writer.html#using-the-tuple-notation-to-specify-which-branch-of-a-union-to-take>`_

    Args:
        schema_registry_client (SchemaRegistryClient): Schema Registry client instance.

        schema_str (str): Avro `Schema Declaration. <https://avro.apache.org/docs/current/spec.html#schemas>`_

        to_dict (callable, optional): Callable(object, SerializationContext) -> dict. Converts object to a dict.

        conf (dict): AvroSerializer configuration.

    """  # noqa: E501
    __slots__ = ['_hash', '_auto_register', '_use_latest_version', '_known_subjects', '_parsed_schema',
                 '_registry', '_schema', '_schema_id', '_schema_name',
                 '_subject_name_func', '_to_dict']

    # default configuration
    _default_conf = {'auto.register.schemas': True,
                     'use.latest.version': False,
                     'subject.name.strategy': topic_subject_name_strategy}

    def __init__(self, schema_str, schema_registry_client=None,
                 to_dict=None, conf=None):
        self._registry = schema_registry_client
        self._schema_id = None
        # Avoid calling registry if schema is known to be registered
        self._known_subjects = set()

        if to_dict is not None and not callable(to_dict):
            raise ValueError("to_dict must be callable with the signature"
                             " to_dict(object, SerializationContext)->dict")

        self._to_dict = to_dict

        # handle configuration
        conf_copy = self._default_conf.copy()
        if conf is not None:
            conf_copy.update(conf)

        self._auto_register = conf_copy.pop('auto.register.schemas')
        if not isinstance(self._auto_register, bool):
            raise ValueError("auto.register.schemas must be a boolean value")

        self._use_latest_version = conf_copy.pop('use.latest.version')
        if not isinstance(self._use_latest_version, bool):
            raise ValueError("use.latest.version must be a boolean value")
        if self._use_latest_version and self._auto_register:
            raise ValueError("cannot enable both use.latest.version and auto.register.schemas")

        self._subject_name_func = conf_copy.pop('subject.name.strategy')
        if not callable(self._subject_name_func):
            raise ValueError("subject.name.strategy must be callable")

        if len(conf_copy) > 0:
            raise ValueError("Unrecognized properties: {}"
                             .format(", ".join(conf_copy.keys())))

        # convert schema_str to Schema instance
        schema = _schema_loads(schema_str)
        schema_dict = loads(schema.schema_str)
        parsed_schema = parse_schema(schema_dict)

        if isinstance(parsed_schema, list):
            # if parsed_schema is a list, we have an Avro union and there
            # is no valid schema name. This is fine because the only use of
            # schema_name is for supplying the subject name to the registry
            # and union types should use topic_subject_name_strategy, which
            # just discards the schema name anyway
            schema_name = None
        else:
            # The Avro spec states primitives have a name equal to their type
            # i.e. {"type": "string"} has a name of string.
            # This function does not comply.
            # https://github.com/fastavro/fastavro/issues/415
            schema_name = parsed_schema.get("name", schema_dict["type"])

        self._schema = schema
        self._schema_name = schema_name
        self._parsed_schema = parsed_schema

    def __call__(self, obj, ctx):
        """
        Serializes an object to the Confluent Schema Registry's Avro binary
        format.

        Args:
            obj (object): object instance to serializes.

            ctx (SerializationContext): Metadata pertaining to the serialization operation.

        Note:
            None objects are represented as Kafka Null.

        Raises:
            SerializerError: if any error occurs serializing obj

        Returns:
            bytes: Confluent Schema Registry formatted Avro bytes

        """
        if obj is None:
            return None

        subject = self._subject_name_func(ctx, self._schema_name)

        if subject not in self._known_subjects:
            if self._use_latest_version:
                latest_schema = self._registry.get_latest_version(subject)
                self._schema_id = latest_schema.schema_id

            else:
                # Check to ensure this schema has been registered under subject_name.
                if self._auto_register:
                    # The schema name will always be the same. We can't however register
                    # a schema without a subject so we set the schema_id here to handle
                    # the initial registration.
                    self._schema_id = self._registry.register_schema(subject,
                                                                     self._schema)
                else:
                    registered_schema = self._registry.lookup_schema(subject,
                                                                     self._schema)
                    self._schema_id = registered_schema.schema_id
            self._known_subjects.add(subject)

        if self._to_dict is not None:
            value = self._to_dict(obj, ctx)
        else:
            value = obj

        with _ContextStringIO() as fo:
            # Write the magic byte and schema ID in network byte order (big endian)
            fo.write(pack('>bI', _MAGIC_BYTE, self._schema_id))
            # write the record to the rest of the buffer
            schemaless_writer(fo, self._parsed_schema, value)

            return fo.getvalue()


class InternalSchemaRegistryClient(object):
    def register_schema(self, subject_name, schema):
        raise NotImplementedError

    def lookup_schema(self, subject_name, schema):
        """
        Returns ``schema`` registration information for ``subject``.

        Args:
            subject_name (str): Subject name the schema is registered under

            schema (Schema): Schema instance.

        Returns:
            RegisteredSchema: Subject registration information for this schema.
        """

        schema_id: int = int(subject_name.removeprefix(loads(schema.schema_str)['name'])
                             .removeprefix("_v")
                             .removesuffix("-value"))

        return RegisteredSchema(schema_id, schema, subject_name, schema_id)

    def get_latest_version(self, subject_name):
        """
        Retrieves latest registered version for subject
    
        Args:
            subject_name (str): Subject name.
    
        Returns:
            RegisteredSchema: Registration information for this version.
    
        Raises:
            SchemaRegistryError: if the version can't be found or is invalid.
    
        See Also:
            `GET Subject Version API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)>`_
    
        """  # noqa: E501
        response = self._rest_client.get('subjects/{}/versions/{}'
                                         .format(_urlencode(subject_name),
                                                 'latest'))

        schema_type = response.get('schemaType', 'AVRO')
        return RegisteredSchema(schema_id=response['id'],
                                schema=Schema(response['schema'],
                                              schema_type,
                                              response.get('references', [])),
                                subject=response['subject'],
                                version=response['version'])


class RegisteredSchema(object):
    """
    Schema registration information.

    Represents a  Schema registered with a subject. Use this class when you need
    a specific version of a subject such as forming a SchemaReference.

    Args:
        schema_id (int): Registered Schema id

        schema (Schema): Registered Schema

        subject (str): Subject this schema is registered under

        version (int): Version of this subject this schema is registered to

    """

    def __init__(self, schema_id, schema, subject, version):
        self.schema_id = schema_id
        self.schema = schema
        self.subject = subject
        self.version = version
