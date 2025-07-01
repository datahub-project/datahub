import json
import logging
from typing import Dict, Optional, Type, TypeVar, Union

from avrogen.dict_wrapper import DictWrapper
from pydantic import BaseModel

import datahub.metadata.schema_classes as models
from datahub.metadata.schema_classes import __SCHEMA_TYPES as SCHEMA_TYPES

logger = logging.getLogger(__name__)

_REMAPPED_SCHEMA_TYPES = {
    k.replace("pegasus2avro.", ""): v for k, v in SCHEMA_TYPES.items()
}
T = TypeVar("T", bound=BaseModel)


class SerializedResourceValue(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    content_type: str
    blob: bytes
    schema_type: Optional[str] = None
    schema_ref: Optional[str] = None

    def as_raw_json(self) -> Optional[Dict]:
        """
        Parse the blob into a Python object based on the schema type and schema
        ref.
        If the schema type is JSON, the blob is parsed into a Python dict.
        If a schema ref is provided, the blob is parsed into a Python object
        assuming the schema ref is a Python class that can be instantiated using
        the parsed dict.
        If the schema type is PEGASUS, the blob is parsed into a DictWrapper
        object using the schema ref.
        """
        if (
            not self.schema_type
            or self.schema_type == models.SerializedValueSchemaTypeClass.JSON
        ):
            # default to JSON parsing
            json_string = self.blob.decode("utf-8")
            object_dict = json.loads(json_string)
            # TODO: Add support for schema ref
            return object_dict
        elif self.schema_type == models.SerializedValueSchemaTypeClass.PEGASUS:
            json_string = self.blob.decode("utf-8")
            object_dict = json.loads(json_string)
            return object_dict
        else:
            logger.warning(
                f"Unsupported schema type {self.schema_type} for parsing value"
            )
            raise ValueError(
                f"Unsupported schema type {self.schema_type} for parsing value"
            )

    def as_pegasus_object(self) -> DictWrapper:
        """
        Parse the blob into a Pegasus-defined Python object based on the schema type and schema
        ref.
        If the schema type is JSON, the blob is parsed into a Python dict.
        If a schema ref is provided, the blob is parsed into a Python object
        assuming the schema ref is a Python class that can be instantiated using
        the parsed dict.
        If the schema type is PEGASUS, the blob is parsed into a DictWrapper
        object using the schema ref.
        """
        assert (
            self.schema_type
            and self.schema_type == models.SerializedValueSchemaTypeClass.PEGASUS
        )
        assert self.schema_ref
        object_dict = self.as_raw_json()
        model_type = _REMAPPED_SCHEMA_TYPES.get(self.schema_ref)
        if model_type:
            assert issubclass(model_type, DictWrapper)
            return model_type.from_obj(object_dict or {})
        else:
            raise ValueError(
                f"Could not find schema ref {self.schema_ref} for parsing value"
            )

    def as_pydantic_object(
        self, model_type: Type[T], validate_schema_ref: bool = False
    ) -> T:
        """
        Parse the blob into a Pydantic-defined Python object based on the schema type and schema
        ref.
        If the schema type is JSON, the blob is parsed into a Python dict.
        If a schema ref is provided, the blob is parsed into a Python object
        assuming the schema ref is a Python class that can be instantiated using
        the parsed dict.
        If the schema type is PEGASUS, the blob is parsed into a DictWrapper
        object using the schema ref.
        """
        assert (
            self.schema_type
            and self.schema_type == models.SerializedValueSchemaTypeClass.JSON
        )
        if validate_schema_ref:
            assert self.schema_ref
            assert self.schema_ref == model_type.__name__
        object_dict = self.as_raw_json()
        return model_type.parse_obj(object_dict)

    @classmethod
    def from_resource_value(
        cls, resource_value: models.SerializedValueClass
    ) -> "SerializedResourceValue":
        return cls(
            content_type=resource_value.contentType,
            blob=resource_value.blob,
            schema_type=resource_value.schemaType,
            schema_ref=resource_value.schemaRef,
        )

    @classmethod
    def create(
        cls, object: Union[DictWrapper, BaseModel, Dict]
    ) -> "SerializedResourceValue":
        if isinstance(object, DictWrapper):
            return SerializedResourceValue(
                content_type=models.SerializedValueContentTypeClass.JSON,
                blob=json.dumps(object.to_obj()).encode("utf-8"),
                schema_type=models.SerializedValueSchemaTypeClass.PEGASUS,
                schema_ref=object.RECORD_SCHEMA.fullname.replace("pegasus2avro.", ""),
            )
        elif isinstance(object, BaseModel):
            return SerializedResourceValue(
                content_type=models.SerializedValueContentTypeClass.JSON,
                blob=json.dumps(object.dict()).encode("utf-8"),
                schema_type=models.SerializedValueSchemaTypeClass.JSON,
                schema_ref=object.__class__.__name__,
            )
        else:
            return SerializedResourceValue(
                content_type=models.SerializedValueContentTypeClass.JSON,
                blob=json.dumps(object).encode("utf-8"),
            )
