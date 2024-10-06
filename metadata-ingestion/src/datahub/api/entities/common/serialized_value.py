import json
import logging
from typing import Any, Dict, Optional, Type, Union

from avrogen.dict_wrapper import DictWrapper
from pydantic import BaseModel

import datahub.metadata.schema_classes as models

logger = logging.getLogger(__name__)


class SerializedResourceValue(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    content_type: str
    object: Optional[dict] = None
    blob: bytes
    schema_type: Optional[models.SerializedValueSchemaTypeClass] = None
    schema_ref: Optional[str] = None

    def get_parsed_value(self) -> Optional[Union[Dict, DictWrapper, BaseModel]]:
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
            if self.schema_ref:
                breakpoint()
                try:
                    # assume that the schema ref is a Python class
                    # e.g. schema_ref = "datahub.types.MyType"
                    # check that schema_ref looks like a valid Python class
                    assert "." in self.schema_ref
                    # make mypy happy
                    model_type = eval(self.schema_ref)
                    assert issubclass(model_type, BaseModel)
                    return model_type(**object_dict)
                except NameError:
                    logger.warning(
                        f"Unsupported schema ref {self.schema_ref} for parsing value"
                    )
            return object_dict
        elif self.schema_type == models.SerializedValueSchemaTypeClass.PEGASUS:
            json_string = self.blob.decode("utf-8")
            object_dict = json.loads(json_string)
            if self.schema_ref:
                model_type = models.__SCHEMA_TYPES.get(self.schema_ref)
                if model_type:
                    assert issubclass(model_type, DictWrapper)
                    return model_type.from_obj(object_dict)
            logger.warning(
                f"Unsupported schema ref {self.schema_ref} for parsing value"
            )
            return object_dict
        else:
            logger.warning(
                f"Unsupported schema type {self.schema_type} for parsing value"
            )
            raise ValueError(
                f"Unsupported schema type {self.schema_type} for parsing value"
            )

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


class TypedResourceValue(BaseModel):

    object: Optional[Union[Dict, DictWrapper, BaseModel]] = None

    @classmethod
    def from_serialized_resource_value(
        cls, serialized_resource_value: SerializedResourceValue
    ) -> "TypedResourceValue":
        parsed_value = serialized_resource_value.get_parsed_value()
        return cls(object=parsed_value)

    def to_serialized_resource_value(self) -> SerializedResourceValue:
        if isinstance(self.object, DictWrapper):
            return SerializedResourceValue(
                content_type=models.SerializedValueContentTypeClass.JSON,
                blob=json.dumps(self.object.to_obj()).encode("utf-8"),
                schema_type=models.SerializedValueSchemaTypeClass.PEGASUS,
                schema_ref=self.object.RECORD_SCHEMA.fullname,
            )
        elif isinstance(self.object, BaseModel):
            return SerializedResourceValue(
                content_type=models.SerializedValueContentTypeClass.JSON,
                blob=json.dumps(self.object.dict()).encode("utf-8"),
                schema_type=models.SerializedValueContentTypeClass.JSON,
                schema_ref=self.object.__class__.__name__,
            )
        else:
            return SerializedResourceValue(
                content_type=models.SerializedValueContentTypeClass.JSON,
                blob=json.dumps(self.object).encode("utf-8"),
            )

    class Config:
        arbitrary_types_allowed = True

        @classmethod
        def schema_extra(
            cls, schema: Dict[str, Any], model: Type["TypedResourceValue"]
        ) -> None:
            props = schema.get("properties", {})
            if "object" in props:
                props["object"]["anyOf"] = [
                    {"type": "object"},
                    {"$ref": "#/definitions/BaseModel"},
                ]
