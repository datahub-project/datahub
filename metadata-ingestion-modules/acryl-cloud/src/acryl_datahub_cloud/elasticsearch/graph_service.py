import datetime
from typing import Any, Dict, List, Optional, get_args, get_origin

import pyarrow as pa
from pydantic import BaseModel


class SchemaField(BaseModel):
    name: str
    type: str


class BaseModelRow(BaseModel):
    def dict(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.__dict__

    @staticmethod
    def pydantic_type_to_pyarrow(type_: Any) -> pa.DataType:  # noqa: C901
        # Handle generic types (List, Optional, Union, etc.)
        origin = get_origin(type_)

        if origin is list:
            # List[X] -> list(X)
            args = get_args(type_)
            if args:
                inner_type = BaseModelRow.pydantic_type_to_pyarrow(args[0])
                return pa.list_(inner_type)
            return pa.list_(pa.string())  # Default to list of strings

        if origin is type(None):
            # Just None type
            return pa.null()

        # Optional[X] is Union[X, None]
        args = get_args(type_)
        if args:
            # Check if this is Optional (Union with None)
            non_none_types = [arg for arg in args if arg is not type(None)]
            if len(non_none_types) < len(args):  # Had None in the union
                if non_none_types:
                    return BaseModelRow.pydantic_type_to_pyarrow(non_none_types[0])
                return pa.null()

        # Handle simple types - check if it's actually a class first
        if not isinstance(type_, type):
            # If it's not a type/class, default to string
            return pa.string()

        if issubclass(type_, bool):
            return pa.bool_()
        elif issubclass(type_, int):
            return pa.int64()
        elif issubclass(type_, float):
            return pa.float64()
        elif issubclass(type_, str):
            return pa.string()
        elif issubclass(type_, datetime.datetime):
            return pa.timestamp("ns")
        elif issubclass(type_, datetime.date):
            return pa.date32()
        # Extend with additional mappings as needed
        else:
            raise ValueError(f"No mapping for type {type_}")

    @staticmethod
    def string_to_pyarrow_type(type_string: str) -> pa.DataType:
        """Convert string representation back to pyarrow type by converting to Python type first."""
        # Mapping of pyarrow string representations to Python types
        type_mapping = {
            "string": str,
            "int64": int,
            "float64": float,
            "bool": bool,
            "timestamp[ns]": datetime.datetime,
            "date32[day]": datetime.date,
        }

        python_type = type_mapping.get(
            type_string, str
        )  # Default to str for unknown types
        return BaseModelRow.pydantic_type_to_pyarrow(python_type)

    @staticmethod
    def _is_optional_type(type_: Any) -> bool:
        """Check if a type annotation is Optional (i.e., Union with None)."""
        origin = get_origin(type_)
        if origin is type(None):
            return True

        args = get_args(type_)
        if args:
            # Check if None is in the Union args (Optional is Union[X, None])
            return type(None) in args

        return False

    @classmethod
    def arrow_schema(cls) -> pa.Schema:
        fields = []
        for field_name, field_model in cls.model_fields.items():
            pyarrow_type = BaseModelRow.pydantic_type_to_pyarrow(field_model.annotation)
            pyarrow_field = pa.field(field_name, pyarrow_type)

            # Check if the type is Optional (Union with None) OR if the field has a default value
            is_nullable = (
                BaseModelRow._is_optional_type(field_model.annotation)
                or not field_model.is_required()
            )

            pyarrow_field = pyarrow_field.with_nullable(is_nullable)
            fields.append(pyarrow_field)
        return pa.schema(fields)

    @classmethod
    def datahub_schema(cls) -> List[SchemaField]:
        fields = []
        for field_name, field_model in cls.model_fields.items():
            pyarrow_type = BaseModelRow.pydantic_type_to_pyarrow(field_model.annotation)
            fields.append(SchemaField(name=field_name, type=str(pyarrow_type)))
        return fields


class ElasticGraphRow(BaseModelRow):
    source_urn: str
    source_entity_type: str
    destination_urn: str
    destination_entity_type: str
    relationship_type: str
    created_on: Optional[float]
    created_by: Optional[str]
    updated_on: Optional[float]
    updated_by: Optional[str]

    @classmethod
    def from_elastic_doc(cls, doc: dict) -> "ElasticGraphRow":
        return cls(
            source_urn=doc["source"]["urn"],
            source_entity_type=doc["source"]["entityType"],
            destination_urn=doc["destination"]["urn"],
            destination_entity_type=doc["destination"]["entityType"],
            relationship_type=doc["relationshipType"],
            created_on=doc.get("createdOn"),
            created_by=doc.get("createdActor"),
            updated_on=doc.get("updatedOn"),
            updated_by=doc.get("updatedActor"),
        )
