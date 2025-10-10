import datetime
from typing import Any, Dict, List, Optional

import pyarrow as pa
from pydantic import BaseModel


class SchemaField(BaseModel):
    name: str
    type: str


class BaseModelRow(BaseModel):
    def dict(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.__dict__

    @staticmethod
    def pydantic_type_to_pyarrow(type_: Any) -> pa.DataType:
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

    @classmethod
    def arrow_schema(cls) -> pa.Schema:
        fields = []
        for field_name, field_model in cls.model_fields.items():
            pyarrow_type = BaseModelRow.pydantic_type_to_pyarrow(field_model.annotation)
            pyarrow_field = pa.field(field_name, pyarrow_type)
            if not field_model.is_required():
                pyarrow_field = pyarrow_field.with_nullable(True)
            else:
                pyarrow_field = pyarrow_field.with_nullable(False)
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
