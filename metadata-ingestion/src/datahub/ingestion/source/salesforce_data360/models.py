from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class Data360Model(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class DmoField(Data360Model):
    name: str
    label: Optional[str] = None
    type: Optional[str] = None
    is_primary_key: bool = Field(default=False, alias="isPrimaryKey")
    creation_type: Optional[str] = Field(default=None, alias="creationType")


class DataModelObject(Data360Model):
    name: str
    label: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    creation_type: Optional[str] = Field(default=None, alias="creationType")
    is_enabled: Optional[bool] = Field(default=None, alias="isEnabled")
    data_space_name: Optional[str] = Field(default=None, alias="dataSpaceName")
    fields: List[DmoField] = Field(default_factory=list)


class RelationshipEnd(Data360Model):
    name: str


class DmoRelationship(Data360Model):
    name: Optional[str] = None
    cardinality: Optional[str] = None
    status: Optional[str] = None
    source_object: RelationshipEnd = Field(alias="sourceObject")
    source_field: RelationshipEnd = Field(alias="sourceField")
    target_object: RelationshipEnd = Field(alias="targetObject")
    target_field: RelationshipEnd = Field(alias="targetField")
