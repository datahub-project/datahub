from datetime import datetime
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field
from pydantic.alias_generators import to_camel
from typing_extensions import Annotated

from datahub.ingestion.source.boomi.constants import LONG_TYPE_TAG, OPERATOR_AND


def parse_long(value: object) -> object:
    if isinstance(value, list) and len(value) == 2 and value[0] == LONG_TYPE_TAG:
        return value[1]
    return value


BoomiLong = Annotated[int, BeforeValidator(parse_long)]


class BoomiModel(BaseModel):
    model_config = ConfigDict(
        extra="ignore", alias_generator=to_camel, populate_by_name=True
    )


class ComponentMetadata(BoomiModel):
    component_id: str
    version: BoomiLong
    name: str
    type: str
    sub_type: Optional[str] = None
    created_by: Optional[str] = None
    created_date: Optional[datetime] = None
    modified_by: Optional[str] = None
    modified_date: Optional[datetime] = None
    deleted: bool = False
    current_version: bool = True
    folder_name: Optional[str] = None
    folder_id: Optional[str] = None
    branch_id: Optional[str] = None
    branch_name: Optional[str] = None


class Folder(BoomiModel):
    id: str
    name: str
    parent_id: Optional[str] = None
    full_path: str
    deleted: bool = False


class DeployedPackage(BoomiModel):
    component_id: str
    component_type: Optional[str] = None
    environment_id: str
    package_version: Optional[str] = None
    active: bool = True
    deployed_date: Optional[datetime] = None
    branch_name: Optional[str] = None


class Environment(BoomiModel):
    id: str
    name: str
    classification: Optional[str] = None


class QueryResponse(BoomiModel):
    response_type: Optional[str] = Field(default=None, alias="@type")
    result: List[Dict[str, object]] = Field(default_factory=list)
    number_of_results: BoomiLong = 0
    query_token: Optional[str] = None


class SimpleExpression(BoomiModel):
    property: str
    operator: str
    argument: List[str]


class GroupingExpression(BoomiModel):
    operator: str = OPERATOR_AND
    nested_expression: List[Union[SimpleExpression, "GroupingExpression"]]


class ProcessComponentInfo(BaseModel):
    description: Optional[str] = None
    folder_full_path: Optional[str] = None
