from datetime import datetime
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field
from pydantic.alias_generators import to_camel
from typing_extensions import Annotated

from datahub.ingestion.source.boomi.constants import (
    LONG_TYPE_TAG,
    OPERATOR_AND,
    OperationDirection,
)


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


class ExecutionRecord(BoomiModel):
    execution_id: str
    execution_time: datetime
    execution_duration: Optional[BoomiLong] = None
    status: str
    process_id: str
    process_name: Optional[str] = None
    atom_id: Optional[str] = None
    atom_name: Optional[str] = None
    execution_type: Optional[str] = None
    parent_execution_id: Optional[str] = None
    top_level_execution_id: Optional[str] = None
    inbound_document_count: Optional[BoomiLong] = None
    outbound_document_count: Optional[BoomiLong] = None
    inbound_error_document_count: Optional[BoomiLong] = None
    recorded_date: Optional[datetime] = None
    message: Optional[str] = None


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


class ConnectorActionShape(BaseModel):
    connector_type: Optional[str] = None
    action_type: Optional[str] = None
    connection_id: Optional[str] = None
    operation_id: Optional[str] = None


class ProcessComponentInfo(BaseModel):
    description: Optional[str] = None
    folder_full_path: Optional[str] = None
    connector_actions: List[ConnectorActionShape] = Field(default_factory=list)
    process_call_ids: List[str] = Field(default_factory=list)


class ConnectionComponent(BaseModel):
    sub_type: Optional[str] = None
    # Legacy Database connector.
    driver_id: Optional[str] = None
    host: Optional[str] = None
    port: Optional[str] = None
    dbname: Optional[str] = None
    additional: Optional[str] = None
    url_format: Optional[str] = None
    # Database V2 connector.
    url: Optional[str] = None
    schema_name: Optional[str] = None
    # Distinct JDBC URLs found in any field of a generic connection.
    jdbc_urls: List[str] = Field(default_factory=list)


class OperationComponent(BaseModel):
    sub_type: Optional[str] = None
    direction: OperationDirection = OperationDirection.UNKNOWN
    # The element that configures the action, e.g. `DatabaseGetAction`.
    action_element: Optional[str] = None
    operation_type: Optional[str] = None
    custom_operation_type: Optional[str] = None
    object_type_id: Optional[str] = None
    object_type_name: Optional[str] = None
    query: Optional[str] = None
    # Distinct SQL statements from fields whose id mentions sql or query.
    sql_statements: List[str] = Field(default_factory=list)
    object_name: Optional[str] = None
    profile_ids: List[str] = Field(default_factory=list)


class DatabaseProfileComponent(BaseModel):
    table_name: Optional[str] = None
    statement_type: Optional[str] = None
    sql: Optional[str] = None


class JdbcLocation(BaseModel):
    platform: Optional[str] = None
    database: Optional[str] = None
    schema_name: Optional[str] = None


class ConnectionTarget(BaseModel):
    platform: str
    platform_instance: Optional[str] = None
    env: str
    database: Optional[str] = None
    default_schema: Optional[str] = None
    convert_urns_to_lowercase: bool = True


class ProcessLineage(BaseModel):
    input_datasets: List[str] = Field(default_factory=list)
    output_datasets: List[str] = Field(default_factory=list)
    child_process_ids: List[str] = Field(default_factory=list)
    # A connection, operation or profile could not be read, so the edges are
    # partial and must not replace lineage stored by an earlier run.
    incomplete: bool = False
