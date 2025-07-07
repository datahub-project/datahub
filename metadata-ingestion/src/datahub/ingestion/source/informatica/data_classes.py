from typing import Optional

from pydantic import BaseModel


# 폴더 정보 모델
class FolderInfo(BaseModel):
    subj_id: int
    subj_name: str
    subj_desc: Optional[str]
    is_shared: Optional[int]
    owner_id: Optional[int]
    creation_time: Optional[str]


# 워크플로우 정보 모델
class WorkflowInfo(BaseModel):
    workflow_id: int
    workflow_name: str
    workflow_desc: Optional[str]
    server_name: Optional[str]
    scheduler_name: Optional[str]
    start_time: Optional[str]
    end_time: Optional[str]
    run_options: Optional[str]
    delta_value: Optional[str]
    user_logic_type: Optional[str]
    frequency_interval: Optional[str]
    daily_logic: Optional[str]
    weekly_logic: Optional[str]
    monthly_logic: Optional[str]
    last_update_time: Optional[str]


# 세션 간 매핑 관계 모델
class SessionMappingEdge(BaseModel):
    from_inst_id: int
    to_inst_id: int
    to_session_id: Optional[int]
    to_mapping_id: Optional[int]
    to_mapping_desc: Optional[str]
    to_mapping_name: Optional[str]
    from_instance: str
    from_task_type: int
    to_instance: str
    to_task_type: int
    flow_depth: Optional[int]


# 위젯 선후관계 모델
class WidgetLineage(BaseModel):
    mapping_id: int
    pre_from_instance_id: Optional[int]
    from_instance_id: Optional[int]
    from_widget_id: Optional[int]
    from_widget_name: Optional[str]
    from_widget_type: Optional[str]
    to_instance_id: Optional[int]
    to_widget_id: Optional[int]
    to_widget_name: Optional[str]
    to_widget_type: Optional[str]


# 소스 위젯 모델
class SourceWidget(BaseModel):
    widget_id: int
    db_description: Optional[str]
    dbtype_name: Optional[str]
    db_name: Optional[str]
    source_name: Optional[str]
    description: Optional[str]


# 소스 퀄리파이어 위젯 모델
class SourceQualifierWidget(BaseModel):
    widget_id: int
    mapping_id: int
    instance_id: int
    full_query: Optional[str]
    full_filter: Optional[str]


# 타겟 위젯 모델
class TargetWidget(BaseModel):
    widget_id: int
    connection_name: Optional[str]
    connect_string: Optional[str]
    schema_name: Optional[str]
    target_name: Optional[str]
    description: Optional[str]
    table_name: Optional[str]
    session_inst_id: Optional[int]
    pv_id: Optional[int]
    pv_table_name: Optional[str]


class Synonyms(BaseModel):
    owner: str
    synonym_name: str
    table_owner: str
    table_name: str

class SourceField(BaseModel):
    widget_id: int
    src_id: int
    col_name: str
    col_desc: Optional[str]
    dbtype: Optional[int]
    datatype_name: Optional[str]
    fld_no: int
    nulltype: int
    keytype: int