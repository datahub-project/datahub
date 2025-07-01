import dataclasses
from enum import Enum
from typing import Optional

from pydantic import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)


class InformaticaType(str, Enum):
    CLOUD = "cloud"
    POWERCENTER = "powercenter"


class InformaticaConfig(PlatformInstanceConfigMixin, EnvConfigMixin):
    # 공통 설정
    type: InformaticaType = Field(
        description="Informatica 환경 타입: cloud 또는 powercenter"
    )

    folder_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for folders to filter for ingestion.",
    )
    workflow_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for workflow to filter for ingestion.",
    )

    # powercenter 설정
    host_port: Optional[str] = Field(
        default=None, description="PowerCenter 메타DB 접속용 host:port"
    )
    username: Optional[str] = Field(default=None, description="PowerCenter DB 사용자")
    password: Optional[str] = Field(default=None, description="PowerCenter DB 패스워드")
    service_name: Optional[str] = Field(
        default=None, description="PowerCenter DB service name"
    )
    scheme: Optional[str] = Field(
        default="oracle+cx_oracle",
        description="PowerCenter 메타DB용 SQLAlchemy 커넥터 스키마",
    )
    metadata_schema: Optional[str] = Field(
        default=None, description="PowerCenter 메타데이터가 저장된 스키마 이름"
    )
    synonym_tracking_enabled: Optional[bool] = Field(
        default=False, description="synonym의 원본테이블 리니지 작성 여부"
    )


## TODO : report용 변수 지정 및 카운트 ++ 하기
@dataclasses.dataclass
class InformaticaSourceReport(StaleEntityRemovalSourceReport):
    total_folders: int = 0
    total_dataflows: int = 0
    total_jobs: int = 0
    total_lineages: int = 0
