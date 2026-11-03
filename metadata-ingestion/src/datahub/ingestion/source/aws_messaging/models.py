from typing import Dict, Optional

from pydantic import BaseModel, ConfigDict, Field


class Arn(BaseModel):
    model_config = ConfigDict(frozen=True)

    partition: str
    service: str
    region: str
    account: str
    resource: str

    @property
    def resource_name(self) -> str:
        # SQS and SNS use `<name>`, Lambda `function:<name>`, EventBridge
        # `rule/<bus>/<name>` and Pipes `pipe/<name>`.
        return self.resource.replace(":", "/").rsplit("/", 1)[-1]

    def __str__(self) -> str:
        return f"arn:{self.partition}:{self.service}:{self.region}:{self.account}:{self.resource}"


class RedrivePolicy(BaseModel):
    dead_letter_target_arn: str = Field(alias="deadLetterTargetArn")
    max_receive_count: Optional[int] = Field(default=None, alias="maxReceiveCount")


class SqsQueue(BaseModel):
    url: str
    arn: Arn
    attributes: Dict[str, str]
    tags: Dict[str, str] = Field(default_factory=dict)

    @property
    def name(self) -> str:
        return self.arn.resource_name
