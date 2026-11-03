from typing import Dict, List, Optional, Set

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

    @property
    def event_bus_name(self) -> str:
        # Event bus ARNs end in `event-bus/<name>`, and partner bus names contain
        # slashes themselves (`aws.partner/<vendor>/<id>/<name>`).
        prefix = "event-bus/"
        if self.resource.startswith(prefix):
            return self.resource[len(prefix) :]
        return self.resource_name

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


class SnsTopic(BaseModel):
    arn: Arn
    attributes: Dict[str, str]
    tags: Dict[str, str] = Field(default_factory=dict)

    @property
    def name(self) -> str:
        return self.arn.resource_name


class SnsSubscription(BaseModel):
    subscription_arn: str
    topic_arn: str
    protocol: str
    endpoint: str
    owner: Optional[str] = None


class LambdaFunctionEdges(BaseModel):
    arn: Arn
    inputs: Set[str] = Field(default_factory=set)
    outputs: Set[str] = Field(default_factory=set)
    input_jobs: Set[str] = Field(default_factory=set)


class RuleTarget(BaseModel):
    arn: str


class EventBridgeRule(BaseModel):
    bus_arn: Arn
    name: str
    state: Optional[str] = None
    event_pattern: Optional[str] = None
    # A scheduled rule (cron or rate) fires on its own; nothing on the bus feeds it.
    schedule_expression: Optional[str] = None
    targets: List[RuleTarget] = Field(default_factory=list)


class EventBridgePipe(BaseModel):
    arn: Arn
    name: str
    source: str
    target: str
    enrichment: Optional[str] = None
    state: Optional[str] = None


class BucketNotification(BaseModel):
    bucket: str
    target_arn: str
    prefix: Optional[str] = None
