from abc import abstractmethod
from typing import Optional, Union

from pydantic import ConfigDict, Field, StrictFloat, StrictInt

from datahub.api.entities.assertion.assertion_trigger import AssertionTrigger
from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import make_assertion_source
from datahub.metadata import schema_classes as models
from datahub.metadata.com.linkedin.pegasus2avro.assertion import AssertionInfo
from datahub.utilities.str_enum import StrEnum


class BaseAssertionProtocol(ConfigModel):
    @abstractmethod
    def get_id(self) -> str:
        pass

    @abstractmethod
    def get_assertion_info_aspect(
        self,
    ) -> AssertionInfo:
        pass

    @abstractmethod
    def get_assertion_trigger(
        self,
    ) -> Optional[AssertionTrigger]:
        pass


class BaseAssertion(ConfigModel):
    id_raw: Optional[str] = Field(
        default=None,
        description="The raw id of the assertion."
        "If provided, this is used when creating identifier for this assertion"
        "along with assertion type and entity.",
    )

    id: Optional[str] = Field(
        default=None,
        description="The id of the assertion."
        "If provided, this is used as identifier for this assertion."
        "If provided, no other assertion fields are considered to create identifier.",
    )

    description: Optional[str] = None

    meta: Optional[dict] = None


class AssertionFailureSeverity(StrEnum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class AssertionFailureSeverityOperator(StrEnum):
    GREATER_THAN = models.AssertionStdOperatorClass.GREATER_THAN
    GREATER_THAN_OR_EQUAL_TO = models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO
    LESS_THAN = models.AssertionStdOperatorClass.LESS_THAN
    LESS_THAN_OR_EQUAL_TO = models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO


class AssertionFailureSeverityRule(ConfigModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    severity: Union[str, AssertionFailureSeverity, models.AssertionResultSeverityClass]
    operator: Union[
        str,
        AssertionFailureSeverityOperator,
        models.AssertionStdOperatorClass,
    ]
    value: Union[StrictInt, StrictFloat]

    def to_model(self) -> models.AssertionFailureSeverityRuleClass:
        return models.AssertionFailureSeverityRuleClass(
            severity=_parse_failure_severity(self.severity),
            operator=_parse_failure_severity_operator(self.operator),
            parameters=models.AssertionStdParametersClass(
                value=models.AssertionStdParameterClass(
                    type=models.AssertionStdParameterTypeClass.NUMBER,
                    value=str(self.value),
                )
            ),
        )


class AssertionFailureSeverityDefaultConfig(ConfigModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    default_severity: Union[
        str, AssertionFailureSeverity, models.AssertionResultSeverityClass
    ]

    def to_model(self) -> models.AssertionFailureSeverityConfigClass:
        return models.AssertionFailureSeverityConfigClass(
            defaultSeverity=_parse_failure_severity(self.default_severity),
            rules=[],
        )


class AssertionFailureSeverityConfig(ConfigModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    default_severity: Union[
        str, AssertionFailureSeverity, models.AssertionResultSeverityClass
    ]
    rules: list[Union[dict[str, object], AssertionFailureSeverityRule]] = Field(
        default_factory=list
    )

    def to_model(self) -> models.AssertionFailureSeverityConfigClass:
        return models.AssertionFailureSeverityConfigClass(
            defaultSeverity=_parse_failure_severity(self.default_severity),
            rules=[
                (
                    rule
                    if isinstance(rule, AssertionFailureSeverityRule)
                    else AssertionFailureSeverityRule.model_validate(rule)
                ).to_model()
                for rule in self.rules
            ],
        )


def _parse_failure_severity(
    severity: Union[str, AssertionFailureSeverity, models.AssertionResultSeverityClass],
) -> models.AssertionResultSeverityClass:
    if isinstance(severity, models.AssertionResultSeverityClass):
        return severity
    if isinstance(severity, AssertionFailureSeverity):
        severity = severity.value
    return getattr(models.AssertionResultSeverityClass, str(severity).upper())


def _parse_failure_severity_operator(
    operator: Union[
        str,
        AssertionFailureSeverityOperator,
        models.AssertionStdOperatorClass,
    ],
) -> models.AssertionStdOperatorClass:
    if isinstance(operator, models.AssertionStdOperatorClass):
        parsed_operator = operator
    else:
        if isinstance(operator, AssertionFailureSeverityOperator):
            operator = operator.value
        operator_name = str(operator).upper()
        parsed_operator = getattr(models.AssertionStdOperatorClass, operator_name)
    if parsed_operator not in [
        models.AssertionStdOperatorClass.GREATER_THAN,
        models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
        models.AssertionStdOperatorClass.LESS_THAN,
        models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
    ]:
        raise ValueError(
            "Failure severity rule operators must be one of GREATER_THAN, "
            "GREATER_THAN_OR_EQUAL_TO, LESS_THAN, or LESS_THAN_OR_EQUAL_TO"
        )
    return parsed_operator


def _ensure_source_created(info: AssertionInfo) -> AssertionInfo:
    """Ensure AssertionInfo has source.created populated."""
    if info.source is None:
        info.source = make_assertion_source()
    elif info.source.created is None:
        info.source.created = make_assertion_source().created
    return info


class BaseEntityAssertion(BaseAssertion):
    entity: str = Field(
        description="The entity urn that the assertion is associated with"
    )

    trigger: Optional[AssertionTrigger] = Field(
        default=None, description="The trigger schedule for assertion", alias="schedule"
    )
    failure_severity_config: Optional[
        Union[AssertionFailureSeverityConfig, AssertionFailureSeverityDefaultConfig]
    ] = Field(
        default=None,
        description="Optional configuration for assigning severities to failed assertion results.",
    )

    @abstractmethod
    def get_assertion_info(self) -> AssertionInfo:
        pass

    def get_assertion_info_aspect(self) -> AssertionInfo:
        return _ensure_source_created(self.get_assertion_info())
