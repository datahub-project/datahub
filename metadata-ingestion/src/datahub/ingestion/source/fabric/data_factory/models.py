from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ActivityDependency:
    """A dependency on another activity within the same pipeline.

    Maps to DependencyActivity in the Fabric API.
    """

    activity: str
    dependency_conditions: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, dependency_dict: Dict[str, Any]) -> ActivityDependency:
        return cls(
            activity=dependency_dict["activity"],
            dependency_conditions=dependency_dict["dependencyConditions"],
        )


@dataclass
class ActivityPolicy:
    """Execution policy for an activity."""

    timeout: Optional[str] = None
    retry: Optional[int] = None
    retry_interval_in_seconds: Optional[int] = None
    secure_input: Optional[bool] = None
    secure_output: Optional[bool] = None

    @classmethod
    def from_dict(cls, policy_dict: Dict[str, Any]) -> ActivityPolicy:
        return cls(
            timeout=policy_dict.get("timeout"),
            retry=policy_dict.get("retry"),
            retry_interval_in_seconds=policy_dict.get("retryIntervalInSeconds"),
            secure_input=policy_dict.get("secureInput"),
            secure_output=policy_dict.get("secureOutput"),
        )


@dataclass
class ExternalReference:
    """External reference to a connection.

    Maps to External Reference in the Fabric API.
    """

    connection: str

    @classmethod
    def from_dict(cls, ref_dict: Dict[str, Any]) -> ExternalReference:
        return cls(connection=ref_dict["connection"])


@dataclass
class PipelineActivity:
    """A single activity parsed from a Fabric Data Pipeline definition.

    Maps to DataPipelineActivity in the Fabric API. Fields use safe defaults
    so missing/extra keys in the API response won't break parsing.

    Reference: https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/definitions/datapipeline-definition
    """

    name: str
    type: str
    state: Optional[str] = None  # "Active" | "InActive"
    on_inactive_mark_as: Optional[str] = None  # "Succeeded" | "Failed" | "Skipped"
    depends_on: List[ActivityDependency] = field(default_factory=list)
    type_properties: Dict[str, Any] = field(default_factory=dict)
    policy: Optional[ActivityPolicy] = None
    external_references: Optional[ExternalReference] = None

    @classmethod
    def from_dict(cls, activity_dict: Dict[str, Any]) -> PipelineActivity:
        """Parse an activity dict from pipeline definition JSON.

        Required fields (name, type, typeProperties) use direct key access
        and will raise KeyError if missing — callers should catch per-activity.
        """
        depends_on = [
            ActivityDependency.from_dict(raw)
            for raw in activity_dict.get("dependsOn", [])
        ]

        policy_dict = activity_dict.get("policy")
        policy = (
            ActivityPolicy.from_dict(policy_dict)
            if isinstance(policy_dict, dict)
            else None
        )

        ext_ref_dict = activity_dict.get("externalReferences")
        ext_ref = (
            ExternalReference.from_dict(ext_ref_dict)
            if isinstance(ext_ref_dict, dict)
            else None
        )

        return cls(
            name=activity_dict["name"],
            type=activity_dict["type"],
            state=activity_dict.get("state"),
            on_inactive_mark_as=activity_dict.get("onInactiveMarkAs"),
            depends_on=depends_on,
            type_properties=activity_dict["typeProperties"],
            policy=policy,
            external_references=ext_ref,
        )
