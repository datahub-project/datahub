from typing import List, Optional

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    AssertionEvaluationSpecClass,
    KafkaAuditHeaderClass,
    MonitorErrorClass,
    MonitorInfoClass,
    MonitorStateClass,
    MonitorStatusClass,
    MonitorTypeClass,
    SystemMetadataClass,
)


class MonitorPatchBuilder(MetadataPatchProposal):
    def __init__(
        self,
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        """Initializes a MonitorPatchBuilder instance.

        Args:
            urn: The URN of the monitor.
            system_metadata: The system metadata of the data job (optional).
            audit_header: The Kafka audit header of the data job (optional).
        """
        super().__init__(
            urn, system_metadata=system_metadata, audit_header=audit_header
        )

    def set_type(self, monitor_type: MonitorTypeClass | str) -> "MonitorPatchBuilder":
        """Sets monitor type.

        Args:
            monitor_type: A MonitorType enum value.

        Returns:
            The MonitorPatchBuilder instance.
        """
        self._add_patch(
            MonitorInfoClass.ASPECT_NAME,
            "add",
            path=("type",),
            value=monitor_type,
        )
        return self

    def set_assertion_monitor_assertions(
        self, assertions: List[AssertionEvaluationSpecClass]
    ) -> "MonitorPatchBuilder":
        """Sets the input data jobs for the MonitorPatchBuilder.

        Args:
            assertions: The assertions to be patched

        Returns:
            The MonitorPatchBuilder instance.

        Notes:
            This method replaces assertion monitor.
        """
        self._add_patch(
            MonitorInfoClass.ASPECT_NAME,
            "add",
            path=(
                "assertionMonitor",
                "assertions",
            ),
            value=assertions,
        )
        return self

    def set_status(self, status: MonitorStatusClass) -> "MonitorPatchBuilder":
        """Sets the input data jobs for the MonitorPatchBuilder.

        Args:
            status: The MonitorStatus .

        Returns:
            The MonitorPatchBuilder instance.

        Notes:
            This method replaces monitor status.
        """
        self._add_patch(
            MonitorInfoClass.ASPECT_NAME,
            "add",
            path=("status",),
            value=status,
        )
        return self

    def set_state(self, state: MonitorStateClass | str) -> "MonitorPatchBuilder":
        """Sets the monitor state for the monitor

        Args:
            status: The monitor state.

        Returns:
            The MonitorPatchBuilder instance.

        Notes:
            This method replaces monitor status.
        """
        self._add_patch(
            MonitorInfoClass.ASPECT_NAME,
            "add",
            path=("status", "state"),
            value=state,
        )
        return self

    def set_error(self, error: MonitorErrorClass | str) -> "MonitorPatchBuilder":
        """Sets the monitor state for the monitor

        Args:
            status: The monitor state.

        Returns:
            The MonitorPatchBuilder instance.

        Notes:
            This method replaces monitor status.
        """
        self._add_patch(
            MonitorInfoClass.ASPECT_NAME,
            "add",
            path=("status", "error"),
            value=error,
        )
        return self
