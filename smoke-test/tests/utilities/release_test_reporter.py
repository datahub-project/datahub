"""
Release Test Reporter - Reports test execution metadata to a reporting DataHub instance.

This module allows release tests to emit metadata about their execution to acryl.acryl.io.
All functions are designed to fail silently with try-catch blocks to prevent test interference.
"""

import logging
import time
import uuid
from typing import Optional, Tuple
from urllib.parse import urlparse

from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    DataFlowSnapshotClass,
    DataJobSnapshotClass,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    IncidentInfoClass,
    IncidentStageClass,
    IncidentStateClass,
    IncidentStatusClass,
    IncidentTypeClass,
)
from datahub.metadata.urns import IncidentUrn
from datahub.utilities.urns.data_job_urn import DataJobUrn
from tests.utilities import env_vars

logger = logging.getLogger(__name__)


def get_actor_urn() -> str:
    """
    Get the actor URN for the release tests.
    """
    return make_user_urn("release-tests")


def get_properties() -> dict[str, str]:
    """
    Get the properties for the release tests.
    """
    github_env = env_vars.get_github_env_vars()
    return {k: v for k, v in github_env.items() if v is not None}


def _get_reporting_client():
    """
    Create and return a DataHub REST emitter for the reporting instance.
    Returns None if reporting is not configured.
    """
    try:
        gms_url = env_vars.get_reporting_datahub_gms_url()
        token = env_vars.get_reporting_datahub_token()

        if not gms_url or not token:
            logger.info(
                "⚠️  Reporting DataHub not configured (missing GMS URL or token). "
                "Skipping metadata reporting."
            )
            return None

        logger.info(f"📡 Connecting to reporting DataHub: {gms_url}")
        return DatahubRestEmitter(gms_server=gms_url, token=token)
    except Exception as e:
        logger.warning(f"❌ Failed to create reporting DataHub client: {e}")
        return None


def _get_pipeline_name() -> str:
    """
    Determine pipeline name based on the remote_instance_url (DATAHUB_FRONTEND_URL).
    Examples:
      - https://deploy-auto.acryl.io -> release-tests-deploy-auto
      - https://deploy-test.acryl.io -> release-tests-deploy-test
    """
    frontend_url = env_vars.get_frontend_url() or "unknown"
    # Extract domain name from URL
    try:
        parsed = urlparse(frontend_url)
        hostname = parsed.hostname or "unknown"
        # Remove .acryl.io suffix if present and clean up
        hostname = hostname.replace(".acryl.io", "").replace(".", "-")
        pipeline_name = f"release-tests-{hostname}"
        logger.info(f"🔧 Derived pipeline name: {pipeline_name} (from {frontend_url})")
        return pipeline_name
    except Exception as e:
        logger.warning(f"⚠️  Failed to parse frontend URL '{frontend_url}': {e}")
        return "release-tests-unknown"


def emit_pipeline_and_task_urns(task_name: str) -> tuple[Optional[str], Optional[str]]:
    """
    Emit DataFlow (pipeline) and DataJob (task) URNs.

    Args:
        task_name: Name of the test task (e.g., "deploy-auto-nightly-pytest")

    Returns:
        Tuple of (pipeline_urn, task_urn). Returns (None, None) if reporting fails.
    """
    try:
        client = _get_reporting_client()
        if not client:
            return None, None

        pipeline_name = _get_pipeline_name()
        orchestrator = "datahub-apps"
        cluster = "PROD"

        # Create DataFlow URN
        pipeline_urn = make_data_flow_urn(
            orchestrator=orchestrator, flow_id=pipeline_name, cluster=cluster
        )

        # Create DataJob URN
        task_urn = make_data_job_urn(
            orchestrator=orchestrator,
            flow_id=pipeline_name,
            job_id=task_name,
            cluster=cluster,
        )

        # Emit DataFlow metadata
        logger.info(f"📊 Creating pipeline entity: {pipeline_name}")
        dataflow_snapshot = DataFlowSnapshotClass(
            urn=pipeline_urn,
            aspects=[
                DataFlowInfoClass(
                    name=pipeline_name,
                    description=f"Release test automation pipeline for {env_vars.get_frontend_url()}",
                    externalUrl=env_vars.get_frontend_url(),
                )
            ],
        )
        client.emit_mce(MetadataChangeEvent(proposedSnapshot=dataflow_snapshot))
        logger.info(f"✅ Pipeline entity created: {pipeline_urn}")

        properties = get_properties()

        # Emit DataJob metadata
        logger.info(f"📋 Creating task entity: {task_name}")
        datajob_snapshot = DataJobSnapshotClass(
            urn=task_urn,
            aspects=[
                DataJobInfoClass(
                    name=task_name,
                    type="TEST",
                    description=f"Release tests running against {env_vars.get_frontend_url()}",
                    externalUrl=properties["GITHUB_WORKFLOW_URL"],
                    customProperties=properties,
                ),
                DataJobInputOutputClass(
                    inputDatasets=[],
                    outputDatasets=[],
                    inputDatajobs=[],
                ),
            ],
        )
        client.emit_mce(MetadataChangeEvent(proposedSnapshot=datajob_snapshot))
        logger.info(f"✅ Task entity created: {task_urn}")

        # Build links to reporting DataHub UI
        gms_url = env_vars.get_reporting_datahub_gms_url()
        base_url = gms_url.replace("/gms", "") if gms_url else "N/A"

        logger.info(
            f"\n{'=' * 80}\n"
            f"🎯 Release Test Metadata Reporting Initialized\n"
            f"{'=' * 80}\n"
            f"Pipeline URN: {pipeline_urn}\n"
            f"Task URN:     {task_urn}\n"
            f"\n📍 View in reporting DataHub:\n"
            f"   Pipeline: {base_url}/pipelines/{pipeline_urn}\n"
            f"   Task:     {base_url}/tasks/{task_urn}\n"
            f"{'=' * 80}\n"
        )
        return pipeline_urn, task_urn

    except Exception as e:
        logger.warning(f"❌ Failed to emit pipeline/task URNs: {e}")
        return None, None


def emit_process_start(
    task_urn: str, attempt_number: int = 1
) -> Optional[Tuple[DataProcessInstance, int]]:
    """
    Emit a DataProcessInstance and start event.

    Args:
        task_urn: URN of the DataJob (template)
        attempt_number: Attempt number for this run (default: 1)

    Returns:
        Tuple of (process_instance, start_timestamp_millis), or None if emission fails
    """
    try:
        client = _get_reporting_client()
        if not client or not task_urn:
            return None

        # Parse the DataJob URN to extract orchestrator and cluster
        datajob_urn = DataJobUrn.from_string(task_urn)

        # Create a unique instance ID for this run
        instance_id = str(uuid.uuid4())

        properties = get_properties()

        # Create a DataProcessInstance linked to the DataJob
        process_instance = DataProcessInstance(
            id=instance_id,
            orchestrator=datajob_urn.get_data_flow_urn().get_orchestrator_name(),
            cluster=datajob_urn.get_data_flow_urn().get_env(),
            template_urn=datajob_urn,
            properties=properties,
        )

        # Get the current timestamp in milliseconds since Unix epoch
        start_timestamp_millis = int(time.time() * 1000)

        # Emit the DataProcessInstance and start event using the built-in method
        process_instance.emit_process_start(
            emitter=client,
            start_timestamp_millis=start_timestamp_millis,
            attempt=attempt_number,
            emit_template=False,  # Don't re-emit the DataJob template
            materialize_iolets=False,
        )

        logger.info(
            f"▶️  Test run STARTED\n"
            f"   Task URN:       {task_urn}\n"
            f"   Instance URN:   {str(process_instance.urn)}\n"
            f"   Instance ID:    {instance_id}\n"
            f"   Attempt:        {attempt_number}"
        )
        return process_instance, start_timestamp_millis

    except Exception as e:
        logger.warning(f"❌ Failed to emit process start: {e}")
        return None


def emit_process_end(
    process_instance: DataProcessInstance,
    success: bool,
    duration_millis: Optional[int] = None,
    start_timestamp_millis: Optional[int] = None,
) -> None:
    """
    Emit a DataProcessInstanceRunEvent for process end.

    Args:
        process_instance: DataProcessInstance from emit_process_start
        success: Whether the process succeeded
        duration_millis: Duration of the process in milliseconds
        start_timestamp_millis: Start timestamp in milliseconds (for calculating duration)
    """
    try:
        client = _get_reporting_client()
        if not client or not process_instance:
            return

        # Determine the result type
        result = InstanceRunResult.SUCCESS if success else InstanceRunResult.FAILURE

        # Get the current timestamp in milliseconds since epoch
        end_timestamp_millis = int(time.time() * 1000)

        # Emit the process end event
        process_instance.emit_process_end(
            emitter=client,
            end_timestamp_millis=end_timestamp_millis,
            result=result,
            start_timestamp_millis=start_timestamp_millis,
        )

        status_emoji = "✅" if success else "❌"
        duration_str = (
            f"{duration_millis / 1000:.2f}s" if duration_millis else "unknown"
        )
        logger.info(
            f"{status_emoji} Test run COMPLETED\n"
            f"   Task URN:       {process_instance.template_urn}\n"
            f"   Instance URN:   {process_instance.urn}\n"
            f"   Result:         {'SUCCESS' if success else 'FAILURE'}\n"
            f"   Duration:       {duration_str}"
        )

    except Exception as e:
        logger.warning(f"❌ Failed to emit process end: {e}")


def raise_incident_on_task(task_urn: str, message: str, title: str) -> None:
    """
    Raise an incident on a task.

    Args:
        task_urn: URN of the DataJob
        message: Detailed failure message
        title: Short incident title
    """
    try:
        client = _get_reporting_client()
        if not client or not task_urn:
            return

        audit_stamp = AuditStampClass(
            time=int(time.time() * 1000),
            actor=get_actor_urn(),
        )
        incident_id = str(uuid.uuid4())
        incident_urn = str(IncidentUrn(incident_id))
        incident_info = IncidentInfoClass(
            type=IncidentTypeClass.OPERATIONAL,
            title=title,
            description=message,
            priority=1,  # HIGH
            status=IncidentStatusClass(
                state=IncidentStateClass.ACTIVE,
                stage=IncidentStageClass.TRIAGE,
                message=message,
                lastUpdated=audit_stamp,
            ),
            entities=[task_urn],
            created=audit_stamp,
        )

        mcp = MetadataChangeProposalWrapper(
            entityType="incident",
            entityUrn=incident_urn,
            aspect=incident_info,
        )

        client.emit_mcp(mcp)

        logger.info(
            f"🚨 INCIDENT raised on task\n"
            f"   Task URN: {task_urn}\n"
            f"   Title:    {title}\n"
            f"   Message:  {message[:100]}{'...' if len(message) > 100 else ''}"
        )

    except Exception as e:
        logger.warning(f"❌ Failed to raise incident on task: {e}")
