import logging
from typing import Dict, Iterable, List, Optional

from datahub.configuration import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    ActionRequestInfoClass,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.utilities.urns.urn import guess_entity_type


logger = logging.getLogger(__name__)


class ActionRequestOwnerSourceConfig(ConfigModel):
    batch_size: int = 20


class ActionRequestOwnerSourceReport(SourceReport):
    total_requests: int = 0
    processed_proposals = 0
    correct_assignees_not_found = 0
    correct_proposal_owners = 0
    incorrect_proposal_owners = 0
    missing_entity_owners = 0
    action_request_info_not_found = 0


ACTION_REQUESTS = """
query listActionRequests($input: ListActionRequestsInput!) {
  listActionRequests(input: $input) {
    start
    count
    total
    actionRequests {
      urn
      type
      entity {
        urn
      }
      subResource
      subResourceType
      assignedUsers
      assignedGroups
      assignedRoles
    }
  }
}
"""

ACTION_REQUEST_ASSIGNEES = """
query getActionRequestAssignee($input: GetActionRequestAssigneeInput!) {
  getActionRequestAssignee(input: $input)
}
"""


@platform_name(id="datahub", platform_name="DataHub")
@config_class(ActionRequestOwnerSourceConfig)
@support_status(SupportStatus.INCUBATING)
class ActionRequestOwnerSource(Source):
    def __init__(self, config: ActionRequestOwnerSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config: ActionRequestOwnerSourceConfig = config
        self.report = ActionRequestOwnerSourceReport()
        self.graph = ctx.require_graph("Proposal Owner source")
        self.event_not_produced_warn = False

    def _process_action_request(
        self, action_request: Dict
    ) -> Optional[MetadataChangeProposalWrapper]:
        self.report.processed_proposals += 1
        action_request_urn = action_request.get("urn")
        action_type = action_request.get("type")
        action_request_entity = action_request.get("entity")
        if action_request_entity is None:
            logger.error(f"Action request entity not found for {action_request_urn}")
            self.report.missing_entity_owners += 1
            return None
        resource_urn = action_request_entity.get("urn")
        sub_resource = action_request.get("subResource")
        sub_resource_type = action_request.get("subResourceType")
        assigned_users = action_request.get("assignedUsers")
        assigned_groups = action_request.get("assignedGroups")
        assigned_roles = action_request.get("assignedRoles")

        correct_assignees = self.graph.execute_graphql(
            query=ACTION_REQUEST_ASSIGNEES,
            variables={
                "input": {
                    "resourceUrn": resource_urn,
                    "actionRequestType": action_type,
                    "subResource": sub_resource,
                    "subResourceType": sub_resource_type,
                }
            },
        ).get("getActionRequestAssignee")
        if correct_assignees is None:
            self.report.correct_assignees_not_found += 1
            logger.error(
                f"Correct assignees not found for action request {action_request_urn}"
            )
            return None
        correct_users = [
            x for x in correct_assignees if guess_entity_type(x) == "corpuser"
        ]
        correct_groups = [
            x for x in correct_assignees if guess_entity_type(x) == "corpGroup"
        ]
        correct_roles = [
            x for x in correct_assignees if guess_entity_type(x) == "dataHubRole"
        ]
        if (
            assigned_users == correct_users
            and assigned_groups == correct_groups
            and assigned_roles == correct_roles
        ):
            self.report.correct_proposal_owners += 1
            return None
        else:
            self.report.incorrect_proposal_owners += 1
        action_request_info = self.graph.get_aspect_v2(
            entity_urn=str(action_request_urn),
            aspect="actionRequestInfo",
            aspect_type=ActionRequestInfoClass,
        )
        if action_request_info is None:
            self.report.action_request_info_not_found += 1
            logger.error(
                f"Action request info not found for action request {action_request_urn}"
            )
            return None
        action_request_info.assignedUsers = correct_users
        action_request_info.assignedGroups = correct_groups
        action_request_info.assignedRoles = correct_roles
        return MetadataChangeProposalWrapper(
            entityUrn=action_request_urn, aspect=action_request_info
        )

    def _get_action_requests(self, start: int) -> List:
        list_action_requests = self.graph.execute_graphql(
            query=ACTION_REQUESTS,
            variables={
                "input": {
                    "status": "PENDING",
                    "allActionRequests": True,
                    "start": start,
                    "count": self.config.batch_size,
                }
            },
        )
        assert list_action_requests is not None
        listActionRequests = list_action_requests.get("listActionRequests")
        assert listActionRequests is not None
        self.report.total_requests = listActionRequests.get("total", 0)
        return listActionRequests.get("actionRequests", [])

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        start = 0
        while True:
            action_requests = self._get_action_requests(start)
            if len(action_requests) == 0:
                break
            for action_request in action_requests:
                result = self._process_action_request(action_request)
                if result is not None:
                    yield result.as_workunit()
            start += self.config.batch_size

    def get_report(self) -> SourceReport:
        return self.report
