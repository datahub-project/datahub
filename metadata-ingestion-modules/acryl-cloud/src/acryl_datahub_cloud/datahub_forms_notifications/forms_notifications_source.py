import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pydantic import BaseModel
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from acryl_datahub_cloud.datahub_forms_notifications.query import (
    GRAPHQL_GET_FEATURE_FLAG,
    GRAPHQL_GET_SEARCH_RESULTS_TOTAL,
    GRAPHQL_SCROLL_FORMS_FOR_NOTIFICATIONS,
    GRAPHQL_SEND_FORM_NOTIFICATION_REQUEST,
)
from acryl_datahub_cloud.notifications.notification_recipient_builder import (
    NotificationRecipientBuilder,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RawSearchFilter
from datahub.metadata.schema_classes import (
    FormInfoClass,
    FormNotificationDetailsClass,
    FormNotificationEntryClass,
    FormNotificationsClass,
    FormSettingsClass,
    FormStateClass,
    FormTypeClass,
)

logger = logging.getLogger(__name__)

USER_URN_PREFIX = "urn:li:corpuser"
GROUP_URN_PREFIX = "urn:li:corpGroup"


class DataHubFormsNotificationsSourceConfig(BaseModel):
    form_urns: Optional[List[str]] = None


class DataHubDatasetSearchRow(BaseModel):
    urn: str
    owners: List[str] = []


@dataclass
class DataHubFormsNotificationsSourceReport(SourceReport):
    notifications_sent: int = (
        0  # the number of recipients we sent notifications out for
    )
    forms_count: int = (
        0  # the number of forms that we sent at least one nitification for
    )


@platform_name(id="datahub", platform_name="DataHub")
@config_class(DataHubFormsNotificationsSourceConfig)
@support_status(SupportStatus.INCUBATING)
class DataHubFormsNotificationsSource(Source):
    """Forms Notification Source that notifies recipients for compliance forms tasks"""

    def __init__(
        self, config: DataHubFormsNotificationsSourceConfig, ctx: PipelineContext
    ):
        super().__init__(ctx)
        self.config: DataHubFormsNotificationsSourceConfig = config
        self.report = DataHubFormsNotificationsSourceReport()
        self.graph: DataHubGraph = ctx.require_graph(
            "Loading default graph coordinates."
        )
        self.group_to_users_map: Dict[str, List[str]] = {}
        self.recipient_builder: NotificationRecipientBuilder = (
            NotificationRecipientBuilder(self.graph)
        )
        self.user_to_form_notifications: Dict[str, FormNotificationsClass] = {}

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        # end early if the feature flag is not enabled
        if not self.is_feature_flag_enabled():
            return []

        self.notify_form_assignees()

        # This source doesn't produce any work units
        return []

    def is_feature_flag_enabled(self) -> bool:
        response = self.execute_graphql_with_retry(
            GRAPHQL_GET_FEATURE_FLAG, variables={}
        )

        result = response.get("appConfig", {})
        featureFlags = result.get("featureFlags", {})
        is_enabled = featureFlags.get("formsNotificationsEnabled", False)

        if not is_enabled:
            logger.error(
                "Tried running datahub-forms-notifications with formsNotificationsEnabled disabled"
            )

        return is_enabled

    def notify_form_assignees(self) -> None:
        for urn, form in self.get_forms():
            if not self.is_form_complete(urn, form.type):
                assignees = self.get_form_assignees(urn, form)
                self.process_notify_on_publish(
                    assignees, form.name, urn, form.description
                )

    def process_notify_on_publish(
        self,
        form_assignees: List[str],
        form_name: str,
        form_urn: str,
        form_details: str | None,
    ) -> None:
        """
        Take in form assignees, find the ones who haven't been notified on publish, and build a notification for them.
        """
        filtered_assignees = self.filter_assignees_to_notify(form_assignees, form_urn)
        recipients = []
        if self.recipient_builder is not None:
            recipients = self.recipient_builder.build_actor_recipients(
                filtered_assignees, "COMPLIANCE_FORM_PUBLISH", True
            )
        recipient_count = len(recipients)

        if recipient_count > 0:
            self.report.notifications_sent += recipient_count
            self.report.forms_count += 1

            parameters = [{"key": "formName", "value": form_name}]
            if form_details is not None:
                parameters.append({"key": "formDetails", "value": form_details})

            response = self.execute_graphql_with_retry(
                GRAPHQL_SEND_FORM_NOTIFICATION_REQUEST,
                variables={
                    "input": {
                        "type": "BROADCAST_COMPLIANCE_FORM_PUBLISH",
                        "parameters": parameters,
                        "recipients": self.recipient_builder.convert_recipients_to_json_objects(
                            recipients
                        ),
                    }
                },
            )

            if not response.get("sendFormNotificationRequest", False):
                logger.error(
                    f"Issue sending the notification request for this job. Response: {response}"
                )
            else:
                unique_actor_urns = set(
                    [
                        recipient.get("actor")
                        for recipient in recipients
                        if recipient.get("actor") is not None
                    ]
                )
                for actor_urn in unique_actor_urns:
                    self.update_form_notifications(actor_urn, form_urn)

    def update_form_notifications(self, user_urn: str, form_urn: str) -> None:
        """
        After sending a notification, update the user's formNotifications aspect
        to track that we sent the notification that we did
        """
        # get or create default formNotifications aspect
        form_notifications = self.user_to_form_notifications.get(user_urn)
        if form_notifications is None:
            form_notifications = FormNotificationsClass(notificationDetails=[])

        # get the notification details for our specific form or create default
        details_for_form = self.get_notification_details_for_form(
            user_urn, form_urn, form_notifications
        )
        if details_for_form is None:
            details_for_form = FormNotificationDetailsClass(
                formUrn=form_urn, notificationLog=[]
            )

        # add new notification log entry for this occasion
        new_notification_log_entry = FormNotificationEntryClass(
            time=int(time.time() * 1000),
            notificationType="BROADCAST_COMPLIANCE_FORM_PUBLISH",
        )
        details_for_form.notificationLog.append(new_notification_log_entry)

        # filter out details for given form so we can add updated one
        final_notification_details = [
            details
            for details in form_notifications.notificationDetails
            if details.formUrn != form_urn
        ]
        final_notification_details.append(details_for_form)

        # update the aspect with the final notification details
        form_notifications.notificationDetails = final_notification_details

        self.user_to_form_notifications[user_urn] = form_notifications

        self.graph.emit(
            MetadataChangeProposalWrapper(
                entityUrn=user_urn,
                aspect=form_notifications,
            )
        )

    @retry(
        retry=retry_if_exception_type((Exception, ConnectionError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
    )
    def execute_graphql_with_retry(
        self, query: str, variables: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute GraphQL query with retry logic"""
        if self.graph is None:
            raise ValueError("Graph client not initialized")
        response = self.graph.execute_graphql(query, variables=variables)
        error = response.get("error")
        if error:
            raise Exception(f"GraphQL error: {error}")
        return response

    def get_forms(self) -> List[Tuple[str, FormInfoClass]]:
        """
        Get forms and their formInfo aspect either from the forms provided in the config
        or search for forms that are published and notifyAssigneesOnPublish = True.
        This method will only return forms that are published and have notifications enabled.
        """
        form_urns = []

        if self.config.form_urns is not None:
            form_urns = self.config.form_urns
        else:
            form_urns = self.search_for_forms()

        form_urns_with_notifications_enabled = (
            self.get_form_urns_with_notifications_enabled(form_urns)
        )

        return self.get_form_infos(form_urns_with_notifications_enabled)

    def get_form_urns_with_notifications_enabled(
        self, form_urns: List[str]
    ) -> List[str]:
        """
        Get formSettings aspects and check if notifications are enabled for a given form urn.
        If notifications are enabled, add to filtered list and return.
        """
        filtered_form_urns: List[str] = []

        if len(form_urns) > 0:
            entities = self.graph.get_entities("form", form_urns, ["formSettings"])
            for urn, entity in entities.items():
                form_tuple = entity.get(FormSettingsClass.ASPECT_NAME, (None, None))
                if form_tuple and form_tuple[0]:
                    if not isinstance(form_tuple[0], FormSettingsClass):
                        logger.error(
                            f"{form_tuple[0]} is not of type FormInfo for urn: {urn}"
                        )
                    else:
                        form_settings = form_tuple[0]
                        if form_settings.notificationSettings.notifyAssigneesOnPublish:
                            filtered_form_urns.append(urn)

        return filtered_form_urns

    def get_form_infos(self, form_urns: List[str]) -> List[Tuple[str, FormInfoClass]]:
        """
        Get formInfo aspects for a list of form urns and return the formInfos of forms
        that are published. If a form is not published, we don't want to notify.
        """
        form_infos: List[Tuple[str, FormInfoClass]] = []

        if len(form_urns) > 0:
            entities = self.graph.get_entities("form", form_urns, ["formInfo"])
            for urn, entity in entities.items():
                form_tuple = entity.get(FormInfoClass.ASPECT_NAME, (None, None))
                if form_tuple and form_tuple[0]:
                    if not isinstance(form_tuple[0], FormInfoClass):
                        logger.error(
                            f"{form_tuple[0]} is not of type FormInfo for urn: {urn}"
                        )
                    else:
                        form_info = form_tuple[0]
                        if form_info.status.state == FormStateClass.PUBLISHED:
                            form_infos.append((urn, form_tuple[0]))

        return form_infos

    def search_for_forms(self) -> List[str]:
        scroll_id: Optional[str] = None
        form_urns: List[str] = []

        try:
            while True:
                next_scroll_id, results = self.scroll_forms_to_notify_for(scroll_id)

                for result in results:
                    form_urn = result.get("entity", {}).get("urn", None)
                    if form_urn is None:
                        self.report.report_warning(
                            message="Failed to resolve entity urn for form! Skipping...",
                            context=f"Response: {str(result)}",
                        )
                    else:
                        form_urns.append(form_urn)

                if next_scroll_id is None:
                    break
                else:
                    scroll_id = next_scroll_id

                time.sleep(1)

        except Exception as e:
            self.report.report_failure(
                title="Failed to search for forms to send notifications for",
                message="Error occurred while searching for forms to send notifications for",
                context=f"message = {str(e)}",
                exc=e,
            )
            return form_urns

        return form_urns

    def scroll_forms_to_notify_for(
        self, scroll_id: Optional[str]
    ) -> Tuple[Optional[str], List[Dict[str, Any]]]:
        """Scroll through shared entities with retry logic"""
        response = self.execute_graphql_with_retry(
            GRAPHQL_SCROLL_FORMS_FOR_NOTIFICATIONS,
            variables={
                "scrollId": scroll_id,
                "count": 500,
            },
        )

        result = response.get("scrollAcrossEntities", {})
        return result.get("nextScrollId"), result.get("searchResults", [])

    def get_form_assignees(self, form_urn: str, form: FormInfoClass) -> List[str]:
        """
        Form assignees are provided explicitly on the form and the owners of assets with this form
        if it's an ownership form.
        For form notifications, we want to get users from a user group and send notifications to
        those users specifically
        """
        user_urns = form.actors.users if form.actors.users is not None else []
        group_urns = form.actors.groups if form.actors.groups is not None else []

        if form.actors.owners:
            (user_owners, group_owners) = self.get_owners_of_assets_for_form(
                form_urn, form
            )
            user_urns.extend(user_owners)
            group_urns.extend(group_owners)

        for group_urn in group_urns:
            user_urns.extend(self._get_users_in_group(group_urn))

        return list(set(user_urns))

    def get_owners_of_assets_for_form(
        self, form_urn: str, form: FormInfoClass
    ) -> Tuple[List[str], List[str]]:
        """
        Filter to get assets that are not complete for this form and using the extra_source_fields parameter
        we pull owners from the asset's elastic row. self.graph.get_results_by_filter will paginate over assets
        """
        user_urns = []
        group_urns = []

        extra_fields = [f for f in DataHubDatasetSearchRow.__fields__]
        results = self.graph.get_results_by_filter(
            extra_or_filters=self._get_incomplete_assets_for_form(form_urn, form.type),
            extra_source_fields=extra_fields,
            skip_cache=True,
        )
        for result in results:
            extra_properties = result["extraProperties"]
            extra_properties_map = {
                x["name"]: json.loads(x["value"]) for x in extra_properties
            }
            search_row = DataHubDatasetSearchRow(**extra_properties_map)
            for owner in search_row.owners:
                if owner.startswith(USER_URN_PREFIX):
                    user_urns.append(owner)
                elif owner.startswith(GROUP_URN_PREFIX):
                    group_urns.append(owner)
                else:
                    logger.warning(
                        f"Found unexpected owner {owner} for asset {search_row.urn}"
                    )

        return (user_urns, group_urns)

    def filter_assignees_to_notify(
        self, user_urns: List[str], form_urn: str
    ) -> List[str]:
        """
        Filter out any users who have already received the publish notification type in the past
        """
        filtered_users = []

        self.populate_user_to_form_notifications(user_urns)

        for user in user_urns:
            form_notifications = self.user_to_form_notifications.get(user)
            if form_notifications is None or not self.has_user_been_sent_notification(
                user, form_urn, form_notifications, "BROADCAST_COMPLIANCE_FORM_PUBLISH"
            ):
                filtered_users.append(user)

        return filtered_users

    def has_user_been_sent_notification(
        self,
        user_urn: str,
        form_urn: str,
        form_notifications: FormNotificationsClass,
        notification_type: str,
    ) -> bool:
        notification_details = self.get_notification_details_for_form(
            user_urn, form_urn, form_notifications
        )
        if notification_details is None:
            return False

        notification_types_sent_for_form = [
            entry.notificationType for entry in notification_details.notificationLog
        ]

        return notification_type in notification_types_sent_for_form

    def get_notification_details_for_form(
        self, user_urn: str, form_urn: str, form_notifications: FormNotificationsClass
    ) -> FormNotificationDetailsClass | None:
        notification_details_for_form = [
            detail
            for detail in form_notifications.notificationDetails
            if detail.formUrn == form_urn
        ]

        notification_details = None
        if len(notification_details_for_form) > 1:
            logger.warning(
                f"Found more than one notificationDetails for a given form for user {user_urn} in {form_notifications}"
            )
            # grab first one
            notification_details = notification_details_for_form[0]
        elif len(notification_details_for_form) == 1:
            notification_details = notification_details_for_form[0]

        return notification_details

    def populate_user_to_form_notifications(self, user_urns: List[str]) -> None:
        new_users = [
            urn for urn in user_urns if urn not in self.user_to_form_notifications
        ]

        if len(new_users) == 0:
            return

        entities = self.graph.get_entities("corpuser", new_users, ["formNotifications"])
        for urn, entity in entities.items():
            user_tuple = entity.get(FormNotificationsClass.ASPECT_NAME, (None, None))
            if user_tuple and user_tuple[0]:
                if not isinstance(user_tuple[0], FormNotificationsClass):
                    logger.error(
                        f"{user_tuple[0]} is not of type FormNotifications for urn: {urn}"
                    )
                else:
                    self.user_to_form_notifications[urn] = user_tuple[0]

    def _get_users_in_group(self, group_urn: str) -> List[str]:
        """
        Using a relationship query, get users inside of a group. Store these users in memory if we've
        already fetched the users for this group.
        """
        if (users_in_group := self.group_to_users_map.get(group_urn)) is not None:
            return users_in_group

        group_member_urns = []
        members = self.graph.get_related_entities(
            group_urn,
            ["IsMemberOfGroup", "IsMemberOfNativeGroup"],
            self.graph.RelationshipDirection.INCOMING,
        )
        member_urns = [member.urn for member in members]
        for member_urn in member_urns:
            if member_urn.startswith(USER_URN_PREFIX):
                group_member_urns.append(member_urn)
            else:
                logger.warning(
                    f"Unexpected group member {member_urn} found in group {group_urn}"
                )
        self.group_to_users_map[group_urn] = group_member_urns

        return group_member_urns

    def _get_verification_form_filter(self, form_urn: str) -> RawSearchFilter:
        return [
            {"and": [{"field": "incompleteForms", "values": [form_urn]}]},
            {
                "and": [
                    {"field": "completedForms", "values": [form_urn]},
                    {"field": "verifiedForms", "values": [form_urn], "negated": True},
                ]
            },
        ]

    def _get_completion_form_filter(self, form_urn: str) -> RawSearchFilter:
        return [{"and": [{"field": "incompleteForms", "values": [form_urn]}]}]

    def _get_incomplete_assets_for_form(
        self, form_urn: str, form_type: str | FormTypeClass
    ) -> RawSearchFilter:
        return (
            self._get_completion_form_filter(form_urn)
            if form_type == FormTypeClass.COMPLETION
            else self._get_verification_form_filter(form_urn)
        )

    def is_form_complete(self, form_urn: str, form_type: str | FormTypeClass) -> int:
        """
        Returns whether this form is complete - meaning no assets have any work left to do for it.
        This takes into account the type of form to know if it's fully complete.
        """
        response = self.execute_graphql_with_retry(
            GRAPHQL_GET_SEARCH_RESULTS_TOTAL,
            variables={
                "count": 0,
                "orFilters": self._get_incomplete_assets_for_form(form_urn, form_type),
            },
        )

        result = response.get("searchAcrossEntities", {})
        total = result.get("total", -1)
        if total < 0:
            logger.warning(
                f"Error evaluating if form with urn {form_urn} is complete. Skipping."
            )
            return True
        else:
            return total == 0

    def get_report(self) -> SourceReport:
        return self.report
