import json
import logging
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Optional

import pandas as pd
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RawSearchFilterRule
from datahub.metadata.schema_classes import (
    DomainPropertiesClass,
    FormAssociationClass,
    FormInfoClass,
    FormsClass,
    FormStateClass,
    FormTypeClass,
)
from pydantic import BaseModel

from acryl_datahub_cloud.elasticsearch.graph_service import BaseModelRow

logger = logging.getLogger(__name__)


class FormStatus(str, Enum):
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "complete"


class QuestionStatus(str, Enum):
    NOT_STARTED = "not_started"
    COMPLETED = "complete"


class FormType(str, Enum):
    DOCUMENTATION = "documentation"
    VERIFICATION = "verification"


class FormReportingRow(BaseModelRow):
    form_urn: str
    form_type: FormType
    form_assigned_date: Optional[date]
    form_completed_date: Optional[date]
    form_status: FormStatus
    question_id: str
    question_status: QuestionStatus
    question_completed_date: Optional[date]
    assignee_urn: Optional[str]
    asset_urn: str
    platform_urn: str
    platform_instance_urn: Optional[str]
    domain_urn: Optional[str]
    parent_domain_urn: Optional[str]
    asset_verified: Optional[bool]
    snapshot_date: date


class FormData:
    def get_data(
        self,
        on_asset_scanned: Optional[Callable[[str], Any]] = None,
        on_form_scanned: Optional[Callable[[str], Any]] = None,
    ) -> Iterable[FormReportingRow]:
        raise NotImplementedError

    def to_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        raise NotImplementedError


class FormRegistry:
    def __init__(self, graph: DataHubGraph):
        self.registry: Dict[str, Optional[FormInfoClass]] = {}
        self.graph = graph

    def get_form(self, form_id: str) -> Optional[FormInfoClass]:
        if form_id not in self.registry:
            self._load_form(form_id)
        return self.registry[form_id]

    def _load_form(self, form_id: str) -> None:
        form_urn = (
            f"urn:li:form:{form_id}"
            if not form_id.startswith("urn:li:form:")
            else form_id
        )

        form_info = self.graph.get_aspect(form_urn, FormInfoClass)
        self.registry[form_id] = form_info


class DomainRegistry:
    def __init__(self, graph: DataHubGraph):
        self.registry: Dict[str, Optional[DomainPropertiesClass]] = {}
        self.graph = graph

    def get_domain(self, domain_id: str) -> Optional[DomainPropertiesClass]:
        if domain_id not in self.registry:
            self._load_domain(domain_id)
        return self.registry[domain_id]

    def _load_domain(self, domain_id: str) -> None:
        domain_urn = (
            f"urn:li:domain:{domain_id}"
            if not domain_id.startswith("urn:li:domain:")
            else domain_id
        )
        domain_props = self.graph.get_aspect(domain_urn, DomainPropertiesClass)
        self.registry[domain_id] = domain_props


class DataHubFormReportingData(FormData):
    class DataHubDatasetSearchRow(BaseModel):
        urn: str
        owners: List[str] = []
        completedForms: List[str] = []
        incompleteForms: List[str] = []
        verifiedForms: List[str] = []
        completedFormsIncompletePromptIds: List[str] = []
        completedFormsIncompletePromptResponseTimes: List[str] = []
        completedFormsCompletedPromptIds: List[str] = []
        completedFormsCompletedPromptResponseTimes: List[str] = []
        incompleteFormsIncompletePromptIds: List[str] = []
        incompleteFormsIncompletePromptResponseTimes: List[str] = []
        incompleteFormsCompletedPromptIds: List[str] = []
        incompleteFormsCompletedPromptResponseTimes: List[str] = []
        platform: str = ""
        platformInstance: Optional[str] = None
        domains: List[str] = []

    def __init__(self, graph: DataHubGraph, allowed_forms: Optional[List[str]] = None):
        self.graph: DataHubGraph = graph
        self.form_registry = FormRegistry(graph)
        self.domain_registry = DomainRegistry(graph)
        self.snapshot_date = datetime.now().date()
        self.allowed_forms = allowed_forms

    def get_dataframe(
        self,
        on_asset_scanned: Callable[[str], Any],
        on_form_scanned: Callable[[str], Any],
    ) -> pd.DataFrame:
        return pd.DataFrame(
            x.dict()
            for x in self.get_data(
                on_asset_scanned=on_asset_scanned, on_form_scanned=on_form_scanned
            )
        )

    def get_form_existence_or_filters(self) -> List[RawSearchFilterRule]:
        """
        Datasets must either have completedForms or incompleteForms assigned to
        them
        """
        if self.allowed_forms:
            return [
                {
                    "field": "completedForms",
                    "condition": "EQUAL",
                    "values": self.allowed_forms,
                },
                {
                    "field": "incompleteForms",
                    "condition": "EQUAL",
                    "values": self.allowed_forms,
                },
            ]
        else:
            return [
                {
                    "field": "completedForms",
                    "condition": "EXISTS",
                },
                {
                    "field": "incompleteForms",
                    "condition": "EXISTS",
                },
            ]

    def is_published(self, form_urn: str) -> bool:
        form_info = self.form_registry.get_form(form_urn)
        return (
            form_info.status.state == FormStateClass.PUBLISHED if form_info else False
        )

    def form_published_time(self, form_urn: str) -> float:
        form_info = self.form_registry.get_form(form_urn)
        is_published = (
            form_info.status.state == FormStateClass.PUBLISHED if form_info else False
        )
        return (
            form_info.status.lastModified.time / 1000
            if form_info and form_info.status.lastModified and is_published
            else 0
        )

    def assigned_to_asset_time(self, form_association: FormAssociationClass) -> float:
        return form_association.created.time / 1000 if form_association.created else 0

    def assignment_time(self, form_association: FormAssociationClass) -> float:
        published_time = self.form_published_time(form_association.urn)
        assigned_to_asset_time = self.assigned_to_asset_time(form_association)
        return max(published_time, assigned_to_asset_time)

    # For a given asset, the assigned date is the more recent of the published date and the time this was actually assigned.
    # Assets can be assigned before publishing, but we don't want to show that date because it's not open to the public yet.
    # Assets can also be assigned after publishing, so we should show that date for those assets.
    def form_assigned_date(
        self, search_row: DataHubDatasetSearchRow
    ) -> Dict[str, Optional[date]]:
        form_assigned_dates: Dict[str, Optional[date]] = {}
        forms = self.graph.get_aspect(search_row.urn, FormsClass)
        if not forms:
            return form_assigned_dates
        assert forms, f"Forms aspect not found for {search_row.urn}"
        for incomplete_form in forms.incompleteForms:
            is_published = self.is_published(incomplete_form.urn)
            assignment_time = self.assignment_time(incomplete_form)
            form_assigned_dates[incomplete_form.urn] = (
                datetime.fromtimestamp(
                    assignment_time,
                    tz=timezone.utc,
                ).date()
                if is_published and assignment_time != 0
                else None
            )
        for completed_form in forms.completedForms:
            is_published = self.is_published(completed_form.urn)
            assignment_time = self.assignment_time(completed_form)
            form_assigned_dates[completed_form.urn] = (
                datetime.fromtimestamp(
                    assignment_time,
                    tz=timezone.utc,
                ).date()
                if is_published and assignment_time != 0
                else None
            )
        return form_assigned_dates

    def form_completed_date(
        self, search_row: DataHubDatasetSearchRow
    ) -> Dict[str, date]:
        # get the maximum timestamp from the
        # incompleteFormsCompletedPromptResponseTimes and
        # completedFormsCompletedPromptResponseTimes
        form_completion_dates = {}
        for form in search_row.completedForms:
            form_info = self.form_registry.get_form(form)
            if not form_info:
                logger.warning(f"Found form attached that does not exist: {form}")
                continue
            form_prompts = [x.id for x in form_info.prompts]
            completed_prompts_map = {
                prompt_id: response_time
                for prompt_id, response_time in zip(
                    search_row.completedFormsCompletedPromptIds,
                    search_row.completedFormsCompletedPromptResponseTimes,
                )
                if prompt_id in form_prompts
            }
            sorted_dates = sorted(completed_prompts_map.values())
            if sorted_dates:
                form_completion_dates[form] = datetime.fromtimestamp(
                    int(sorted(completed_prompts_map.values())[-1]) / 1000,
                    tz=timezone.utc,
                ).date()
            else:
                logger.warning(
                    f"Form {form} on asset {search_row.urn} has no completed prompts but is marked as completed"
                )
        return form_completion_dates

    def get_assignees(self, form_info: FormInfoClass, owners: List[str]) -> List[str]:
        assignees = []
        actors = form_info.actors
        if actors.owners:
            # form applies to owners
            assignees.extend(owners)
        if actors.users:
            assignees.extend(actors.users)
        if actors.groups:
            assignees.extend(actors.groups)
        return list(set(assignees))

    def get_data(  # noqa: C901
        self,
        on_asset_scanned: Optional[Callable[[str], Any]] = None,
        on_form_scanned: Optional[Callable[[str], Any]] = None,
    ) -> Iterable[FormReportingRow]:
        extra_fields = [f for f in self.DataHubDatasetSearchRow.__fields__.keys()]
        result = self.graph.get_results_by_filter(
            extra_or_filters=self.get_form_existence_or_filters(),
            extra_source_fields=extra_fields,
            skip_cache=True,
        )
        forms_scanned = set()
        row_index = 0
        for row in result:
            row_index += 1
            if row_index % 100 == 0:
                logger.info(f"Scanned {row_index} assets")
            extra_properties = row["extraProperties"]

            extra_properties_map = {
                x["name"]: json.loads(x["value"]) for x in extra_properties
            }
            search_row = self.DataHubDatasetSearchRow(**extra_properties_map)
            if on_asset_scanned:
                on_asset_scanned(search_row.urn)
            form_assigned_dates = self.form_assigned_date(search_row)
            form_completed_dates = self.form_completed_date(search_row)
            domain = None
            parent_domain = None
            if search_row.domains:
                domain = search_row.domains[0]
                domain_props = self.domain_registry.get_domain(domain)
                if not domain_props:
                    logger.warning(
                        f"Domain {search_row.domains[0]} not found. Will record the domain urn regardless"
                    )
                else:
                    if domain_props.parentDomain:
                        parent_domain = domain_props.parentDomain
            # for owner in owners:
            incomplete_forms = (
                [x for x in search_row.incompleteForms if x in self.allowed_forms]
                if self.allowed_forms
                else search_row.incompleteForms
            )
            for form_id in incomplete_forms:
                if form_id not in forms_scanned:
                    if on_form_scanned:
                        on_form_scanned(form_id)
                    forms_scanned.add(form_id)
                form_info = self.form_registry.get_form(form_id)
                if not form_info:
                    logger.warning(
                        f"Found form attached that does not exist: {form_id}"
                    )
                    continue
                form_prompts = [x.id for x in form_info.prompts]
                form_incomplete_prompts = [
                    p
                    for p in search_row.incompleteFormsIncompletePromptIds
                    if p in form_prompts
                ]
                form_status = (
                    FormStatus.IN_PROGRESS
                    if len(form_incomplete_prompts) < len(form_prompts)
                    else FormStatus.NOT_STARTED
                )
                form_type = (
                    FormType.DOCUMENTATION
                    if form_info.type == FormTypeClass.COMPLETION
                    else FormType.VERIFICATION
                )
                assignees = self.get_assignees(form_info, search_row.owners)
                for prompt_id in [
                    p
                    for p in search_row.incompleteFormsIncompletePromptIds
                    if p in form_prompts
                ]:
                    for owner in assignees:
                        yield FormReportingRow(
                            form_urn=form_id,
                            form_assigned_date=form_assigned_dates[form_id],
                            form_completed_date=None,
                            form_status=form_status,
                            form_type=form_type,
                            assignee_urn=owner,
                            asset_urn=search_row.urn,
                            platform_urn=search_row.platform,
                            platform_instance_urn=search_row.platformInstance,
                            domain_urn=domain,
                            parent_domain_urn=parent_domain,
                            asset_verified=(
                                False if form_type == FormType.VERIFICATION else None
                            ),
                            question_id=str(prompt_id),
                            question_status=QuestionStatus.NOT_STARTED,
                            question_completed_date=None,
                            snapshot_date=self.snapshot_date,
                        )
                for prompt_id, prompt_response_time in [
                    (p, p_response_time)
                    for (p, p_response_time) in zip(
                        search_row.incompleteFormsCompletedPromptIds,
                        search_row.incompleteFormsCompletedPromptResponseTimes,
                    )
                    if p in form_prompts
                ]:
                    for owner in assignees:
                        yield FormReportingRow(
                            form_urn=form_id,
                            form_assigned_date=form_assigned_dates[form_id],
                            form_completed_date=None,
                            form_status=form_status,
                            form_type=form_type,
                            assignee_urn=owner,
                            asset_urn=search_row.urn,
                            platform_urn=search_row.platform,
                            platform_instance_urn=search_row.platformInstance,
                            domain_urn=domain,
                            parent_domain_urn=parent_domain,
                            asset_verified=(
                                False if form_type == FormType.VERIFICATION else None
                            ),
                            question_id=str(prompt_id),
                            question_status=QuestionStatus.COMPLETED,
                            question_completed_date=datetime.fromtimestamp(
                                float(prompt_response_time) / 1000, tz=timezone.utc
                            ),
                            snapshot_date=self.snapshot_date,
                        )
            complete_forms = (
                [x for x in search_row.completedForms if x in self.allowed_forms]
                if self.allowed_forms
                else search_row.completedForms
            )
            verified_forms_list = (
                [x for x in search_row.verifiedForms if x in self.allowed_forms]
                if self.allowed_forms
                else search_row.verifiedForms
            )
            verified_forms = set(verified_forms_list)
            for form_id in complete_forms:
                if form_id not in forms_scanned:
                    if on_form_scanned:
                        on_form_scanned(form_id)
                    forms_scanned.add(form_id)
                form_info = self.form_registry.get_form(form_id)
                if not form_info:
                    logger.warning(
                        f"Found form attached that does not exist: {form_id}"
                    )
                    continue
                form_type = (
                    FormType.DOCUMENTATION
                    if form_info.type == FormTypeClass.COMPLETION
                    else FormType.VERIFICATION
                )
                is_verification_form = form_type == FormType.VERIFICATION
                is_form_verified = form_id in verified_forms
                form_status = (
                    FormStatus.COMPLETED
                    if is_form_verified or not is_verification_form
                    else FormStatus.IN_PROGRESS
                )
                assignees = self.get_assignees(form_info, search_row.owners)
                form_prompts = [x.id for x in form_info.prompts]
                for prompt_id in [
                    p
                    for p in search_row.completedFormsIncompletePromptIds
                    if p in form_prompts
                ]:
                    for owner in assignees:
                        yield FormReportingRow(
                            form_urn=form_id,
                            form_assigned_date=form_assigned_dates[form_id],
                            form_completed_date=form_completed_dates.get(
                                form_id, form_assigned_dates[form_id]
                            ),
                            form_status=form_status,
                            form_type=form_type,
                            assignee_urn=owner,
                            asset_urn=search_row.urn,
                            platform_urn=search_row.platform,
                            platform_instance_urn=search_row.platformInstance,
                            domain_urn=domain,
                            parent_domain_urn=parent_domain,
                            asset_verified=(
                                True
                                if is_verification_form and is_form_verified
                                else None
                            ),
                            question_id=str(prompt_id),
                            question_status=QuestionStatus.NOT_STARTED,
                            question_completed_date=None,
                            snapshot_date=self.snapshot_date,
                        )
                for prompt_id, prompt_response_time in [
                    (p, p_response_time)
                    for (p, p_response_time) in zip(
                        search_row.completedFormsCompletedPromptIds,
                        search_row.completedFormsCompletedPromptResponseTimes,
                    )
                    if p in form_prompts
                ]:
                    for owner in assignees:
                        yield FormReportingRow(
                            form_urn=form_id,
                            form_assigned_date=form_assigned_dates[form_id],
                            form_completed_date=form_completed_dates.get(
                                form_id, form_assigned_dates[form_id]
                            ),
                            form_status=form_status,
                            form_type=form_type,
                            assignee_urn=owner,
                            asset_urn=search_row.urn,
                            platform_urn=search_row.platform,
                            platform_instance_urn=search_row.platformInstance,
                            domain_urn=domain,
                            parent_domain_urn=parent_domain,
                            asset_verified=(
                                True
                                if is_verification_form and is_form_verified
                                else None
                            ),
                            question_id=str(prompt_id),
                            question_status=QuestionStatus.COMPLETED,
                            question_completed_date=datetime.fromtimestamp(
                                float(prompt_response_time) / 1000, tz=timezone.utc
                            ),
                            snapshot_date=self.snapshot_date,
                        )


if __name__ == "__main__":
    from datahub.ingestion.graph.client import get_default_graph

    with get_default_graph() as graph:
        form_data = DataHubFormReportingData(graph)
        for row in form_data.get_data():
            print(row)
