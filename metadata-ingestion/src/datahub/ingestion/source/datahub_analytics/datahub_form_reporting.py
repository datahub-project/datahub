import json
import logging
from datetime import date, datetime
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Optional

import pandas as pd
from pydantic import BaseModel

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import SearchFilterRule
from datahub.metadata.schema_classes import (
    DomainPropertiesClass,
    FormInfoClass,
    FormsClass,
)

logger = logging.getLogger(__name__)


class FormStatus(str, Enum):
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "complete"


class QuestionStatus(str, Enum):
    Assigned = "not_started"
    Completed = "complete"
    In_Progress = "in_progress"


class FormType(str, Enum):
    DOCUMENTATION = "documentation"
    VERIFICATION = "verification"


class FormSnapshotColumns(str, Enum):
    form_id = "form_id"
    form_assigned_date = "form_assigned_date"
    form_completed_date = "form_completed_date"
    form_status = "form_status"
    form_type = "form_type"
    question_id = "question_id"
    question_status = "question_status"
    question_completed_date = "question_completed_date"
    assignee_urn = "assignee_urn"
    asset_urn = "asset_urn"
    platform = "platform"
    platform_instance = "platform_instance"
    domain = "domain"
    subdomain = "subdomain"
    snapshot_date = "snapshot_date"


class FormReportingRow(BaseModel):
    form_id: str
    form_assigned_date: date
    form_completed_date: Optional[date]
    form_status: FormStatus
    form_type: FormType
    assignee_urn: Optional[str]
    asset_urn: str
    platform: str
    platform_instance: str
    domain: Optional[str]
    subdomain: Optional[str]
    asset_verified: Optional[bool]
    question_id: str
    question_status: QuestionStatus
    question_completed_date: Optional[date]
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
        completedFormsIncompletePromptIds: List[str] = []
        completedFormsIncompletePromptResponseTimes: List[str] = []
        completedFormsCompletedPromptIds: List[str] = []
        completedFormsCompletedPromptResponseTimes: List[str] = []
        incompleteFormsIncompletePromptIds: List[str] = []
        incompleteFormsIncompletePromptResponseTimes: List[str] = []
        incompleteFormsCompletedPromptIds: List[str] = []
        incompleteFormsCompletedPromptResponseTimes: List[str] = []
        platform: str = ""
        platformInstance: str = ""
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

    def get_form_existence_or_filters(self) -> List[SearchFilterRule]:
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

    def form_assigned_date(
        self, search_row: DataHubDatasetSearchRow
    ) -> Dict[str, date]:

        form_assigned_dates: Dict[str, date] = {}
        # Since the search row does not contain the form assigned date, we
        # need to calculate it from the raw aspect
        # for form in search_row.incompleteForms:
        #     form_assigned_dates[form] = datetime.now().date() - timedelta(days=365)
        # for form in search_row.completedForms:
        #     form_assigned_dates[form] = datetime.now().date() - timedelta(days=365)
        # return form_assigned_dates
        forms = self.graph.get_aspect(search_row.urn, FormsClass)
        if not forms:
            return form_assigned_dates
        assert forms, f"Forms aspect not found for {search_row.urn}"
        for incomplete_form in forms.incompleteForms:
            form_assigned_dates[incomplete_form.urn] = datetime.fromtimestamp(
                incomplete_form.created.time / 1000 if incomplete_form.created else 0
            ).date()
        for completed_form in forms.completedForms:
            form_assigned_dates[completed_form.urn] = datetime.fromtimestamp(
                completed_form.created.time / 1000 if completed_form.created else 0
            ).date()
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
            assert form_info, f"Form {form} not found"
            form_prompts = [x.id for x in form_info.prompts]
            completed_prompts_map = {
                prompt_id: response_time
                for prompt_id, response_time in zip(
                    search_row.completedFormsCompletedPromptIds,
                    search_row.completedFormsCompletedPromptResponseTimes,
                )
                if prompt_id in form_prompts
            }
            form_completion_dates[form] = datetime.fromtimestamp(
                int(sorted(completed_prompts_map.values())[-1]) / 1000
            ).date()
        return form_completion_dates

    def get_data(
        self,
        on_asset_scanned: Optional[Callable[[str], Any]] = None,
        on_form_scanned: Optional[Callable[[str], Any]] = None,
    ) -> Iterable[FormReportingRow]:
        extra_fields = [f for f in self.DataHubDatasetSearchRow.__fields__.keys()]
        result = self.graph.get_results_by_filter(
            entity_types=["dataset"],
            extra_or_filters=self.get_form_existence_or_filters(),
            extra_source_fields=extra_fields,
        )
        forms_scanned = set()
        row_index = 0
        for row in result:
            # breakpoint()
            row_index += 1
            if row_index % 10 == 0:
                logger.info(f"Scanned {row_index} assets")
            # if row_index % 1000 == 0:
            #     breakpoint()
            extra_properties = row["extraProperties"]

            extra_properties_map = {
                x["name"]: json.loads(x["value"]) for x in extra_properties
            }
            search_row = self.DataHubDatasetSearchRow(**extra_properties_map)
            if on_asset_scanned:
                on_asset_scanned(search_row.urn)
            owners: List[Optional[str]] = search_row.owners or [None]  # type: ignore
            form_assigned_dates = self.form_assigned_date(search_row)
            form_completed_dates = self.form_completed_date(search_row)
            domain = None
            subdomain = None
            if search_row.domains:
                domain_props = self.domain_registry.get_domain(search_row.domains[0])
                assert domain_props, f"Domain {search_row.domains[0]} not found"
                if domain_props.parentDomain:
                    domain = domain_props.parentDomain
                    subdomain = search_row.domains[0]
                else:
                    domain = search_row.domains[0]
                    subdomain = None
            for owner in owners:
                for form_id in search_row.incompleteForms:
                    if form_id not in forms_scanned:
                        if on_form_scanned:
                            on_form_scanned(form_id)
                        forms_scanned.add(form_id)
                    form_info = self.form_registry.get_form(form_id)
                    assert form_info, f"Form {form_id} not found"
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
                    for i, prompt_id in enumerate(
                        [
                            p
                            for p in search_row.incompleteFormsIncompletePromptIds
                            if p in form_prompts
                        ]
                    ):
                        yield FormReportingRow(
                            form_id=form_id,
                            form_assigned_date=form_assigned_dates[form_id],
                            form_completed_date=None,
                            form_status=form_status,
                            form_type=FormType.DOCUMENTATION,
                            assignee_urn=owner,
                            asset_urn=search_row.urn,
                            platform=search_row.platform,
                            platform_instance=search_row.platformInstance,
                            domain=domain,
                            subdomain=subdomain,
                            asset_verified=None,
                            question_id=str(prompt_id),
                            question_status=QuestionStatus.In_Progress,
                            question_completed_date=None,
                            snapshot_date=self.snapshot_date,
                        )
                    for i, (prompt_id, prompt_reponse_time) in enumerate(
                        [
                            (p, p_response_time)
                            for (p, p_response_time) in zip(
                                search_row.incompleteFormsCompletedPromptIds,
                                search_row.incompleteFormsCompletedPromptResponseTimes,
                            )
                            if p in form_prompts
                        ]
                    ):
                        yield FormReportingRow(
                            form_id=form_id,
                            form_assigned_date=form_assigned_dates[form_id],
                            form_completed_date=None,
                            form_status=form_status,
                            form_type=FormType.DOCUMENTATION,
                            assignee_urn=owner,
                            asset_urn=search_row.urn,
                            platform=search_row.platform,
                            platform_instance=search_row.platformInstance,
                            domain=domain,
                            subdomain=subdomain,
                            asset_verified=None,
                            question_id=str(prompt_id),
                            question_status=QuestionStatus.Completed,
                            question_completed_date=prompt_reponse_time,
                            snapshot_date=self.snapshot_date,
                        )
                for form_id in search_row.completedForms:
                    if form_id not in forms_scanned:
                        if on_form_scanned:
                            on_form_scanned(form_id)
                        forms_scanned.add(form_id)
                    form_info = self.form_registry.get_form(form_id)
                    assert form_info, f"Form {form_id} not found"
                    form_prompts = [x.id for x in form_info.prompts]
                    for i, prompt_id in enumerate(
                        [
                            p
                            for p in search_row.completedFormsIncompletePromptIds
                            for p in form_prompts
                        ]
                    ):
                        logger.warning("Unexpected incomplete prompt in completed form")
                        yield FormReportingRow(
                            form_id=form_id,
                            form_assigned_date=form_assigned_dates[form_id],
                            form_completed_date=form_completed_dates[form_id],
                            form_status=FormStatus.COMPLETED,
                            form_type=FormType.DOCUMENTATION,
                            assignee_urn=owner,
                            asset_urn=search_row.urn,
                            platform=search_row.platform,
                            platform_instance=search_row.platformInstance,
                            domain=domain,
                            subdomain=subdomain,
                            asset_verified=None,
                            question_id=str(prompt_id),
                            question_status=QuestionStatus.In_Progress,
                            question_completed_date=None,
                            snapshot_date=self.snapshot_date,
                        )
                    for i, (prompt_id, prompt_reponse_time) in enumerate(
                        [
                            (p, p_response_time)
                            for (p, p_response_time) in zip(
                                search_row.completedFormsCompletedPromptIds,
                                search_row.completedFormsCompletedPromptResponseTimes,
                            )
                            if p in form_prompts
                        ]
                    ):
                        yield FormReportingRow(
                            form_id=form_id,
                            form_assigned_date=form_assigned_dates[form_id],
                            form_completed_date=form_completed_dates[form_id],
                            form_status=FormStatus.COMPLETED,
                            form_type=FormType.DOCUMENTATION,
                            assignee_urn=owner,
                            asset_urn=search_row.urn,
                            platform=search_row.platform,
                            platform_instance=search_row.platformInstance,
                            domain=domain,
                            subdomain=subdomain,
                            asset_verified=None,
                            question_id=str(prompt_id),
                            question_status=QuestionStatus.Completed,
                            question_completed_date=prompt_reponse_time,
                            snapshot_date=self.snapshot_date,
                        )


if __name__ == "__main__":
    from datahub.ingestion.graph.client import get_default_graph

    with get_default_graph() as graph:
        form_data = DataHubFormReportingData(graph)
        for row in form_data.get_data():
            print(row)
