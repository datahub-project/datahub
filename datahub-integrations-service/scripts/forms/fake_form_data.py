from enum import Enum
import pathlib
import random
from typing import Any, Dict, Iterable, List, Optional, Tuple
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    StructuredPropertyParamsClass,
    StructuredPropertyDefinitionClass,
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
)
from datahub.metadata.schema_classes import _Aspect

from datahub.emitter.mce_builder import (
    make_dataset_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.filters import SearchFilterRule
from pydantic import BaseModel, validator
from datetime import date, datetime, timedelta, time
from loguru import logger
from progressbar import progressbar
import pandas as pd
from datahub.utilities.urns.urn import Urn
from datahub.metadata.schema_classes import (
    AuditStampClass,
    FormActorAssignmentClass,
    FormInfoClass,
    FormAssociationClass,
    FormPromptAssociationClass,
    FormPromptClass,
    FormPromptFieldAssociationsClass,
    FormPromptTypeClass,
    FormsClass,
    FormTypeClass,
    FormVerificationAssociationClass,
    DynamicFormAssignmentClass,
    PropertyValueClass,
    DomainsClass,
    DomainPropertiesClass,
)
from datahub.api.entities.structuredproperties.structuredproperties import (
    StructuredProperties,
)
from faker import Faker
from datahub.metadata.schema_classes import (
    OwnershipClass,
    OwnerClass,
    OwnershipTypeClass,
)

from datahub.ingestion.source.datahub_reporting.datahub_form_reporting import (
    QuestionStatus,
    FormStatus,
    FormData,
    FormReportingRow,
)

# Some urn generation functions


def make_form_urn(form_id: str | int) -> str:
    if isinstance(form_id, int):
        form_id = str(form_id)
    if form_id.startswith("urn:li:form:"):
        return form_id
    return f"urn:li:form:{form_id}"


def make_domain_urn(domain: str) -> str:
    if domain.startswith("urn:li:domain:"):
        return domain
    return f"urn:li:domain:{domain}"


def make_platform_urn(platform: str) -> str:
    if platform.startswith("urn:li:platform:"):
        return platform
    return f"urn:li:platform:{platform}"


class FakeDataModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True


class FakeAssetData(FakeDataModel):

    def _name_to_urn(self, name: str) -> str:
        # 10% of the time, we have a group instead of a user
        # Commenting this out as this leads to problems downstream
        # if random.randint(0, 100) < 10:
        #     return "urn:li:corpGroup:" + name.replace(" ", "_").lower()
        return "urn:li:corpuser:" + name.replace(" ", "_").lower()

    num_assets: int = 1000000  # 1 million!!!
    num_owners: int = 1000  # 1k owners in the company
    domains_with_subdomains: dict[str, list[str]] = {
        "Finance": ["Accounting", "Tax", "Audit"],
        "HR": ["Recruitment", "Benefits", "Compensation"],
        "Engineering": ["Frontend", "Backend", "DevOps"],
        "Sales": ["Enterprise", "SMB", "Channel"],
        "Marketing": ["Digital", "Content", "Product"],
    }

    data_platforms: list[str] = [
        "snowflake",
        "bigquery",
        "redshift",
        "hive",
        "presto",
        "druid",
        "cassandra",
        "hbase",
        "elasticsearch",
        "kafka",
        "kinesis",
        "s3",
        "gcs",
        "azure_blob",
        "azure_data_lake",
        "teradata",
        "oracle",
        "mysql",
        "mssql",
        "postgresql",
        "sqlite",
        "db2",
        "sybase",
        "informix",
        "netezza",
        "vertica",
        "greenplum",
        "hana",
        "firebird",
        "interbase",
        "access",
        "dynamodb",
    ]
    data_platform_instances: list[str] = [
        "prod",
        "staging",
        "dev",
        "test",
        "qa",
        "sandbox",
        "prod2",
        "prod3",
        "prod4",
        "prod5",
        "prod6",
        "prod7",
        "prod8",
        "prod9",
        "prod10",
        "prod11",
        "prod12",
        "prod13",
        "prod14",
        "prod15",
        "prod16",
        "prod17",
        "prod18",
        "prod19",
        "prod20",
    ]
    random_generator_initialized: bool = False
    steady_fake: Faker = None
    all_owners: list[str] = []
    data_cache = []

    def _initialize_random_generators(self):
        if not self.random_generator_initialized:
            self.steady_fake = Faker()
            self.steady_fake.seed_instance(12345)
            random.seed(12345)
            self.random_generator_initialized = True
            self._initialize_people()

    def _initialize_people(self):
        all_relevant_people = [self.steady_fake.name() for _ in range(self.num_owners)]
        self.all_owners = [self._name_to_urn(x) for x in all_relevant_people]

    def get_data(self) -> Iterable[Dict[str, Any]]:
        if self.data_cache:
            yield from self.data_cache
        else:
            for x in self._get_data_uncached():
                self.data_cache.append(x)
                yield x

    def _get_data_uncached(self) -> Iterable[Dict[str, Any]]:
        self._initialize_random_generators()
        logger.info(f"Generating data for {self.num_assets} assets")
        all_relevant_owners = self.all_owners
        for asset_id in range(self.num_assets):
            # 5% of assets will not have a domain
            if random.randint(0, 100) < 5:
                parent_domain = None
                domain = None
            else:
                # 5% of remaining assets will not have a parent domain
                if random.randint(0, 100) < 5:
                    parent_domain = None
                    domain = random.choice(list(self.domains_with_subdomains.keys()))
                else:
                    parent_domain = random.choice(
                        list(self.domains_with_subdomains.keys())
                    )
                    leaf_domain = random.choice(
                        self.domains_with_subdomains[parent_domain]
                    )
                    domain = f"{parent_domain}.{leaf_domain}"

            platform = random.choice(self.data_platforms)

            # 5% of assets will not have a platform instance
            if random.randint(0, 100) < 5:
                instance = None
            else:
                instance = random.choice(self.data_platform_instances)
            dataset_urn = (
                make_dataset_urn_with_platform_instance(
                    platform=platform,
                    platform_instance=instance,
                    name=f"asset_{asset_id}",
                )
                if instance
                else make_dataset_urn(platform, f"asset_{asset_id}")
            )
            row = {
                "asset_id": asset_id,
                "dataset": dataset_urn,
                "platform": platform,
                "platform_instance": instance,
                "domain": domain,
                "parent_domain": parent_domain,
                "owners": random.choices(all_relevant_owners, k=random.randint(1, 3)),
            }
            yield row

    def to_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        self._initialize_random_generators()
        for domain, subdomains in self.domains_with_subdomains.items():
            yield MetadataChangeProposalWrapper(
                entityUrn=f"urn:li:domain:{domain}",
                aspect=DomainPropertiesClass(
                    name=domain,
                    description=self.steady_fake.text(),
                    parentDomain=None,
                ),
            )
            for subdomain in subdomains:
                yield MetadataChangeProposalWrapper(
                    entityUrn=f"urn:li:domain:{domain}.{subdomain}",
                    aspect=DomainPropertiesClass(
                        name=subdomain,
                        description=self.steady_fake.text(),
                        parentDomain=f"urn:li:domain:{domain}",
                    ),
                )

        for row in self.get_data():
            dataset_urn = row["dataset"]
            dataset_name = row["asset_id"]
            dataset_description = self.steady_fake.text()
            custom_props = {
                "platform": row["platform"],
            }
            if row["platform_instance"]:
                custom_props["platform_instance"] = row["platform_instance"]
            if row["domain"]:
                custom_props["domain"] = row["domain"]
            if row["parent_domain"]:
                custom_props["parent_domain"] = row["parent_domain"]

            aspects = []

            dataset_properties = DatasetPropertiesClass(
                name=str(dataset_name),
                description=dataset_description,
                customProperties=custom_props,
            )
            aspects.append(dataset_properties)

            ownership = OwnershipClass(
                owners=[
                    OwnerClass(owner=owner_urn, type=OwnershipTypeClass.TECHNICAL_OWNER)
                    for owner_urn in row["owners"]
                ]
            )
            aspects.append(ownership)

            if row["domain"]:
                domains = DomainsClass(domains=[f"urn:li:domain:{row['domain']}"])
                aspects.append(domains)
            else:
                domains = DomainsClass(domains=[])
                aspects.append(domains)

            if row["platform_instance"]:
                from datahub.metadata.schema_classes import DataPlatformInstanceClass
                from datahub.emitter.mce_builder import make_dataplatform_instance_urn

                aspects.append(
                    DataPlatformInstanceClass(
                        platform=make_platform_urn(row["platform"]),
                        instance=make_dataplatform_instance_urn(
                            row["platform"], row["platform_instance"]
                        ),
                    )
                )

            for mcp in MetadataChangeProposalWrapper.construct_many(
                entityUrn=dataset_urn,
                aspects=aspects,
            ):
                yield mcp


class FakeFormData(FakeDataModel, FormData):

    num_forms: int = 1  # just doing one form for now
    num_questions: int = 10  # number of questions per form
    num_assigned_assets: int = 1000000  # 1 million!!!
    # default assignment start date is beginning of last year
    earliest_assignment_start_date: date = datetime(
        datetime.now().year - 1, 1, 1
    ).date()
    dataset_verified_percentage: int = (
        95  # 95% of datasets that have all questions completed are verified
    )
    today_date: date = datetime.now().date()
    # questions are filled at a default rate of 10% per month
    question_filling_rate_per_month: int = 10
    previous_form_data: Optional[Dict[str, Any]] = None
    random_generator_initialized: bool = False
    steady_fake: Faker = None
    fake_asset_data: FakeAssetData = None

    @validator("earliest_assignment_start_date")
    def start_date_must_be_before_today(cls, v, values):
        if v > values["today_date"]:
            raise ValueError("start_date must be before today's date")
        return v

    def _initialize_random_generators(self):
        if not self.random_generator_initialized:
            self.steady_fake = Faker()
            self.steady_fake.seed_instance(12345)
            random.seed(12345)
            self.fake_asset_data = FakeAssetData(num_assets=self.num_assigned_assets)
            self.fake_asset_data._initialize_random_generators()
            self.random_generator_initialized = True

    def get_data(self) -> Iterable[FormReportingRow]:
        """
        Generate data in the form of
        Form    Question            Entity  Platform    Domain  Subdomain
        Owner   Dataset Status  Dataset Assigned Date   Dataset Verified Date
        """
        self._initialize_random_generators()
        if not self.previous_form_data:
            yield from self._generate_new_data()
        else:
            yield from self._generate_from_previous_data()

    def _generate_from_previous_data(self) -> Iterable[FormReportingRow]:
        # TODO: add logic to use previous form data to generate new data
        # For now, just generate new data
        yield from self._generate_new_data()

    def get_random_start_date(self) -> datetime:
        now = datetime.now()

        # Define date ranges
        one_year_ago = now - timedelta(days=365)
        one_week_ago = now - timedelta(days=7)
        thirty_days_ago = now - timedelta(days=30)
        ninety_days_ago = now - timedelta(days=90)

        # Define probabilities
        probabilities = [
            (one_week_ago, 0.1),
            (thirty_days_ago, 0.3),
            (ninety_days_ago, 0.5),
        ]

        # Select a random date range based on probabilities
        selected_range = random.choices(
            [one_year_ago] + [x[0] for x in probabilities],
            [0.1] + [x[1] for x in probabilities],
        )[0]

        # Generate random date within the selected range
        random_date = selected_range + timedelta(
            days=random.randint(0, (now - selected_range).days)
        )

        return random_date.date()

    def _generate_new_data(self) -> Iterable[FormReportingRow]:
        fake_asset_data = self.fake_asset_data

        for asset in fake_asset_data.get_data():
            domain = asset["domain"]
            parent_domain = asset["parent_domain"]
            platform = asset["platform"]
            instance = asset["platform_instance"]
            dataset_urn = asset["dataset"]
            for form_id in range(self.num_forms):
                # based on number of months, we can calculate the number of
                # questions that have been filled
                assignment_start_date = self.get_random_start_date()
                form_days_between = (
                    assignment_start_date - self.earliest_assignment_start_date
                ).days

                form_creation_date: date = (
                    self.earliest_assignment_start_date
                    + timedelta(days=random.randint(0, form_days_between))
                )
                months_between = (self.today_date - assignment_start_date).days // 30
                num_questions_filled = (
                    self.num_questions
                    * (months_between * self.question_filling_rate_per_month)
                    // 100
                )
                # 5 percent of the time, we have a form that is not started
                if random.randint(0, 100) < 5:
                    num_questions_filled = 0
                form_status = (
                    FormStatus.COMPLETED
                    if num_questions_filled >= self.num_questions
                    else (
                        FormStatus.IN_PROGRESS
                        if num_questions_filled > 0
                        else FormStatus.NOT_STARTED
                    )
                )
                form_type = random.choices(
                    [FormType.DOCUMENTATION, FormType.VERIFICATION],
                    weights=[50, 50],
                )[0]
                asset_verified = (
                    None
                    if form_type == FormType.DOCUMENTATION
                    else num_questions_filled >= self.num_questions
                    and random.randint(0, 100) < self.dataset_verified_percentage
                )
                for question_id in range(self.num_questions):
                    from datahub.emitter.mce_builder import (
                        make_dataplatform_instance_urn,
                    )

                    # assignment start date is 1st of the month, but a
                    # random month before today
                    row = FormReportingRow(
                        form_urn=make_form_urn(form_id),
                        form_type=form_type,
                        form_assigned_date=assignment_start_date,
                        form_completed_date=None,
                        form_status=form_status,
                        # question-level details
                        question_id=str(question_id),
                        question_status=(
                            QuestionStatus.Completed
                            if question_id < num_questions_filled
                            else None
                        ),
                        question_completed_date=(
                            assignment_start_date
                            + timedelta(
                                days=random.randint(
                                    0, (self.today_date - assignment_start_date).days
                                )
                            )
                            if question_id < num_questions_filled
                            else None
                        ),
                        assignee_urn=None,
                        asset_urn=dataset_urn,
                        platform_urn=make_platform_urn(platform),
                        platform_instance_urn=(
                            make_dataplatform_instance_urn(
                                platform=platform, instance=instance
                            )
                            if instance
                            else None
                        ),
                        domain_urn=make_domain_urn(domain) if domain else None,
                        parent_domain_urn=(
                            make_domain_urn(parent_domain) if parent_domain else None
                        ),
                        snapshot_date=self.today_date,
                        asset_verified=asset_verified,
                    )
                    if num_questions_filled >= self.num_questions:
                        row.form_completed_date = assignment_start_date
                                + timedelta(
                                    days=random.randint(
                                        0,
                                        (self.today_date - assignment_start_date).days,
                                    )
                                )
                    for owner in asset["owners"]:
                        row.assignee_urn = owner
                        yield row

    def get_question_id_and_status(
        self, row: FormReportingRow
    ) -> Tuple[int, QuestionStatus]:
        question_id = row.question_id
        question_status = row.question_status
        question_completed_date = row.question_completed_date
        if question_status == QuestionStatus.Completed:
            return (question_id, QuestionStatus.Completed, question_completed_date)
        else:
            return (question_id, QuestionStatus.In_Progress, question_completed_date)

    def update_current_asset(
        self, current_asset: Tuple[str, Dict[str, _Aspect]], row: FormReportingRow
    ):
        # update current asset information with the latest row
        (form_id, form_status) = (row.form_urn, row.form_status)
        form_urn = make_form_urn(form_id)
        midnight = time(0, 0)
        form_created_auditstamp = AuditStampClass(
            actor="urn:li:corpuser:datahub",
            time=1609459200000,   # A dummy value representing 01-01-2021
        )
        forms_aspect: FormsClass = current_asset[1]["forms"]
        structured_properties_aspect: StructuredPropertiesClass = current_asset[1][
            "structuredProperties"
        ]
        if form_status == FormStatus.COMPLETED:
            form_associations = [
                x for x in forms_aspect.completedForms if x.urn == form_urn
            ]
            if form_associations:
                form_association = form_associations[0]
            else:
                form_association = FormAssociationClass(
                    urn=form_urn, created=form_created_auditstamp
                )
                forms_aspect.completedForms.append(form_association)
        else:
            form_associations = [
                x for x in forms_aspect.incompleteForms if x.urn == form_urn
            ]
            if form_associations:
                form_association = form_associations[0]
            else:
                form_association = FormAssociationClass(
                    urn=form_urn, created=form_created_auditstamp
                )
                forms_aspect.incompleteForms.append(form_association)
        (question_id, completion_status, question_completion_date) = (
            self.get_question_id_and_status(row)
        )
        question_completion_date_in_millis = (
            int(
                datetime.combine(
                    question_completion_date, datetime.min.time()
                ).timestamp()
                * 1000
            )
            if question_completion_date
            else int(datetime.now().timestamp() * 1000)
        )
        assignee_urn = row["assignee_urn"]
        # if the assignee is a group, we don't want to record any activity from them
        if not assignee_urn.startswith("urn:li:corpGroup:"):
            lastModified = AuditStampClass(
                actor=row["assignee_urn"], time=question_completion_date_in_millis
            )
            if completion_status == QuestionStatus.Completed:
                if str(question_id) not in form_association.completedPrompts:
                    form_association.completedPrompts.append(
                        FormPromptAssociationClass(
                            id=str(question_id), lastModified=lastModified
                        )
                    )
                # generate the structured property association
                property_urn = "urn:li:structuredProperty:" + str(question_id)
                existing_property = [
                    x
                    for x in structured_properties_aspect.properties
                    if x.propertyUrn == property_urn
                ]
                if not existing_property:
                    structured_properties_aspect.properties.append(
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn="urn:li:structuredProperty:" + str(question_id),
                            values=[self.steady_fake.text()],
                            created=lastModified,
                            lastModified=lastModified,
                        )
                    )
            else:
                if str(question_id) not in form_association.incompletePrompts:
                    form_association.incompletePrompts.append(
                        FormPromptAssociationClass(
                            id=str(question_id), lastModified=lastModified
                        )
                    )

    def to_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        import time

        self._initialize_random_generators()

        # First generate the people
        logger.info("Generating User MCPS")
        from datahub.metadata.schema_classes import CorpUserInfoClass

        for owner in self.fake_asset_data.all_owners:
            display_name = " ".join(
                [p.capitalize() for p in owner.split(":")[-1].split("_")]
            )
            urn = Urn.create_from_string(owner)
            if urn.entity_type == "corpGroup":
                from datahub.metadata.schema_classes import CorpGroupInfoClass

                display_name = "Group " + display_name
                yield MetadataChangeProposalWrapper(
                    entityUrn=owner,
                    aspect=CorpGroupInfoClass(
                        admins=["urn:li:corpuser:admin"],
                        members=[],
                        groups=[],
                        displayName=display_name,
                    ),
                )
            else:
                yield MetadataChangeProposalWrapper(
                    entityUrn=owner,
                    aspect=CorpUserInfoClass(
                        active=True,
                        displayName=display_name,
                    ),
                )

        logger.info("Generating Dataset MCPS")
        # Then generate the fake assets
        for mcp in self.fake_asset_data.to_mcps():
            yield mcp

        logger.info("Generating Structured Prop and Form Info MCPS")
        # First generate structured properties to back each question
        for prompt_id in range(self.num_questions):
            structuredproperty = StructuredProperties(
                id=str(prompt_id),
                qualified_name="io.datahub.structuredProperty." + str(prompt_id),
                type="string",
                display_name="Prompt " + str(prompt_id),
                description=self.steady_fake.text(),
                entity_types=["dataset"],
                cardinality="SINGLE",
            )
            mcp = MetadataChangeProposalWrapper(
                entityUrn=structuredproperty.urn,
                aspect=StructuredPropertyDefinitionClass(
                    qualifiedName=structuredproperty.fqn,
                    valueType=Urn.make_data_type_urn(structuredproperty.type),
                    displayName=structuredproperty.display_name,
                    description=structuredproperty.description,
                    entityTypes=[
                        Urn.make_entity_type_urn(entity_type)
                        for entity_type in structuredproperty.entity_types or []
                    ],
                    cardinality=structuredproperty.cardinality,
                    allowedValues=(
                        [
                            PropertyValueClass(value=v.value, description=v.description)
                            for v in structuredproperty.allowed_values
                        ]
                        if structuredproperty.allowed_values
                        else None
                    ),
                    typeQualifier=(
                        {
                            "allowedTypes": structuredproperty.type_qualifier.allowed_types
                        }
                        if structuredproperty.type_qualifier
                        else None
                    ),
                ),
            )
            yield mcp

        # Then generate the form mcps
        for form_id in range(self.num_forms):
            form_urn = "urn:li:form:" + str(form_id)
            form_info = FormInfoClass(
                name="Form " + str(form_id),
                description=self.steady_fake.text(),
                type=FormTypeClass.COMPLETION,
                prompts=[
                    FormPromptClass(
                        id=str(prompt_id),
                        title="Prompt " + str(prompt_id),
                        type=FormPromptTypeClass.STRUCTURED_PROPERTY,
                        description=self.steady_fake.text(),
                        required=True,
                        structuredPropertyParams=StructuredPropertyParamsClass(
                            urn="urn:li:structuredProperty:" + str(prompt_id)
                        ),
                    )
                    for prompt_id in range(self.num_questions)
                ],
                actors=FormActorAssignmentClass(
                    owners=True,
                    users=[
                        "urn:li:corpuser:admin",
                        "urn:li:corpuser:shirshanka.das@acryl.io",
                        "urn:li:corpuser:maggie.hays@acryl.io",
                        "urn:li:corpuser:sam.black@acryl.io",
                        "urn:li:corpuser:chris.collins@acryl.io",
                    ],  # admin should always be assigned, so we can see the form
                ),
            )
            yield MetadataChangeProposalWrapper(entityUrn=form_urn, aspect=form_info)

        logger.info("Generating Reporting MCPS")
        # Finally use the reporting data to generate the form association mcps
        current_asset: Tuple[str, Dict[str, _Aspect]] = None
        # we keep track of current asset (dataset) and form association
        urns_with_form_association_yielded = set()
        for row in self.get_data():
            if current_asset and current_asset[0] == row["asset_urn"]:
                # update current asset information with the latest row
                self.update_current_asset(current_asset, row)
            else:
                # yield mcp from current asset
                if current_asset:
                    yield from MetadataChangeProposalWrapper.construct_many(
                        entityUrn=current_asset[0],
                        aspects=[a for a in current_asset[1].values() if a],
                    )
                    urns_with_form_association_yielded.add(current_asset[0])
                # start a new asset
                current_asset = (
                    row["asset_urn"],
                    {
                        "forms": FormsClass(completedForms=[], incompleteForms=[]),
                        "structuredProperties": StructuredPropertiesClass(
                            properties=[]
                        ),
                    },
                )
                self.update_current_asset(current_asset, row)
        # yield mcp from last asset
        if current_asset:
            urns_with_form_association_yielded.add(current_asset[0])
            yield from MetadataChangeProposalWrapper.construct_many(
                entityUrn=current_asset[0],
                aspects=[a for a in current_asset[1].values() if a],
            )
        logger.info(
            f"Yielded {len(urns_with_form_association_yielded)} assets with form associations and structured properties"
        )

    def get_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self.get_data())
