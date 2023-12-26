import logging
from typing import Callable, Dict, Iterable, List, Optional

from snowflake.connector import SnowflakeConnection

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake import snowflake_user
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_query import (
    SnowflakeQuery,
    SnowflakeQueryExecutor,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDatabase,
    SnowflakeRole,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeCommonMixin,
    SnowflakeConnectionMixin,
    SnowflakeQueryMixin,
)
from datahub.metadata.schema_classes import (
    DataPolicyInfoClass,
    DataPolicyTypeClass,
    ResourceIdentifierTypeClass,
    ResourcePrincipalPolicyClass,
    ResourceReferenceClass,
    RolePropertiesClass,
)

logger: logging.Logger = logging.getLogger(__name__)


def make_role_map(
    roles: List[SnowflakeRole],
) -> Dict[str, List[SnowflakeRole]]:
    # OBJECT_PRIVILEGES table contains entry for each privilege assign to a Role
    # For example if Role as three privileges (SELECT, INSERT, DELETE) then there
    # would be three rows in OBJECT_PRIVILEGES
    role_map: Dict[str, List[SnowflakeRole]] = {}
    for role in roles:
        role_urn = builder.make_role_urn(f"{role.name}")

        if role_map.get(role_urn) is None:
            role_map[role_urn] = [role]
            continue

        role_map[role_urn].append(role)

    return role_map


class SnowflakeDataPolicyExtractor(
    SnowflakeQueryMixin,
    SnowflakeConnectionMixin,
    SnowflakeCommonMixin,
):
    def __init__(
        self,
        config: SnowflakeV2Config,
        report: SnowflakeV2Report,
        dataset_urn_builder: Callable[[str], str],
        datapolicy_urn_builder: Callable[[str], str],
    ) -> None:
        self.config = config
        self.report = report
        self.dataset_urn_builder = dataset_urn_builder
        self.datapolicy_urn_builder = datapolicy_urn_builder
        self.logger = logger

    def generate_workunits(
        self, role_map: Dict[str, List[SnowflakeRole]]
    ) -> Iterable[MetadataWorkUnit]:

        policy_count: int = 0  # variable to print number of policy generated
        # Generate ResourcePrincipalPolicy
        for role_urn in role_map:
            for role in role_map[role_urn]:

                dataset_identifier: str = self.get_dataset_identifier(
                    table_name=role.dataset_name,
                    schema_name=role.schema_name,
                    db_name=role.database_name,
                )

                datapolicy_identifier = self.get_datapolicy_identifier(
                    dataset_identifier=dataset_identifier,
                    role_name=role.name,
                    privilege=role.privilege,
                )

                dataset_urn: str = self.dataset_urn_builder(dataset_identifier)

                datapolicy_urn: str = self.datapolicy_urn_builder(datapolicy_identifier)

                resource_reference = ResourceReferenceClass(
                    type=ResourceIdentifierTypeClass.RESOURCE_URN,
                    resourceUrn=dataset_urn,
                )

                resource_principal_policy = ResourcePrincipalPolicyClass(
                    principal=role_urn,
                    resourceRef=resource_reference,
                    permission=role.privilege,
                    isAllow=True,
                )

                data_policy_info_class = DataPolicyInfoClass(
                    type=DataPolicyTypeClass.ResourcePrincipalPolicy,
                    resourcePrincipalPolicy=resource_principal_policy,
                )

                policy_count = policy_count + 1

                yield MetadataChangeProposalWrapper(
                    entityUrn=datapolicy_urn,
                    aspect=data_policy_info_class,
                ).as_workunit()

        self.logger.info(f"{policy_count} ResourcePrincipalPolicy are ingested")


class SnowflakeRoleExtractor:
    """
    Extracts Role metadata from Snowflake.
    """

    def generate_workunits(
        self, role_map: Dict[str, List[SnowflakeRole]]
    ) -> Iterable[MetadataWorkUnit]:

        for role_urn in role_map:

            role: SnowflakeRole = role_map[role_urn][
                0
            ]  # take single role as all subsequent are same

            yield MetadataChangeProposalWrapper(
                entityUrn=role_urn, aspect=RolePropertiesClass(name=role.name)
            ).as_workunit()


class SnowflakeAccessControl(
    SnowflakeQueryMixin, SnowflakeConnectionMixin, SnowflakeCommonMixin
):
    def __init__(
        self,
        config: SnowflakeV2Config,
        report: SnowflakeV2Report,
        snowflake_query_executor: SnowflakeQueryExecutor,
        dataset_urn_builder: Callable[[str], str],
        datapolicy_urn_builder: Callable[[str], str],
    ) -> None:
        self.config = config
        self.report = report
        self.dataset_urn_builder = dataset_urn_builder
        self.datapolicy_urn_builder = datapolicy_urn_builder
        self.report = report
        self.logger = logger
        self.connection: Optional[SnowflakeConnection] = None
        self.snowflake_query_executor = snowflake_query_executor

    def get_workunits(
        self, databases: List[SnowflakeDatabase]
    ) -> Iterable[MetadataWorkUnit]:

        self.logger.debug("Extracting access control metadata")

        response = self.snowflake_query_executor.execute_query(
            query=SnowflakeQuery.role_privileges(
                [database.name for database in databases]
            )
        )

        if response is None:
            self.logger.debug(
                "Skipping access control metadata extraction as role not available"
            )
            return

        roles: List[SnowflakeRole] = [SnowflakeRole.create(row) for row in response]

        role_map: Dict[str, List[SnowflakeRole]] = make_role_map(roles)

        yield from SnowflakeRoleExtractor().generate_workunits(role_map=role_map)

        yield from SnowflakeDataPolicyExtractor(
            config=self.config,
            report=self.report,
            dataset_urn_builder=self.dataset_urn_builder,
            datapolicy_urn_builder=self.datapolicy_urn_builder,
        ).generate_workunits(role_map=role_map)

        yield from snowflake_user.create_extractor(
            snowflake_query_executor=self.snowflake_query_executor,
            role_map=role_map,
        ).get_work_units()
