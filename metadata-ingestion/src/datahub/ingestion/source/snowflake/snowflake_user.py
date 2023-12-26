import logging
from functools import lru_cache
from typing import Dict, Iterable, List, Optional, Protocol, Set

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_query import (
    SnowflakeQuery,
    SnowflakeQueryExecutor,
)
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeRole,
    SnowflakeUser,
    SnowflakeUserGrant,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeCommonMixin,
    SnowflakeConnectionMixin,
    SnowflakeQueryMixin,
)
from datahub.metadata._schema_classes import OverProvisionedCorpUserClass
from datahub.metadata.schema_classes import CorpUserInfoClass

logger: logging.Logger = logging.getLogger(__name__)


class WorkUnitGenerator(Protocol):
    def gen_work_unit(self) -> Iterable[MetadataWorkUnit]:
        ...


class OverProvisionedUserExtractor(WorkUnitGenerator):
    def __init__(
        self,
        role_map: Dict[str, List[SnowflakeRole]],
        snowflake_users: List[SnowflakeUser],
        snowflake_query_executor: SnowflakeQueryExecutor,
    ):
        self.role_map = role_map
        self.snowflake_users = snowflake_users
        self.snowflake_query_executor = snowflake_query_executor

    @staticmethod
    def _is_over_provisioned(
        role_name: str, user_name: str, role_privilege_set: Set[str]
    ) -> bool:
        return True

    def gen_work_unit(self) -> Iterable[MetadataWorkUnit]:

        logger.info("Generating over provisioned user aspects")

        for user in self.snowflake_users:
            response = self.snowflake_query_executor.execute_query(
                query=SnowflakeQuery.user_grants(user.login_name)
            )

            if response is None:
                logger.debug(
                    f"Response is none. Skipping over-provisioned role determination for {user.display_name}"
                )
                continue

            user_grants: List[SnowflakeUserGrant] = [
                SnowflakeUserGrant.crate(row) for row in response
            ]

            roles: List[str] = []
            for user_grant in user_grants:
                role_list: Optional[List[SnowflakeRole]] = self.role_map.get(
                    builder.make_role_urn(user_grant.role)
                )
                if role_list is None:
                    logger.debug(f"role's privileges not found for {user_grant.role}")
                    continue

                if OverProvisionedUserExtractor._is_over_provisioned(
                    role_name=user_grant.role,
                    user_name=user.login_name,
                    role_privilege_set=set(role.privilege for role in role_list),
                ):
                    roles.append(builder.make_role_urn(user_grant.role))

            if len(roles) > 0:
                yield MetadataChangeProposalWrapper(
                    entityUrn=builder.make_user_urn(user.login_name),
                    aspect=OverProvisionedCorpUserClass(status=True, roles=roles),
                ).as_workunit()


class SnowflakeUserInfoExtractor(
    SnowflakeQueryMixin,
    SnowflakeConnectionMixin,
    SnowflakeCommonMixin,
    WorkUnitGenerator,
):
    def __init__(
        self,
        snowflake_query_executor: SnowflakeQueryExecutor,
    ) -> None:
        self.snowflake_query_executor = snowflake_query_executor
        self.logger = logger

    @lru_cache(maxsize=1)  # maxsize=1: Cache will hold up to 1 most recent results
    def get_snowflake_users(self) -> List[SnowflakeUser]:

        response = self.snowflake_query_executor.execute_query(
            query=SnowflakeQuery.users()
        )

        if response is None:
            return []

        snowflake_users: List[SnowflakeUser] = [
            SnowflakeUser.create(row) for row in response
        ]

        return snowflake_users

    def gen_work_unit(
        self,
    ) -> Iterable[MetadataWorkUnit]:

        self.logger.info("Generating user info aspects")

        yield from [
            MetadataChangeProposalWrapper(
                entityUrn=builder.make_user_urn(snowflake_user.login_name),
                aspect=CorpUserInfoClass(
                    displayName=snowflake_user.display_name,
                    email=snowflake_user.email,
                    active=snowflake_user.disabled,
                ),
            ).as_workunit()
            for snowflake_user in self.get_snowflake_users()
        ]


class SnowflakeUserExtractor:
    def __init__(self, generators: List[WorkUnitGenerator]):
        self.generators = generators

    def get_work_units(self):
        for generator in self.generators:
            yield from generator.gen_work_unit()


def create_extractor(
    snowflake_query_executor: SnowflakeQueryExecutor,
    role_map: Dict[str, List[SnowflakeRole]],
) -> SnowflakeUserExtractor:

    snowflake_user_info_extractor = SnowflakeUserInfoExtractor(
        snowflake_query_executor=snowflake_query_executor,
    )

    over_provisioned_user_extractor = OverProvisionedUserExtractor(
        snowflake_query_executor=snowflake_query_executor,
        role_map=role_map,
        snowflake_users=snowflake_user_info_extractor.get_snowflake_users(),
    )

    return SnowflakeUserExtractor(
        generators=[
            snowflake_user_info_extractor,
            over_provisioned_user_extractor,
        ]
    )
