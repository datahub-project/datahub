import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Optional, Tuple

import sqlalchemy
from sqlalchemy import text

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    CorpGroupInfoClass,
    CorpRoleInfoClass,
    CorpUserInfoClass,
    DataPolicyInfoClass,
    DataPolicyTypeClass,
    DatasetKeyClass,
    DictWrapper,
    InheritedRoleClass,
    ResourceIdentifierTypeClass,
    ResourcePrincipalPolicyClass,
    ResourceReferenceClass,
    RoleMembershipClass,
)

logger: logging.Logger = logging.getLogger(__name__)


class DataGovernancePlatform(Enum):
    AWS_REDSHIFT = "redshift"
    SNOWFLAKE = "snowflake"


# class to get config from parent source
@dataclass
class DataGovernanceConfig:
    platform: DataGovernancePlatform
    platform_instance: Optional[str]
    environment: str


class IDataGovernanceAccessor(ABC):
    @abstractmethod
    def generate_work_units(self) -> List[MetadataWorkUnit]:
        pass

    @abstractmethod
    def generate_data_policy_work_unit(
        self, dataset_urn: str
    ) -> List[MetadataWorkUnit]:
        pass


class Query:
    class QueryParameter:
        def __init__(self, connection: sqlalchemy.engine.Connection, query: str):
            self.connection = connection
            self.query = query

    @staticmethod
    def __result_set(query_parameter: QueryParameter) -> Any:
        select_instance = text(query_parameter.query)
        return query_parameter.connection.execute(select_instance)

    @staticmethod
    def all_record(query_parameter: QueryParameter) -> List[dict]:
        result_set = Query.__result_set(query_parameter)
        result = []
        for row in result_set.fetchall():
            result.append(row)

        return result

    @staticmethod
    def single_record(query_parameter: QueryParameter) -> dict:
        result_set = Query.__result_set(query_parameter)
        return result_set.fetchone()


class DataHubEntityBuilder:
    """
    Utility class to build common Datahub entities
    """

    @staticmethod
    def new_user_info(
        corp_user_urn: str, corp_user_info: DictWrapper
    ) -> MetadataChangeProposalWrapper:
        return MetadataChangeProposalWrapper(
            entityType="corpUser",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=corp_user_urn,
            aspectName="corpUserInfo",
            aspect=corp_user_info,
        )

    @staticmethod
    def new_group_info(
        corp_group_urn: str, corp_group_info: DictWrapper
    ) -> MetadataChangeProposalWrapper:
        return MetadataChangeProposalWrapper(
            entityType="corpGroup",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=corp_group_urn,
            aspectName="corpGroupInfo",
            aspect=corp_group_info,
        )

    @staticmethod
    def new_role_info(
        corp_role_urn: str, corp_role_info: DictWrapper
    ) -> MetadataChangeProposalWrapper:
        return MetadataChangeProposalWrapper(
            entityType="corpRole",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=corp_role_urn,
            aspectName="corpRoleInfo",
            aspect=corp_role_info,
        )

    @staticmethod
    def new_role_membership(
        corp_user_urn: str, role_membership: DictWrapper
    ) -> MetadataChangeProposalWrapper:
        return MetadataChangeProposalWrapper(
            entityType="corpUser",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=corp_user_urn,
            aspectName="roleMembership",
            aspect=role_membership,
        )

    @staticmethod
    def new_data_policy_info(
        data_policy_urn: str, data_policy_info: DictWrapper
    ) -> MetadataChangeProposalWrapper:
        return MetadataChangeProposalWrapper(
            entityType="dataPolicy",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=data_policy_urn,
            aspectName="dataPolicyInfo",
            aspect=data_policy_info,
        )

    @staticmethod
    def make_principal_urn(principal_type: str, identifier: str) -> str:
        make_principal_urn_map = {
            "role": builder.make_role_urn,
            "group": builder.make_group_urn,
            "user": builder.make_user_urn,
        }

        return make_principal_urn_map[principal_type](identifier)

    @staticmethod
    def split_data_set_key_name(dataset_key: DatasetKeyClass) -> Tuple:
        database = dataset_key.name.split(".")[0]
        schema = dataset_key.name.split(".")[1]
        table = dataset_key.name.split(".")[2]

        return database, schema, table

    @staticmethod
    def new_inherited_role(
        corp_role_urn: str, parent_role_aspect: DictWrapper
    ) -> MetadataChangeProposalWrapper:
        return MetadataChangeProposalWrapper(
            entityType="corpRole",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=corp_role_urn,
            aspectName="inheritedRole",
            aspect=parent_role_aspect,
        )


class AWSRedshift(IDataGovernanceAccessor):
    USERNAME = "username"
    GROUP_NAME = "group_name"
    ROLE_NAME = "role_name"
    PERMISSION = "permission"
    ADMIN_PERMISSION = "admin_permission"
    IDENTITY_TYPE = "identity_type"
    IDENTITY_NAME = "identity_name"
    USER_ID_LIST = "user_id_list"
    USER_ID = "user_id"
    ROLE_ID = "role_id"
    ID = "id"

    def __init__(self, engine: sqlalchemy.engine.Engine, config: DataGovernanceConfig):
        self._engine = engine
        self._config = config

    def _create_user_mcps(self):
        query = f"""
            SELECT usename as {AWSRedshift.USERNAME},
                   usesysid as {AWSRedshift.ID}
            FROM pg_user
        """

        mcps = []

        with self._engine.connect() as connection:
            users = Query.all_record(
                Query.QueryParameter(connection=connection, query=query)
            )
            logger.info("Number of users = {}".format(len(users)))

        for user in users:
            corp_user_info = CorpUserInfoClass(
                displayName=user[AWSRedshift.USERNAME], active=True
            )
            corp_user_urn = builder.make_user_urn(user[AWSRedshift.ID])
            # Construct a MetadataChangeProposalWrapper object.
            corp_user_mcp = DataHubEntityBuilder.new_user_info(
                corp_user_urn, corp_user_info
            )
            mcps.append(corp_user_mcp)

        return mcps

    def _create_group_mcps(self):
        query = f"""
            SELECT groname as {AWSRedshift.GROUP_NAME},
                   grosysid as {AWSRedshift.ID},
                   grolist as {AWSRedshift.USER_ID_LIST}
            FROM pg_group
        """

        mcps = []

        with self._engine.connect() as connection:
            groups = Query.all_record(
                Query.QueryParameter(connection=connection, query=query)
            )
            logger.info("Number of groups = {}".format(len(groups)))

        for group in groups:
            members = []
            if group[AWSRedshift.USER_ID_LIST] is not None:
                members = [
                    builder.make_user_urn(identifier)
                    for identifier in group[AWSRedshift.USER_ID_LIST]
                ]

            corp_group_info = CorpGroupInfoClass(
                displayName=group[AWSRedshift.GROUP_NAME],
                admins=[],
                members=members,
                groups=[],
            )
            corp_group_urn = builder.make_group_urn(group[AWSRedshift.ID])
            # Construct a MetadataChangeProposalWrapper object.
            mcps.append(
                DataHubEntityBuilder.new_group_info(corp_group_urn, corp_group_info)
            )

        return mcps

    def _create_role_mcps(self):
        query = f"""
            SELECT role_name as {AWSRedshift.ROLE_NAME},
                   role_id as {AWSRedshift.ID}
            FROM svv_roles;
        """

        mcps = []

        with self._engine.connect() as connection:
            roles = Query.all_record(
                Query.QueryParameter(connection=connection, query=query)
            )
            logger.info("Number of roles = {}".format(len(roles)))

        for role in roles:
            corp_role_info = CorpRoleInfoClass(displayName=role[AWSRedshift.ROLE_NAME])
            corp_role_urn = builder.make_role_urn(role[AWSRedshift.ID])
            # Construct a MetadataChangeProposalWrapper object.
            mcps.append(
                DataHubEntityBuilder.new_role_info(corp_role_urn, corp_role_info)
            )

        return mcps

    def _create_role_membership_mcps(self):
        query = f"""
            SELECT user_id as {AWSRedshift.USER_ID},
                   role_id as {AWSRedshift.ROLE_ID}
            FROM SVV_USER_GRANTS
        """
        mcps = []

        with self._engine.connect() as connection:
            role_grants = Query.all_record(
                Query.QueryParameter(connection=connection, query=query)
            )
            logger.info("Number of role_grants = {}".format(len(role_grants)))

        # Arrange roles as per user_id
        user_to_role_map: Any = {}
        for rg in role_grants:
            if rg[AWSRedshift.USER_ID] in user_to_role_map:
                user_to_role_map[rg[AWSRedshift.USER_ID]].append(
                    rg[AWSRedshift.ROLE_ID]
                )
                continue

            user_to_role_map[rg[AWSRedshift.USER_ID]] = [rg[AWSRedshift.ROLE_ID]]

        # Create roleMembership for each user_id present in user_to_role_map
        for user_id in user_to_role_map.keys():
            role_membership = RoleMembershipClass(
                roles=[
                    builder.make_role_urn(role_id)
                    for role_id in user_to_role_map[user_id]
                ]
            )
            # Construct a MetadataChangeProposalWrapper object.
            mcps.append(
                DataHubEntityBuilder.new_role_membership(
                    builder.make_user_urn(user_id), role_membership
                )
            )

        return mcps

    def generate_data_policy_work_unit(
        self, dataset_urn: str
    ) -> List[MetadataWorkUnit]:
        from datahub.emitter import mce_builder

        dataset_key = mce_builder.dataset_urn_to_key(dataset_urn)
        if dataset_key is None:
            return []

        database, schema, table_name = DataHubEntityBuilder.split_data_set_key_name(
            dataset_key=dataset_key
        )
        logger.info("Generating data-policies for dataset {}".format(dataset_urn))
        # Query the permissions for the given dataset
        query = f"""
            SELECT privilege_type as {AWSRedshift.PERMISSION},
                   identity_id as {AWSRedshift.ID},
                   admin_option as {AWSRedshift.ADMIN_PERMISSION},
                   identity_type as {AWSRedshift.IDENTITY_TYPE},
                   identity_name as {AWSRedshift.IDENTITY_NAME}
            FROM SVV_RELATION_PRIVILEGES
            WHERE identity_type in ('user', 'group', 'role') and namespace_name = '{schema}' and relation_name = '{table_name}'
        """

        wrk_units = []

        with self._engine.connect() as connection:
            privileges = Query.all_record(
                Query.QueryParameter(connection=connection, query=query)
            )
            logger.info(
                "Number of privileges for dataset {} = {}".format(
                    dataset_urn, len(privileges)
                )
            )

        for privilege in privileges:
            resource_reference = ResourceReferenceClass(
                type=ResourceIdentifierTypeClass.RESOURCE_URN,
                resourceUrn=dataset_urn,
            )

            resource_principal_policy = ResourcePrincipalPolicyClass(
                principal=DataHubEntityBuilder.make_principal_urn(
                    privilege[AWSRedshift.IDENTITY_TYPE], privilege[AWSRedshift.ID]
                ),
                resourceRef=resource_reference,
                permission=privilege[AWSRedshift.PERMISSION],
                isAllow=True,
            )

            data_policy_info_class = DataPolicyInfoClass(
                type=DataPolicyTypeClass.ResourcePrincipalPolicy,
                resourcePrincipalPolicy=resource_principal_policy,
                displayName="Dataset {} access policy".format(dataset_key.name),
            )

            data_policy_urn = builder.make_data_policy_urn_with_platform_instance(
                platform=self._config.platform.value,
                name="{}#{}#{}#{}".format(
                    dataset_key.name,
                    privilege[AWSRedshift.IDENTITY_TYPE],
                    privilege[AWSRedshift.IDENTITY_NAME],
                    privilege[AWSRedshift.PERMISSION],
                ),
                platform_instance=self._config.platform_instance,
                env=self._config.environment,
            )

            data_policy_mcp = DataHubEntityBuilder.new_data_policy_info(
                data_policy_urn, data_policy_info_class
            )

            wrk_units.append(
                MetadataWorkUnit(
                    id="{}-{}-{}".format(
                        self._config.platform.value,
                        data_policy_mcp.entityUrn,
                        data_policy_mcp.aspectName,
                    ),
                    mcp=data_policy_mcp,
                )
            )

        return wrk_units

    def generate_work_units(self) -> List[MetadataWorkUnit]:
        logging.info(
            "Creating work units for DataGovernance platform {}".format(
                self._config.platform.value
            )
        )
        work_units = []
        mcps = []
        mcps.extend(self._create_user_mcps())
        mcps.extend(self._create_group_mcps())
        mcps.extend(self._create_role_mcps())
        mcps.extend(self._create_role_membership_mcps())

        for mcp in mcps:
            wrk_unit = MetadataWorkUnit(
                id="{}-{}-{}".format(
                    self._config.platform.value, mcp.entityUrn, mcp.aspectName
                ),
                mcp=mcp,
            )
            work_units.append(wrk_unit)

        logging.debug("Exiting from AWSRedshift data-governance platform")
        return work_units


class Snowflake(IDataGovernanceAccessor):
    DISPLAY_NAME = "display_name"
    FIRST_NAME = "first_name"
    LAST_NAME = "last_name"
    EMAIL = "email"
    LOGIN_NAME = "login_name"
    ROLE_NAME = "name"
    GRANTED_ON = "granted_on"
    ROLE = "role"
    TABLE_NAME = "table_name"
    PERMISSION = "permission"
    # In Snowflake only role can be granted the privileges
    IDENTITY_TYPE = "role"

    def __init__(self, engine: sqlalchemy.engine.Engine, config: DataGovernanceConfig):
        self._engine = engine
        self._config = config

    def _create_user_mcps(self):
        query = """
            SHOW USERS
        """

        user_role_membership = """
            SHOW GRANTS TO USER {}
        """

        mcps = []
        users = []
        user_grants = {}
        with self._engine.connect() as connection:
            users = Query.all_record(
                Query.QueryParameter(connection=connection, query=query)
            )
            logger.debug("Number of users = {}".format(len(users)))
            # Capture role granted to the user
            for user in users:
                user_grants[user[Snowflake.LOGIN_NAME]] = Query.all_record(
                    Query.QueryParameter(
                        connection=connection,
                        query=user_role_membership.format(user[Snowflake.LOGIN_NAME]),
                    )
                )

        for user in users:
            corp_user_info = CorpUserInfoClass(
                displayName=user[Snowflake.DISPLAY_NAME],
                firstName=user[Snowflake.FIRST_NAME],
                lastName=user[Snowflake.LAST_NAME],
                email=user[Snowflake.EMAIL],
                active=True,
            )
            corp_user_urn = builder.make_user_urn(user[Snowflake.LOGIN_NAME])
            # Construct a MetadataChangeProposalWrapper object.
            corp_user_mcp = DataHubEntityBuilder.new_user_info(
                corp_user_urn, corp_user_info
            )
            mcps.append(corp_user_mcp)
            # RoleMembership
            corp_role_urns = [
                builder.make_role_urn(role[Snowflake.ROLE])
                for role in user_grants[user[Snowflake.LOGIN_NAME]]
                if role is not None and user[Snowflake.LOGIN_NAME] is not None
            ]
            role_membership = RoleMembershipClass(roles=corp_role_urns)
            mcps.append(
                DataHubEntityBuilder.new_role_membership(corp_user_urn, role_membership)
            )

        return mcps

    def _create_role_mcps(self):
        list_role = """
            SHOW ROLES
        """
        list_role_granted_to = """
            SHOW GRANTS TO ROLE {}
        """

        mcps = []

        with self._engine.connect() as connection:
            roles = Query.all_record(
                Query.QueryParameter(connection=connection, query=list_role)
            )
            logger.info("Number of roles = {}".format(len(roles)))

        for role in roles:
            corp_role_info = CorpRoleInfoClass(displayName=role[Snowflake.ROLE_NAME])
            corp_role_urn = builder.make_role_urn(role[Snowflake.ROLE_NAME])
            # Construct a MetadataChangeProposalWrapper object.
            mcps.append(
                DataHubEntityBuilder.new_role_info(corp_role_urn, corp_role_info)
            )
            # Find out child roles
            grants = []
            with self._engine.connect() as connection:
                grants = Query.all_record(
                    Query.QueryParameter(
                        connection=connection,
                        query=list_role_granted_to.format(role[Snowflake.ROLE_NAME]),
                    )
                )

                logger.debug("RoleName = {}".format(role[Snowflake.ROLE_NAME]))
            # Final if check (i.e. grant[Snowflake.GRANTED_ON] == "ROLE") is important to filter only granted roles
            parent_role_urns = [
                builder.make_role_urn(grant[Snowflake.ROLE_NAME])
                for grant in grants
                if grant is not None
                and grant[Snowflake.ROLE_NAME] is not None
                and grant[Snowflake.GRANTED_ON] == "ROLE"
            ]
            logger.debug(
                "Roles granted to {} = {}".format(corp_role_urn, parent_role_urns)
            )
            parent_role_aspect = InheritedRoleClass(roles=parent_role_urns)
            mcps.append(
                DataHubEntityBuilder.new_inherited_role(
                    corp_role_urn, parent_role_aspect
                )
            )

            parent_role_urns = []

        return mcps

    def generate_data_policy_work_unit(
        self, dataset_urn: str
    ) -> List[MetadataWorkUnit]:
        from datahub.emitter import mce_builder

        dataset_key = mce_builder.dataset_urn_to_key(dataset_urn)
        if dataset_key is None:
            return []

        database, schema, table_name = DataHubEntityBuilder.split_data_set_key_name(
            dataset_key=dataset_key
        )
        logger.info("Generating data-policies for dataset {}".format(dataset_urn))
        # Query the permissions for the given dataset
        query = f"""
            SELECT GRANTEE as {Snowflake.ROLE_NAME},
                   OBJECT_NAME as {Snowflake.TABLE_NAME},
                   PRIVILEGE_TYPE as {Snowflake.PERMISSION},
                   OBJECT_SCHEMA as schema
            FROM  INFORMATION_SCHEMA.OBJECT_PRIVILEGES
            WHERE OBJECT_TYPE = 'TABLE' and table_name = UPPER('{table_name}') and schema = UPPER('{schema}')
        """

        wrk_units = []

        with self._engine.connect() as connection:
            privileges = Query.all_record(
                Query.QueryParameter(connection=connection, query=query)
            )
            logger.debug(
                "Number of privileges for dataset {} = {}".format(
                    dataset_urn, len(privileges)
                )
            )

        for privilege in privileges:
            resource_reference = ResourceReferenceClass(
                type=ResourceIdentifierTypeClass.RESOURCE_URN,
                resourceUrn=dataset_urn,
            )

            resource_principal_policy = ResourcePrincipalPolicyClass(
                principal=DataHubEntityBuilder.make_principal_urn(
                    Snowflake.IDENTITY_TYPE, privilege[Snowflake.ROLE_NAME]
                ),
                resourceRef=resource_reference,
                permission=privilege[Snowflake.PERMISSION],
                isAllow=True,
            )

            data_policy_info_class = DataPolicyInfoClass(
                type=DataPolicyTypeClass.ResourcePrincipalPolicy,
                resourcePrincipalPolicy=resource_principal_policy,
                displayName="Dataset {} access policy".format(dataset_key.name),
            )

            data_policy_urn = builder.make_data_policy_urn_with_platform_instance(
                platform=self._config.platform.value,
                name="{}#{}#{}#{}".format(
                    dataset_key.name,
                    Snowflake.IDENTITY_TYPE,
                    privilege[Snowflake.ROLE_NAME],
                    privilege[Snowflake.PERMISSION],
                ),
                platform_instance=self._config.platform_instance,
                env=self._config.environment,
            )

            data_policy_mcp = DataHubEntityBuilder.new_data_policy_info(
                data_policy_urn, data_policy_info_class
            )

            wrk_units.append(
                MetadataWorkUnit(
                    id="{}-{}-{}".format(
                        self._config.platform.value,
                        data_policy_mcp.entityUrn,
                        data_policy_mcp.aspectName,
                    ),
                    mcp=data_policy_mcp,
                )
            )

        return wrk_units

    def generate_work_units(self) -> List[MetadataWorkUnit]:
        logging.info(
            "Creating work units for DataGovernance platform {}".format(
                self._config.platform.value
            )
        )
        work_units = []
        mcps = []
        mcps.extend(self._create_user_mcps())
        mcps.extend(self._create_role_mcps())
        # mcps.extend(self._create_role_membership_mcps())

        for mcp in mcps:
            wrk_unit = MetadataWorkUnit(
                id="{}-{}-{}".format(
                    self._config.platform.value, mcp.entityUrn, mcp.aspectName
                ),
                mcp=mcp,
            )
            work_units.append(wrk_unit)

        logging.debug("Exiting from Snowflake data-governance platform")
        return work_units


def new_data_governance_source(
    engine: sqlalchemy.engine.Engine, config: DataGovernanceConfig
) -> IDataGovernanceAccessor:
    def new_redshift(
        engine: sqlalchemy.engine.Engine, config: DataGovernanceConfig
    ) -> AWSRedshift:
        """
        create new instance of AWSRedshift DataGovernance source
        """
        return AWSRedshift(engine, config)

    def new_snowflake(
        engine: sqlalchemy.engine.Engine, config: DataGovernanceConfig
    ) -> Snowflake:
        """
        create new instance of Snowflake DataGovernance source
        """
        return Snowflake(engine, config)

    data_governance_creator = {
        DataGovernancePlatform.AWS_REDSHIFT: new_redshift,
        DataGovernancePlatform.SNOWFLAKE: new_snowflake,
    }

    if config.platform not in data_governance_creator.keys():
        raise Exception("Source {} is not supported".format(config.platform))

    return data_governance_creator[config.platform](engine, config)
