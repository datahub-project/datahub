import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.datapolicy import DataMaskingPolicy
from datahub.metadata.schema_classes import (
    ActorsClass,
    ChangeTypeClass,
    CorpUserInfoClass,
    DataPolicyInfoClass,
    DataPolicyTypeClass,
    DatasetPropertiesClass,
    InheritedRoleClass,
    ResourceIdentifierTypeClass,
    ResourceKeywordClass,
    ResourcePrincipalPolicyClass,
    ResourceReferenceClass,
    RolePropertiesClass,
    RoleUserClass,
)

corp_role_info = RolePropertiesClass(name="analyst")
corp_role_urn = builder.make_role_urn("analyst")

corp_role_soap_analyst_info = RolePropertiesClass(name="db_analyst")
db_analyst_analyst_urn = builder.make_role_urn("db_analyst")
corp_parent_role = InheritedRoleClass(roles=[corp_role_urn])

corp_user_info1 = CorpUserInfoClass(displayName="John", active=True)
corp_user_1_urn = builder.make_user_urn("john@acryldata.io")

corp_user_info2 = CorpUserInfoClass(displayName="David", active=True)
corp_user_2_urn = builder.make_user_urn("david@acryldata.io")

dataset_info = DatasetPropertiesClass(description="sales dataset")
dataset_urn = builder.make_dataset_urn("postgres", "db.public.sale")

actors = ActorsClass(
    users=[RoleUserClass(user=corp_user_1_urn), RoleUserClass(user=corp_user_2_urn)]
)


resource_reference = ResourceReferenceClass(
    type=ResourceIdentifierTypeClass.RESOURCE_URN,
    resourceUrn=dataset_urn,
)

resource_principal_policy = ResourcePrincipalPolicyClass(
    principal=builder.make_role_urn("analyst"),
    resourceRef=resource_reference,
    permission="SELECT",
    isAllow=True,
)

data_policy_info_class = DataPolicyInfoClass(
    type=DataPolicyTypeClass.ResourcePrincipalPolicy,
    resourcePrincipalPolicy=resource_principal_policy,
    displayName="Dataset {} access policy".format("sale"),
)

data_policy_urn = builder.make_data_policy_urn(
    platform="redshift", name="db.public.sale#analyst#SELECT"
)

resource_masking_policy = DataMaskingPolicy(
    resourceRef=resource_reference,
    externalPolicyId="snowflake_policy_1",
    defaultMaskedValue="""
        CREATE OR REPLACE MASKING POLICY email_mask AS (val string) returns string ->
        CASE
            WHEN current_role() IN ('ANALYST') THEN VAL
            ELSE '*********'
        END;
    """,
)

masking_policy_info_class = DataPolicyInfoClass(
    type=DataPolicyTypeClass.DataMaskingPolicy,
    dataMaskingPolicy=resource_masking_policy,
    displayName="Dataset {} masking policy".format("sale"),
)

masking_policy_urn = builder.make_data_policy_urn(
    platform="snowflake", name="email_mask"
)


# Construct a MetadataChangeProposalWrapper object.
role_mcp = MetadataChangeProposalWrapper(
    entityType="role",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=corp_role_urn,
    aspectName="roleProperties",
    aspect=corp_role_info,
)

analyst_role_users = MetadataChangeProposalWrapper(  # role users
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=corp_role_urn,
    aspect=actors,
)

db_analyst_analyst_urn_mcp = MetadataChangeProposalWrapper(
    entityType="role",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=db_analyst_analyst_urn,
    aspectName="roleProperties",
    aspect=corp_role_soap_analyst_info,
)

db_analyst_parent_mcp = MetadataChangeProposalWrapper(
    entityType="role",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=db_analyst_analyst_urn,
    aspectName="inheritedRole",
    aspect=corp_parent_role,
)


corp_user_info1_mcp = MetadataChangeProposalWrapper(
    entityType="corpUser",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=corp_user_1_urn,
    aspectName="corpUserInfo",
    aspect=corp_user_info1,
)

corp_user_info2_mcp = MetadataChangeProposalWrapper(
    entityType="corpUser",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=corp_user_2_urn,
    aspectName="corpUserInfo",
    aspect=corp_user_info2,
)

corp_data_policy_mcp = MetadataChangeProposalWrapper(
    entityType="dataPolicy",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=data_policy_urn,
    aspectName="dataPolicyInfo",
    aspect=data_policy_info_class,
)

dataset_mcp = MetadataChangeProposalWrapper(
    entityType="dataset",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=dataset_urn,
    aspectName="datasetProperties",
    aspect=dataset_info,
)

# Samples data-policy for Special Resource "ALL"

resource_reference_all = ResourceReferenceClass(
    type=ResourceIdentifierTypeClass.SPECIAL,
    special=ResourceKeywordClass.ALL,
)

resource_principal_policy_all = ResourcePrincipalPolicyClass(
    principal=builder.make_role_urn("analyst"),
    resourceRef=resource_reference_all,
    permission="UPDATE",
    isAllow=True,
)

data_policy_info_class_all = DataPolicyInfoClass(
    type=DataPolicyTypeClass.ResourcePrincipalPolicy,
    resourcePrincipalPolicy=resource_principal_policy_all,
    displayName="Update on ALL resources",
)
data_policy_urn_all = builder.make_data_policy_urn(
    platform="redshift", name="db.public.sale#analyst#UPDATE"
)

corp_data_policy_mcp_all = MetadataChangeProposalWrapper(
    entityType="dataPolicy",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=data_policy_urn_all,
    aspectName="dataPolicyInfo",
    aspect=data_policy_info_class_all,
)

data_masking_policy_mcp = MetadataChangeProposalWrapper(
    entityType="dataPolicy",
    entityUrn=masking_policy_urn,
    aspect=masking_policy_info_class,
)
# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

mcps = [
    dataset_mcp,
    role_mcp,
    corp_user_info1_mcp,
    corp_user_info2_mcp,
    corp_data_policy_mcp,
    corp_data_policy_mcp_all,
    db_analyst_analyst_urn_mcp,
    db_analyst_parent_mcp,
    analyst_role_users,
    data_masking_policy_mcp,
]

for mcp in mcps:
    print(f"Emitting {mcp.entityUrn}")
    emitter.emit_mcp(mcp)

print("Emission completed...")
