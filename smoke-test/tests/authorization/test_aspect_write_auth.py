"""
E2E authorization smoke tests for logicalParent, data product membership,
and data product rename via updateName.
"""

import logging
import uuid
from pathlib import Path

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DomainPropertiesClass,
    DomainsClass,
    MySqlDDLClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)
from tests.consistency_utils import wait_for_writes_to_sync
from tests.privileges.utils import (
    clear_polices,
    create_metadata_policy,
    create_user,
    is_graphql_auth_denied,
    remove_policy,
    remove_user,
    set_base_platform_privileges_policy_status,
    set_view_dataset_sensitive_info_policy_status,
    set_view_entity_profile_privileges_policy_status,
    wait_until_graphql_auth_denied,
)
from tests.utils import (
    get_frontend_session,
    get_frontend_url,
    login_as,
    with_test_retry,
)

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.no_cypress_suite1, pytest.mark.global_policy_mutator]

_UNIQUE = uuid.uuid4().hex[:8]
TEST_USER_EMAIL = f"aspect.auth.test.{_UNIQUE}@smoke.datahub.test"
TEST_USER_URN = f"urn:li:corpuser:{TEST_USER_EMAIL}"
TEST_USER_PASSWORD = "user"

TARGET_DATASET_URN = (
    f"urn:li:dataset:(urn:li:dataPlatform:kafka,auth-target-{_UNIQUE},PROD)"
)
PARENT_DATASET_URN = (
    f"urn:li:dataset:(urn:li:dataPlatform:kafka,auth-parent-{_UNIQUE},PROD)"
)
PHYSICAL_SCHEMA_FIELD_URN = f"urn:li:schemaField:({TARGET_DATASET_URN},col1)"
LOGICAL_SCHEMA_FIELD_URN = f"urn:li:schemaField:({PARENT_DATASET_URN},col1)"
KAFKA_PLATFORM_URN = "urn:li:dataPlatform:kafka"
MEMBER_DATASET_URN = (
    f"urn:li:dataset:(urn:li:dataPlatform:kafka,auth-member-{_UNIQUE},PROD)"
)
CROSS_DOMAIN_MEMBER_DATASET_URN = (
    f"urn:li:dataset:(urn:li:dataPlatform:kafka,auth-cross-member-{_UNIQUE},PROD)"
)
ASSET_SIDE_ADD_DATASET_URN = (
    f"urn:li:dataset:(urn:li:dataPlatform:kafka,auth-asset-add-{_UNIQUE},PROD)"
)
ASSET_SIDE_REMOVE_DATASET_URN = (
    f"urn:li:dataset:(urn:li:dataPlatform:kafka,auth-asset-remove-{_UNIQUE},PROD)"
)
TEST_DOMAIN_URN = f"urn:li:domain:auth-domain-{_UNIQUE}"
CROSS_DOMAIN_URN = f"urn:li:domain:auth-cross-domain-{_UNIQUE}"

SET_LOGICAL_PARENT_MUTATION = """
mutation setLogicalParent($input: SetLogicalParentInput!) {
  setLogicalParent(input: $input)
}
"""

BATCH_SET_DATA_PRODUCT_MUTATION = (
    Path(__file__).resolve().parents[1]
    / "dataproduct"
    / "queries"
    / "setassets_dataproduct.graphql"
).read_text()

BATCH_UNSET_DATA_PRODUCT_MUTATION = """
mutation batchUnsetDataProduct($resourceUrns: [String!]!) {
  batchSetDataProduct(input: { dataProductUrn: null, resourceUrns: $resourceUrns })
}
"""

BATCH_ADD_TO_DATA_PRODUCTS_MUTATION = """
mutation batchAddToDataProducts($dataProductUrns: [String!]!, $resourceUrns: [String!]!) {
  batchAddToDataProducts(
    input: { dataProductUrns: $dataProductUrns, resourceUrns: $resourceUrns }
  )
}
"""

BATCH_REMOVE_FROM_DATA_PRODUCTS_MUTATION = """
mutation batchRemoveFromDataProducts($dataProductUrns: [String!]!, $resourceUrns: [String!]!) {
  batchRemoveFromDataProducts(
    input: { dataProductUrns: $dataProductUrns, resourceUrns: $resourceUrns }
  )
}
"""

UPDATE_NAME_MUTATION = """
mutation updateName($input: UpdateNameInput!) {
  updateName(input: $input)
}
"""

CREATE_DATA_PRODUCT_MUTATION = (
    Path(__file__).resolve().parents[1]
    / "dataproduct"
    / "queries"
    / "add_dataproduct.graphql"
).read_text()

DATA_PRODUCT_URN: str = ""


def _schema_metadata_with_field(field_path: str = "col1") -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="auth_test_schema",
        platform=KAFKA_PLATFORM_URN,
        version=0,
        hash="",
        platformSchema=MySqlDDLClass(tableSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath=field_path,
                nativeDataType="string",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            )
        ],
    )


ASPECT_WRITE_POLICY_PREFIXES = ["Test EDIT_ENTITY", "Test MANAGE_DATA_PRODUCTS"]


@pytest.fixture(scope="module", autouse=True)
def auth_test_setup(graph_client, auth_session):
    yield from _auth_test_setup_impl(graph_client, auth_session)


def _auth_test_setup_impl(graph_client, auth_session):
    global DATA_PRODUCT_URN
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=TARGET_DATASET_URN,
            aspect=DatasetPropertiesClass(
                name=f"auth-target-{_UNIQUE}",
                description="Target dataset for logicalParent auth test",
            ),
        )
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=PARENT_DATASET_URN,
            aspect=DatasetPropertiesClass(
                name=f"auth-parent-{_UNIQUE}",
                description="Parent dataset for logicalParent auth test",
            ),
        )
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=TARGET_DATASET_URN,
            aspect=_schema_metadata_with_field(),
        )
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=PARENT_DATASET_URN,
            aspect=_schema_metadata_with_field(),
        )
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=MEMBER_DATASET_URN,
            aspect=DatasetPropertiesClass(
                name=f"auth-member-{_UNIQUE}",
                description="Member dataset for data product auth test",
            ),
        )
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=MEMBER_DATASET_URN,
            aspect=DomainsClass(domains=[TEST_DOMAIN_URN]),
        )
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=CROSS_DOMAIN_MEMBER_DATASET_URN,
            aspect=DatasetPropertiesClass(
                name=f"auth-cross-member-{_UNIQUE}",
                description="Cross-domain member dataset for data product auth test",
            ),
        )
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=CROSS_DOMAIN_MEMBER_DATASET_URN,
            aspect=DomainsClass(domains=[CROSS_DOMAIN_URN]),
        )
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=ASSET_SIDE_ADD_DATASET_URN,
            aspect=DatasetPropertiesClass(
                name=f"auth-asset-add-{_UNIQUE}",
                description="Dataset for asset-side add auth test",
            ),
        )
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=ASSET_SIDE_REMOVE_DATASET_URN,
            aspect=DatasetPropertiesClass(
                name=f"auth-asset-remove-{_UNIQUE}",
                description="Dataset for asset-side remove auth test",
            ),
        )
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=ASSET_SIDE_REMOVE_DATASET_URN,
            aspect=DomainsClass(domains=[TEST_DOMAIN_URN]),
        )
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=TEST_DOMAIN_URN,
            aspect=DomainPropertiesClass(
                name=f"Auth Domain {_UNIQUE}",
                description="Domain for data product auth test",
            ),
        )
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=CROSS_DOMAIN_URN,
            aspect=DomainPropertiesClass(
                name=f"Auth Cross Domain {_UNIQUE}",
                description="Second domain for cross-domain data product auth test",
            ),
        )
    )
    wait_for_writes_to_sync()

    create_result = graph_client.execute_graphql(
        CREATE_DATA_PRODUCT_MUTATION,
        {
            "domainUrn": TEST_DOMAIN_URN,
            "name": f"Auth Data Product {_UNIQUE}",
            "description": "Data product for membership auth test",
        },
    )
    DATA_PRODUCT_URN = create_result["createDataProduct"]["urn"]
    wait_for_writes_to_sync()

    admin_session = get_frontend_session()
    clear_polices(admin_session, name_prefixes=ASPECT_WRITE_POLICY_PREFIXES)
    set_base_platform_privileges_policy_status("INACTIVE", admin_session)
    set_view_dataset_sensitive_info_policy_status("INACTIVE", admin_session)
    set_view_entity_profile_privileges_policy_status("INACTIVE", admin_session)
    wait_for_writes_to_sync()

    admin_session = create_user(admin_session, TEST_USER_EMAIL, TEST_USER_PASSWORD)
    yield

    remove_user(admin_session, TEST_USER_URN)
    clear_polices(admin_session, name_prefixes=ASPECT_WRITE_POLICY_PREFIXES)
    set_base_platform_privileges_policy_status("ACTIVE", admin_session)
    set_view_dataset_sensitive_info_policy_status("ACTIVE", admin_session)
    set_view_entity_profile_privileges_policy_status("ACTIVE", admin_session)
    wait_for_writes_to_sync()

    for urn in [
        TARGET_DATASET_URN,
        PARENT_DATASET_URN,
        MEMBER_DATASET_URN,
        CROSS_DOMAIN_MEMBER_DATASET_URN,
        ASSET_SIDE_ADD_DATASET_URN,
        ASSET_SIDE_REMOVE_DATASET_URN,
        TEST_DOMAIN_URN,
        CROSS_DOMAIN_URN,
        DATA_PRODUCT_URN,
    ]:
        try:
            graph_client.hard_delete_entity(urn=urn)
        except Exception:
            logger.warning("Failed to delete %s during cleanup", urn)


@with_test_retry(max_attempts=10)
def _post_graphql_as_user(email: str, password: str, payload: dict) -> dict:
    user_session = login_as(email, password)
    response = user_session.post(f"{get_frontend_url()}/api/v2/graphql", json=payload)
    response.raise_for_status()
    return response.json()


def _assert_graphql_auth_denied(res: dict) -> None:
    assert is_graphql_auth_denied(res), f"Expected authorization failure, got: {res}"


def _data_product_rename_probe_payload() -> dict:
    return {
        "query": UPDATE_NAME_MUTATION,
        "variables": {
            "input": {
                "urn": DATA_PRODUCT_URN,
                "name": f"Policy cache probe {_UNIQUE}",
            }
        },
    }


def _wait_until_data_product_rename_denied() -> None:
    wait_until_graphql_auth_denied(
        lambda: _post_graphql_as_user(
            TEST_USER_EMAIL,
            TEST_USER_PASSWORD,
            _data_product_rename_probe_payload(),
        ),
        description="data product rename denial for test user",
    )


def _schema_field_logical_parent_payload() -> dict:
    return {
        "query": SET_LOGICAL_PARENT_MUTATION,
        "variables": {
            "input": {
                "resourceUrn": PHYSICAL_SCHEMA_FIELD_URN,
                "parentUrn": LOGICAL_SCHEMA_FIELD_URN,
            }
        },
    }


def _wait_until_schema_field_logical_parent_denied() -> None:
    wait_until_graphql_auth_denied(
        lambda: _post_graphql_as_user(
            TEST_USER_EMAIL,
            TEST_USER_PASSWORD,
            _schema_field_logical_parent_payload(),
        ),
        description="schema field setLogicalParent denial for test user",
    )


def _prepare_denied_data_product_rename_tests(admin_session) -> None:
    clear_polices(admin_session, name_prefixes=ASPECT_WRITE_POLICY_PREFIXES)
    wait_for_writes_to_sync()
    _wait_until_data_product_rename_denied()


def _prepare_schema_field_denied_tests(admin_session) -> None:
    clear_polices(admin_session, name_prefixes=ASPECT_WRITE_POLICY_PREFIXES)
    wait_for_writes_to_sync()
    _wait_until_schema_field_logical_parent_denied()


def test_set_logical_parent_denied_without_edit_entity_on_target():
    """Cannot set logicalParent without EDIT_ENTITY on the target asset."""
    payload = {
        "query": SET_LOGICAL_PARENT_MUTATION,
        "variables": {
            "input": {
                "resourceUrn": TARGET_DATASET_URN,
                "parentUrn": PARENT_DATASET_URN,
            }
        },
    }
    res = _post_graphql_as_user(TEST_USER_EMAIL, TEST_USER_PASSWORD, payload)
    _assert_graphql_auth_denied(res)


def test_set_logical_parent_denied_without_edit_entity_on_parent():
    """Cannot set logicalParent without EDIT_ENTITY on the proposed parent."""
    admin_session = get_frontend_session()
    policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test EDIT_ENTITY target only {_UNIQUE}",
        description="Grant EDIT_ENTITY on target dataset only",
        privileges=["EDIT_ENTITY"],
        user_urn=TEST_USER_URN,
        resource_urn=TARGET_DATASET_URN,
    )
    wait_for_writes_to_sync()

    payload = {
        "query": SET_LOGICAL_PARENT_MUTATION,
        "variables": {
            "input": {
                "resourceUrn": TARGET_DATASET_URN,
                "parentUrn": PARENT_DATASET_URN,
            }
        },
    }
    res = _post_graphql_as_user(TEST_USER_EMAIL, TEST_USER_PASSWORD, payload)
    _assert_graphql_auth_denied(res)

    remove_policy(policy_urn, admin_session)


def test_set_logical_parent_allowed_with_edit_entity_on_target_and_parent(auth_session):
    """SetLogicalParent succeeds when user has EDIT_ENTITY on child and parent."""
    admin_session = get_frontend_session()
    target_policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test EDIT_ENTITY target {_UNIQUE}",
        description="Grant EDIT_ENTITY on target dataset",
        privileges=["EDIT_ENTITY"],
        user_urn=TEST_USER_URN,
        resource_urn=TARGET_DATASET_URN,
    )
    parent_policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test EDIT_ENTITY parent {_UNIQUE}",
        description="Grant EDIT_ENTITY on parent dataset",
        privileges=["EDIT_ENTITY"],
        user_urn=TEST_USER_URN,
        resource_urn=PARENT_DATASET_URN,
    )
    wait_for_writes_to_sync()

    payload = {
        "query": SET_LOGICAL_PARENT_MUTATION,
        "variables": {
            "input": {
                "resourceUrn": TARGET_DATASET_URN,
                "parentUrn": PARENT_DATASET_URN,
            }
        },
    }
    res = _post_graphql_as_user(TEST_USER_EMAIL, TEST_USER_PASSWORD, payload)
    assert res.get("data", {}).get("setLogicalParent") is True, res

    remove_policy(target_policy_urn, admin_session)
    remove_policy(parent_policy_urn, admin_session)
    wait_for_writes_to_sync()


def test_set_logical_parent_schema_field_denied_without_edit_entity_on_logical_dataset():
    """Schema field logicalParent denied when only the physical dataset is authorized."""
    admin_session = get_frontend_session()
    _prepare_schema_field_denied_tests(admin_session)
    policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test EDIT_ENTITY physical dataset only {_UNIQUE}",
        description="Grant EDIT_ENTITY on physical dataset only for schema field link",
        privileges=["EDIT_ENTITY"],
        user_urn=TEST_USER_URN,
        resource_urn=TARGET_DATASET_URN,
    )
    wait_for_writes_to_sync()

    res = _post_graphql_as_user(
        TEST_USER_EMAIL,
        TEST_USER_PASSWORD,
        _schema_field_logical_parent_payload(),
    )
    _assert_graphql_auth_denied(res)

    remove_policy(policy_urn, admin_session)


def test_set_logical_parent_schema_field_denied_without_edit_entity_on_physical_dataset():
    """Schema field logicalParent denied when only the logical dataset is authorized."""
    admin_session = get_frontend_session()
    _prepare_schema_field_denied_tests(admin_session)
    policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test EDIT_ENTITY logical dataset only {_UNIQUE}",
        description="Grant EDIT_ENTITY on logical dataset only for schema field link",
        privileges=["EDIT_ENTITY"],
        user_urn=TEST_USER_URN,
        resource_urn=PARENT_DATASET_URN,
    )
    wait_for_writes_to_sync()

    res = _post_graphql_as_user(
        TEST_USER_EMAIL,
        TEST_USER_PASSWORD,
        _schema_field_logical_parent_payload(),
    )
    _assert_graphql_auth_denied(res)

    remove_policy(policy_urn, admin_session)


def test_set_logical_parent_schema_field_allowed_via_dataset_policies_only(
    auth_session,
):
    """Schema field logicalParent succeeds with EDIT_ENTITY on physical and logical datasets."""
    admin_session = get_frontend_session()
    physical_policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test EDIT_ENTITY physical dataset schema field {_UNIQUE}",
        description="Grant EDIT_ENTITY on physical dataset for schema field delegation",
        privileges=["EDIT_ENTITY"],
        user_urn=TEST_USER_URN,
        resource_urn=TARGET_DATASET_URN,
    )
    logical_policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test EDIT_ENTITY logical dataset schema field {_UNIQUE}",
        description="Grant EDIT_ENTITY on logical dataset for schema field delegation",
        privileges=["EDIT_ENTITY"],
        user_urn=TEST_USER_URN,
        resource_urn=PARENT_DATASET_URN,
    )
    wait_for_writes_to_sync()

    payload = {
        "query": SET_LOGICAL_PARENT_MUTATION,
        "variables": {
            "input": {
                "resourceUrn": PHYSICAL_SCHEMA_FIELD_URN,
                "parentUrn": LOGICAL_SCHEMA_FIELD_URN,
            }
        },
    }
    res = _post_graphql_as_user(TEST_USER_EMAIL, TEST_USER_PASSWORD, payload)
    assert res.get("data", {}).get("setLogicalParent") is True, res

    remove_policy(physical_policy_urn, admin_session)
    remove_policy(logical_policy_urn, admin_session)


def test_set_logical_parent_schema_field_allowed_with_mixed_dataset_and_field_grants(
    auth_session,
):
    """Schema field link allows dataset grant on child and schema-field grant on parent."""
    admin_session = get_frontend_session()
    physical_policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test EDIT_ENTITY physical dataset mixed {_UNIQUE}",
        description="Grant EDIT_ENTITY on physical dataset only",
        privileges=["EDIT_ENTITY"],
        user_urn=TEST_USER_URN,
        resource_urn=TARGET_DATASET_URN,
    )
    logical_field_policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test EDIT_ENTITY logical schema field mixed {_UNIQUE}",
        description="Grant EDIT_ENTITY on logical schema field only",
        privileges=["EDIT_ENTITY"],
        user_urn=TEST_USER_URN,
        resource_urn=LOGICAL_SCHEMA_FIELD_URN,
    )
    wait_for_writes_to_sync()

    payload = {
        "query": SET_LOGICAL_PARENT_MUTATION,
        "variables": {
            "input": {
                "resourceUrn": PHYSICAL_SCHEMA_FIELD_URN,
                "parentUrn": LOGICAL_SCHEMA_FIELD_URN,
            }
        },
    }
    res = _post_graphql_as_user(TEST_USER_EMAIL, TEST_USER_PASSWORD, payload)
    assert res.get("data", {}).get("setLogicalParent") is True, res

    remove_policy(physical_policy_urn, admin_session)
    remove_policy(logical_field_policy_urn, admin_session)


def test_batch_set_data_product_denied_with_asset_side_privilege_only(auth_session):
    """BatchSetDataProduct requires MANAGE_DATA_PRODUCTS on domain, not asset-side privilege."""
    admin_session = get_frontend_session()
    policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test EDIT_ENTITY_DATA_PRODUCTS member {_UNIQUE}",
        description="Grant asset-side data product privilege only",
        privileges=["EDIT_ENTITY_DATA_PRODUCTS"],
        user_urn=TEST_USER_URN,
        resource_urn=MEMBER_DATASET_URN,
    )
    wait_for_writes_to_sync()

    payload = {
        "query": BATCH_SET_DATA_PRODUCT_MUTATION,
        "variables": {
            "dataProductUrn": DATA_PRODUCT_URN,
            "resourceUrns": [MEMBER_DATASET_URN],
        },
    }
    res = _post_graphql_as_user(TEST_USER_EMAIL, TEST_USER_PASSWORD, payload)
    _assert_graphql_auth_denied(res)

    remove_policy(policy_urn, admin_session)


def test_batch_set_data_product_allowed_with_manage_data_products_on_domain(
    auth_session,
):
    """BatchSetDataProduct succeeds with MANAGE_DATA_PRODUCTS on the data product domain."""
    admin_session = get_frontend_session()
    policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test MANAGE_DATA_PRODUCTS domain {_UNIQUE}",
        description="Grant MANAGE_DATA_PRODUCTS on domain",
        privileges=["MANAGE_DATA_PRODUCTS"],
        user_urn=TEST_USER_URN,
        resource_urn=TEST_DOMAIN_URN,
    )
    wait_for_writes_to_sync()

    payload = {
        "query": BATCH_SET_DATA_PRODUCT_MUTATION,
        "variables": {
            "dataProductUrn": DATA_PRODUCT_URN,
            "resourceUrns": [MEMBER_DATASET_URN],
        },
    }
    res = _post_graphql_as_user(TEST_USER_EMAIL, TEST_USER_PASSWORD, payload)
    assert res.get("data", {}).get("batchSetDataProduct") is True, res

    remove_policy(policy_urn, admin_session)


def test_batch_set_data_product_allowed_cross_domain_with_manage_on_product_domain(
    auth_session,
):
    """Product-side manage on product domain allows linking assets in a different domain."""
    admin_session = get_frontend_session()
    policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test MANAGE_DATA_PRODUCTS product domain only {_UNIQUE}",
        description="Grant MANAGE_DATA_PRODUCTS on product domain only",
        privileges=["MANAGE_DATA_PRODUCTS"],
        user_urn=TEST_USER_URN,
        resource_urn=TEST_DOMAIN_URN,
    )
    wait_for_writes_to_sync()

    payload = {
        "query": BATCH_SET_DATA_PRODUCT_MUTATION,
        "variables": {
            "dataProductUrn": DATA_PRODUCT_URN,
            "resourceUrns": [CROSS_DOMAIN_MEMBER_DATASET_URN],
        },
    }
    res = _post_graphql_as_user(TEST_USER_EMAIL, TEST_USER_PASSWORD, payload)
    assert res.get("data", {}).get("batchSetDataProduct") is True, res

    remove_policy(policy_urn, admin_session)


def test_batch_add_to_data_products_allowed_with_asset_side_privilege_only(
    auth_session,
):
    """Asset profile add succeeds with EDIT_ENTITY_DATA_PRODUCTS on the asset."""
    admin_session = get_frontend_session()
    policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test EDIT_ENTITY_DATA_PRODUCTS add {_UNIQUE}",
        description="Grant asset-side data product privilege for add",
        privileges=["EDIT_ENTITY_DATA_PRODUCTS"],
        user_urn=TEST_USER_URN,
        resource_urn=ASSET_SIDE_ADD_DATASET_URN,
    )
    wait_for_writes_to_sync()

    payload = {
        "query": BATCH_ADD_TO_DATA_PRODUCTS_MUTATION,
        "variables": {
            "dataProductUrns": [DATA_PRODUCT_URN],
            "resourceUrns": [ASSET_SIDE_ADD_DATASET_URN],
        },
    }
    res = _post_graphql_as_user(TEST_USER_EMAIL, TEST_USER_PASSWORD, payload)
    assert res.get("data", {}).get("batchAddToDataProducts") is True, res

    remove_policy(policy_urn, admin_session)


def _seed_data_product_membership(graph_client, dataset_urn: str) -> None:
    graph_client.execute_graphql(
        BATCH_SET_DATA_PRODUCT_MUTATION,
        {
            "dataProductUrn": DATA_PRODUCT_URN,
            "resourceUrns": [dataset_urn],
        },
    )
    wait_for_writes_to_sync()


def test_batch_unset_data_product_allowed_with_asset_side_privilege_only(
    auth_session, graph_client
):
    """Asset profile remove succeeds with EDIT_ENTITY_DATA_PRODUCTS on the asset."""
    _seed_data_product_membership(graph_client, ASSET_SIDE_REMOVE_DATASET_URN)

    admin_session = get_frontend_session()
    asset_policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test EDIT_ENTITY_DATA_PRODUCTS unset {_UNIQUE}",
        description="Grant asset-side data product privilege for unset",
        privileges=["EDIT_ENTITY_DATA_PRODUCTS"],
        user_urn=TEST_USER_URN,
        resource_urn=ASSET_SIDE_REMOVE_DATASET_URN,
    )
    wait_for_writes_to_sync()

    payload = {
        "query": BATCH_UNSET_DATA_PRODUCT_MUTATION,
        "variables": {"resourceUrns": [ASSET_SIDE_REMOVE_DATASET_URN]},
    }
    res = _post_graphql_as_user(TEST_USER_EMAIL, TEST_USER_PASSWORD, payload)
    assert res.get("data", {}).get("batchSetDataProduct") is True, res

    remove_policy(asset_policy_urn, admin_session)


def test_batch_remove_from_data_products_allowed_with_asset_side_privilege_only(
    auth_session, graph_client
):
    """BatchRemoveFromDataProducts succeeds with EDIT_ENTITY_DATA_PRODUCTS on the asset."""
    _seed_data_product_membership(graph_client, ASSET_SIDE_REMOVE_DATASET_URN)

    admin_session = get_frontend_session()
    asset_policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test EDIT_ENTITY_DATA_PRODUCTS remove {_UNIQUE}",
        description="Grant asset-side data product privilege for remove",
        privileges=["EDIT_ENTITY_DATA_PRODUCTS"],
        user_urn=TEST_USER_URN,
        resource_urn=ASSET_SIDE_REMOVE_DATASET_URN,
    )
    wait_for_writes_to_sync()

    payload = {
        "query": BATCH_REMOVE_FROM_DATA_PRODUCTS_MUTATION,
        "variables": {
            "dataProductUrns": [DATA_PRODUCT_URN],
            "resourceUrns": [ASSET_SIDE_REMOVE_DATASET_URN],
        },
    }
    res = _post_graphql_as_user(TEST_USER_EMAIL, TEST_USER_PASSWORD, payload)
    assert res.get("data", {}).get("batchRemoveFromDataProducts") is True, res

    remove_policy(asset_policy_urn, admin_session)


def test_update_data_product_name_denied_without_privilege():
    """UpdateName requires MANAGE_DATA_PRODUCTS on domain or EDIT_ENTITY on product."""
    admin_session = get_frontend_session()
    _prepare_denied_data_product_rename_tests(admin_session)

    payload = {
        "query": UPDATE_NAME_MUTATION,
        "variables": {
            "input": {
                "urn": DATA_PRODUCT_URN,
                "name": f"Denied rename {_UNIQUE}",
            }
        },
    }
    res = _post_graphql_as_user(TEST_USER_EMAIL, TEST_USER_PASSWORD, payload)
    _assert_graphql_auth_denied(res)


def test_update_data_product_name_denied_with_asset_side_privilege_only(auth_session):
    """UpdateName does not accept asset-side EDIT_ENTITY_DATA_PRODUCTS privilege."""
    admin_session = get_frontend_session()
    policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test EDIT_ENTITY_DATA_PRODUCTS rename {_UNIQUE}",
        description="Grant asset-side data product privilege only",
        privileges=["EDIT_ENTITY_DATA_PRODUCTS"],
        user_urn=TEST_USER_URN,
        resource_urn=MEMBER_DATASET_URN,
    )
    wait_for_writes_to_sync()

    payload = {
        "query": UPDATE_NAME_MUTATION,
        "variables": {
            "input": {
                "urn": DATA_PRODUCT_URN,
                "name": f"Denied asset-side rename {_UNIQUE}",
            }
        },
    }
    res = _post_graphql_as_user(TEST_USER_EMAIL, TEST_USER_PASSWORD, payload)
    _assert_graphql_auth_denied(res)

    remove_policy(policy_urn, admin_session)


def test_update_data_product_name_allowed_with_manage_data_products_on_domain(
    auth_session,
):
    """UpdateName succeeds with MANAGE_DATA_PRODUCTS on the data product domain."""
    admin_session = get_frontend_session()
    policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test MANAGE_DATA_PRODUCTS rename {_UNIQUE}",
        description="Grant MANAGE_DATA_PRODUCTS on domain for rename",
        privileges=["MANAGE_DATA_PRODUCTS"],
        user_urn=TEST_USER_URN,
        resource_urn=TEST_DOMAIN_URN,
    )
    wait_for_writes_to_sync()

    payload = {
        "query": UPDATE_NAME_MUTATION,
        "variables": {
            "input": {
                "urn": DATA_PRODUCT_URN,
                "name": f"Renamed via manage {_UNIQUE}",
            }
        },
    }
    res = _post_graphql_as_user(TEST_USER_EMAIL, TEST_USER_PASSWORD, payload)
    assert res.get("data", {}).get("updateName") is True, res

    remove_policy(policy_urn, admin_session)


def test_update_data_product_name_allowed_with_edit_entity_on_data_product(
    auth_session,
):
    """UpdateName succeeds with EDIT_ENTITY on the data product itself."""
    admin_session = get_frontend_session()
    policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test EDIT_ENTITY data product rename {_UNIQUE}",
        description="Grant EDIT_ENTITY on data product for rename",
        privileges=["EDIT_ENTITY"],
        user_urn=TEST_USER_URN,
        resource_urn=DATA_PRODUCT_URN,
    )
    wait_for_writes_to_sync()

    payload = {
        "query": UPDATE_NAME_MUTATION,
        "variables": {
            "input": {
                "urn": DATA_PRODUCT_URN,
                "name": f"Renamed via edit entity {_UNIQUE}",
            }
        },
    }
    res = _post_graphql_as_user(TEST_USER_EMAIL, TEST_USER_PASSWORD, payload)
    assert res.get("data", {}).get("updateName") is True, res

    remove_policy(policy_urn, admin_session)
