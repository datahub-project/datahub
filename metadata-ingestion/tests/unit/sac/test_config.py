import pytest
from pydantic import ValidationError

from datahub.ingestion.source.sac.sac import SACSourceConfig

_BASE = dict(
    tenant_url="http://tenant",
    token_url="http://tenant.authentication/oauth/token",
    client_id="foo",
    client_secret="bar",
)


@pytest.mark.parametrize(
    "field", ["public_root_folder_name", "private_root_folder_name"]
)
def test_blank_root_folder_name_is_rejected(field: str) -> None:
    # A blank label would rewrite the root to an empty browse-path segment ("/sac//Folder");
    # null (disable canonicalization) is the intended way to opt out, not an empty string.
    with pytest.raises(ValidationError):
        SACSourceConfig(**_BASE, **{field: "   "})


@pytest.mark.parametrize(
    "field", ["public_root_folder_name", "private_root_folder_name"]
)
def test_none_root_folder_name_disables_canonicalization(field: str) -> None:
    config = SACSourceConfig(**_BASE, **{field: None})
    assert getattr(config, field) is None


def test_system_type_mapping_rejects_datasphere() -> None:
    # DWC lineage is resolved from connection_mapping.datasphere_space, not externalId.
    with pytest.raises(ValidationError):
        SACSourceConfig(
            **_BASE,
            system_type_mapping={
                "DWC": {"platform": "sap-datasphere", "external_id_format": "QUERY"}
            },
        )


def test_system_type_mapping_rejects_transient_query_prefix_on_view() -> None:
    with pytest.raises(ValidationError, match="transient_query_prefix"):
        SACSourceConfig(
            **_BASE,
            system_type_mapping={
                "MY_SYSTEM_TYPE": {
                    "platform": "hana",
                    "external_id_format": "VIEW",
                    "transient_query_prefix": "2C",
                }
            },
        )
