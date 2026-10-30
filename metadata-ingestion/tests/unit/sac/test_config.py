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
