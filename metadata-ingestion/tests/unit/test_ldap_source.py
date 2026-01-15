from unittest.mock import MagicMock, patch

import ldap as ldap_module
import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.ldap import (
    LDAPSource,
    LDAPSourceConfig,
    logger,
    parse_groups,
    parse_ldap_dn,
    parse_users,
)


@pytest.mark.parametrize(
    "input, expected",
    [
        (
            b"uid=firstname.surname,ou=People,dc=internal,dc=machines",
            "firstname.surname",
        ),
        (
            b"cn=group_name,ou=Groups,dc=internal,dc=machines",
            "group_name",
        ),
        (
            b"cn=comma group (one\\, two\\, three),ou=Groups,dc=internal,dc=machines",
            "comma group (one, two, three)",
        ),
    ],
)
def test_parse_ldap_dn(input, expected):
    assert parse_ldap_dn(input) == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        (
            {
                "admins": [
                    b"uid=A.B,ou=People,dc=internal,dc=machines",
                    b"uid=C.D,ou=People,dc=internal,dc=machines",
                ]
            },
            ["urn:li:corpuser:A.B", "urn:li:corpuser:C.D"],
        ),
        (
            {
                "not_admins": [
                    b"doesntmatter",
                ]
            },
            [],
        ),
    ],
)
def test_parse_users(input, expected):
    assert (
        parse_users(
            input,
            "admins",
        )
        == expected
    )


@pytest.mark.parametrize(
    "input, expected",
    [
        (
            {
                "memberOf": [
                    b"cn=group1,ou=Groups,dc=internal,dc=machines",
                    b"cn=group2,ou=Groups,dc=internal,dc=machines",
                ]
            },
            ["urn:li:corpGroup:group1", "urn:li:corpGroup:group2"],
        ),
        (
            {
                "not_member": [
                    b"doesntmatter",
                ]
            },
            [],
        ),
    ],
)
def test_parse_groups(input, expected):
    assert (
        parse_groups(
            input,
            "memberOf",
        )
        == expected
    )


@pytest.mark.parametrize(
    "tls_verify_value, expected",
    [
        (None, False),  # Default value should be False for backwards compatibility
        (False, False),  # Explicitly set to False
        (True, True),  # Explicitly set to True for secure connections
    ],
)
def test_ldap_config_tls_verify(tls_verify_value, expected):
    """Test that tls_verify config option works correctly."""
    config_dict = {
        "ldap_server": "ldaps://example.com",
        "ldap_user": "cn=admin,dc=example,dc=com",
        "ldap_password": "password",
        "base_dn": "dc=example,dc=com",
    }
    if tls_verify_value is not None:
        config_dict["tls_verify"] = tls_verify_value

    config = LDAPSourceConfig.model_validate(config_dict)
    assert config.tls_verify is expected


def test_ldap_logger_configured():
    """Test that the LDAP source module has a properly configured logger."""
    assert logger is not None
    assert logger.name == "datahub.ingestion.source.ldap"


@patch("datahub.ingestion.source.ldap.ldap")
def test_tls_verify_false_sets_allow_and_logs_warning(mock_ldap):
    """Test that tls_verify=False sets OPT_X_TLS_ALLOW (insecure) and logs warning."""
    mock_ldap.OPT_X_TLS_REQUIRE_CERT = ldap_module.OPT_X_TLS_REQUIRE_CERT
    mock_ldap.OPT_X_TLS_ALLOW = ldap_module.OPT_X_TLS_ALLOW
    mock_ldap.OPT_REFERRALS = ldap_module.OPT_REFERRALS
    mock_ldap.initialize.return_value = MagicMock()

    config = LDAPSourceConfig.model_validate(
        {
            "ldap_server": "ldaps://example.com",
            "ldap_user": "cn=admin,dc=example,dc=com",
            "ldap_password": "password",
            "base_dn": "dc=example,dc=com",
            "tls_verify": False,
        }
    )

    ctx = PipelineContext(run_id="test")

    with patch("datahub.ingestion.source.ldap.logger") as mock_logger:
        LDAPSource(ctx, config)

        # Verify ldap.set_option was called with OPT_X_TLS_ALLOW (insecure)
        mock_ldap.set_option.assert_any_call(
            ldap_module.OPT_X_TLS_REQUIRE_CERT, ldap_module.OPT_X_TLS_ALLOW
        )

        # Verify security warning was logged
        mock_logger.warning.assert_called_once()
        warning_msg = mock_logger.warning.call_args[0][0]
        assert "tls_verify=False" in warning_msg
        assert "Man-in-the-Middle" in warning_msg


@patch("datahub.ingestion.source.ldap.ldap")
def test_tls_verify_true_sets_demand(mock_ldap):
    """Test that tls_verify=True sets OPT_X_TLS_DEMAND (secure) and does not log warning."""
    mock_ldap.OPT_X_TLS_REQUIRE_CERT = ldap_module.OPT_X_TLS_REQUIRE_CERT
    mock_ldap.OPT_X_TLS_DEMAND = ldap_module.OPT_X_TLS_DEMAND
    mock_ldap.OPT_REFERRALS = ldap_module.OPT_REFERRALS
    mock_ldap.initialize.return_value = MagicMock()

    config = LDAPSourceConfig.model_validate(
        {
            "ldap_server": "ldaps://example.com",
            "ldap_user": "cn=admin,dc=example,dc=com",
            "ldap_password": "password",
            "base_dn": "dc=example,dc=com",
            "tls_verify": True,
        }
    )

    ctx = PipelineContext(run_id="test")

    with patch("datahub.ingestion.source.ldap.logger") as mock_logger:
        LDAPSource(ctx, config)

        # Verify ldap.set_option was called with OPT_X_TLS_DEMAND (secure)
        mock_ldap.set_option.assert_any_call(
            ldap_module.OPT_X_TLS_REQUIRE_CERT, ldap_module.OPT_X_TLS_DEMAND
        )

        # Verify NO security warning was logged (secure mode)
        mock_logger.warning.assert_not_called()
