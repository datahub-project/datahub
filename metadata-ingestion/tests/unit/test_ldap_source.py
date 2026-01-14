import pytest

from datahub.ingestion.source.ldap import (
    LDAPSourceConfig,
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
