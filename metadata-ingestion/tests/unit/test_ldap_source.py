import pytest


@pytest.mark.integration
def test_strip_ldap_info():
    from datahub.ingestion.source.ldap import strip_ldap_info

    assert (
        strip_ldap_info(b"uid=firstname.surname,ou=People,dc=internal,dc=machines")
        == "firstname.surname"
    )


@pytest.mark.integration
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
def test_parse_from_attrs(input, expected):
    from datahub.ingestion.source.ldap import parse_from_attrs

    assert (
        parse_from_attrs(
            input,
            "admins",
        )
        == expected
    )
