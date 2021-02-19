import mce_helpers

from datahub.ingestion.run.pipeline import Pipeline


def test_ldap_ingest(mysql, pytestconfig, tmp_path):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/ldap"

    pipeline = Pipeline.create(
        {
            "run-id": "ldap-test",
            "source": {
                "type": "ldap",
                "config": {
                    "ldap_server": "ldap://localhost",
                    "ldap_user": "cn=admin,dc=example,dc=org",
                    "ldap_password": "admin",
                    "base_dn": "dc=example,dc=org",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/ldap_mces.json",
                },
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    output = mce_helpers.load_json_file(str(tmp_path / "ldap_mces.json"))
    golden = mce_helpers.load_json_file(
        str(test_resources_dir / "ldap_mce_golden.json")
    )
    mce_helpers.assert_mces_equal(output, golden)
