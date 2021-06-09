#!/bin/bash
set -euxo pipefail

# We allow for failures in this step. Usually you'll be invoking this
# script to fix a build failure.
pytest --basetemp=tmp || true

# Update the golden files.
cp tmp/test_serde_to_json_tests_unit_0/output.json tests/unit/serde/test_serde_large.json
cp tmp/test_serde_to_json_tests_unit_1/output.json tests/unit/serde/test_serde_chart_snapshot.json
cp tmp/test_ldap_ingest0/ldap_mces.json tests/integration/ldap/ldap_mce_golden.json
cp tmp/test_mysql_ingest0/mysql_mces.json tests/integration/mysql/mysql_mce_golden.json
cp tmp/test_mssql_ingest0/mssql_mces.json tests/integration/sql_server/mssql_mce_golden.json
cp tmp/test_mongodb_ingest0/mongodb_mces.json tests/integration/mongodb/mongodb_mce_golden.json
cp tmp/test_feast_ingest0/feast_mces.json tests/integration/feast/feast_mce_golden.json

# Print success message.
set +x
echo ''
echo 'Make sure to check `git diff` to verify the changes!'
