"""
A script to provision a warehouse on DataHub (and Iceberg).

This script uses environment variables to configure the Iceberg client and
provision a warehouse on DataHub. The required environment variables are:
- DH_ICEBERG_CLIENT_ID: The client ID for the Icebreaker service.
- DH_ICEBERG_CLIENT_SECRET: The client secret for the Icebreaker service.
- DH_ICEBERG_AWS_ROLE: The test role for the Icebreaker service.
- DH_ICEBERG_DATA_ROOT: The root directory for Icebreaker data.

The script asserts the presence of these environment variables and then
executes a system command to create the warehouse using the DataHub Iceberg CLI.

Usage:
    Ensure the required environment variables are set, then run the script.

Example:
    $ export DH_ICEBERG_CLIENT_ID="your_client_id"
    $ export DH_ICEBERG_CLIENT_SECRET="your_client_secret"
    $ export DH_ICEBERG_AWS_ROLE="your_test_role"
    $ export DH_ICEBERG_DATA_ROOT="your_data_root"
    $ python provision_warehouse.py
"""

import os

from constants import warehouse

# Assert that env variables are present

assert os.environ.get("DH_ICEBERG_CLIENT_ID"), (
    "DH_ICEBERG_CLIENT_ID variable is not present"
)
assert os.environ.get("DH_ICEBERG_CLIENT_SECRET"), (
    "DH_ICEBERG_CLIENT_SECRET variable is not present"
)
assert os.environ.get("DH_ICEBERG_AWS_ROLE"), (
    "DH_ICEBERG_AWS_ROLE variable is not present"
)
assert os.environ.get("DH_ICEBERG_DATA_ROOT"), (
    "DH_ICEBERG_DATA_ROOT variable is not present"
)

assert os.environ.get("DH_ICEBERG_DATA_ROOT", "").startswith("s3://")

os.system(
    f"datahub iceberg create --warehouse {warehouse} --data_root $DH_ICEBERG_DATA_ROOT/{warehouse} --client_id $DH_ICEBERG_CLIENT_ID --client_secret $DH_ICEBERG_CLIENT_SECRET --region 'us-east-1' --role $DH_ICEBERG_AWS_ROLE"
)
