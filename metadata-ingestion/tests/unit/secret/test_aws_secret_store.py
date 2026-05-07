import boto3
from moto import mock_aws

from datahub.secret.aws_secret_store import AwsSecretsManagerStore

DEFAULT_CONFIG = {"region": "us-east-1", "prefix": "datahub-", "cache_ttl": 300}


class TestAwsSecretsManagerStore:
    @mock_aws
    def test_get_secret_values_with_prefix(self):
        client = boto3.client("secretsmanager", region_name="us-east-1")
        client.create_secret(Name="datahub-DB_PASS", SecretString="secret123")
        client.create_secret(Name="datahub-API_KEY", SecretString="key456")

        store = AwsSecretsManagerStore.create(DEFAULT_CONFIG)
        result = store.get_secret_values(["DB_PASS", "API_KEY"])

        assert result == {"DB_PASS": "secret123", "API_KEY": "key456"}
        assert store.get_id() == "aws-sm"

    @mock_aws
    def test_get_secret_values_with_custom_prefix(self):
        client = boto3.client("secretsmanager", region_name="us-east-1")
        client.create_secret(Name="myapp-prod-DB_PASS", SecretString="secret123")

        store = AwsSecretsManagerStore.create(
            {"region": "us-east-1", "prefix": "myapp-prod-", "cache_ttl": 300}
        )
        result = store.get_secret_values(["DB_PASS"])

        assert result == {"DB_PASS": "secret123"}

    @mock_aws
    def test_missing_secret_returns_none(self):
        store = AwsSecretsManagerStore.create(DEFAULT_CONFIG)
        result = store.get_secret_values(["NONEXISTENT"])

        assert result == {"NONEXISTENT": None}

    @mock_aws
    def test_mixed_found_and_missing(self):
        client = boto3.client("secretsmanager", region_name="us-east-1")
        client.create_secret(Name="datahub-EXISTS", SecretString="value1")

        store = AwsSecretsManagerStore.create(DEFAULT_CONFIG)
        result = store.get_secret_values(["EXISTS", "MISSING"])

        assert result["EXISTS"] == "value1"
        assert result["MISSING"] is None

    @mock_aws
    def test_get_secret_value_single(self):
        client = boto3.client("secretsmanager", region_name="us-east-1")
        client.create_secret(Name="datahub-TOKEN", SecretString="tok123")

        store = AwsSecretsManagerStore.create(DEFAULT_CONFIG)

        assert store.get_secret_value("TOKEN") == "tok123"
        assert store.get_secret_value("NONEXISTENT") is None

    def test_empty_list_returns_empty_dict(self):
        store = AwsSecretsManagerStore.create(DEFAULT_CONFIG)
        assert store.get_secret_values([]) == {}
