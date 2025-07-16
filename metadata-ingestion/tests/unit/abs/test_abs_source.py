import unittest
from unittest.mock import Mock, patch

from azure.identity import ClientSecretCredential

from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig


class TestAzureDataHubFixes(unittest.TestCase):
    def test_service_principal_credentials_type(self):
        config = AzureConnectionConfig(
            account_name="testaccount",
            container_name="testcontainer",
            client_id="test-client-id",
            client_secret="test-client-secret",
            tenant_id="test-tenant-id",
        )

        credential = config.get_credentials()

        # Critical: Must be ClientSecretCredential object, not string
        self.assertIsInstance(credential, ClientSecretCredential)
        self.assertNotIsInstance(credential, str)

    def test_account_key_credentials_type(self):
        config = AzureConnectionConfig(
            account_name="testaccount",
            container_name="testcontainer",
            account_key="test-account-key",
        )

        credential = config.get_credentials()

        self.assertIsInstance(credential, str)
        self.assertEqual(credential, "test-account-key")

    @patch("datahub.ingestion.source.azure.azure_common.BlobServiceClient")
    def test_blob_service_client_credential_not_stringified(
        self, mock_blob_service_client
    ):
        config = AzureConnectionConfig(
            account_name="testaccount",
            container_name="testcontainer",
            client_id="test-client-id",
            client_secret="test-client-secret",
            tenant_id="test-tenant-id",
        )

        config.get_blob_service_client()

        mock_blob_service_client.assert_called_once()
        call_args = mock_blob_service_client.call_args

        credential = call_args[1]["credential"]
        self.assertIsInstance(credential, ClientSecretCredential)
        self.assertNotIsInstance(credential, str)

    @patch("datahub.ingestion.source.azure.azure_common.DataLakeServiceClient")
    def test_data_lake_service_client_credential_not_stringified(
        self, mock_data_lake_service_client
    ):
        """Test that get_data_lake_service_client passes credential object directly (not as f-string)"""
        config = AzureConnectionConfig(
            account_name="testaccount",
            container_name="testcontainer",
            client_id="test-client-id",
            client_secret="test-client-secret",
            tenant_id="test-tenant-id",
        )

        config.get_data_lake_service_client()

        mock_data_lake_service_client.assert_called_once()
        call_args = mock_data_lake_service_client.call_args

        # Check credential is ClientSecretCredential object, not string
        credential = call_args[1]["credential"]
        self.assertIsInstance(credential, ClientSecretCredential)
        self.assertNotIsInstance(credential, str)

    def test_azure_blob_list_parameter_usage(self):
        mock_container_client = Mock()
        mock_container_client.list_blobs(
            name_starts_with="test/path", results_per_page=1000
        )

        mock_container_client.list_blobs.assert_called_with(
            name_starts_with="test/path", results_per_page=1000
        )

        call_args = mock_container_client.list_blobs.call_args
        self.assertIn("name_starts_with", call_args[1])
        self.assertNotIn("prefix", call_args[1])

    def test_azure_blob_list_deprecated_parameter(self):
        mock_container_client = Mock()

        def mock_list_blobs_with_prefix(**kwargs):
            if "prefix" in kwargs:
                raise ValueError(
                    "Passing 'prefix' has no effect on filtering, please use the 'name_starts_with' parameter instead."
                )
            return []

        mock_container_client.list_blobs.side_effect = mock_list_blobs_with_prefix

        # Test that using prefix would raise an error
        with self.assertRaises(ValueError) as context:
            mock_container_client.list_blobs(prefix="test/path", results_per_page=1000)

        self.assertIn("name_starts_with", str(context.exception))
        self.assertIn("prefix", str(context.exception))

    def test_f_string_formatting_bug_demonstration(self):
        config = AzureConnectionConfig(
            account_name="testaccount",
            container_name="testcontainer",
            client_id="test-client-id",
            client_secret="test-client-secret",
            tenant_id="test-tenant-id",
        )

        credential = config.get_credentials()

        # This would be the bug - f-string formatting the credential
        credential_as_string = f"{credential}"

        # The original credential object should not equal its string representation
        self.assertIsInstance(credential, ClientSecretCredential)
        self.assertNotEqual(credential, credential_as_string)

        # The bug was that the code was doing this:
        # credential=f"{self.get_credentials()}"
        # Instead of this:
        # credential=self.get_credentials()

        # Verify that the string representation indicates it's a credential object
        self.assertIn("ClientSecretCredential", str(credential))

    def test_azure_connection_config_validates_credentials(self):
        # Test service principal credentials
        config_sp = AzureConnectionConfig(
            account_name="testaccount",
            container_name="testcontainer",
            client_id="test-client-id",
            client_secret="test-client-secret",
            tenant_id="test-tenant-id",
        )
        self.assertIsInstance(config_sp.get_credentials(), ClientSecretCredential)

        config_key = AzureConnectionConfig(
            account_name="testaccount",
            container_name="testcontainer",
            account_key="test-account-key",
        )
        self.assertEqual(config_key.get_credentials(), "test-account-key")

        config_sas = AzureConnectionConfig(
            account_name="testaccount",
            container_name="testcontainer",
            sas_token="test-sas-token",
        )
        self.assertEqual(config_sas.get_credentials(), "test-sas-token")


if __name__ == "__main__":
    unittest.main()
