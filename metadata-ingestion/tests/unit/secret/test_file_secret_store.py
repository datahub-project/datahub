import os
import tempfile
from unittest.mock import patch

from datahub.secret.file_secret_store import FileSecretStore, FileSecretStoreConfig


class TestFileSecretStore:
    def test_init_with_default_config(self):
        config = FileSecretStoreConfig()
        store = FileSecretStore(config)

        assert store.config.basedir == "/mnt/secrets"
        assert store.config.max_length == 1024768

    def test_init_with_custom_config(self):
        config = FileSecretStoreConfig(basedir="/custom/path", max_length=512)
        store = FileSecretStore(config)

        assert store.config.basedir == "/custom/path"
        assert store.config.max_length == 512

    def test_get_secret_value_file_exists(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a test secret file
            secret_file = os.path.join(temp_dir, "test_secret")
            with open(secret_file, "w") as f:
                f.write("secret_value")

            config = FileSecretStoreConfig(basedir=temp_dir)
            store = FileSecretStore(config)

            result = store.get_secret_value("test_secret")

            assert result == "secret_value"

    def test_get_secret_value_file_not_exists(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config = FileSecretStoreConfig(basedir=temp_dir)
            store = FileSecretStore(config)

            result = store.get_secret_value("nonexistent_secret")

            assert result is None

    def test_get_secret_value_with_trailing_whitespace(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            secret_file = os.path.join(temp_dir, "test_secret")
            with open(secret_file, "w") as f:
                f.write("secret_value\n\t ")

            config = FileSecretStoreConfig(basedir=temp_dir)
            store = FileSecretStore(config)

            result = store.get_secret_value("test_secret")

            assert result == "secret_value"

    def test_get_secret_value_exceeds_max_length(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            secret_file = os.path.join(temp_dir, "large_secret")
            large_content = "a" * 100
            with open(secret_file, "w") as f:
                f.write(large_content)

            config = FileSecretStoreConfig(basedir=temp_dir, max_length=50)
            store = FileSecretStore(config)

            with patch("datahub.secret.file_secret_store.logger") as mock_logger:
                result = store.get_secret_value("large_secret")

                assert result == "a" * 50
                mock_logger.warning.assert_called_once()
                assert "longer than 50" in mock_logger.warning.call_args[0][0]

    def test_get_secret_values(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test secret files
            for i, content in enumerate(["value1", "value2"], 1):
                secret_file = os.path.join(temp_dir, f"secret{i}")
                with open(secret_file, "w") as f:
                    f.write(content)

            config = FileSecretStoreConfig(basedir=temp_dir)
            store = FileSecretStore(config)

            result = store.get_secret_values(["secret1", "secret2", "nonexistent"])

            assert result == {
                "secret1": "value1",
                "secret2": "value2",
                "nonexistent": None,
            }

    def test_get_secret_values_empty_list(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config = FileSecretStoreConfig(basedir=temp_dir)
            store = FileSecretStore(config)

            result = store.get_secret_values([])

            assert result == {}

    def test_get_id(self):
        config = FileSecretStoreConfig()
        store = FileSecretStore(config)

        assert store.get_id() == "file"

    def test_close(self):
        config = FileSecretStoreConfig()
        store = FileSecretStore(config)

        # Should not raise an exception
        store.close()

    def test_create_classmethod(self):
        config_dict = {"basedir": "/test/path", "max_length": 2048}

        store = FileSecretStore.create(config_dict)

        assert isinstance(store, FileSecretStore)
        assert store.config.basedir == "/test/path"
        assert store.config.max_length == 2048

    def test_create_classmethod_with_invalid_config(self):
        config_dict = {"invalid_field": "value"}

        # Pydantic will ignore unknown fields by default, so this creates a store with defaults
        store = FileSecretStore.create(config_dict)
        assert isinstance(store, FileSecretStore)
        assert store.config.basedir == "/mnt/secrets"  # Default value
        assert store.config.max_length == 1024768  # Default value

    def test_get_secret_value_empty_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            secret_file = os.path.join(temp_dir, "empty_secret")
            with open(secret_file, "w") as f:
                f.write("")

            config = FileSecretStoreConfig(basedir=temp_dir)
            store = FileSecretStore(config)

            result = store.get_secret_value("empty_secret")

            assert result == ""

    def test_get_secret_value_exactly_max_length(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            secret_file = os.path.join(temp_dir, "exact_length_secret")
            content = "a" * 100
            with open(secret_file, "w") as f:
                f.write(content)

            config = FileSecretStoreConfig(basedir=temp_dir, max_length=100)
            store = FileSecretStore(config)

            with patch("datahub.secret.file_secret_store.logger") as mock_logger:
                result = store.get_secret_value("exact_length_secret")

                assert result == content
                # Should not log warning for exact length
                mock_logger.warning.assert_not_called()

    def test_file_secret_store_config_defaults(self):
        config = FileSecretStoreConfig()

        assert config.basedir == "/mnt/secrets"
        assert config.max_length == 1024768

    def test_file_secret_store_config_custom_values(self):
        config = FileSecretStoreConfig(basedir="/custom", max_length=512)

        assert config.basedir == "/custom"
        assert config.max_length == 512
