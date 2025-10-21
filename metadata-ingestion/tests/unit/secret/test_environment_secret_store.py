import os
from unittest.mock import patch

from datahub.secret.environment_secret_store import EnvironmentSecretStore


class TestEnvironmentSecretStore:
    def test_init(self):
        config: dict = {}
        store = EnvironmentSecretStore(config)
        assert store is not None

    def test_get_secret_values_with_existing_env_vars(self):
        store = EnvironmentSecretStore({})

        with patch.dict(os.environ, {"SECRET1": "value1", "SECRET2": "value2"}):
            result = store.get_secret_values(["SECRET1", "SECRET2"])

            assert result == {"SECRET1": "value1", "SECRET2": "value2"}

    def test_get_secret_values_with_missing_env_vars(self):
        store = EnvironmentSecretStore({})

        with patch.dict(os.environ, {}, clear=True):
            result = store.get_secret_values(["NONEXISTENT1", "NONEXISTENT2"])

            assert result == {"NONEXISTENT1": None, "NONEXISTENT2": None}

    def test_get_secret_values_mixed_existing_and_missing(self):
        store = EnvironmentSecretStore({})

        with patch.dict(os.environ, {"SECRET1": "value1"}, clear=True):
            result = store.get_secret_values(["SECRET1", "NONEXISTENT"])

            assert result == {"SECRET1": "value1", "NONEXISTENT": None}

    def test_get_secret_value_existing(self):
        store = EnvironmentSecretStore({})

        with patch.dict(os.environ, {"SECRET1": "value1"}):
            result = store.get_secret_value("SECRET1")

            assert result == "value1"

    def test_get_secret_value_nonexistent(self):
        store = EnvironmentSecretStore({})

        with patch.dict(os.environ, {}, clear=True):
            result = store.get_secret_value("NONEXISTENT")

            assert result is None

    def test_get_secret_value_empty_string(self):
        store = EnvironmentSecretStore({})

        with patch.dict(os.environ, {"EMPTY_SECRET": ""}):
            result = store.get_secret_value("EMPTY_SECRET")

            assert result == ""

    def test_get_id(self):
        store = EnvironmentSecretStore({})
        assert store.get_id() == "env"

    def test_create_classmethod(self):
        config = {"some_key": "some_value"}
        store = EnvironmentSecretStore.create(config)

        assert isinstance(store, EnvironmentSecretStore)

    def test_get_secret_values_empty_list(self):
        store = EnvironmentSecretStore({})
        result = store.get_secret_values([])

        assert result == {}

    def test_get_secret_values_with_special_characters(self):
        store = EnvironmentSecretStore({})

        with patch.dict(os.environ, {"SECRET_WITH_SPECIAL": "value!@#$%^&*()"}):
            result = store.get_secret_values(["SECRET_WITH_SPECIAL"])

            assert result == {"SECRET_WITH_SPECIAL": "value!@#$%^&*()"}
