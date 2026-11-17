# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import importlib
import inspect
from typing import Any, Union

from datahub._version import __package_name__
from datahub.secret.datahub_secret_store import DataHubSecretStore
from datahub.secret.environment_secret_store import EnvironmentSecretStore
from datahub.secret.file_secret_store import FileSecretStore
from datahub.secret.secret_store import SecretStore


def _is_importable(path: str) -> bool:
    return "." in path or ":" in path


def import_path(path: str) -> Any:
    """
    Import an item from a package, where the path is formatted as 'package.module.submodule.ClassName'
    or 'package.module.submodule:ClassName.classmethod'. The dot-based format assumes that the bit
    after the last dot is the item to be fetched. In cases where the item to be imported is embedded
    within another type, the colon-based syntax can be used to disambiguate.
    """
    assert _is_importable(path), "path must be in the appropriate format"

    if ":" in path:
        module_name, object_name = path.rsplit(":", 1)
    else:
        module_name, object_name = path.rsplit(".", 1)

    item = importlib.import_module(module_name)
    for attr in object_name.split("."):
        item = getattr(item, attr)
    return item


# A secret registry is used to store SecretStore name -> class mappings for instantiation later.
class SecretStoreRegistry:
    # Map of name to class reference or class type.
    _mapping: dict[str, Union[str, type[SecretStore], Exception]]

    def __init__(self) -> None:
        self._mapping = {}
        self._register_defaults()

    def _register_defaults(self) -> None:
        # Register "built-in" secret store implementations.
        self.register("env", EnvironmentSecretStore)
        self.register("datahub", DataHubSecretStore)
        self.register("file", FileSecretStore)
        # Cloud secret stores — lazy-loaded, only imported when configured.
        self.register_lazy(
            "aws-sm", "datahub.secret.aws_secret_store.AwsSecretsManagerStore"
        )
        self.register_lazy(
            "gcp-sm", "datahub.secret.gcp_secret_store.GcpSecretManagerStore"
        )

    def _get_registered_type(self) -> type[SecretStore]:
        return SecretStore

    def _check_cls(self, cls: type[SecretStore]) -> None:
        if inspect.isabstract(cls):
            raise ValueError(
                f"cannot register an abstract type in the registry; got {cls}"
            )
        super_cls = self._get_registered_type()
        if not issubclass(cls, super_cls):
            raise ValueError(f"must be derived from {super_cls}; got {cls}")

    def _register(
        self,
        key: str,
        tp: Union[str, type[SecretStore], Exception],
        override: bool = False,
    ) -> None:
        if not override and key in self._mapping:
            raise KeyError(f"key already in use - {key}")
        if _is_importable(key):
            raise KeyError(f"key cannot contain special characters; got {key}")
        self._mapping[key] = tp

    def register(
        self, key: str, cls: type[SecretStore], override: bool = False
    ) -> None:
        self._check_cls(cls)
        self._register(key, cls, override=override)

    def register_lazy(self, key: str, import_path: str) -> None:
        self._register(key, import_path)

    def register_disabled(
        self, key: str, reason: Exception, override: bool = False
    ) -> None:
        self._register(key, reason, override=override)

    def _ensure_not_lazy(self, key: str) -> Union[type[SecretStore], Exception]:
        path = self._mapping[key]
        if isinstance(path, str):
            try:
                plugin_class = import_path(path)
                self.register(key, plugin_class, override=True)
                return plugin_class
            except (AssertionError, ModuleNotFoundError, ImportError) as e:
                self.register_disabled(key, e, override=True)
                return e
        else:
            return path

    def is_enabled(self, key: str) -> bool:
        tp = self._mapping.get(key)
        return tp is not None and not isinstance(tp, Exception)

    def get(self, key: str) -> type[SecretStore]:
        if _is_importable(key):
            # If the key contains a dot or colon, we treat it as a import path and attempt
            # to load it dynamically.
            clazz = import_path(key)
            self._check_cls(clazz)
            return clazz

        if key not in self._mapping:
            raise KeyError(
                f"Did not find a registered class for SecretStore with type {key}"
            )

        tp = self._ensure_not_lazy(key)
        if isinstance(tp, ModuleNotFoundError):
            raise OSError(  # Todo use a better error.
                f"{key} is disabled; try running: pip install '{__package_name__}[{key}]'"  # TODO: Think about hown to handle this case.
            ) from tp
        elif isinstance(tp, Exception):
            raise OSError(
                f"{key} is disabled due to an error in initialization"
            ) from tp
        else:
            # If it's not an exception, then it's a registered type.
            return tp
