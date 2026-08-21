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

import errno
import importlib.util
import json
import logging
import os
import re
import shutil
import subprocess
from collections.abc import Sequence
from typing import Any

import pydantic

from datahub.executor.common.config import PermissiveConfigModel
from datahub.executor.context.execution_context import ExecutionContext
from datahub.executor.context.executor_context import ExecutorContext
from datahub.executor.execution import venv_utils
from datahub.masking.bootstrap import initialize_secret_masking
from datahub.masking.secret_registry import SecretRegistry

logger = logging.getLogger(__name__)


class SubProcessTaskUtil:
    MAX_LOG_LINES = 2000

    # The original value is 64kb (https://github.com/python/cpython/blob/7528e2c06c8baf809b56f406bcc50e8436c9647c/Lib/asyncio/streams.py#L23).
    # Increasing it should improve performance.
    SUBPROCESS_BUFFER_SIZE = 2**20  # 1mb

    # GMS / mysql has a 4mb limit on the size of a data packet.
    # Doing 90% of that so we have some buffer.
    MAX_LOG_SIZE_BYTES = int(0.9 * 2**22)  # 90% of 4mb

    # We want to truncate long lines so that we can show more lines in the logs.
    MAX_BYTES_PER_LINE = 2**12  # 4kb

    @staticmethod
    def format_subprocess_error(e: Exception) -> str:
        """
        Extract detailed error message from subprocess exceptions.

        For CalledProcessError, includes captured subprocess output from stderr or output attributes.
        For other exceptions, returns str(e).

        Args:
            e: The exception to format

        Returns:
            Formatted error message with subprocess output if available
        """
        if isinstance(e, subprocess.CalledProcessError):
            base_msg = str(e)

            # Try stderr first (formatted by runner.py), then output
            error_details = getattr(e, "stderr", None) or getattr(e, "output", None)

            if error_details:
                return f"{base_msg}\n\n{error_details}"

        return str(e)

    @staticmethod
    def _format_log_lines(lines: Sequence[str]) -> str:
        text = "".join(lines)

        # Python slices are super permissive on index bounds, so this works.
        text = text[-SubProcessTaskUtil.MAX_LOG_SIZE_BYTES :]

        if len(lines) >= SubProcessTaskUtil.MAX_LOG_LINES:
            # lines is a deque, so len(lines) won't be larger than MAX_LOG_LINES.
            text = f"[earlier logs truncated...]\n{text}"

        return text

    @staticmethod
    def _resolve_secrets(secret_names: list[str], ctx: ExecutorContext) -> dict:
        # Attempt to resolve secret using by checking each configured secret store.
        secret_stores = ctx.get_secret_stores()
        store_ids = [s.get_id() for s in secret_stores]
        logger.info(
            f"Resolving {len(secret_names)} secret(s) across {len(secret_stores)} store(s): {store_ids}"
        )
        final_secret_values = dict({})

        for secret_store in secret_stores:
            try:
                # Retrieve secret values from the store.
                secret_values_dict = secret_store.get_secret_values(secret_names)
                # Overlay secret values from each store, if not None.
                resolved_count = 0
                for secret_name, secret_value in secret_values_dict.items():
                    if secret_value is not None:
                        final_secret_values[secret_name] = secret_value
                        resolved_count += 1
                if resolved_count > 0:
                    logger.info(
                        f"Store '{secret_store.get_id()}' resolved {resolved_count}/{len(secret_names)} secret(s)"
                    )
            except Exception:
                logger.exception(
                    f"Failed to fetch secret values from secret store with id {secret_store.get_id()}"
                )
        logger.info(
            f"Secret resolution complete: {len(final_secret_values)}/{len(secret_names)} resolved from stores"
        )
        return final_secret_values

    @staticmethod
    def _warn_on_bad_secret_value(ctx: ExecutionContext, key: str, val: str) -> None:
        # Log a warning if the value is a valid JSON document (dict or list)
        # to hint AWS Secret Manager users of a wrong type.
        # We only warn for complex structures, not simple scalar values like numbers.
        try:
            parsed = json.loads(val)
            # Only warn if it's a dict or list (actual JSON documents), not scalars
            if isinstance(parsed, (dict, list)):
                ctx.get_report().report_error(
                    f"Secret variable ${{{key}}} appears to contain a JSON document while string is expected. "
                    "If you are using AWS Secret Manager, make sure to pass secret as plain text and not as a key/value pair."
                )
        except Exception:
            pass

    @staticmethod
    def _resolve_recipe(
        recipe: str, execution_ctx: ExecutionContext, executor_ctx: ExecutorContext
    ) -> tuple[dict, dict[str, str]]:
        """Resolve secrets in a recipe and return the recipe dict + resolved secrets.

        Secrets are resolved from stores first, then os.environ as fallback.
        Secrets are NOT written to the executor's os.environ — they are returned
        as a dict to be passed to the subprocess via stdin.

        Returns:
            Tuple of (recipe_dict, secret_values_dict) where secret_values_dict
            contains all resolved secret name→value pairs.
        """
        secret_pattern = re.compile(r"\$\{(\w+)\}")

        resolved_recipe = recipe
        secret_matches = secret_pattern.findall(resolved_recipe)

        secrets_to_resolve: list[str] = []
        if secret_matches:
            for match in secret_matches:
                secrets_to_resolve.append(match)

        logger.info(f"Found {len(secrets_to_resolve)} secret variable(s) in recipe")

        # Resolve secret values from stores
        secret_values_dict = SubProcessTaskUtil._resolve_secrets(
            secrets_to_resolve, executor_ctx
        )

        # Fall back to os.environ for any secrets not found in stores
        for secret_name in secrets_to_resolve:
            if (
                secret_name not in secret_values_dict
                or secret_values_dict[secret_name] is None
            ):
                env_value = os.environ.get(secret_name)
                if env_value is not None:
                    logger.info(
                        f"Secret '{secret_name}' not found in secret stores, using value from environment variable"
                    )
                    secret_values_dict[secret_name] = env_value
                else:
                    logger.warning(
                        f"Secret '{secret_name}' not found in secret stores or environment, using empty string"
                    )
                    secret_values_dict[secret_name] = ""

        # Set up secret masking in the executor process (for masking logs/reports)
        if secrets_to_resolve:
            try:
                initialize_secret_masking(force=True)
                registry = SecretRegistry.get_instance()
                for secret_name in secrets_to_resolve:
                    secret_value = secret_values_dict.get(secret_name)
                    if secret_value:
                        registry.register_secret(secret_name, secret_value)

                logger.info(
                    f"Secret masking enabled for {registry.get_count()} secret(s)"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to set up secret masking: {e}. Continuing without masking."
                )

        # Validate secret values and warn on potential issues
        if secret_matches:
            for match in secret_matches:
                secret_value = secret_values_dict.get(match, "")
                SubProcessTaskUtil._warn_on_bad_secret_value(
                    execution_ctx, match, secret_value
                )

        json_recipe = json.loads(resolved_recipe, strict=False)
        json_recipe["run_id"] = execution_ctx.exec_id

        return json_recipe, secret_values_dict

    @staticmethod
    def _get_plugin_from_recipe(recipe: dict) -> str:
        # The source type -- ASSUMPTION ALERT: This should always correspond to the plugin name.
        return recipe["source"]["type"]

    @staticmethod
    def _remove_directory(dir_path: str) -> None:
        try:
            shutil.rmtree(dir_path)
        except FileNotFoundError:
            # Directory was never created or was already removed. Non-fatal.
            logger.warning("Cleanup: directory %s does not exist; skipping.", dir_path)
        except OSError as e:
            # e.g. ENOTEMPTY when a subprocess core dump is still being written,
            # EACCES/EPERM on permission issues, EBUSY for an active mount.
            errno_name = errno.errorcode.get(e.errno or 0, str(e.errno))
            logger.exception(
                "Cleanup: failed to remove directory %s (%s: %s). Non-fatal.",
                dir_path,
                errno_name,
                e.strerror or str(e),
            )


class SubProcessRecipeTaskArgs(PermissiveConfigModel):
    recipe: str
    version: str = "latest"

    extra_pip_requirements: list[str] = []
    extra_pip_plugins: list[str] = []
    extra_env_vars: dict = {}

    @pydantic.field_validator(
        "extra_pip_requirements", "extra_pip_plugins", mode="before"
    )
    @classmethod
    def parse_json_list_fields(cls, v: Any) -> list:
        if isinstance(v, str):
            # Handle corner case where UI passes an empty string
            return [] if v == "" else json.loads(v)
        return v

    @pydantic.field_validator("extra_env_vars", mode="before")
    @classmethod
    def parse_json_dict_field(cls, v: Any) -> dict:
        if isinstance(v, str):
            # Handle corner case where UI passes an empty string
            return {} if v == "" else json.loads(v)
        return v

    def get_venv_name(self, plugin: str) -> str:
        """Generate venv name, consistent with VenvConfig.get_stable_venv_name().

        Delegates to VenvConfig so that env-var templates in extra_pip_requirements
        are expanded before hashing — matching what setup_venv() actually installs.
        """
        from datahub.executor.execution.runner import VenvConfig

        config = VenvConfig(
            version=self.version,
            main_plugin=plugin,
            extra_pip_requirements=self.extra_pip_requirements,
            extra_pip_plugins=self.extra_pip_plugins,
        )
        expanded = config.resolve_pip_requirements()
        name = config.get_stable_venv_name(expanded_pip_reqs=expanded)
        if name is not None:
            return name
        # Fallback for ephemeral/bundled/native versions that have no stable name.
        return venv_utils.get_venv_name(
            plugin=plugin,
            version=self.version,
            extra_pip_requirements=self.extra_pip_requirements,
            extra_pip_plugins=self.extra_pip_plugins,
        )

    def should_use_bundled_venv(self) -> bool:
        """Check if this configuration should use a Bundled (pre-packaged) venv."""
        return venv_utils.should_use_bundled_venv(self.version)

    def get_combined_env_vars(self) -> dict:
        # Combines os.environ and user-provided custom env vars.
        # User's extra_env_vars will override system environment variables to allow
        # users to explicitly configure their ingestion environment.
        # Filter out empty string values from extra_env_vars to prevent them from overriding
        # non-empty system environment variables with empty values
        filtered_extra_vars = {k: v for k, v in self.extra_env_vars.items() if v != ""}
        combined = {
            **os.environ,  # System vars as base
            **filtered_extra_vars,  # User vars override (non-empty only)
        }

        return combined


def resolve_wrapper_script(module_name: str) -> str:
    """Absolute path to a wrapper module, for invoking it as a script.

    Deliberately a path rather than ``python -m``. ``-m`` puts the subprocess's current
    working directory on ``sys.path[0]``, so a stray module there shadows real imports
    and kills the run before any wrapper code executes -- e.g. a ``yaml.py`` sitting in
    the worker's CWD (``/tmp`` in the shipped image) is imported instead of PyYAML.
    Invoking by path puts the wrapper's own directory on ``sys.path[0]`` instead, which
    matches the console-script entry points this replaced: their ``<venv>/bin`` held no
    importable modules either.

    ``PYTHONSAFEPATH`` / ``-P`` would also fix it but are 3.11+, and this package
    supports 3.10.

    Uses ``find_spec`` rather than importing: resolving a path must not execute the
    wrapper in the *parent* process.
    """
    spec = importlib.util.find_spec(module_name)
    if spec is None or spec.origin is None:
        raise RuntimeError(
            f"Could not locate the wrapper module {module_name!r}. This is a packaging "
            "problem: the executor's wrappers must ship with it."
        )
    return spec.origin
