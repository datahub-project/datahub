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
from collections.abc import Callable, Sequence
from typing import Any

import pydantic

from datahub.cli import config_utils
from datahub.cli.cli_utils import is_same_gms_origin
from datahub.configuration.config_loader import resolve_env_variables
from datahub.executor.common.config import PermissiveConfigModel
from datahub.executor.context.execution_context import ExecutionContext
from datahub.executor.context.executor_context import ExecutorContext
from datahub.executor.execution import venv_utils
from datahub.executor.execution.runner import _expand_pip_req, referenced_env_values
from datahub.executor.execution.task import TaskError
from datahub.masking.bootstrap import initialize_secret_masking
from datahub.masking.secret_registry import SecretRegistry

logger = logging.getLogger(__name__)

# Environment variables that decide which DataHub server the executor's own credentials go to,
# carry those credentials, or route that traffic through a proxy. A run's extra_env_vars must not
# override them. Compared upper-cased, since requests also honors lower-case proxy variables.
_PROTECTED_ENV_VARS = frozenset(
    {
        "DATAHUB_GMS_URL",
        "DATAHUB_GMS_HOST",
        "DATAHUB_GMS_PORT",
        "DATAHUB_GMS_PROTOCOL",
        "DATAHUB_GMS_TOKEN",
        "DATAHUB_SYSTEM_CLIENT_ID",
        "DATAHUB_SYSTEM_CLIENT_SECRET",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
    }
)
# Env-configured OAuth for DataHub clients (DATAHUB_AUTH_TYPE, DATAHUB_AUTH_CLIENT_ID, ...).
_PROTECTED_ENV_VAR_PREFIX = "DATAHUB_AUTH_"

# ${NAME}, ${NAME:-default} and $NAME references to the executor's DataHub credentials or its
# env-configured OAuth settings. Recipe placeholders and pip requirement templates are filled
# from the executor's environment.
_CREDENTIAL_ENV_REFERENCE = re.compile(
    r"\$\{?\s*(DATAHUB_SYSTEM_CLIENT_ID|DATAHUB_SYSTEM_CLIENT_SECRET|DATAHUB_GMS_TOKEN"
    r"|DATAHUB_AUTH_\w+)\b"
)
# ${!NAME} expands the variable named by NAME's value, so the text does not show which variable
# it reads. Recipes have no need for it.
_INDIRECT_ENV_REFERENCE = re.compile(r"\$\{\s*!")

# Credentials whose values must not appear in a resolved recipe or pip requirement, whatever
# placeholder syntax produced them. Values shorter than the minimum are not compared, since
# they would match unrelated text.
_CREDENTIAL_ENV_VARS = (
    "DATAHUB_SYSTEM_CLIENT_SECRET",
    "DATAHUB_GMS_TOKEN",
    "DATAHUB_AUTH_CLIENT_SECRET",
    "DATAHUB_AUTH_AZURE_CLIENT_SECRET",
)
_MIN_COMPARED_CREDENTIAL_LENGTH = 8

# Recipe keys whose value is a DataHub client config (DatahubClientConfig or a subclass).
_DATAHUB_CLIENT_CONFIG_KEYS = frozenset({"datahub_api", "datahub", "rest_fallback"})


def find_credential_redirects(
    recipe: dict, args: "SubProcessRecipeTaskArgs", secret_values: dict[str, str]
) -> list[str]:
    """Settings in a run that would send the executor's DataHub credentials elsewhere.

    A DataHub client built without a token picks up the executor's credentials from the
    environment. Newer clients only send them to the env-configured server, but the run may
    use an older CLI version, so the executor checks before starting it:

    - a DataHub client config in the recipe whose server is not the env-configured one and
      that has no token of its own;
    - references to the credential environment variables in the recipe or pip settings,
      including indirect ${!NAME} references, and credential values in the resolved recipe
      or pip requirements, except in the token of a client for the configured server;
    - extra_env_vars that redirect the configured server, replace its credentials or set a
      proxy.

    This checks configuration only. It is not an isolation boundary: the subprocess still
    inherits the executor's environment, so a run that installs its own code (pip
    requirements, plugins, or a CLI version without the client-side check) can read it.
    """
    # Check the servers, tokens and values the subprocess will actually see, placeholders
    # filled in. If resolution fails here, the reference checks below still apply.
    try:
        effective_recipe = resolve_env_variables(
            recipe, environ={**args.get_combined_env_vars(), **secret_values}
        )
    except Exception:
        effective_recipe = recipe
    env_url = config_utils.get_url_from_env()

    problems = [
        f"extra_env_vars.{name}"
        for name in args.extra_env_vars
        if name.strip().upper() in _PROTECTED_ENV_VARS
        or name.strip().upper().startswith(_PROTECTED_ENV_VAR_PREFIX)
    ]
    # A client config that talks to the configured server may carry the executor's own token
    # (for example `token: ${DATAHUB_GMS_TOKEN}`), so its `token` field is exempt from the
    # reference and value checks. Its `auth` block is not: a token provider there can send a
    # secret to an endpoint of its own, such as an OIDC token endpoint.
    # Paths come from the resolved recipe and are matched against checks of the raw recipe;
    # placeholder resolution only changes values, never keys, so the paths line up.
    own_server_tokens: list[str] = []
    for path, client_config in _datahub_client_configs(effective_recipe, ""):
        server = client_config.get("server")
        if not isinstance(server, str) or not server:
            continue
        if env_url is not None and is_same_gms_origin(env_url, server):
            own_server_tokens.append(f"recipe.{path}.token")
        elif not client_config.get("token"):
            # Only a token counts as the client's own credential. An `auth` block does not:
            # CLI versions that predate it ignore the unknown key and fall back to the
            # executor's credentials.
            problems.append(f"{path}.server")

    credential_problems: list[str] = []
    for label, value in (
        ("recipe", recipe),
        ("extra_pip_requirements", args.extra_pip_requirements),
        ("extra_pip_plugins", args.extra_pip_plugins),
    ):
        credential_problems.extend(_credential_references(value, label))
    credential_values = _credential_values()
    if credential_values:
        credential_problems.extend(
            _paths_containing(effective_recipe, "recipe", credential_values)
        )
        credential_problems.extend(
            _paths_containing(
                [_expand_pip_req_or_raw(req) for req in args.extra_pip_requirements],
                "extra_pip_requirements",
                credential_values,
            )
        )
    problems.extend(
        problem for problem in credential_problems if problem not in own_server_tokens
    )
    # A reference that resolves to a credential is reported once, not per check.
    return list(dict.fromkeys(problems))


def _credential_values() -> list[str]:
    values = []
    for name in _CREDENTIAL_ENV_VARS:
        credential = os.environ.get(name)
        if not credential:
            continue
        if len(credential) < _MIN_COMPARED_CREDENTIAL_LENGTH:
            logger.debug(
                "%s is shorter than %d characters; not comparing its value against "
                "the recipe. References to it by name are still checked.",
                name,
                _MIN_COMPARED_CREDENTIAL_LENGTH,
            )
            continue
        values.append(credential)
    return values


def check_credential_redirects(
    recipe: dict, args: "SubProcessRecipeTaskArgs", secret_values: dict[str, str]
) -> None:
    problems = find_credential_redirects(recipe, args, secret_values)
    if problems:
        raise TaskError(
            "This run cannot use "
            + ", ".join(problems)
            + ": these settings would send the executor's DataHub credentials to another"
            " server. Remove them, or give the DataHub connection its own token."
        )


def _references_credential(text: str) -> bool:
    return bool(
        _CREDENTIAL_ENV_REFERENCE.search(text) or _INDIRECT_ENV_REFERENCE.search(text)
    )


def _credential_references(value: Any, path: str) -> list[str]:
    return _matching_paths(value, path, _references_credential)


def _paths_containing(value: Any, path: str, needles: list[str]) -> list[str]:
    return _matching_paths(value, path, lambda text: any(n in text for n in needles))


def _matching_paths(value: Any, path: str, matches: Callable[[str], bool]) -> list[str]:
    """Paths of the strings (dict keys included) in `value` for which `matches` is true."""
    if isinstance(value, str):
        return [path] if matches(value) else []
    found: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            if isinstance(key, str) and matches(key):
                found.append(child_path)
            found.extend(_matching_paths(child, child_path, matches))
    elif isinstance(value, list):
        for i, child in enumerate(value):
            found.extend(_matching_paths(child, f"{path}[{i}]", matches))
    return found


def _expand_pip_req_or_raw(req: str) -> str:
    # setup_venv expands pip requirements the same way and fails the run on a bad template.
    try:
        return _expand_pip_req(req)
    except RuntimeError:
        return req


def _datahub_client_configs(value: Any, path: str) -> list[tuple[str, dict]]:
    found: list[tuple[str, dict]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            if key in _DATAHUB_CLIENT_CONFIG_KEYS and isinstance(child, dict):
                found.append((child_path, child))
            elif (
                key == "sink"
                and isinstance(child, dict)
                and isinstance(child.get("config"), dict)
            ):
                found.append((f"{child_path}.config", child["config"]))
            found.extend(_datahub_client_configs(child, child_path))
    elif isinstance(value, list):
        for i, child in enumerate(value):
            found.extend(_datahub_client_configs(child, f"{path}[{i}]"))
    return found


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
    def _resolve_secrets(
        secret_names: list[str], ctx: ExecutorContext
    ) -> dict[str, str]:
        # Attempt to resolve secret using by checking each configured secret store.
        secret_stores = ctx.get_secret_stores()
        store_ids = [s.get_id() for s in secret_stores]
        logger.info(
            f"Resolving {len(secret_names)} secret(s) across {len(secret_stores)} store(s): {store_ids}"
        )
        final_secret_values: dict[str, str] = {}

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

        if secrets_to_resolve:
            initialize_secret_masking()
            SecretRegistry.get_instance().register_secrets_batch(
                {
                    name: secret_values_dict[name]
                    for name in secrets_to_resolve
                    if secret_values_dict.get(name)
                }
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
    def subprocess_env_secrets(args: "SubProcessRecipeTaskArgs") -> dict[str, str]:
        """Env values referenced in pip requirements, for the subprocess stdin
        envelope. Names the user overrides via extra_env_vars are excluded so
        recipe resolution keeps get_combined_env_vars precedence (user value
        wins); setup_venv registers all referenced values for masking."""
        return {
            name: value
            for name, value in referenced_env_values(
                args.extra_pip_requirements
            ).items()
            if name not in args.extra_env_vars
        }

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

    ``PYTHONSAFEPATH`` / ``-P`` would also fix it (3.11+). We still invoke by
    path so ``sys.path[0]`` matches the console-script entry points this replaced.

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
