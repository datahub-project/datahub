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

import dataclasses
import errno
import importlib.util
import json
import logging
import os
import re
import shutil
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Optional

import pydantic
import yaml

from datahub.executor.common.config import PermissiveConfigModel
from datahub.executor.context.execution_context import ExecutionContext
from datahub.executor.context.executor_context import ExecutorContext
from datahub.executor.execution import venv_utils
from datahub.executor.execution.runner import (
    LogHolder,
    SubprocessRunner,
    VenvConfig,
    VenvReference,
    referenced_env_values,
    setup_venv,
)
from datahub.executor.execution.task import TaskError
from datahub.masking.bootstrap import initialize_secret_masking
from datahub.masking.constants import SENTINEL_MESSAGES
from datahub.masking.masking_filter import SecretMaskingFilter
from datahub.masking.secret_registry import (
    SENSITIVE_KEY_HINTS,
    SecretRegistry,
)

logger = logging.getLogger(__name__)


# Stands in for a structured report that could not be masked. It reaches GMS
# and is surfaced to operators, so the only safe substitute for "could not
# mask" is "not shown" -- see masking_filter.py's fail-closed guarantee.
MASKING_FAILED_REPORT = json.dumps(
    {
        "error": (
            "the task's report could not be masked, so it is withheld rather "
            "than shown unmasked; see the executor logs for the cause"
        )
    }
)

# The same, for the task's logs, which are rendered in the UI and returned to
# the agent verbatim.
MASKING_FAILED_LOGS = (
    "[logs withheld: they could not be masked, and showing them unmasked could "
    "leak a credential. See the executor logs for the cause.]"
)


@dataclasses.dataclass
class PreparedRun:
    """What `prepare_recipe_run` resolved, ready to spawn a child with."""

    recipe: dict
    plugin: str
    venv_ref: VenvReference
    subprocess_env: dict
    stdin_envelope: str


def _plain_and_inline_secret_values(
    obj: object, under_sensitive: bool = False
) -> tuple[set[str], set[str]]:
    """Split a raw recipe's string scalars into (plain, inline-secret).

    "Plain" means stated under a key no sensitivity hint matches, with no
    ``${`` left in it. Those are the values the recipe discloses in the clear.

    The hints come from datahub.masking.secret_registry rather than a copy
    here. The copy carried a comment justifying itself as avoiding a
    dependency on the ingestion AGENT package -- but the canonical list is in
    the masking package, which this module already imports three lines up, so
    the dependency being avoided was one that already existed. The two lists
    were identical, and the cost of keeping them apart was not theoretical:
    the recursion bug described below was written into both independently and
    fixed twice.

    ``under_sensitive`` carries the parent's verdict down, and it has to:
    sensitivity was decided per key and then dropped at the recursion, so a
    sensitive key holding a MAPPING had its children judged by their own
    names. ``token: {access: ...}`` and ``credential: {private_key: {pem:
    ...}}`` both came back as plainly disclosed, and a disclosed value is
    exempted from registration -- so the parent's logs and structured report
    stopped masking the credential. Nothing under a sensitive key is
    disclosed, whatever its children are called.
    """
    plain: set[str] = set()
    inline: set[str] = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            sensitive = under_sensitive or any(
                h in str(k).lower() for h in SENSITIVE_KEY_HINTS
            )
            if isinstance(v, str):
                if not v or "${" in v:
                    continue
                (inline if sensitive else plain).add(v)
            else:
                sub_plain, sub_inline = _plain_and_inline_secret_values(v, sensitive)
                plain |= sub_plain
                inline |= sub_inline
    elif isinstance(obj, list):
        for item in obj:
            # A string sitting directly in a list is a value the recipe
            # states, the same as one under a key -- `schema_allow:
            # [public, analytics]`. Recursing without handling it dropped
            # them, so the executor's split disagreed with
            # plain_config_values about the same recipe: canonical returned
            # {'public','analytics','staging'} where this returned nothing,
            # and a disclosed value that one layer does not see is a value
            # the two layers exempt differently.
            if isinstance(item, str):
                if item and "${" not in item:
                    (inline if under_sensitive else plain).add(item)
                continue
            sub_plain, sub_inline = _plain_and_inline_secret_values(
                item, under_sensitive
            )
            plain |= sub_plain
            inline |= sub_inline
    return plain, inline


def unprotectable_disclosed_values(recipe: str) -> set[str]:
    """Values this recipe states in the clear, which masking cannot protect.

    A resolved secret equal to one of these is not made safer by registering
    it. The recipe already states the value under a non-secret key, consumers
    legitimately have to print it -- a probe verdict's ``target`` is a
    qualified identifier, and a log line is full of ordinary words -- and the
    mask is itself what tells a reader that the secret equals the identifier
    they can already see. A password of "datahub" otherwise rewrites
    ``datahub_executor.coordinator.ingestion`` into
    ``***REDACTED:PW***_executor.coordinator.ingestion``.

    Exempts only what the recipe does NOT also carry as an inline secret
    literal: a recipe with ``password: p`` and ``database: p`` discloses the
    credential itself, and a report travels further than a recipe does -- to
    GMS, the logs, and an LLM -- so that one keeps its mask.

    Best-effort and never raises. Refs are registered before the recipe is
    parsed so a parse error cannot echo an unmasked secret; this must not
    disturb that ordering, and an unparseable recipe simply discloses nothing.
    """
    try:
        parsed = json.loads(recipe, strict=False)
    except Exception:
        return set()
    plain, inline = _plain_and_inline_secret_values(parsed)
    return plain - inline


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
            disclosed = unprotectable_disclosed_values(recipe)
            SecretRegistry.get_instance().register_secrets_batch(
                {
                    name: secret_values_dict[name]
                    for name in secrets_to_resolve
                    if secret_values_dict.get(name)
                    and secret_values_dict[name] not in disclosed
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

    # ---------------------------------------------------------------- shared
    # skeleton
    #
    # Every recipe task resolves a recipe, builds a venv, composes a subprocess
    # environment and a stdin envelope, and finalizes a structured report. Each
    # task had its own copy of those five steps, and the copies had drifted --
    # one envelope was missing `subprocess_env_secrets`, which is a masking gap
    # rather than a style problem, and the two finalize paths disagreed about
    # what to do when masking fails. They live here so the next task inherits
    # one answer instead of copying whichever neighbour it was written beside.

    @staticmethod
    async def setup_task_venv(
        args: "SubProcessRecipeTaskArgs",
        plugin: str,
        exec_out_dir: str,
        *,
        version: Optional[str] = None,
        logs: Optional[LogHolder] = None,
    ) -> VenvReference:
        """Resolve the venv a recipe task will run in.

        `version` overrides the version on `args`, for a task that has to run
        in a specific environment -- one needing the connector client libraries
        of the executor image rather than a per-run install, say.

        `logs` lets a caller surface the setup output; the ingestion task
        threads its subprocess log holder through so venv progress appears in
        the run's logs. Callers that pass nothing still get a holder, because a
        failure reports what it captured either way.
        """
        resolved_version = version if version is not None else args.version
        venv_config = VenvConfig(
            version=resolved_version,
            main_plugin=plugin,
            extra_pip_requirements=args.extra_pip_requirements,
            extra_pip_plugins=args.extra_pip_plugins,
            extra_env_vars=args.extra_env_vars,
        )

        holder = logs if logs is not None else LogHolder()
        message = (
            f"Setting up venv for plugin '{plugin}' with version '{resolved_version}'"
        )
        logger.info(message)
        holder.append(f"{message}\n")
        # resolved_version, not args: `version` overrides args.version, and
        # consulting args here described the mode the run did not use.
        if venv_utils.should_use_bundled_venv(resolved_version):
            holder.append("Using Bundled startup (pre-built) venv\n")
        else:
            holder.append("Creating dynamic venv - this may take a few minutes...\n")

        try:
            venv_ref = await setup_venv(
                venv_config=venv_config,
                runner=SubprocessRunner(logs=holder),
                tmp_dir=Path(exec_out_dir),
            )
        except Exception as e:
            error_msg = SubProcessTaskUtil.format_subprocess_error(e)
            logger.error(f"Venv setup failed: {error_msg}")
            holder.append(f"❌ Venv setup failed: {error_msg}\n")
            raise TaskError(f"Failed to set up virtual environment: {error_msg}") from e

        logger.info(f"Venv ready at: {venv_ref.venv_loc}")
        holder.append(f"✅ Venv ready at: {venv_ref.venv_loc}\n")
        return venv_ref

    @staticmethod
    def build_subprocess_env(
        args: "SubProcessRecipeTaskArgs",
        venv_ref: VenvReference,
        *,
        extra: Optional[dict] = None,
    ) -> dict:
        """The environment every recipe subprocess gets.

        Note what is NOT here: the resolved secrets. They travel in the stdin
        envelope, so they stay out of /proc/<pid>/environ and `ps e` and are
        not inherited by everything the child spawns. An env-SOURCED secret is
        still present, because os.environ is inherited wholesale.

        `venv_ref.extra_envs()` is deliberately not merged in. It returns
        `extra_env_vars` unfiltered, which would re-add the empty-string values
        `get_combined_env_vars` strips on purpose -- overriding a real
        os.environ value with "".
        """
        return {
            **args.get_combined_env_vars(),
            "VENV_PATH": str(venv_ref.venv_loc),
            **(extra or {}),
            # Last, so nothing can turn it off. The child reads this flag to
            # decide whether to register secrets at all, so a caller passing
            # DATAHUB_ENABLE_SECRET_MASKING=false in `extra` made every later
            # mask in that process a no-op. A guarantee that an extension
            # point can switch off by accident is not one.
            #
            # The user's extra_env_vars were never able to reach it -- they
            # merge first -- so this closes the same door on the internal
            # extension point. Everything else in `extra` still arrives.
            "DATAHUB_ENABLE_SECRET_MASKING": "true",
        }

    @staticmethod
    def build_stdin_envelope(
        args: "SubProcessRecipeTaskArgs",
        recipe: dict,
        secret_values: dict,
        *,
        extra: Optional[dict] = None,
    ) -> str:
        """The recipe and its secrets, in the format `datahub ingest -c -` takes.

        Dunder keys distinguish envelope entries from recipe content. Per-run
        values only, never the whole registry.

        `subprocess_env_secrets` is merged here rather than at each call site
        because forgetting it is silent: those are env values referenced from
        `extra_pip_requirements`, which `setup_venv` registers for masking only
        on the dynamic path -- so a task pinning a static venv covers them by
        neither mechanism, and a private index URL with an embedded token
        streams out unmasked. Recipe-resolved values win on a name collision.
        """
        # `extra` FIRST, so the two keys that define the envelope cannot be
        # overridden by it. Merged last, an `extra` carrying
        # __recipe_yaml__ or __secrets__ silently replaced the real ones --
        # the recipe the child runs, or every secret it masks against.
        return json.dumps(
            {
                **(extra or {}),
                "__recipe_yaml__": yaml.dump(recipe),
                "__secrets__": {
                    **SubProcessTaskUtil.subprocess_env_secrets(args),
                    **secret_values,
                },
            }
        )

    @staticmethod
    def finalize_task_output(
        report_file: str,
        exec_out_dir: str,
        log_lines: Sequence[str],
        ctx: ExecutionContext,
        *,
        masking_filter: Optional[SecretMaskingFilter] = None,
        venv_ref: Optional[VenvReference] = None,
    ) -> None:
        """Attach the structured report and logs, then clean up.

        Every step is guarded, including building the masking filter, so this is
        safe to call from a `finally` block: nothing here can replace the
        exception a caller is already propagating -- a TaskError, or the
        CancelledError that is the only thing saying a run was cancelled.

        Masking is fail-closed for BOTH outputs, which is what
        masking_filter.py documents: "when masking cannot be performed while
        secrets are registered, output is replaced with a fixed marker, never
        leaked". Content that cannot be masked is withheld rather than shipped
        -- the report reaches GMS and the logs are rendered in the UI. The
        per-task copies shipped both unmasked on a masking failure, which
        inverts that guarantee exactly where it matters.

        The log text is masked as one buffer rather than per line: a
        service-account JSON, a key-pair PEM or a private key echoed in a
        connector traceback spans lines, and a per-line pass cannot match it.
        """
        # Constructing the filter can itself fail (an opened circuit breaker, a
        # registry error). Doing it outside a guard made a masking failure
        # escape a method whose whole contract is that it does not.
        filt: Optional[SecretMaskingFilter]
        try:
            filt = masking_filter or SecretMaskingFilter()
        except Exception:
            logger.exception(
                "Could not build a masking filter; the report and logs will be "
                "withheld rather than attached unmasked"
            )
            filt = None

        def _masked(text: str) -> Optional[str]:
            """`text` masked, or None when it could not be.

            The sentinel check is what makes the None branch reachable at all.
            mask_text does not raise and does not return None: it has a blanket
            `except` that reports failure by RETURNING one of
            SENTINEL_MESSAGES -- `[REDACTED: Masking Circuit Open]`,
            `[MASKING_ERROR - OUTPUT_SUPPRESSED_FOR_SECURITY]`. Those were
            non-None, so they passed straight through as the value, and the
            structured report sent to GMS became that bare sentinel instead of
            MASKING_FAILED_REPORT -- not even valid JSON, for a consumer that
            parses it. The fail-closed substitution below was unreachable while
            the thing it substitutes for was being shipped.

            Matched on the whole string, not a substring: a sentinel is the
            entire return value when masking fails, and a report that merely
            quotes one is a real report.
            """
            if filt is None:
                return None
            try:
                masked = filt.mask_text(text)
            except Exception:
                logger.exception("Could not mask task output; withholding it")
                return None
            if masked in SENTINEL_MESSAGES:
                logger.error(
                    "Masking reported failure via %r; withholding this output",
                    masked,
                )
                return None
            return masked

        if os.path.exists(report_file):
            try:
                with open(report_file) as fp:
                    report_content = fp.read()
                # `is None`, not `or`: _masked returns None ONLY when masking
                # failed, and an empty report is a legitimate result. Joining
                # them with `or` told an operator their quiet run's report was
                # "withheld ... could leak a credential", which is both false
                # and the fastest way to teach them to ignore the message when
                # it is true.
                masked_report = _masked(report_content)
                ctx.get_report().set_structured_report(
                    MASKING_FAILED_REPORT if masked_report is None else masked_report
                )
            except Exception:
                logger.exception(
                    "Failed to process structured report from %s", report_file
                )

        try:
            log_text = SubProcessTaskUtil._format_log_lines(log_lines)
            # Same distinction as the report above: a run that printed nothing
            # is not a run whose logs had to be withheld.
            masked_logs = _masked(log_text)
            ctx.get_report().set_logs(
                MASKING_FAILED_LOGS if masked_logs is None else masked_logs
            )
        except Exception:
            logger.exception("Failed to set logs on execution report")

        # Before the directory removal and guarded like it: the shared lock on
        # a cached venv is what keeps eviction from deleting it mid-run, so it
        # has to be let go of here or the entry becomes immortal and the cache
        # can never be trimmed. An ephemeral venv carries no lock, so this is a
        # no-op for every path where the cache is off or unusable.
        try:
            if venv_ref is not None and venv_ref.lock is not None:
                venv_ref.lock.release()
        except Exception:
            logger.exception("Failed to release the venv cache lock")

        # Last, and guarded separately: this directory holds the run's reports,
        # with real object names in them, so leaving it behind on a failure
        # further up is the worst outcome available.
        try:
            SubProcessTaskUtil._remove_directory(exec_out_dir)
        except Exception:
            logger.exception("Failed to remove execution directory %s", exec_out_dir)

    @staticmethod
    async def prepare_recipe_run(
        args: "SubProcessRecipeTaskArgs",
        *,
        execution_ctx: ExecutionContext,
        executor_ctx: ExecutorContext,
        exec_out_dir: str,
        venv_version: Optional[str] = None,
        venv_logs: Optional[LogHolder] = None,
        env_extra: Optional[dict] = None,
        envelope_extra: Optional[dict] = None,
    ) -> "PreparedRun":
        """Everything a recipe task needs before it can spawn its child.

        `exec_out_dir` is taken rather than derived so a caller that also lays
        out artifact directories under it (the ingestion task) computes it once.
        """
        recipe, secret_values = SubProcessTaskUtil._resolve_recipe(
            args.recipe, execution_ctx=execution_ctx, executor_ctx=executor_ctx
        )
        plugin = SubProcessTaskUtil._get_plugin_from_recipe(recipe)
        # Whether THIS call brings the directory into being, which decides
        # whether it is ours to remove below.
        ours = not Path(exec_out_dir).exists()
        Path(exec_out_dir).mkdir(0o755, parents=True, exist_ok=True)

        try:
            venv_ref = await SubProcessTaskUtil.setup_task_venv(
                args, plugin, exec_out_dir, version=venv_version, logs=venv_logs
            )
        except Exception:
            # setup_venv writes an EXPANDED requirements file in here -- env-var
            # templates resolved, so a private index URL carries its token in
            # clear text on disk. The only caller invokes this outside the try
            # whose finally calls finalize_task_output, so nothing else was
            # scheduled to remove it and a failed run left the credential
            # behind.
            #
            # Only what this call created. exec_out_dir may already exist
            # because a caller laid artifact directories out under it first,
            # and deleting someone else's directory while unwinding a failure
            # is a worse bug than the one being fixed.
            if ours:
                SubProcessTaskUtil._remove_directory(exec_out_dir)
            raise
        return PreparedRun(
            recipe=recipe,
            plugin=plugin,
            venv_ref=venv_ref,
            subprocess_env=SubProcessTaskUtil.build_subprocess_env(
                args, venv_ref, extra=env_extra
            ),
            stdin_envelope=SubProcessTaskUtil.build_stdin_envelope(
                args, recipe, secret_values, extra=envelope_extra
            ),
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
