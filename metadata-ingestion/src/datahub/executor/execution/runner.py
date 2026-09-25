import collections
import contextlib
import functools
import json
import logging
import os
import pathlib
import shlex
import shutil
import subprocess
import sys
from collections.abc import Generator, Iterator
from datetime import datetime, timezone
from typing import Mapping, Optional, Union
from urllib.parse import urlparse, urlsplit, urlunsplit

# Note: BaseExceptionGroup handling removed for Python 3.9 compatibility
import anyio
import anyio.abc
import anyio.streams.text

# TODO: promote to a public config_loader helper.
from datahub.configuration.config_loader import _extract_env_var_names
from datahub.executor.common.env_config import (
    get_bundled_venv_path,
    get_dependency_resolution_enabled,
)
from datahub.executor.execution.venv_cache import (
    VenvCache,
)
from datahub.executor.execution.venv_config import (
    VenvConfig,
    VenvReference,
    _pages_wheel_url,
)
from datahub.executor.execution.venv_utils import (
    VENV_NO_DATAHUB,
    VENV_VERSION_BUNDLED,
    VENV_VERSION_LATEST,
    VENV_VERSION_NATIVE,
    VenvKind,
    classify_version,
)
from datahub.masking.masking_filter import SecretMaskingFilter
from datahub.masking.secret_registry import SecretRegistry

logger = logging.getLogger(__name__)


def referenced_env_values(
    reqs: list[str], env: Optional[Mapping[str, str]] = None
) -> dict[str, str]:
    """Values of only the env vars the user references in pip requirements.

    `env` defaults to the ambient environment. Callers that build with a
    different one -- setup_venv applies extra_env_vars on top -- pass it, so
    the value actually used is the value registered for masking.
    """
    source = os.environ if env is None else env
    values: dict[str, str] = {}
    for req in reqs:
        for name in _extract_env_var_names(req):
            value = source.get(name)
            if value is not None:
                values[name] = value
    return values


_DEFAULT_MAX_LOG_LINES = 2000
_DEFAULT_MAX_BYTES_PER_LINE = 2**12  # 4kb
# Kafka has a 1mb limit on the size of a data packet.
# Doing 90% of that so we have some buffer for other things.
_DEFAULT_MAX_LOG_SIZE_BYTES = int(0.9 * 2**18)  # 90% of 1mb

BUNDLED_VENV_PATH_ENV = "DATAHUB_BUNDLED_VENV_PATH"


def _validate_wheel_url(url: str) -> bool:
    """Validate that a wheel URL is from an allowed domain."""
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        logger.error(f"Invalid URL format: {url}")
        return False
    if not parsed.netloc.endswith(".datahub-wheels.pages.dev"):
        logger.error(f"URL domain not allowed: {parsed.netloc}")
        return False
    return True


def _bundled_constraints_path(venv_loc: pathlib.Path) -> Optional[pathlib.Path]:
    """Locate datahub/constraints.txt inside an installed acryl-datahub."""
    matches = list(venv_loc.glob("lib/python*/site-packages/datahub/constraints.txt"))
    if not matches:
        logger.warning("No bundled constraints.txt in installed acryl-datahub.")
        return None
    return matches[0]


@functools.cache
def _find_uv() -> str:
    # If we're running with a venv activated, then uv should be in the path.
    uv = shutil.which("uv", path=f"{sys.prefix}/bin")
    if uv is not None:
        return uv

    # The other possibility is that uv is installed globally.
    uv = shutil.which("uv")
    if uv is not None:
        return uv

    raise RuntimeError("uv not found in PATH.")


class LogHolder:
    def __init__(
        self,
        max_log_lines: Union[int, None] = _DEFAULT_MAX_LOG_LINES,
        max_bytes_per_line: int = _DEFAULT_MAX_BYTES_PER_LINE,
        max_log_size_bytes: int = _DEFAULT_MAX_LOG_SIZE_BYTES,
        echo_to_stdout_prefix: Union[str, None] = None,
    ):
        self._max_log_lines = max_log_lines
        self._total_log_lines = 0
        self._max_bytes_per_line = max_bytes_per_line
        self._max_log_size_bytes = max_log_size_bytes
        self._echo_logs_prefix = echo_to_stdout_prefix

        self._lines: collections.deque[str] = collections.deque(
            maxlen=self._max_log_lines
        )
        self._create_new_line: bool = True
        self.most_recent_log_ts: Union[datetime, None] = None

    def clear(self) -> None:
        self._lines.clear()
        self._create_new_line = True
        self.most_recent_log_ts = None

    def append_masked(self, content: str) -> None:
        """Masks the whole buffer before splitting, so multi-line secrets cannot straddle lines."""
        masked = SecretMaskingFilter(SecretRegistry.get_instance()).mask_text(content)
        for line in masked.splitlines():
            self.append(f"{line}\n")

    def append(self, partial_line: str) -> None:
        self.most_recent_log_ts = datetime.now(tz=timezone.utc)

        if self._create_new_line:
            self._lines.append("")
            self._create_new_line = False
            self._total_log_lines += 1

        current_line_length = len(self._lines[-1])
        if current_line_length < self._max_bytes_per_line:
            allowed_length = self._max_bytes_per_line - current_line_length

            if len(partial_line) > allowed_length:
                add_to_line = f"{partial_line[:allowed_length]} [...truncated]\n"
            else:
                add_to_line = partial_line

            self._lines[-1] += add_to_line
        else:
            # If we've already reached the max line length, then we simply ignore the rest of the line.
            pass

        # If partial_line ends with a '\n', then the line is complete.
        if partial_line.endswith("\n"):
            if self._echo_logs_prefix is not None:
                logger.debug(
                    "%s%s", self._echo_logs_prefix, self._lines[-1].rstrip("\n")
                )

            # On the next append, we'll create a new line.
            self._create_new_line = True

    @contextlib.contextmanager
    def changed_echo_prefix(self, new_prefix: Union[str, None]) -> Iterator[None]:
        old_prefix = self._echo_logs_prefix
        self._echo_logs_prefix = new_prefix
        try:
            yield
        finally:
            self._echo_logs_prefix = old_prefix

    def set_command(self, command: str) -> None:
        self.append(f"+{command}\n")

    def force_new_line(self) -> None:
        if not self._create_new_line:
            # This means the existing output did not end with a newline.
            self.append("\n")

    def get_logs(self, skip_lines: int = 0) -> str:
        text = "".join(list(self._lines)[skip_lines:])

        # Python slices are super permissive on index bounds, so this works.
        text = text[-self._max_log_size_bytes :]

        if self._max_log_lines and len(self._lines) >= self._max_log_lines:
            lines_truncated = self._total_log_lines - len(self._lines)
            text = f"[{lines_truncated} earlier log lines truncated...]\n{text}"

        return text

    # Added functionality on top of base.
    def get_lines(self) -> list[str]:
        """Get the lines as a list for compatibility with existing code."""
        return list(self._lines)


# Simplified exception group handling for anyio task groups
@contextlib.contextmanager
def collapse_excgroups() -> Generator[None, None, None]:
    """
    Collapse single-exception groups from anyio task groups.
    This provides a consistent exception interface across Python versions.
    """
    try:
        yield
    except BaseException as exc:
        # On Python 3.11+, anyio may wrap exceptions in groups
        # Try to unwrap single-exception groups
        if hasattr(exc, "exceptions") and len(getattr(exc, "exceptions", [])) == 1:
            exc = exc.exceptions[0]  # type: ignore[attr-defined]
        raise exc


class SubprocessRunner:
    def __init__(self, logs: Union[LogHolder, None] = None) -> None:
        self._logs = logs or LogHolder()
        self._process: Union[anyio.abc.Process, None] = None

    @property
    def logs(self) -> LogHolder:
        return self._logs

    @property
    def pid(self) -> Optional[int]:
        return self._process.pid if self._process else None

    async def execute(
        self,
        command: list[str],
        env: Union[dict[str, str], None] = None,
        cwd: Union[str, pathlib.Path, None] = None,
    ) -> None:
        self._logs.force_new_line()

        self._logs.set_command(shlex.join(command))
        self._process = await anyio.open_process(
            command,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=cwd,
        )
        # Track if subprocess failed (will raise after task group exits)
        error_returncode: Union[int, None] = None

        with collapse_excgroups():
            async with self._process, anyio.create_task_group() as tg:
                tg.start_soon(self._read_logs, name="read_logs")  # type: ignore[arg-type]

                try:
                    await self._process.wait()

                except anyio.get_cancelled_exc_class():
                    # On cancellation, gracefully kill the subprocess.
                    if self._process.returncode is None:
                        with anyio.CancelScope(shield=True):
                            await self.kill()

                    raise

                else:
                    # Store error returncode if process exited with non-zero status.
                    # We'll raise the exception after the task group exits, when logs are fully captured.
                    if (
                        self._process.returncode is not None
                        and self._process.returncode != 0
                    ):
                        error_returncode = self._process.returncode

                finally:
                    tg.cancel_scope.cancel()

        # Now that the task group has exited and _read_logs has completed,
        # we can safely read the captured logs and raise the error with full output.
        if error_returncode is not None:
            captured_output = self._logs.get_logs()
            error = subprocess.CalledProcessError(
                returncode=error_returncode,
                cmd=command,
                output=captured_output,
            )
            # Set stderr to the captured output so it's visible in exception handlers
            if captured_output:
                error.stderr = (
                    f"Command failed with captured output:\n{captured_output}"
                )
            raise error

    async def _read_logs(self) -> None:
        assert self._process is not None
        assert self._process.stdout is not None

        try:
            async with (
                self._process.stdout,
                anyio.streams.text.TextReceiveStream(
                    self._process.stdout
                ) as text_stream,
            ):
                async for text in text_stream:
                    # Split into newline-delimited chunks, where the last chunk may not end with a newline.
                    lines = text.split("\n")
                    for line in lines[:-1]:
                        self._logs.append(line + "\n")
                    if lines[-1] != "":
                        # The last chunk did not end with a newline, so we have a partial line at the end
                        self._logs.append(lines[-1])
        finally:
            self._logs.force_new_line()

    async def kill(self, graceful_wait_sec: int = 5) -> None:
        # First send a SIGTERM to the process.
        # If hasn't exited after a few seconds, then send a SIGKILL.
        # In general, we shouldn't need to use this directly, since we can just cancel the task.

        assert self._process is not None

        try:
            if graceful_wait_sec:
                self._process.terminate()

                with anyio.move_on_after(graceful_wait_sec):
                    await self._process.wait()
        finally:
            if self._process.returncode is None:
                self._process.kill()

                with anyio.CancelScope(shield=True):
                    await self._process.wait()


# How long setup_venv is willing to wait for a peer that holds the same cache
# entry. Deliberately far too short to "wait for the build": a real venv build
# takes minutes, and a task that has one holds the entry SHARED for its whole
# life -- hours, for an ingestion run. This budget exists only to absorb the
# sub-second windows in which a peer holds the entry EXCLUSIVE to check
# completeness, discard a partial directory or write the completion marker.
# Past it we build a per-run venv instead, which is what happened for every
# concurrent run before this cache existed: slower than a hit, always correct,
# and it cannot hang. 10 x 0.15s caps the wait at ~1.35s, small enough to be
# invisible next to a build and long enough to cover a peer that is only
# stat-ing the entry.


async def _install_extra_requirements(
    runner: SubprocessRunner,
    venv_loc: pathlib.Path,
    expanded_pip_reqs: list[str],
    venv_env: dict[str, str],
) -> bool:
    """Install extra_pip_requirements, leaving no credential on disk.

    Returns whether the requirements file is known to be gone. False means the
    expanded token may still be in the entry, so the caller must not publish
    it to the shared cache -- the same contract as
    _scrub_direct_url_credentials, and for the same reason: an entry other
    tasks can reach must not carry a credential.

    These are the EXPANDED requirements: a private index URL arrives here with
    its token already substituted in. The file is only an input to `uv pip
    install -r` and nothing reads it afterwards, so it does not outlive the
    install.

    For a cacheable venv `venv_loc` is the cache root, which
    get_venv_cache_path deliberately places outside the directory
    finalize_task_output removes -- so nothing else would ever clean this up,
    and the entry is readable by every task on the node.
    """
    extra_req_file = venv_loc / "extra-requirements.txt"
    removed = False
    # The try opens before the file is CREATED, not before the install: once
    # the path is chosen, write_text and the log appends can each fail with
    # the token already on disk.
    try:
        # 0600 before the contents are written, so there is no window at the
        # ambient umask. chmod as well as touch(mode=...), because mode only
        # applies when touch creates the file and a discarded incomplete venv
        # can leave a stale one behind.
        extra_req_file.touch(mode=0o600, exist_ok=True)
        extra_req_file.chmod(0o600)
        extra_req_file.write_text("\n".join(expanded_pip_reqs))
        runner._logs.append(f"Installing extra requirements from: {extra_req_file}\n")
        runner._logs.append_masked("\n".join(expanded_pip_reqs))
        await runner.execute(
            [_find_uv(), "pip", "install", "-r", str(extra_req_file)],
            env=venv_env,
        )
    finally:
        # In a `finally` because an install failing on a bad token is exactly
        # the run whose requirements file must not be left behind. Guarded so
        # a full or read-only cache filesystem cannot turn a build that
        # otherwise succeeded into a failure -- but the failure is REPORTED
        # rather than only logged, because "the file is still there" and "the
        # entry is safe to share" are the same question.
        try:
            extra_req_file.unlink(missing_ok=True)
            removed = True
        except OSError:
            logger.warning(
                "Could not remove %s; it holds expanded requirements and may "
                "contain a credential",
                extra_req_file,
                exc_info=True,
            )
    return removed


def _scrub_direct_url_credentials(venv_loc: pathlib.Path) -> bool:
    """Remove credentials uv recorded inside the installed packages.

    Returns whether the venv is known clean. False means at least one record
    could not be read or rewritten, so a credential may still be in there --
    the caller must not publish the entry to the shared cache.

    PEP 610 has the installer write
    site-packages/<pkg>.dist-info/direct_url.json for anything installed from
    a URL, and uv writes the EXPANDED requirement -- so
    `pkg @ https://user:${TOKEN}@host/pkg.whl` leaves the token in the venv,
    where _install_extra_requirements' own cleanup cannot see it. It removes
    the requirements FILE; this removes what the install copied out of it.

    Before the venv cache this was bounded: the venv lived under exec_out_dir
    and went away with the run. A cacheable venv is deliberately outside
    exec_out_dir, created by `uv venv` at the ambient umask, shared by every
    task on the node, and kept until eviction.

    Userinfo and query string both go: basic-auth credentials and signed-URL
    tokens are equally common in private indexes. The scheme, host and path
    stay, because "installed from this host" is useful and is not the secret.

    The netloc is rewritten by cutting at the last `@` rather than through
    `parts.hostname`/`parts.port`. hostname strips the brackets from an IPv6
    literal (`[::1]:8080` becomes an unparseable `::1:8080`), and reading
    .port raises ValueError on a non-numeric port -- from a function whose
    failure must never reach the caller as an exception.
    """
    clean = True
    for record in venv_loc.glob(
        "lib/python*/site-packages/*.dist-info/direct_url.json"
    ):
        try:
            payload = json.loads(record.read_text())
            if not isinstance(payload, dict):
                continue
            url = payload.get("url")
            if not isinstance(url, str):
                continue
            parts = urlsplit(url)
            if "@" not in parts.netloc and not parts.query:
                continue
            payload["url"] = urlunsplit(
                (
                    parts.scheme,
                    parts.netloc.rsplit("@", 1)[-1],
                    parts.path,
                    "",
                    parts.fragment,
                )
            )
            record.write_text(json.dumps(payload))
        except Exception:
            # Every step is inside, not just the read: urlsplit raises on a
            # malformed IPv6 URL and json.loads on anything non-JSON, and an
            # escape from here fails an otherwise complete venv build.
            clean = False
            logger.warning(
                "Could not redact a credential possibly recorded in %s; this "
                "venv will not be published to the shared cache",
                record,
                exc_info=True,
            )
    return clean


# I had to change this from the base file because we needed to introduce
# support for handling bundled venvs.
def _resolve_fixed_venv(
    venv_config: VenvConfig,
    runner: SubprocessRunner,
    bundled_venv_path: Optional[pathlib.Path],
) -> Optional[VenvReference]:
    """The venv for a version that is not built at runtime, if this is one.

    `native` runs in the executor's own interpreter; `bundled` uses a venv
    baked into the image at build time and is only verified, never built.
    None means the caller has to build one.
    """
    if venv_config.version == VENV_VERSION_NATIVE:
        return VenvReference(
            venv_loc=pathlib.Path(sys.prefix),
            venv_config=venv_config,
        )

    # New: Handle bundled startup venvs
    if venv_config.version == VENV_VERSION_BUNDLED:
        if bundled_venv_path is None:
            bundled_venv_path = pathlib.Path(get_bundled_venv_path())

        if venv_config.main_plugin is None:
            raise ValueError(
                "Cannot determine venv name for bundled version: main_plugin is required"
            )

        # Use simple naming scheme for bundled venvs: plugin-bundled
        venv_name = f"{venv_config.main_plugin}-bundled"
        venv_loc = bundled_venv_path / venv_name

        if not venv_loc.exists() or not (venv_loc / "bin/python").exists():
            raise FileNotFoundError(
                f"Bundled startup venv not found: {venv_loc}\n"
                f"Expected venv name: {venv_name}\n"
                f"The requested venv was not built during Docker image creation. "
                f"This indicates the Dockerfile build process failed or the venv naming doesn't match."
            )

        runner._logs.append(f"Using existing bundled startup venv: {venv_loc}\n")
        return VenvReference(
            venv_loc=venv_loc,
            venv_config=venv_config,
        )
    return None


async def _install_datahub(
    runner: SubprocessRunner,
    venv_config: VenvConfig,
    venv_loc: pathlib.Path,
    venv_env: dict,
) -> None:
    """Install acryl-datahub into a freshly created venv.

    Three shapes: a caller-supplied requirements file installed verbatim,
    nothing at all for NO_ACRYL_DATAHUB, or acryl-datahub composed as a
    named requirement. The last does two passes -- bare install to read
    the wheel's bundled constraints, then the plugin extras under them.
    """
    version = venv_config.version
    if venv_config.requirements_file is not None:
        # Case 2: the caller supplied its own requirements file, so install from it
        # verbatim rather than composing an acryl-datahub requirement line.
        runner._logs.append(
            f"Installing requirements from: {venv_config.requirements_file}\n"
        )
        runner._logs.append_masked(venv_config.requirements_file.read_text())
        install_cmd = [
            _find_uv(),
            "pip",
            "install",
            "-r",
            str(venv_config.requirements_file),
        ]
        runner._logs.append(f"Installing datahub: {' '.join(install_cmd)}\n")
        await runner.execute(install_cmd, env=venv_env)
    elif version == VENV_NO_DATAHUB:
        pass
    else:
        # Case 1: install acryl-datahub as a named requirement. uv keys its cache
        # by source path for local wheels, so installing from a per-run wheel path
        # wrote a fresh ~20mb archive entry for the same version on every run.
        plugins_list = list(
            filter(None, [venv_config.main_plugin, *venv_config.extra_pip_plugins])
        )
        plugins = f"[{','.join(plugins_list)}]" if plugins_list else ""

        url = ""
        is_dev_build = classify_version(version) is VenvKind.DEV_BUILD
        if is_dev_build:
            if not _validate_wheel_url(version):
                raise RuntimeError(
                    f"Invalid wheel URL: {version}. "
                    "Non-.whl URLs must be from *.datahub-wheels.pages.dev."
                )
            url = version if version.endswith(".whl") else _pages_wheel_url(version)

        def _requirement(extras: str) -> str:
            if is_dev_build:
                return f"acryl-datahub{extras} @ {url}"
            if version == VENV_VERSION_LATEST:
                return f"acryl-datahub{extras}"
            return f"acryl-datahub{extras}=={version}"

        # Dev builds bypass the uv cache: always re-fetch a rebuilt wheel, persist nothing.
        # This makes usage of dev packages inefficient, but prevents cache build up which causes
        # executor pods to over-consume storage.
        install_env = {**venv_env, "UV_NO_CACHE": "1"} if is_dev_build else venv_env

        # Install acryl-datahub alone first to read its bundled constraints, then
        # install with plugins under those constraints.
        bootstrap_cmd = [
            _find_uv(),
            "pip",
            "install",
            "--no-deps",
            _requirement(""),
        ]
        runner._logs.append(
            f"Installing datahub (constraints bootstrap): {' '.join(bootstrap_cmd)}\n"
        )
        await runner.execute(bootstrap_cmd, env=install_env)
        constraints_path = _bundled_constraints_path(venv_loc)

        install_cmd = [_find_uv(), "pip", "install", _requirement(plugins)]
        if constraints_path:
            install_cmd.extend(["--constraint", str(constraints_path)])

        runner._logs.append(f"Installing datahub: {' '.join(install_cmd)}\n")
        await runner.execute(install_cmd, env=install_env)


async def setup_venv(
    venv_config: VenvConfig,
    runner: SubprocessRunner,
    tmp_dir: pathlib.Path,
    bundled_venv_path: Optional[pathlib.Path] = None,
) -> VenvReference:
    """
    Set up a virtual environment based on the configuration.

    Args:
        venv_config: Configuration for the venv
        runner: Subprocess runner for executing commands
        tmp_dir: Temporary directory for dynamic venvs
        bundled_venv_path: Path where bundled startup venvs are stored

    Returns:
        VenvReference: Reference to the created/found venv

    Raises:
        ValueError: If dependency resolution is disabled and non-bundled version requested
        FileNotFoundError: If bundled venv is requested but not found
        subprocess.CalledProcessError: If venv creation fails
    """
    # Validate dependency resolution compatibility
    validate_dependency_resolution_enabled(venv_config.version)

    fixed = _resolve_fixed_venv(venv_config, runner, bundled_venv_path)
    if fixed is not None:
        return fixed

    # Register BOTH the ambient value and the extra_env_vars override: the
    # venv is built from {**os.environ, **extra_env_vars}, so the override is
    # what a failing index URL echoes back. Two calls rather than a merged
    # dict, because both live under the same NAME and the registry keeps
    # MAX_SECRET_VERSIONS of those -- merging would keep only the last.
    _registry = SecretRegistry.get_instance()
    _registry.register_secrets_batch(
        referenced_env_values(venv_config.extra_pip_requirements)
    )
    _registry.register_secrets_batch(
        referenced_env_values(
            venv_config.extra_pip_requirements,
            {**os.environ, **venv_config.extra_env_vars},
        )
    )

    # Expand env-var templates once so that the venv cache key and the
    # requirements file see the same os.environ snapshot.
    expanded_pip_reqs = venv_config.resolve_pip_requirements()

    cache_name = venv_config.cache_name(expanded_pip_reqs)
    cache = VenvCache(tmp_dir)
    entry = await cache.acquire(cache_name)
    venv_loc, lock = entry.venv_loc, entry.lock

    venv_reference = VenvReference(
        venv_loc=venv_loc,
        venv_config=venv_config,
        lock=lock,
    )

    try:
        if cache.resolve_existing(entry, runner):
            return venv_reference

        runner._logs.append(f"Creating new venv: {venv_loc}\n")

        # Create the venv. We need to pass --python <executable> so that uv uses the same
        # Python as the current process.
        await runner.execute(
            [_find_uv(), "venv", "--python", sys.executable, str(venv_loc)]
        )

        venv_env = {
            **os.environ,
            **venv_config.extra_env_vars,
            "VIRTUAL_ENV": str(venv_loc),
        }

        await _install_datahub(runner, venv_config, venv_loc, venv_env)

        # Pass 2: Install extra_pip_requirements without constraints.
        requirements_removed = True
        if venv_config.requirements_file is None and expanded_pip_reqs:
            requirements_removed = await _install_extra_requirements(
                runner, venv_loc, expanded_pip_reqs, venv_env
            )

        # BOTH gate publication: the expanded requirements file, and what uv
        # copied out of it into direct_url.json. An entry that could not be
        # fully cleaned stays unmarked -- this run uses it, the next claimant
        # rebuilds it rather than inheriting a token. The scrub runs either
        # way; it is worth doing for this run's own venv regardless.
        scrubbed = _scrub_direct_url_credentials(venv_loc)
        if requirements_removed and scrubbed:
            venv_reference.lock = cache.publish(venv_reference.lock, venv_loc)
        else:
            logger.warning(
                "Not publishing %s to the venv cache: %s.",
                venv_loc,
                "the expanded requirements file could not be removed"
                if not requirements_removed
                else "a credential recorded by the installer could not be redacted",
            )

        return venv_reference
    except BaseException:
        # Scrub before unwinding: uv writes direct_url.json as each
        # requirement installs, so a run that failed on the second of two
        # private-index requirements has already left an expanded token in a
        # half-built entry sitting in the shared cache root. Guarded so it
        # cannot replace the exception in flight.
        try:
            _scrub_direct_url_credentials(venv_loc)
        except Exception:
            logger.exception("Cleanup: failed to scrub credentials from %s", venv_loc)
        if lock is not None:
            lock.release()
        raise


def validate_dependency_resolution_enabled(version: str) -> None:
    """
    Validate that the requested version is compatible with dependency resolution settings.

    Raises:
        ValueError: If version is incompatible with dependency resolution settings.
    """
    dependency_resolution_enabled = get_dependency_resolution_enabled()

    if not dependency_resolution_enabled and version != VENV_VERSION_BUNDLED:
        raise ValueError(
            f"Version '{version}' is not supported when INGESTION_DEPENDENCY_RESOLUTION_ENABLED=false. "
            f"Only version 'bundled' is allowed when dynamic dependency resolution is disabled. "
            f"This ensures that only bundled, pre-built venvs are used without runtime package installation."
        )
