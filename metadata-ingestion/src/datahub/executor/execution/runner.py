import asyncio
import collections
import contextlib
import dataclasses
import functools
import hashlib
import json
import logging
import os
import pathlib
import shlex
import shutil
import subprocess
import sys
import time
from collections.abc import Generator, Iterator
from datetime import datetime, timezone
from typing import Annotated, Any, Mapping, Optional, Union
from urllib.parse import urlparse, urlsplit, urlunsplit

# Note: BaseExceptionGroup handling removed for Python 3.9 compatibility
import anyio
import anyio.abc
import anyio.streams.text
import pydantic
from expandvars import (
    ExpandvarsException,
    UnboundVariable,
    expand as _expandvars_expand,
)
from packaging.requirements import InvalidRequirement, Requirement

# TODO: promote to a public config_loader helper.
from datahub.configuration.config_loader import _extract_env_var_names
from datahub.executor.common.env_config import (
    get_bundled_venv_path,
    get_dependency_resolution_enabled,
    get_venv_cache_enabled,
    get_venv_cache_latest_ttl_sec,
    get_venv_cache_max_age_sec,
    get_venv_cache_max_entries,
)
from datahub.executor.execution.venv_cache import EntryLock, evict_stale_entries
from datahub.executor.execution.venv_utils import (
    COMPLETE_MARKER,
    built_at,
    is_venv_complete,
    mark_venv_complete,
    touch_last_used,
    venv_location,
)
from datahub.masking.masking_filter import SecretMaskingFilter
from datahub.masking.secret_registry import SecretRegistry

logger = logging.getLogger(__name__)


def _expand_pip_req(req: str) -> str:
    """Expand ${VAR:-default} templates in a pip requirement string.

    Only expands entries that contain ${, so plain pip specs and URLs with bare
    $ mid-string (e.g. ?sig=$TOKEN) are passed through unchanged.

    Uses nounset semantics — matching config_loader.py's pattern — so that
    ${VAR} with no default raises a RuntimeError rather than silently expanding
    to an empty string and producing a blank pip requirement that uv skips.
    """
    if "${" not in req:
        return req
    try:
        return _expandvars_expand(req, nounset=True)
    except UnboundVariable as e:
        raise RuntimeError(
            f"pip requirement {req!r} references unset environment variable {e}. "
            "Set the variable or add a default (e.g. ${VAR:-fallback})."
        ) from e
    except ExpandvarsException as e:
        raise RuntimeError(
            f"pip requirement {req!r} has invalid environment variable syntax: {e}"
        ) from e


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

VENV_VERSION_LATEST = "latest"
VENV_VERSION_BUNDLED = "bundled"
VENV_VERSION_NATIVE = "native"
VENV_NO_DATAHUB = "NO_ACRYL_DATAHUB"

BUNDLED_VENV_PATH_ENV = "DATAHUB_BUNDLED_VENV_PATH"


def _pages_wheel_url(base_url: str) -> str:
    """Build the wheel download URL for a DataHub Pages dev build, with cache-busting timestamp."""
    now = datetime.now(tz=timezone.utc)
    return f"{base_url}/artifacts/wheels/acryl_datahub-0.0.0.dev1-py3-none-any.whl?ts={now.timestamp()}"


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


def pydantic_parse_json(v: Any) -> Any:
    if isinstance(v, str):
        return json.loads(v)
    return v


class VenvConfig(pydantic.BaseModel):
    version: str = VENV_VERSION_LATEST
    main_plugin: Union[str, None] = None
    extra_pip_requirements: Annotated[
        list[str], pydantic.BeforeValidator(pydantic_parse_json)
    ] = []
    extra_pip_plugins: Annotated[
        list[str], pydantic.BeforeValidator(pydantic_parse_json)
    ] = []
    extra_env_vars: Annotated[dict, pydantic.BeforeValidator(pydantic_parse_json)] = {}
    requirements_file: Union[pathlib.Path, None] = None

    def set_main_plugin(self, plugin: str) -> None:
        self.main_plugin = plugin

    def resolve_pip_requirements(self) -> list[str]:
        """Expand env-var templates in extra_pip_requirements."""
        return [_expand_pip_req(r) for r in self.extra_pip_requirements]

    def get_stable_venv_name(
        self, expanded_pip_reqs: Union[list[str], None] = None
    ) -> Union[str, None]:
        if self.requirements_file is not None:
            suffix = hashlib.sha256()
            suffix.update(self.requirements_file.read_bytes())
            return f"req-{suffix.digest().hex()[:16]}"

        if self.main_plugin is None:
            return None
        if (
            self.version == VENV_VERSION_LATEST
            or self.version == VENV_VERSION_NATIVE
            or self.version == VENV_VERSION_BUNDLED
            or self.version == VENV_NO_DATAHUB
            or self.version.startswith("http")
        ):
            return None

        # Generate a stable name for the venv.
        # Hash the expanded values so that changing DATAHUB_INTEGRATIONS_PACKAGE_SPEC
        # (or any other env-var template) forces a new venv rather than reusing a
        # cached one with the old spec. Callers may pass pre-expanded reqs (from
        # resolve_pip_requirements()) so that hash time and install time use the same
        # os.environ snapshot.
        suffix = hashlib.sha256()
        suffix.update(self.version.encode("utf-8"))
        reqs_for_hash = (
            expanded_pip_reqs
            if expanded_pip_reqs is not None
            else self.resolve_pip_requirements()
        )
        suffix.update(str(reqs_for_hash).encode("utf-8"))
        suffix.update(str(self.extra_pip_plugins).encode("utf-8"))

        return f"{self.main_plugin}-{suffix.digest().hex()[:16]}"

    def get_acryl_datahub_requirement_line(self) -> str:
        plugins = ""
        plugins_list = filter(None, [self.main_plugin, *self.extra_pip_plugins])
        if plugins_list:
            plugins = f"[{','.join(plugins_list)}]"

        if self.version == VENV_VERSION_LATEST:
            return f"acryl-datahub{plugins}"
        elif self.version == VENV_NO_DATAHUB:
            return "# acryl-datahub is explicitly not requested."
        elif self.version.startswith("http"):
            url = (
                self.version
                if self.version.endswith(".whl")
                else _pages_wheel_url(self.version)
            )
            return f"acryl-datahub{plugins} @ {url}"
        else:
            return f"acryl-datahub{plugins}=={self.version}"


@dataclasses.dataclass
class VenvReference:
    venv_loc: pathlib.Path
    venv_config: VenvConfig
    # Held for the task's life when this venv came from the cache, so eviction
    # cannot delete it mid-run. None for ephemeral venvs and whenever the cache
    # is off or unusable. finalize_task_output releases it.
    lock: Optional["EntryLock"] = None

    def command(self, cmd: str) -> str:
        return str(self.venv_loc / "bin" / cmd)

    def extra_envs(self) -> dict[str, str]:
        return {
            **self.venv_config.extra_env_vars,
            # TODO: Do we need to add this?
            # "VIRTUAL_ENV": str(self.venv_loc),
        }


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


def _node_local_stable_name(
    venv_config: VenvConfig, expanded_pip_reqs: list[str]
) -> Optional[str]:
    """A cache name for the versions get_stable_venv_name() refuses.

    It refuses `latest` because a moving target could be stale indefinitely,
    and that holds for a cache outliving the process. This one is node-local
    and dies with the pod, so the staleness window is the pod's lifetime.

    Dev-build wheel URLs are included, for the opposite reason: the build
    pipeline hands out a per-deployment address, which names exactly one
    immutable build, so it is a content address in a way `latest` is not. A
    new commit is a new deployment and therefore a new key.

    They were excluded over storage -- every wheel tested leaves an entry
    behind. evict_stale_entries reclaims those on both axes: a dev-build entry
    is dropped once the cache is over DATAHUB_VENV_CACHE_MAX_ENTRIES, and also
    once nothing has used it for DATAHUB_VENV_CACHE_MAX_AGE_HOURS, so a
    one-off wheel test no longer lives as long as the pod does.

    A branch alias served from the same host DOES move, and hashing one would
    pin a stale build if nothing else intervened. _is_fresh_hit covers that:
    every URL version is treated as moving and expires after
    DATAHUB_VENV_CACHE_LATEST_TTL_HOURS. Nothing in the pipeline emits such a
    URL --
    it publishes the deployment address -- so this is only reachable by
    hand-writing one.

    Only the VENV becomes reusable. The PACKAGE cache stays bypassed for dev
    builds (UV_NO_CACHE=1 in setup_venv) and must: every dev wheel ships as the
    same name and version, so a cache keyed on those would hand one commit's
    build to another. Different cache, different key, different argument.
    """
    version = venv_config.version
    if venv_config.main_plugin is None:
        return None
    is_dev_build = version.startswith(("http://", "https://"))
    if version != VENV_VERSION_LATEST and not is_dev_build:
        return None
    suffix = hashlib.sha256()
    # The version STRING, not the URL the install resolves to: _pages_wheel_url
    # appends a cache-busting timestamp, so hashing the resolved URL would make
    # every run a miss and quietly restore the behaviour this removes.
    suffix.update(version.encode("utf-8"))
    # The list the caller already expanded, never a fresh resolve_pip_requirements():
    # get_stable_venv_name() documents that the hash and the install must see one
    # os.environ snapshot, and re-expanding here would let a template that changed
    # between the two reads name the entry after requirements nobody installs.
    suffix.update(str(expanded_pip_reqs).encode("utf-8"))
    suffix.update(str(venv_config.extra_pip_plugins).encode("utf-8"))
    # "latest" keeps its existing shape so entries built before this survive.
    tag = "dev" if is_dev_build else VENV_VERSION_LATEST
    return f"{venv_config.main_plugin}-{tag}-{suffix.digest().hex()[:16]}"


def _extra_env_vars_cache_suffix(extra_env_vars: Mapping[str, object]) -> str:
    """Short digest distinguishing cache entries that differ only in extra_env_vars.

    extra_env_vars is user-supplied per recipe (package index URLs, private-
    index credentials) and IS merged into the environment the venv is built
    and installed under (`venv_env`/`install_env` in setup_venv), but
    get_stable_venv_name() does not hash it. Two recipes differing only in
    extra_env_vars would therefore share one node-local cache entry, and one
    of them would silently get a venv built against the other's index. Before
    the cache was pod-global, that collision was impossible: the name lived
    under the per-execution tmp_dir.

    Hashing here rather than using the value is safe even though these can be
    secrets -- this is a truncated digest, not the value. 16 hex characters,
    not 8: the whole job of this suffix is to keep two different package
    indexes apart, so a collision reintroduces exactly the bug it exists to
    prevent, and 32 bits is a birthday collision at a few tens of thousands
    of distinct environments. Widening it costs nothing but directory-name
    length.

    Each field is LENGTH-PREFIXED rather than delimited. Widening the digest
    only addresses accidental collisions; framing pairs as `key=value\\n`
    leaves a structural one wide open, because a value containing a newline
    impersonates an extra pair. `{"A": "b", "C": "d"}` and `{"A": "b\\nC=d"}`
    digest identically under that framing, and so do
    `{"CREDS": ..., "UV_INDEX_URL": "https://prod/simple"}` and the single key
    that absorbs the second pair into its value. These values are unvalidated
    beyond json.loads and routinely hold multi-line content -- service-account
    JSON, PEM keys, pip/uv config -- so the collision needs no malice to
    happen, and on a shared executor pod a deliberate one is trivial to
    construct. A length prefix cannot be forged from inside a field.
    """
    digest = hashlib.sha256()
    for key, value in sorted(extra_env_vars.items()):
        for field in (key.encode("utf-8"), str(value).encode("utf-8")):
            digest.update(f"{len(field)}:".encode("ascii"))
            digest.update(field)
    return digest.hexdigest()[:16]


def _name_dynamic_venv(
    venv_config: VenvConfig, expanded_pip_reqs: list[str]
) -> tuple[str, bool]:
    """Pick the venv's name and whether it is cacheable.

    A pinned version always gets a stable name. Two more join it while the
    cache is on, both because this cache is node-local and dies with the pod:

      - `latest`, a moving target, whose staleness is therefore bounded by the
        pod's life. It is also the default for every recipe, so excluding it
        would leave the cache almost never hit -- and sharing one entry makes
        a probe and the ingestion run it predicts install the same version,
        which resolving twice does not.
      - A dev-build wheel URL, which unlike `latest` names one immutable
        build. Excluded until now over storage, which evict_stale_entries now
        bounds by both entry count and age. It is the only
        version a probe can run before the `recipe probe` command ships, so
        leaving it uncacheable made every probe re-download its wheel --
        about 4.4s of a 7-9s probe, every time.

    Everything else still gets a random, per-run name.

    The kill switch is consulted BEFORE _node_local_stable_name, not after:
    with the cache disabled, both must fall back to today's ephemeral random
    name exactly, not keep a stable cache-shaped name that just happens to
    live under tmp_dir. A pinned version keeps its stable name either way --
    that's today's behaviour too.
    """
    cache_enabled = get_venv_cache_enabled()
    stable_name = venv_config.get_stable_venv_name(expanded_pip_reqs=expanded_pip_reqs)
    if stable_name is None and cache_enabled:
        stable_name = _node_local_stable_name(venv_config, expanded_pip_reqs)
    cacheable = stable_name is not None and cache_enabled
    if cacheable and venv_config.extra_env_vars:
        # An empty dict -- the overwhelmingly common case -- must leave the
        # name byte-identical to today, so this only applies when there is
        # something to distinguish.
        assert stable_name is not None
        suffix = _extra_env_vars_cache_suffix(venv_config.extra_env_vars)
        stable_name = f"{stable_name}-{suffix}"
    venv_name = stable_name or f"eph-{hashlib.sha256(os.urandom(32)).hexdigest()[:16]}"
    return venv_name, cacheable


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
_CACHE_LOCK_ATTEMPTS = 10
_CACHE_LOCK_RETRY_SEC = 0.15


@functools.lru_cache(maxsize=None)
def _warn_cache_unavailable_once(cache_root: str) -> None:
    """Warn that the cache root cannot be used -- once per root, not per task.

    The lru_cache IS the once-ness: this runs on every task in a long-lived
    pod, and a broken cache root stays broken, so logging per call would emit
    the same line for the pod's lifetime.
    """
    logger.warning(
        "venv cache unavailable at %s (unwritable, or a filesystem without "
        "flock support); falling back to per-run venvs",
        cache_root,
    )


@dataclasses.dataclass(frozen=True)
class _CacheEntry:
    """Where a dynamic venv lives, and the lock this call holds on it."""

    venv_loc: pathlib.Path
    lock: Optional["EntryLock"]
    # False when the venv is not cacheable at all, and also when it was but
    # the cache turned out to be unusable or busy -- in which case venv_loc has
    # already been rebuilt under tmp_dir, matching the ephemeral layout exactly.
    cacheable: bool
    # True when the venv is already complete: the caller returns it as-is and
    # builds nothing. Usually paired with a SHARED `lock`, but not always --
    # a downgrade that loses its hold leaves a usable entry and no lock.
    ready: bool


def _is_pinned_requirement(req: str) -> bool:
    """Whether this requirement names one immutable artifact.

    Only an exact-version pin does. A direct URL does NOT: the artifact
    behind an address can be republished, which is the same reason
    _node_local_stable_name treats a dev-build wheel URL as a moving target.

    An unparseable requirement counts as unpinned. Being wrong that way costs
    a periodic rebuild; being wrong the other way freezes the entry for the
    pod's life.
    """
    try:
        parsed = Requirement(req)
    except InvalidRequirement:
        return False
    if parsed.url:
        return False
    return any(spec.operator in ("==", "===") for spec in parsed.specifier)


def _requirements_file_is_pinned(path: pathlib.Path) -> bool:
    """Whether every line of a requirements file names an immutable artifact.

    Unreadable counts as unpinned, for the same reason an unparseable
    requirement does.
    """
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return False
    for raw in lines:
        line = raw.split("#", 1)[0].strip()
        if not line or line.startswith("-"):
            # Blank, comment, or a pip flag (-r, --index-url). A nested -r
            # is not followed, so treat any flag line as unpinnable.
            if line.startswith("-"):
                return False
            continue
        if not _is_pinned_requirement(line):
            return False
    return True


def _resolves_to_moving_target(
    venv_config: VenvConfig, expanded_pip_reqs: list[str]
) -> bool:
    """Whether this venv's contents can differ tomorrow under the same key.

    The cache key is built from requirement STRINGS, not from what they
    resolve to, so "immutable" has to be judged on the strings.

    `version` alone is not enough, and that was the gap: a pinned CLI
    version with `extra_pip_requirements: ["some-lib"]` produced a stable
    name and no TTL, so the pod served day-one's resolution of that
    dependency for its whole life -- and a daily schedule keeps the
    last-used marker fresh, so the age-based eviction never fired either.
    Before this cache existed, a stable-named venv still lived under the
    per-execution directory and was rebuilt every run, so nothing was frozen.

    The common case -- no extra requirements -- is unchanged: the answer
    still comes down to `version`.
    """
    version = venv_config.version
    if version == VENV_VERSION_LATEST or version.startswith(("http://", "https://")):
        return True
    if any(not _is_pinned_requirement(req) for req in expanded_pip_reqs):
        return True
    if venv_config.requirements_file is not None:
        return not _requirements_file_is_pinned(venv_config.requirements_file)
    return False


def _is_fresh_hit(venv_loc: pathlib.Path, *, moving: bool) -> bool:
    """Whether this entry may be served, rather than merely being complete.

    Completeness alone is the right question for a pinned version: the name
    is a content address, so an entry that finished building is the right
    bytes forever.

    It is not the right question for a moving version, and `latest` is the
    default for every recipe. With no age bound the hit path never
    re-resolves: a pod that starts on Monday keeps executing Monday's
    acryl-datahub until it restarts, so a connector fix published on Tuesday
    silently never arrives -- and because test-connection deliberately shares
    the entry, re-testing after the fix reports the old behaviour too. That is
    the "I shipped the fix, the customer re-ran, still broken" ticket.

    Measured from BUILD time, not last use. touch_last_used fires on every
    hit, so an age taken from it would make the busiest entry -- the shared
    `latest` one -- the one thing that never expires.
    """
    if not is_venv_complete(venv_loc):
        return False
    if not moving:
        return True
    age = time.time() - built_at(venv_loc)
    ttl = get_venv_cache_latest_ttl_sec()
    if age <= ttl:
        return True
    logger.info(
        "venv cache: %s was built from a moving version %.1f hours ago "
        "(TTL %.1f hours); rebuilding so it re-resolves.",
        venv_loc.name,
        age / 3600,
        ttl / 3600,
    )
    return False


def _discard_incomplete(venv_loc: pathlib.Path) -> bool:
    """Clear a half-built entry so a build can start clean. False if it survives.

    Only safe under an EXCLUSIVE hold on the entry.

    The marker goes first, so a removal that fails part-way leaves something
    that reads as incomplete rather than a husk the hit path would serve.

    Errors are NOT ignored, which is the whole point. `ignore_errors=True`
    makes a completely failed discard indistinguishable from a successful
    one, and the build that follows then either dies forever on that key --
    `uv venv` on a surviving non-venv directory fails hard with "exists, but
    it's not a virtual environment" -- or, if pyvenv.cfg happened to survive,
    recreates the venv over the leftovers and gets stamped COMPLETE, so every
    later run reuses a venv that may mix two builds' site-packages and
    nothing on the hit path re-validates. The causes are ordinary: an NFS
    .nfsXXXX silly-rename, a uid mismatch on a shared volume, a read-only
    remount. Reporting the failure lets the caller use a per-run venv, which
    is slower and always correct.
    """
    try:
        (venv_loc / COMPLETE_MARKER).unlink(missing_ok=True)
        shutil.rmtree(venv_loc)
        return True
    except OSError:
        logger.warning(
            "Could not discard the incomplete venv at %s; falling back to a "
            "per-run venv. Clear this directory to re-enable the cache entry.",
            venv_loc,
            exc_info=True,
        )
        return False


async def _acquire_cache_entry(
    venv_name: str, tmp_dir: pathlib.Path, cacheable: bool, *, moving: bool = False
) -> _CacheEntry:
    """Resolve a dynamic venv against the cache and take the lock guarding it.

    NO PATH HERE MAY BLOCK INDEFINITELY. flock() is a synchronous syscall, so
    a blocking acquire inside this coroutine freezes the OS thread and with it
    the entire event loop the task runs on -- not even an in-loop timeout could
    fire to rescue it. Every acquire below is non-blocking and all waiting is
    an `await asyncio.sleep`.

    The protocol, in order:

    1. SHARED, non-blocking. A complete, fresh entry is served immediately.
       This is the warm hit, it is the common case, and it must never wait: a
       running task holds its entry SHARED for the whole run, so taking
       EXCLUSIVE here would make two recipes on the same `latest` entry --
       the default, and therefore the norm -- serialize behind the longer one.
    2. Otherwise a build is needed, and building needs EXCLUSIVE. Retry
       non-blocking on a short budget, re-attempting the shared hit each pass:
       a peer that finishes its build downgrades to SHARED and keeps it, so an
       exclusive-only retry could never succeed again once it lost the race.
    3. Whenever EXCLUSIVE is won, re-check INSIDE the lock -- another process
       may have finished building while we waited.
    4. When the budget runs out, someone else is building. Fall back to a
       per-run venv rather than waiting on them.

    Everything inside an EXCLUSIVE hold runs under a handler that releases it.
    This function is called ABOVE setup_venv's own `try`, whose
    `except BaseException` exists precisely to release the lock, so anything
    raised here -- a bad DATAHUB_VENV_CACHE_* value, an eviction pass hitting
    an unreadable root -- would otherwise escape with the entry still locked.
    That key could then never be built, hit or evicted again for the life of
    the process, and every task on it would fall back to a full per-run build.
    """
    venv_loc = pathlib.Path(venv_location(venv_name, str(tmp_dir), cacheable=cacheable))
    if not cacheable:
        return _CacheEntry(venv_loc, None, False, False)

    def per_run_fallback() -> _CacheEntry:
        return _CacheEntry(
            pathlib.Path(venv_location(venv_name, str(tmp_dir), cacheable=False)),
            None,
            False,
            False,
        )

    lock = EntryLock(venv_loc.parent / f"{venv_loc.name}.lock")
    # A complete entry that is only past its TTL. Worth remembering: if no
    # attempt ever wins EXCLUSIVE to refresh it, serving the stale venv beats
    # the per-run build that would otherwise be the fallback. See below.
    stale_but_complete = False
    for attempt in range(_CACHE_LOCK_ATTEMPTS):
        if lock.acquire(exclusive=False):
            # Guarded like the exclusive branch below. _is_fresh_hit reaches
            # Path.exists(), which re-raises EACCES -- an entry made
            # unreadable by a uid mismatch on a shared volume, or a
            # part-removed tree. This runs ABOVE setup_venv's own try, so an
            # escape here would leak the SHARED hold, and EntryLock has no
            # __del__: every later run on the key would raise again and leak
            # another fd.
            try:
                if _is_fresh_hit(venv_loc, moving=moving):
                    touch_last_used(venv_loc)
                    return _CacheEntry(venv_loc, lock, True, True)
                stale_but_complete = is_venv_complete(venv_loc)
            except BaseException:
                lock.release()
                raise
            # Nothing there yet, a build killed midway, or a moving entry past
            # its TTL. Either way this call has to build, and building needs
            # the entry exclusively.
            lock.release()
        elif lock.unusable:
            break

        if lock.acquire(exclusive=True):
            try:
                if _is_fresh_hit(venv_loc, moving=moving):
                    touch_last_used(venv_loc)
                    # flock has no atomic downgrade -- the kernel drops the
                    # exclusive hold before it looks for conflicts -- so a
                    # peer granted EXCLUSIVE in that window leaves us with
                    # nothing. Re-taking SHARED outright is the recovery;
                    # returning a "ready" entry with no lock is not, because
                    # the caller then runs a child out of a directory the
                    # next eviction pass is free to rmtree.
                    if lock.downgrade_to_shared() or lock.acquire(exclusive=False):
                        # Re-validate: the hold was momentarily gone either
                        # way, and _remove_entry deliberately leaves the
                        # .lock file behind, so a competing eviction can have
                        # deleted the directory while the lock we just took
                        # survived. Without this the child is spawned against
                        # <venv>/bin/python and dies with FileNotFoundError.
                        if is_venv_complete(venv_loc):
                            return _CacheEntry(venv_loc, lock, True, True)
                        lock.release()
                    logger.info(
                        "venv cache entry %s could not be held after the "
                        "downgrade; using a per-run venv",
                        venv_loc.name,
                    )
                    return per_run_fallback()
                # A directory here is a build killed midway, or a moving
                # entry past its TTL. Either way it must be removed rather
                # than built on top of, and if it cannot be removed this key
                # is unusable until an operator clears it.
                #
                # Deliberately NOT on a worker thread, unlike the eviction
                # pass below. This deletes the directory THIS coroutine's
                # lock protects, and awaiting makes the wait cancellable:
                # a cancellation would unwind into the handler below,
                # release the flock, and leave the worker still rmtree-ing
                # while a peer takes the same key and starts `uv venv` into
                # the directory being deleted. Blocking the loop for one
                # half-built venv's unlinks is the lesser cost. (Eviction is
                # safe to thread because each victim is protected by a lock
                # the worker itself takes and releases.)
                if venv_loc.exists() and not _discard_incomplete(venv_loc):
                    lock.release()
                    return per_run_fallback()
                # Eviction runs here and nowhere else: on the build path only,
                # and after we hold this entry, so it cannot select the
                # directory we are about to write into (it skips anything it
                # cannot take exclusively). Also off the loop, and for the
                # same reason -- an age pass can select every entry at once.
                await asyncio.to_thread(
                    evict_stale_entries,
                    venv_loc.parent,
                    max_entries=get_venv_cache_max_entries(),
                    max_age_sec=get_venv_cache_max_age_sec(),
                )
            except BaseException:
                lock.release()
                raise
            return _CacheEntry(venv_loc, lock, True, False)
        if lock.unusable:
            break

        if attempt + 1 < _CACHE_LOCK_ATTEMPTS:
            await asyncio.sleep(_CACHE_LOCK_RETRY_SEC)

    if lock.unusable:
        _warn_cache_unavailable_once(str(venv_loc.parent))
        return per_run_fallback()

    # Stale-while-in-use. Refreshing an expired moving entry needs EXCLUSIVE,
    # and every in-flight run holds the same entry SHARED for its whole life
    # -- and `latest` is the default, so essentially every run shares one
    # entry. On a pod whose runs overlap continuously there may be no instant
    # inside this budget with zero holders, so the refresh never happens.
    # Falling through to a per-run build there would make the TTL strictly
    # worse than not having one: every task would pay a full build forever,
    # which is the cost this cache exists to remove. Serving the stale venv
    # is the lesser evil, and the refresh still happens on the first attempt
    # that finds the entry idle.
    if stale_but_complete and lock.acquire(exclusive=False):
        # Guarded for the same reason the two branches above are: this sits
        # ABOVE setup_venv's own try, and is_venv_complete reaches
        # Path.exists(), which re-raises EACCES -- stranding the hold and
        # its fd for the life of the process.
        try:
            still_there = is_venv_complete(venv_loc)
        except BaseException:
            lock.release()
            raise
        if still_there:
            touch_last_used(venv_loc)
            logger.info(
                "venv cache entry %s is past its TTL but in use by another "
                "run, so it cannot be rebuilt right now; serving it as-is",
                venv_loc.name,
            )
            return _CacheEntry(venv_loc, lock, True, True)
        lock.release()

    logger.info(
        "venv cache entry %s is held by another build; using a per-run venv",
        venv_loc.name,
    )
    return per_run_fallback()


def _resolve_existing_venv(entry: _CacheEntry, runner: SubprocessRunner) -> bool:
    """Whether the entry can be returned as-is, without building anything.

    A cache hit is already decided -- in _acquire_cache_entry, under the lock,
    which is the only place is_venv_complete() can be trusted, and so is
    discarding a half-built entry: both need the EXCLUSIVE hold that only
    exists there. What is left here is the non-cached legacy check, where the
    interpreter's presence alone is enough since certain systems clean up
    files in temp directories but not the directories themselves.
    """
    venv_loc = entry.venv_loc
    if entry.ready:
        runner._logs.append(f"Reusing cached venv at {venv_loc}.\n")
        return True

    if not entry.cacheable:
        if venv_loc.exists() and (venv_loc / "bin/python").exists():
            runner._logs.append(f"venv at {venv_loc} already exists, skipping setup.\n")
            return True
    return False


async def _install_extra_requirements(
    runner: SubprocessRunner,
    venv_loc: pathlib.Path,
    expanded_pip_reqs: list[str],
    venv_env: dict[str, str],
) -> None:
    """Install extra_pip_requirements, leaving no credential on disk.

    These are the EXPANDED requirements: a private index URL arrives here with
    its token already substituted in. The file is only an input to `uv pip
    install -r` and nothing reads it afterwards, so it does not outlive the
    install.

    It used to, and the venv cache is what made that matter. `venv_loc` for a
    cacheable venv is the node-local cache root, which get_venv_cache_path
    places outside exec_out_dir on purpose -- "deliberately not inside ... the
    directory finalize_task_output removes when a task ends". So the token
    stopped being cleaned up at all: it sat in a directory that survives by
    design and is shared by every task on the node, until LRU eviction
    happened to reclaim it. Before the cache it went into exec_out_dir and was
    removed with the run.

    Extracted from setup_venv rather than inlined: the cleanup pushed that
    function one step past ruff's complexity limit, and this is a self
    contained step with its own invariant to state.
    """
    extra_req_file = venv_loc / "extra-requirements.txt"
    # The try opens BEFORE the file is created, not before the install. Once
    # the path is chosen, every later step can fail with the token already on
    # disk: write_text can hit a full or read-only filesystem, and the log
    # appends after it can raise too. Opening the try at the install instead
    # left those failures leaking the credential into the node-local cache,
    # which nothing cleans up because it deliberately outlives the task.
    try:
        # 0600 before the contents are written rather than after, so there is
        # no window at the ambient umask -- the cache directory itself is
        # umask-default and may be group- or world-readable. chmod as well as
        # touch(mode=...), because mode only applies when touch CREATES the
        # file and a discarded incomplete venv can leave a stale one behind.
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
        # the run whose requirements file must not be left behind. Guarded for
        # the reason mark_venv_complete is: a full or read-only cache
        # filesystem must not turn a build that otherwise succeeded into a
        # failure.
        try:
            extra_req_file.unlink(missing_ok=True)
        except OSError:
            logger.warning(
                "Could not remove %s; it holds expanded requirements and may "
                "contain a credential",
                extra_req_file,
                exc_info=True,
            )


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


def _publish_cache_entry(
    venv_reference: "VenvReference", venv_loc: pathlib.Path
) -> None:
    """Make a freshly built entry reusable, and keep holding it for this run.

    No-op for an uncached venv, which has no lock and nothing to publish.
    """
    if venv_reference.lock is None:
        return

    # LAST, so a build killed before this point leaves an entry that fails
    # is_venv_complete() and is rebuilt rather than reused empty.
    try:
        mark_venv_complete(venv_loc)
    except OSError:
        # The cache is an optimisation: a full or read-only cache filesystem
        # must not fail a build that otherwise succeeded. An unmarked venv just
        # looks incomplete and gets rebuilt next time -- the same degradation
        # touch_last_used already accepts.
        #
        # WARNING rather than DEBUG, because the degradation is not local to
        # this run. This entry stays locked SHARED for the task's whole life,
        # so for as long as that lasts every peer on the same key pays the
        # full retry budget, finds the entry incomplete, cannot take it
        # exclusively, and rebuilds per-run. A cache that has silently stopped
        # caching should be visible without turning on debug logging.
        logger.warning(
            "Could not mark venv complete at %s; this entry will be rebuilt "
            "rather than reused, and peers on the same key will fall back to "
            "per-run venvs while this task holds it.",
            venv_loc,
            exc_info=True,
        )
    touch_last_used(venv_loc)

    # flock has no atomic downgrade, so the exclusive hold is already gone by
    # the time the conversion is refused. Re-requesting SHARED outright is the
    # recovery and nearly always succeeds -- the window is sub-second.
    if venv_reference.lock.downgrade_to_shared() or venv_reference.lock.acquire(
        exclusive=False
    ):
        return

    # Nothing left to hold. Carrying the object would only let
    # finalize_task_output release a lock nobody has, and would report the
    # entry as protected when eviction is free to take it. The venv itself is
    # complete and this task is about to run out of it, so the honest state is
    # "usable, unprotected" -- said out loud, because an eviction pass landing
    # here kills the child with an ImportError on a deleted file.
    logger.warning(
        "Lost the venv cache hold on %s after building it; this run continues "
        "against an entry eviction may reclaim.",
        venv_loc,
    )
    venv_reference.lock = None


# I had to change this from the base file because we needed to introduce
# support for handling bundled venvs.
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

    # Handle dynamic venvs
    #
    # Both the ambient value and the one extra_env_vars overrides it with. The
    # venv below is built from {**os.environ, **extra_env_vars}, so the
    # override is what pip receives and what a failing index URL echoes back --
    # and registering only os.environ left exactly that value maskable
    # nowhere. The stdin envelope cannot cover it either: subprocess_env_secrets
    # excludes overridden names on purpose, to keep get_combined_env_vars
    # precedence in the child.
    #
    # Two calls rather than one merged dict: both values live under the same
    # NAME, and the registry keeps MAX_SECRET_VERSIONS of those, so merging
    # would silently keep only the last.
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

    venv_name, cacheable = _name_dynamic_venv(venv_config, expanded_pip_reqs)
    entry = await _acquire_cache_entry(
        venv_name,
        tmp_dir,
        cacheable,
        moving=_resolves_to_moving_target(venv_config, expanded_pip_reqs),
    )
    venv_loc, lock, cacheable = entry.venv_loc, entry.lock, entry.cacheable

    venv_reference = VenvReference(
        venv_loc=venv_loc,
        venv_config=venv_config,
        lock=lock,
    )

    try:
        if _resolve_existing_venv(entry, runner):
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
            is_dev_build = version.startswith(("http://", "https://"))
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

        # Pass 2: Install extra_pip_requirements without constraints.
        if venv_config.requirements_file is None and expanded_pip_reqs:
            await _install_extra_requirements(
                runner, venv_loc, expanded_pip_reqs, venv_env
            )

        # Before the entry is published, so a credential never becomes
        # visible to another task through the cache. A venv that could not be
        # fully scrubbed is deliberately NOT published: it stays unmarked,
        # this run uses it, and the next claimant discards and rebuilds it
        # rather than inheriting someone else's token.
        if _scrub_direct_url_credentials(venv_loc):
            _publish_cache_entry(venv_reference, venv_loc)
        else:
            logger.warning(
                "Not publishing %s to the venv cache: a credential recorded "
                "by the installer could not be redacted.",
                venv_loc,
            )

        return venv_reference
    except BaseException:
        # Any failure during the build -- a failed subprocess, or
        # cancellation -- must release an exclusive lock before propagating.
        # Without this, an exception path never returns the VenvReference,
        # so nobody else ever gets a chance to release it, and the entry stays
        # exclusively locked for the rest of this process's life: every later
        # task in the same long-lived pod that wants that venv falls back to a
        # per-run build, and eviction -- which needs a non-blocking exclusive
        # -- can never reclaim the directory either.
        # Scrub before unwinding too. uv writes direct_url.json as each
        # requirement is installed, so a run that failed on the SECOND of two
        # private-index requirements -- or was cancelled -- has already left
        # an expanded token inside a half-built entry that sits in the shared
        # cache root until some later build discards it. The sibling cleanup
        # in _install_extra_requirements is in a `finally` for exactly this
        # reason; this one has to be as well. Guarded so it cannot replace
        # the exception in flight.
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
