import collections
import contextlib
import dataclasses
import functools
import hashlib
import json
import os
import pathlib
import shlex
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from typing import Any, Deque, Generator, Iterator, Optional, Type

import anyio
import anyio.abc
import anyio.streams.text
import pydantic
from loguru import logger

_DEFAULT_MAX_LOG_LINES = 2000
_DEFAULT_MAX_BYTES_PER_LINE = 2**12  # 4kb
# Kafka has a 1mb limit on the size of a data packet.
# Doing 90% of that so we have some buffer for other things.
_DEFAULT_MAX_LOG_SIZE_BYTES = int(0.9 * 2**18)  # 90% of 1mb

VENV_VERSION_LATEST = "latest"
VENV_VERSION_NATIVE = "native"
VENV_NO_DATAHUB = "NO_ACRYL_DATAHUB"


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
        max_log_lines: int | None = _DEFAULT_MAX_LOG_LINES,
        max_bytes_per_line: int = _DEFAULT_MAX_BYTES_PER_LINE,
        max_log_size_bytes: int = _DEFAULT_MAX_LOG_SIZE_BYTES,
        echo_to_stdout_prefix: str | None = None,
    ):
        self._max_log_lines = max_log_lines
        self._total_log_lines = 0
        self._max_bytes_per_line = max_bytes_per_line
        self._max_log_size_bytes = max_log_size_bytes
        self._echo_logs_prefix = echo_to_stdout_prefix

        self._lines: Deque[str] = collections.deque(maxlen=self._max_log_lines)
        self._create_new_line: bool = True
        self.most_recent_log_ts: datetime | None = None

    def clear(self) -> None:
        self._lines.clear()
        self._create_new_line = True
        self.most_recent_log_ts = None

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
                logger.opt(raw=True).debug(f"{self._echo_logs_prefix}{self._lines[-1]}")

            # On the next append, we'll create a new line.
            self._create_new_line = True

    @contextlib.contextmanager
    def changed_echo_prefix(self, new_prefix: str | None) -> Iterator[None]:
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


def pydantic_parse_json(field: str) -> classmethod:
    def _parse_from_json(cls: Type, v: Any) -> dict:
        if isinstance(v, str):
            return json.loads(v)
        return v

    return pydantic.validator(field, pre=True, allow_reuse=True)(_parse_from_json)


class VenvConfig(pydantic.BaseModel):
    version: str = VENV_VERSION_LATEST

    main_plugin: str | None = None
    extra_pip_requirements: list[str] = []
    extra_pip_plugins: list[str] = []
    extra_env_vars: dict = {}

    # If a requirements file is specified, then the version and all other extra_* fields are ignored.
    requirements_file: pathlib.Path | None = None

    # TODO: Not sure if we still need to copy these over.
    _json_extra_pip_requirements = pydantic_parse_json("extra_pip_requirements")
    _json_extra_pip_plugins = pydantic_parse_json("extra_pip_plugins")
    _json_extra_env_vars = pydantic_parse_json("extra_env_vars")

    def set_main_plugin(self, plugin: str) -> None:
        self.main_plugin = plugin

    def get_stable_venv_name(self) -> str | None:
        if self.requirements_file is not None:
            suffix = hashlib.sha256()
            suffix.update(self.requirements_file.read_bytes())
            return f"req-{suffix.digest().hex()[:16]}"

        if self.main_plugin is None:
            return None
        if (
            self.version == VENV_VERSION_LATEST
            or self.version == VENV_VERSION_NATIVE
            or self.version == VENV_NO_DATAHUB
            or self.version.startswith("http")
        ):
            return None

        # Generate a stable name for the venv.
        # env vars are not included in the hash.
        suffix = hashlib.sha256()
        suffix.update(self.version.encode("utf-8"))
        suffix.update(str(self.extra_pip_requirements).encode("utf-8"))
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
            if self.version.endswith(".whl"):
                return f"acryl-datahub{plugins} @ {self.version}"
            else:
                # Adding a timestamp to the URL to force cache busting.
                now = datetime.now(tz=timezone.utc)
                return f"acryl-datahub{plugins} @ {self.version}/artifacts/wheels/acryl_datahub-0.0.0.dev1-py3-none-any.whl?ts={now.timestamp()}"
        else:
            return f"acryl-datahub{plugins}=={self.version}"


@dataclasses.dataclass
class VenvReference:
    venv_loc: pathlib.Path
    venv_config: VenvConfig

    def command(self, cmd: str) -> str:
        return str(self.venv_loc / "bin" / cmd)

    def extra_envs(self) -> dict[str, str]:
        return {
            **self.venv_config.extra_env_vars,
            # TODO: Do we need to add this?
            # "VIRTUAL_ENV": str(self.venv_loc),
        }


# From https://anyio.readthedocs.io/en/stable/migration.html#task-groups-now-wrap-single-exceptions-in-groups
@contextlib.contextmanager
def collapse_excgroups() -> Generator[None, None, None]:
    try:
        yield
    except BaseException as exc:
        while isinstance(exc, BaseExceptionGroup) and len(exc.exceptions) == 1:
            exc = exc.exceptions[0]

        raise exc


class SubprocessRunner:
    def __init__(self, logs: LogHolder | None = None) -> None:
        self._logs = logs or LogHolder()
        self._process: anyio.abc.Process | None = None

    @property
    def logs(self) -> LogHolder:
        return self._logs

    @property
    def pid(self) -> Optional[int]:
        return self._process.pid if self._process else None

    async def execute(
        self,
        command: list[str],
        env: dict[str, str] | None = None,
        cwd: str | pathlib.Path | None = None,
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
        with collapse_excgroups():
            async with self._process, anyio.create_task_group() as tg:
                tg.start_soon(self._read_logs, name="read_logs")

                try:
                    await self._process.wait()

                except anyio.get_cancelled_exc_class():
                    # On cancellation, gracefully kill the subprocess.
                    if self._process.returncode is None:
                        with anyio.CancelScope(shield=True):
                            await self.kill()

                    raise

                else:
                    # Raise if the process exited with a non-zero return code.
                    if (
                        self._process.returncode is not None
                        and self._process.returncode != 0
                    ):
                        raise subprocess.CalledProcessError(
                            returncode=self._process.returncode, cmd=command
                        )

                finally:
                    tg.cancel_scope.cancel()

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


async def setup_venv(
    venv_config: VenvConfig, runner: SubprocessRunner, tmp_dir: pathlib.Path
) -> VenvReference:
    if venv_config.version == VENV_VERSION_NATIVE:
        return VenvReference(
            venv_loc=pathlib.Path(sys.prefix),
            venv_config=venv_config,
        )

    # Versions that are "moving targets" get random names, and everything else gets a stable name.
    venv_name = venv_config.get_stable_venv_name()
    if venv_name is None:
        venv_name = f"eph-{hashlib.sha256(os.urandom(32)).hexdigest()[:16]}"

    # Setup the venv.
    venv_loc = tmp_dir / venv_name
    venv_reference = VenvReference(
        venv_loc=venv_loc,
        venv_config=venv_config,
    )
    if venv_loc.exists() and (venv_loc / "bin/python").exists():
        # Certain systems clean up the files in temp directories, but not the directories themselves.
        # By checking for the python binary, we can be reasonably sure that the venv is still usable.
        runner._logs.append(f"venv at {venv_loc} already exists, skipping setup.\n")
        return venv_reference

    # Create the venv. We need to pass --python <executable> so that uv uses the same
    # Python as the current process.
    await runner.execute(
        [_find_uv(), "venv", "--python", sys.executable, str(venv_loc)]
    )

    # Assemble the requirements file.
    if venv_config.requirements_file is None:
        requirements = "\n".join(
            [
                f"# Generated at {datetime.now(tz=timezone.utc).isoformat()}",
                venv_config.get_acryl_datahub_requirement_line(),
                *venv_config.extra_pip_requirements,
            ]
        )
        requirements_file = venv_loc / "requirements.txt"
        requirements_file.write_text(requirements)
    else:
        requirements_file = venv_config.requirements_file

    # Install the requirements.
    await runner.execute(["cat", str(requirements_file)])
    await runner.execute(
        [_find_uv(), "pip", "install", "-r", str(requirements_file)],
        env={
            **os.environ,
            **venv_config.extra_env_vars,
            "VIRTUAL_ENV": str(venv_loc),
        },
    )

    return venv_reference
