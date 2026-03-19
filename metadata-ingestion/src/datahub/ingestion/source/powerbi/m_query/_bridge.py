import atexit
import gzip
import json
import logging
import platform
import shutil
import stat
import subprocess
import sys
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

NodeIdMap = dict[int, dict]

# Maps (sys.platform, platform.machine()) → .gz filename in binaries/
_PLATFORM_BINARY_MAP: dict[str, str] = {
    "linux-x86_64": "mquery-parser-linux-x64.gz",
    "linux-aarch64": "mquery-parser-linux-aarch64.gz",
    "linux-arm64": "mquery-parser-linux-aarch64.gz",
    "darwin-x86_64": "mquery-parser-darwin-x64.gz",
    "darwin-arm64": "mquery-parser-darwin-arm64.gz",
    "win32-AMD64": "mquery-parser-win32-x64.exe.gz",
}

_BINARIES_DIR = Path(__file__).parent / "mquery_bridge" / "binaries"


class MQueryBridgeError(RuntimeError):
    """Bridge process crashed or produced malformed output."""

    pass


class MQueryParseError(RuntimeError):
    """Parser returned a structured parse error for a specific expression."""

    def __init__(self, message: str, expression: str = "") -> None:
        super().__init__(message)
        self.expression = expression


def _read_parser_version() -> str:
    """Read the pinned @microsoft/powerquery-parser version from package.json."""
    pkg_json = Path(__file__).parent / "mquery_bridge" / "package.json"
    try:
        data = json.loads(pkg_json.read_text())
        return data["dependencies"]["@microsoft/powerquery-parser"]
    except Exception:
        return "unknown"


def _prepare_binary() -> Path:
    """
    Locate the .gz binary for the current platform, decompress to
    /tmp/datahub/mquery-parser/<version>/, make executable, return path.
    Raises ImportError if no binary exists for this platform.
    """
    platform_key = f"{sys.platform}-{platform.machine()}"
    gz_name = _PLATFORM_BINARY_MAP.get(platform_key)
    if gz_name is None:
        raise ImportError(
            f"PowerBI M-Query parsing requires a platform binary unavailable for "
            f"{platform_key}. Supported platforms: {list(_PLATFORM_BINARY_MAP.keys())}. "
            f"See the DataHub PowerBI connector documentation."
        )

    gz_path = _BINARIES_DIR / gz_name
    if not gz_path.exists():
        raise ImportError(
            f"Expected binary not found in package: {gz_path}. "
            f"Re-installing acryl-datahub[powerbi] may fix this."
        )

    version = _read_parser_version()
    cache_dir = Path("/tmp/datahub/mquery-parser") / version
    cache_dir.mkdir(parents=True, exist_ok=True)

    binary_name = gz_name.removesuffix(".gz")
    binary_path = cache_dir / binary_name

    if not binary_path.exists():
        logger.debug("Decompressing M-Query bridge binary to %s", binary_path)
        with gzip.open(gz_path, "rb") as f_in, open(binary_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        if sys.platform != "win32":
            binary_path.chmod(binary_path.stat().st_mode | stat.S_IEXEC)

    return binary_path


class MQueryBridge:
    def __init__(self) -> None:
        binary = _prepare_binary()
        self._proc = subprocess.Popen(
            [str(binary)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        atexit.register(self.close)

    def parse(self, expression: str) -> NodeIdMap:
        """
        Parse an M-Query expression.
        Returns nodeIdMap as dict[int, dict].
        Not thread-safe — callers must be single-threaded.

        Raises:
            MQueryParseError: parser returned a structured error for this expression.
            MQueryBridgeError: bridge process died or returned malformed output.
        """
        if self._proc.stdin is None or self._proc.stdout is None:
            raise MQueryBridgeError(
                "Bridge process was not started correctly (stdin/stdout unavailable)."
            )

        req = json.dumps({"text": expression}) + "\n"
        self._proc.stdin.write(req.encode())
        self._proc.stdin.flush()

        line = self._proc.stdout.readline()
        if not line:
            stderr = b""
            if self._proc.stderr:
                stderr = self._proc.stderr.read()
            raise MQueryBridgeError(
                f"M-Query bridge process exited unexpectedly. "
                f"stderr: {stderr.decode(errors='replace')}"
            )

        try:
            result = json.loads(line)
        except json.JSONDecodeError as e:
            raise MQueryBridgeError(
                f"M-Query bridge returned malformed JSON: {e}. Raw line: {line!r}"
            ) from e
        if not result["ok"]:
            raise MQueryParseError(result["error"], expression=expression)
        return {int(node_id): node for node_id, node in result["nodeIdMap"]}

    def close(self) -> None:
        if self._proc.poll() is None:
            if self._proc.stdin:
                self._proc.stdin.close()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.terminate()
                self._proc.wait()


_bridge_instance: Optional[MQueryBridge] = None


def get_bridge() -> MQueryBridge:
    global _bridge_instance
    if _bridge_instance is None:
        _bridge_instance = MQueryBridge()
    return _bridge_instance


def _clear_bridge() -> None:
    global _bridge_instance
    if _bridge_instance is not None:
        atexit.unregister(_bridge_instance.close)
        _bridge_instance.close()
    _bridge_instance = None
