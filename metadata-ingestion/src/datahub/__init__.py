# Patch setproctitle on macOS before any fork can occur (avoids SIGSEGV in child processes).
# test: connector-tests e2e validation (concurrency test commit B)
import datahub._setproctitle_patch
from datahub._version import __package_name__, __version__
