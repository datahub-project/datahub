# Patch setproctitle on macOS before any fork can occur (avoids SIGSEGV in child processes).
import datahub._setproctitle_patch
from datahub._version import __package_name__, __version__
