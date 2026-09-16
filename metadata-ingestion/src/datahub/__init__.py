# These are imported for their side effects, before any other datahub.* module:
# - _setproctitle_patch avoids a SIGSEGV on macOS when a multi-threaded process
#   forks and something calls setproctitle.
# - _pkg_resources_finder appends a sys.meta_path finder that provides a
#   pkg_resources shim when setuptools>=82 has removed it, deferring to a real
#   pkg_resources whenever one is installed.
import datahub._pkg_resources_finder
import datahub._setproctitle_patch
from datahub._version import __package_name__, __version__
