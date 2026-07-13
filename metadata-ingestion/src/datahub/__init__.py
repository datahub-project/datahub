# These are imported for their side effects, before any other datahub.* module
# (and thus any sqlglot import) can load:
# - _force_pure_python_sqlglot honours DATAHUB_SQLGLOT_DISABLE_C by loading sqlglot
#   from .py instead of the mypyc .so extensions. A .pth also installs it at
#   interpreter startup, but .pth placement is unreliable across wheel installs,
#   so this import is the mechanism we rely on.
# - _setproctitle_patch avoids a SIGSEGV on macOS when a multi-threaded process
#   forks and something calls setproctitle.
import datahub._force_pure_python_sqlglot
import datahub._setproctitle_patch
from datahub._version import __package_name__, __version__
