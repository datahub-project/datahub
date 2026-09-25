# These are imported for their side effects, before any other datahub.* module
# (and thus any sqlglot import) can load:
# - _force_pure_python_sqlglot honours DATAHUB_SQLGLOT_DISABLE_C by loading sqlglot
#   from .py instead of the mypyc .so extensions. A .pth installs it at interpreter
#   startup for the earliest possible activation; importing it here is the safety
#   net that guarantees it runs whenever a datahub package is imported (including
#   custom Python SDK usage), before datahub touches sqlglot.
# - _setproctitle_patch avoids a SIGSEGV on macOS when a multi-threaded process
#   forks and something calls setproctitle.
import datahub._force_pure_python_sqlglot
import datahub._setproctitle_patch
from datahub._version import __package_name__, __version__
