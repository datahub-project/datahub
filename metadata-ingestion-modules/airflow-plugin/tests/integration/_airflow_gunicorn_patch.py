"""
Patch setproctitle/gunicorn on macOS to prevent SIGSEGV crashes after fork().

When used as PYTHONSTARTUP (e.g. in Airflow integration tests), this module:
1. Imports datahub._setproctitle_patch to no-op setproctitle globally.
2. Patches gunicorn.util._setproctitle so gunicorn workers never call the C extension.

The setproctitle library causes segmentation faults on macOS when called from
gunicorn workers (or any forked child). This is a known issue:
https://github.com/apache/airflow/issues/55838
"""

import os
import platform
import sys

# Apply global setproctitle no-op first (covers OpenLineage and any other caller).
try:
    import datahub._setproctitle_patch  # noqa: F401
except ImportError:
    pass

# Log that this startup file was loaded
print(
    f"[GUNICORN-PATCH] PYTHONSTARTUP loaded from PID {os.getpid()}",
    file=sys.stderr,
    flush=True,
)


def patch_gunicorn_setproctitle():
    """Disable setproctitle in gunicorn on macOS to prevent SIGSEGV crashes."""
    # Only patch on macOS (Darwin)
    if platform.system() != "Darwin":
        print(
            f"[GUNICORN-PATCH] Skipping patch, not on macOS (platform={platform.system()})",
            file=sys.stderr,
            flush=True,
        )
        return

    try:
        import gunicorn.util

        # Replace the _setproctitle function with a no-op
        def _setproctitle_noop(title):
            pass

        gunicorn.util._setproctitle = _setproctitle_noop
        print(
            f"[GUNICORN-PATCH] Successfully disabled gunicorn setproctitle on macOS (PID {os.getpid()})",
            file=sys.stderr,
            flush=True,
        )
    except ImportError as e:
        # gunicorn not installed or not imported yet
        print(
            f"[GUNICORN-PATCH] Could not import gunicorn.util: {e}",
            file=sys.stderr,
            flush=True,
        )


# Apply the patch immediately when this module is imported
patch_gunicorn_setproctitle()
