"""
Patch gunicorn's setproctitle function to prevent SIGSEGV crashes on macOS.

This module should be imported early in the Airflow process to patch gunicorn
before it forks worker processes. The setproctitle library causes segmentation
faults on macOS when called from gunicorn workers after fork().

This is a known issue: https://github.com/apache/airflow/issues/55838
"""

import os
import platform
import sys

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
