import pathlib
import platform
import site

from datahub.testing.pytest_hooks import (  # noqa: F401
    load_golden_flags,
    pytest_addoption,
)

# The integration tests run Airflow, with our plugin, in a subprocess.
# To get more accurate coverage, we need to ensure that the coverage
# library is available in the subprocess.
# See https://coverage.readthedocs.io/en/latest/subprocess.html#configuring-python-for-sub-process-measurement
coverage_startup_code = "import coverage; coverage.process_startup()"
site_packages_dir = pathlib.Path(site.getsitepackages()[0])
pth_file_path = site_packages_dir / "datahub_coverage_startup.pth"
pth_file_path.write_text(coverage_startup_code)

# For Airflow 3.0+ on macOS, patch OpenLineage listener to prevent SIGSEGV crashes
# caused by setproctitle after fork(). See https://github.com/apache/airflow/issues/55838
if platform.system() == "Darwin":
    # Patch the OpenLineage listener to not import getproctitle on macOS
    # because it causes SIGSEGV in forked worker processes
    listener_file = (
        site_packages_dir
        / "airflow"
        / "providers"
        / "openlineage"
        / "plugins"
        / "listener.py"
    )
    if listener_file.exists():
        content = listener_file.read_text()
        # Replace the import block to avoid importing getproctitle at all on macOS
        old_import = """if sys.platform == "darwin":
    from setproctitle import getproctitle

    setproctitle = lambda title: logging.getLogger(__name__).debug("Mac OS detected, skipping setproctitle")
else:
    from setproctitle import getproctitle, setproctitle"""

        new_import = """if sys.platform == "darwin":
    # Avoid importing setproctitle entirely on macOS to prevent SIGSEGV in forked processes
    getproctitle = lambda: "airflow"
    setproctitle = lambda title: logging.getLogger(__name__).debug("Mac OS detected, skipping setproctitle")
else:
    from setproctitle import getproctitle, setproctitle"""

        if old_import in content:
            content = content.replace(old_import, new_import)
            listener_file.write_text(content)
            print(
                "[PATCH] Patched OpenLineage listener to avoid setproctitle import on macOS"
            )
