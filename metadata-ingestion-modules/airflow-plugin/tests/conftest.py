import pathlib
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
