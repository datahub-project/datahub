# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import pathlib
import shutil

PYTHON_BUILD_DIR = pathlib.Path(__file__).parent
ROOT_DIR = PYTHON_BUILD_DIR.parent
WHEEL_OUTPUT_DIR = PYTHON_BUILD_DIR / "wheels"

# These should line up with the build.gradle file.
wheel_dirs = [
    ROOT_DIR / "metadata-ingestion/dist",
    ROOT_DIR / "datahub-actions/dist",
    ROOT_DIR / "metadata-ingestion-modules/airflow-plugin/dist",
    ROOT_DIR / "metadata-ingestion-modules/dagster-plugin/dist",
    ROOT_DIR / "metadata-ingestion-modules/prefect-plugin/dist",
    ROOT_DIR / "metadata-ingestion-modules/gx-plugin/dist",
]

# Delete and recreate the output directory.
if WHEEL_OUTPUT_DIR.exists():
    shutil.rmtree(WHEEL_OUTPUT_DIR)
WHEEL_OUTPUT_DIR.mkdir(parents=True)

# Copy things over.
for wheel_dir in wheel_dirs:
    for wheel_file in wheel_dir.glob("*"):
        shutil.copy(wheel_file, WHEEL_OUTPUT_DIR)

print("Copied wheels to", WHEEL_OUTPUT_DIR)
