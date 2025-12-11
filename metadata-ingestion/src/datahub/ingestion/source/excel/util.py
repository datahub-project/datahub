# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import os


def gen_dataset_name(path: str, sheet_name: str, lower_case: bool) -> str:
    sheet_name = sheet_name.strip()
    directory, filename = os.path.split(path)

    if not directory:
        excel_path = f"[{filename}]"
    else:
        excel_path = os.path.join(directory, f"[{filename}]")

    name = f"{excel_path}{sheet_name}"

    if lower_case:
        name = name.lower()

    return name
