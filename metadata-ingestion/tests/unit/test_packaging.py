# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import pytest

import datahub._version as datahub_version


@pytest.mark.filterwarnings(
    "ignore:pkg_resources is deprecated as an API:DeprecationWarning"
)
def test_datahub_version():
    # Simply importing pkg_resources checks for unsatisfied dependencies.
    import pkg_resources  # type: ignore[import-untyped]

    assert pkg_resources.get_distribution(datahub_version.__package_name__).version
