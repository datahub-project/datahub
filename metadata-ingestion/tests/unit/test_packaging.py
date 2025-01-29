import pytest

import datahub._version as datahub_version


@pytest.mark.filterwarnings(
    "ignore:pkg_resources is deprecated as an API:DeprecationWarning"
)
def test_datahub_version():
    # Simply importing pkg_resources checks for unsatisfied dependencies.
    import pkg_resources

    assert pkg_resources.get_distribution(datahub_version.__package_name__).version
