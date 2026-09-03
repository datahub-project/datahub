import importlib.metadata

import datahub._version as datahub_version


def test_datahub_version():
    # Checks that the installed distribution metadata is present and resolvable.
    assert importlib.metadata.version(datahub_version.__package_name__)
