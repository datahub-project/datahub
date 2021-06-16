import setuptools

import datahub as datahub_metadata


def test_datahub_version():
    # Simply importing pkg_resources checks for unsatisfied dependencies.
    import pkg_resources

    assert pkg_resources.get_distribution(datahub_metadata.__package_name__).version


def test_package_discovery():
    where = "./src"
    assert setuptools.find_packages(where) == setuptools.find_namespace_packages(where)
