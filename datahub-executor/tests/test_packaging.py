import datahub_executor as package_metadata


def test_package_version() -> None:
    # Simply importing pkg_resources checks for unsatisfied dependencies.
    import pkg_resources

    assert pkg_resources.get_distribution(package_metadata.__package_name__).version

    # Just check that this doesn't raise an exception.
    assert package_metadata.nice_version_name()
