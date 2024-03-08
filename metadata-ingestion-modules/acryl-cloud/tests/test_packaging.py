import setuptools


def test_package_list_match_inits() -> None:
    where = "./src"
    package_list = set(setuptools.find_packages(where))
    namespace_packages = set(setuptools.find_namespace_packages(where))
    assert package_list == namespace_packages, "are you missing a package init file?"
