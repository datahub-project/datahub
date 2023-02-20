import setuptools

def test_package_list_match_inits():
    where = "./src"
    package_list = set(setuptools.find_packages(where))
    namespace_packages = set(setuptools.find_namespace_packages(where))
    diff1 = package_list.difference(namespace_packages)
    diff2 = namespace_packages.difference(package_list)
    assert len(diff1) == 0
    assert len(diff2) == 0