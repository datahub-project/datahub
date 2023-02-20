import setuptools
import pprint

def test_oracle_config():
    where = "./src"
    package_list = set(setuptools.find_packages(where))
    namespace_packages = set(setuptools.find_namespace_packages(where))
    diff1 = package_list.difference(namespace_packages)
    if len(diff1) > 0:
        pprint.pprint(diff1)
    diff2 = namespace_packages.difference(package_list)
    if len(diff2) > 0:
        pprint.pprint(diff2)

    assert len(diff1) == 0
    assert len(diff2) == 0