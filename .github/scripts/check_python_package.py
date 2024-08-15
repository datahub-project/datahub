import setuptools
import os

folders = ["./smoke-test/tests"]

for folder in folders:
    print(f"Checking folder {folder}")
    packages = [i for i in setuptools.find_packages(folder) if "cypress" not in i]
    namespace_packages = [
        i for i in setuptools.find_namespace_packages(folder) if "cypress" not in i
    ]

    print("Packages found:", packages)
    print("Namespace packages found:", namespace_packages)

    in_packages_not_namespace = set(packages) - set(namespace_packages)
    in_namespace_not_packages = set(namespace_packages) - set(packages)

    if in_packages_not_namespace:
        print(f"Packages not in namespace packages: {in_packages_not_namespace}")
    if in_namespace_not_packages:
        print(f"Namespace packages not in packages: {in_namespace_not_packages}")
        for pkg in in_namespace_not_packages:
            pkg_path = os.path.join(folder, pkg.replace(".", os.path.sep))
            print(f"Contents of {pkg_path}:")
            print(os.listdir(pkg_path))

    assert (
        len(in_packages_not_namespace) == 0
    ), f"Found packages in {folder} that are not in namespace packages: {in_packages_not_namespace}"
    assert (
        len(in_namespace_not_packages) == 0
    ), f"Found namespace packages in {folder} that are not in packages: {in_namespace_not_packages}"
