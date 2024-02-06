import setuptools

folders = ["./smoke-test/tests"]

for folder in folders:
    print(f"Checking folder {folder}")
    a = [i for i in setuptools.find_packages(folder) if "cypress" not in i]
    b = [i for i in setuptools.find_namespace_packages(folder) if "cypress" not in i]

    in_a_not_b = set(a) - set(b)
    in_b_not_a = set(b) - set(a)

    assert (
        len(in_a_not_b) == 0
    ), f"Found packages in {folder} that are not in namespace packages: {in_a_not_b}"
    assert (
        len(in_b_not_a) == 0
    ), f"Found namespace packages in {folder} that are not in packages: {in_b_not_a}"
