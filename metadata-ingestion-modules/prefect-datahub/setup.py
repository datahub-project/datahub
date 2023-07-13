from setuptools import find_packages, setup

package_metadata: dict = {}
with open("./prefect_datahub/__init__.py") as fp:
    exec(fp.read(), package_metadata)

with open("requirements.txt") as install_requires_file:
    install_requires = install_requires_file.read().strip().split("\n")

with open("requirements-dev.txt") as dev_requires_file:
    dev_requires = dev_requires_file.read().strip().split("\n")

with open("README.md") as readme_file:
    readme = readme_file.read()

setup(
    name=package_metadata["__package_name__"],
    version=package_metadata["__version__"],
    description="Metadata emitter for datahub",
    license="Apache License 2.0",
    author="Acryl Data",
    author_email="shubham.jagtap@gslab.com",
    keywords="prefect",
    url="https://github.com/PrefectHQ/prefect-datahub",
    long_description=readme,
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=("tests", "docs")),
    python_requires=">=3.7",
    install_requires=install_requires,
    extras_require={"dev": dev_requires},
    entry_points={
        "prefect.collections": [
            "prefect_datahub = prefect_datahub",
        ]
    },
    classifiers=[
        "Natural Language :: English",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Software Development :: Libraries",
    ],
)
