import os
import pathlib

import setuptools

package_metadata: dict = {}
with open("./src/datahub_gx_plugin/_version.py") as fp:
    exec(fp.read(), package_metadata)


def get_long_description():
    root = os.path.dirname(__file__)
    return pathlib.Path(os.path.join(root, "README.md")).read_text()


rest_common = {"requests", "requests_file"}

_version: str = package_metadata["__version__"]
_self_pin = (
    f"=={_version}"
    if not (_version.endswith(("dev0", "dev1")) or "docker" in _version)
    else ""
)

base_requirements = {
    # Actual dependencies.
    # This is temporary lower bound that we're open to loosening/tightening as requirements show up
    "sqlalchemy>=1.4.39, <2",
    # GE added handling for higher version of jinja2 in version 0.15.12
    # https://github.com/great-expectations/great_expectations/pull/5382/files
    # TODO: support GX 0.18.0
    "great-expectations>=0.15.12, <1.0.0",
    # datahub does not depend on traitlets directly but great expectations does.
    # https://github.com/ipython/traitlets/issues/741
    "traitlets<5.2.2",
    *rest_common,
    f"acryl-datahub[datahub-rest,sql-parser]{_self_pin}",
}

mypy_stubs = {
    "types-dataclasses",
    "sqlalchemy-stubs",
    "types-setuptools",
    "types-six",
    "types-python-dateutil",
    "types-requests",
    "types-toml",
    "types-PyYAML",
    "types-freezegun",
    "types-cachetools",
    # versions 0.1.13 and 0.1.14 seem to have issues
    "types-click==0.1.12",
    "types-tabulate",
    # avrogen package requires this
    "types-pytz",
}

base_dev_requirements = {
    *base_requirements,
    *mypy_stubs,
    "coverage>=5.1",
    "ruff==0.9.1",
    "mypy>=1.4.0",
    # pydantic 1.8.2 is incompatible with mypy 0.910.
    # See https://github.com/samuelcolvin/pydantic/pull/3175#issuecomment-995382910.
    "pydantic>=1.10.0,!=1.10.3",
    "pytest>=6.2.2",
    "pytest-asyncio>=0.16.0",
    "pytest-cov>=2.8.1",
    "tox",
    # Missing numpy requirement in 8.0.0
    "deepdiff!=8.0.0",
    "requests-mock",
    "freezegun",
    "jsonpickle",
    "build",
    "twine",
    "packaging",
}

dev_requirements = {
    *base_dev_requirements,
}

integration_test_requirements = {
    *dev_requirements,
    "psycopg2-binary",
    "pyspark",
    f"acryl-datahub[testing-utils]{_self_pin}",
    "pytest-docker>=1.1.0",
}

entry_points = {
    "gx.plugins": "acryl-datahub-gx-plugin = datahub_gx_plugin.action:DataHubValidationAction"
}


setuptools.setup(
    # Package metadata.
    name=package_metadata["__package_name__"],
    version=package_metadata["__version__"],
    url="https://datahubproject.io/",
    project_urls={
        "Documentation": "https://datahubproject.io/docs/",
        "Source": "https://github.com/datahub-project/datahub",
        "Changelog": "https://github.com/datahub-project/datahub/releases",
    },
    license="Apache License 2.0",
    description="Datahub GX plugin to capture executions and send to Datahub",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Unix",
        "Operating System :: POSIX :: Linux",
        "Environment :: Console",
        "Environment :: MacOS X",
        "Topic :: Software Development",
    ],
    # Package info.
    zip_safe=False,
    python_requires=">=3.8",
    package_dir={"": "src"},
    packages=setuptools.find_namespace_packages(where="./src"),
    entry_points=entry_points,
    # Dependencies.
    install_requires=list(base_requirements),
    extras_require={
        "ignore": [],  # This is a dummy extra to allow for trailing commas in the list.
        "dev": list(dev_requirements),
        "integration-tests": list(integration_test_requirements),
    },
)
