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

# Everything except great-expectations, which is pinned differently per
# environment below. Each requirement set must end up with exactly one
# great-expectations entry: two in the same set resolve unpredictably and make
# .github/scripts/dep-analyzer.py report a different bound run to run.
common_requirements = {
    # Actual dependencies.
    # This is temporary lower bound that we're open to loosening/tightening as requirements show up
    "sqlalchemy>=1.4.39, <2",
    "pydantic>=2.1.0",
    # datahub does not depend on traitlets directly but great expectations does.
    # https://github.com/ipython/traitlets/issues/741
    "traitlets!=5.2.2",
    *rest_common,
    f"acryl-datahub[datahub-rest,sql-parser]{_self_pin}",
}

base_requirements = {
    *common_requirements,
    # GE added handling for higher version of jinja2 in version 0.15.12
    # https://github.com/great-expectations/great_expectations/pull/5382
    # GX v0.17.15 is the earliest version that supports Pydantic v2.
    # See https://github.com/great-expectations/great_expectations/pull/8604
    # GX Core 1.x is supported via datahub_gx_plugin.action_v1 (additive).
    # Keep using datahub_gx_plugin.action for GX 0.17/0.18.
    "great-expectations>=0.17.15",
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

dev_tool_requirements = {
    "coverage>=5.1",
    "ruff==0.15.22",
    "mypy==1.17.1",
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

base_dev_requirements = {
    *common_requirements,
    *mypy_stubs,
    *dev_tool_requirements,
    # Keep the 0.x action/test suite on GX <1 in local/CI installs.
    # Package install_requires still allows GX 1.x for action_v1 users.
    "great-expectations>=0.17.15, <1.0.0",
}

dev_requirements = {
    *base_dev_requirements,
}

# GX Core 1.x test environment for action_v1. Installed into a separate venv
# because the 0.x pin above and GX 1.x cannot coexist in one resolution.
gx1_dev_requirements = {
    *common_requirements,
    *mypy_stubs,
    *dev_tool_requirements,
    "great-expectations>=1.0.0, <2.0.0",
    # tests/conftest.py imports datahub.testing.docker_utils unconditionally.
    "pytest-docker>=1.1.0",
}

integration_test_requirements = {
    *dev_requirements,
    "psycopg2-binary",
    "pyspark",
    f"acryl-datahub[testing-utils]{_self_pin}",
    "pytest-docker>=1.1.0",
}

entry_points = {
    # GX 0.x discovery. For GX Core 1.x, instantiate
    # datahub_gx_plugin.action_v1.DataHubValidationAction in Python (Fluent API).
    "gx.plugins": "acryl-datahub-gx-plugin = datahub_gx_plugin.action:DataHubValidationAction"
}


setuptools.setup(
    # Package metadata.
    name=package_metadata["__package_name__"],
    version=package_metadata["__version__"],
    url="https://datahub.com/",
    project_urls={
        "Documentation": "https://docs.datahub.com/",
        "Source": "https://github.com/datahub-project/datahub",
        "Changelog": "https://github.com/acryldata/datahub/releases",
        "Releases": "https://github.com/acryldata/datahub/releases",
    },
    license="Apache-2.0",
    description="DataHub Great Expectations plugin — send data quality assertion results from GX checkpoints into your DataHub catalog",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "Operating System :: Unix",
        "Operating System :: POSIX :: Linux",
        "Environment :: Console",
        "Environment :: MacOS X",
        "Topic :: Software Development",
    ],
    # Package info.
    zip_safe=False,
    python_requires=">=3.10",
    package_dir={"": "src"},
    packages=setuptools.find_namespace_packages(where="./src"),
    entry_points=entry_points,
    # Dependencies.
    install_requires=list(base_requirements),
    extras_require={
        "ignore": [],  # This is a dummy extra to allow for trailing commas in the list.
        "dev": list(dev_requirements),
        "dev-gx1": list(gx1_dev_requirements),
        "integration-tests": list(integration_test_requirements),
    },
)
