import os
import pathlib
from typing import Dict, Set

import setuptools

package_metadata: dict = {}
with open("./src/datahub_airflow_plugin/__init__.py") as fp:
    exec(fp.read(), package_metadata)


def get_long_description():
    root = os.path.dirname(__file__)
    return pathlib.Path(os.path.join(root, "README.md")).read_text()


_version = package_metadata["__version__"]
_self_pin = f"=={_version}" if not _version.endswith("dev0") else ""


rest_common = {"requests", "requests_file"}

base_requirements = {
    # Compatibility.
    "dataclasses>=0.6; python_version < '3.7'",
    "mypy_extensions>=0.4.3",
    # Actual dependencies.
    "pydantic>=1.5.1",
    "apache-airflow >= 2.0.2",
    *rest_common,
}

plugins: Dict[str, Set[str]] = {
    "datahub-rest": {
        f"acryl-datahub[datahub-rest]{_self_pin}",
    },
    "datahub-kafka": {
        f"acryl-datahub[datahub-kafka]{_self_pin}",
    },
    "datahub-file": {
        f"acryl-datahub[sync-file-emitter]{_self_pin}",
    },
    "plugin-v1": set(),
    "plugin-v2": {
        # The v2 plugin requires Python 3.8+.
        f"acryl-datahub[sql-parser]{_self_pin}",
        "openlineage-airflow==1.2.0; python_version >= '3.8'",
    },
}

# Include datahub-rest in the base requirements.
base_requirements.update(plugins["datahub-rest"])


mypy_stubs = {
    "types-dataclasses",
    "sqlalchemy-stubs",
    "types-pkg_resources",
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
}

dev_requirements = {
    *base_requirements,
    *mypy_stubs,
    "black==22.12.0",
    "coverage>=5.1",
    "flake8>=3.8.3",
    "flake8-tidy-imports>=4.3.0",
    "isort>=5.7.0",
    "mypy>=1.4.0",
    # pydantic 1.8.2 is incompatible with mypy 0.910.
    # See https://github.com/samuelcolvin/pydantic/pull/3175#issuecomment-995382910.
    "pydantic>=1.10",
    "pytest>=6.2.2",
    "pytest-asyncio>=0.16.0",
    "pytest-cov>=2.8.1",
    "tox",
    "deepdiff",
    "tenacity",
    "requests-mock",
    "freezegun",
    "jsonpickle",
    "build",
    "twine",
    "packaging",
}

integration_test_requirements = {
    *dev_requirements,
    *plugins["datahub-file"],
    *plugins["datahub-kafka"],
    f"acryl-datahub[testing-utils]{_self_pin}",
    # Extra requirements for loading our test dags.
    "apache-airflow[snowflake]>=2.0.2",
    # https://github.com/snowflakedb/snowflake-sqlalchemy/issues/350
    # Eventually we want to set this to "snowflake-sqlalchemy>=1.4.3".
    # However, that doesn't work with older versions of Airflow. Instead
    # of splitting this into integration-test-old and integration-test-new,
    # adding a bound to SQLAlchemy was the simplest solution.
    "sqlalchemy<1.4.42",
    # To avoid https://github.com/snowflakedb/snowflake-connector-python/issues/1188,
    # we need https://github.com/snowflakedb/snowflake-connector-python/pull/1193
    "snowflake-connector-python>=2.7.10",
    "virtualenv",  # needed by PythonVirtualenvOperator
    "apache-airflow-providers-sqlite",
}


entry_points = {
    "airflow.plugins": "acryl-datahub-airflow-plugin = datahub_airflow_plugin.datahub_plugin:DatahubPlugin",
    "apache_airflow_provider": ["provider_info=datahub_provider:get_provider_info"],
}


setuptools.setup(
    # Package metadata.
    name=package_metadata["__package_name__"],
    version=_version,
    url="https://datahubproject.io/",
    project_urls={
        "Documentation": "https://datahubproject.io/docs/",
        "Source": "https://github.com/datahub-project/datahub",
        "Changelog": "https://github.com/datahub-project/datahub/releases",
    },
    license="Apache License 2.0",
    description="Datahub Airflow plugin to capture executions and send to Datahub",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
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
    python_requires=">=3.7",
    package_data={
        "datahub_airflow_plugin": ["py.typed"],
    },
    package_dir={"": "src"},
    packages=setuptools.find_namespace_packages(where="./src"),
    entry_points=entry_points,
    # Dependencies.
    install_requires=list(base_requirements),
    extras_require={
        **{plugin: list(dependencies) for plugin, dependencies in plugins.items()},
        "dev": list(dev_requirements),
        "integration-tests": list(integration_test_requirements),
    },
)
