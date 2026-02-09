import os
import pathlib
from typing import Dict, Set

import setuptools

package_metadata: dict = {}
with open("./src/datahub_airflow_plugin/_version.py") as fp:
    exec(fp.read(), package_metadata)


def get_long_description():
    root = os.path.dirname(__file__)
    return pathlib.Path(os.path.join(root, "README.md")).read_text()


_version: str = package_metadata["__version__"]
_self_pin = (
    f"=={_version}"
    if not (_version.endswith(("dev0", "dev1")) or "docker" in _version)
    else ""
)


base_requirements = {
    f"acryl-datahub[sql-parser,datahub-rest]{_self_pin}",
    "pydantic>=2.4.0",
    # We require Airflow 2.5.x at minimum, since we need the new DAG listener API.
    # We support both Airflow 2.x and 3.x with full backward compatibility.
    "apache-airflow>=2.5.0,<4.0.0",
    # Note: OpenLineage dependencies are version-specific and provided via extras:
    # - airflow2: for Airflow 2.x (uses standalone openlineage-airflow package)
    # - airflow3: for Airflow 3.x (uses native apache-airflow-providers-openlineage)
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
    # airflow2: For Airflow 2.x, use standalone openlineage-airflow package
    "airflow2": {
        "openlineage-airflow>=1.2.0",
    },
    # airflow3: For Airflow 3.x, use native OpenLineage provider
    "airflow3": {
        "apache-airflow-providers-openlineage>=1.0.0",
    },
}

# Require some plugins by default.
base_requirements.update(plugins["datahub-rest"])


mypy_stubs = {
    "types-dataclasses",
    "sqlalchemy-stubs",
    "types-setuptools",
    "types-six",
    "types-python-dateutil",
    "types-requests",
    "types-toml",
    "types-PyYAML",
    "types-cachetools",
    # versions 0.1.13 and 0.1.14 seem to have issues
    "types-click==0.1.12",
    "types-tabulate",
}

dev_requirements = {
    *base_requirements,
    *mypy_stubs,
    "coverage>=5.1",
    "mypy==1.17.1",
    "ruff==0.11.7",
    "pytest>=6.2.2",
    "pytest-cov>=2.8.1",
    "tox",
    "tox-uv",
    # Missing numpy requirement in 8.0.0
    "deepdiff!=8.0.0",
    "tenacity",
    "build",
    "twine",
    "packaging",
}

integration_test_requirements = {
    *plugins["datahub-file"],
    *plugins["datahub-kafka"],
    f"acryl-datahub[testing-utils]{_self_pin}",
    # Extra requirements for loading our test dags.
    "apache-airflow[snowflake,amazon,google]>=2.0.2",
    # A collection of issues we've encountered:
    # - Connexion's new version breaks Airflow:
    #   See https://github.com/apache/airflow/issues/35234.
    # - https://github.com/snowflakedb/snowflake-sqlalchemy/issues/350
    #   Eventually we want to set this to "snowflake-sqlalchemy>=1.4.3".
    # - To avoid https://github.com/snowflakedb/snowflake-connector-python/issues/1188,
    #   we need https://github.com/snowflakedb/snowflake-connector-python/pull/1193
    "snowflake-connector-python>=2.7.10",
    "virtualenv",  # needed by PythonVirtualenvOperator
    "apache-airflow-providers-sqlite",
    "apache-airflow-providers-teradata",
}


entry_points = {
    "airflow.plugins": "acryl-datahub-airflow-plugin = datahub_airflow_plugin.datahub_plugin:DatahubPlugin",
    "apache_airflow_provider": ["provider_info=datahub_provider:get_provider_info"],
}


setuptools.setup(
    # Package metadata.
    name=package_metadata["__package_name__"],
    version=_version,
    url="https://docs.datahub.com/",
    project_urls={
        "Documentation": "https://docs.datahub.com/docs/",
        "Source": "https://github.com/datahub-project/datahub",
        "Changelog": "https://github.com/datahub-project/datahub/releases",
    },
    license="Apache-2.0",
    description="Datahub Airflow plugin to capture executions and send to Datahub",
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
        "Operating System :: Unix",
        "Operating System :: POSIX :: Linux",
        "Environment :: Console",
        "Environment :: MacOS X",
        "Topic :: Software Development",
    ],
    # Package info.
    zip_safe=False,
    python_requires=">=3.10",
    package_data={
        "datahub_airflow_plugin": ["py.typed"],
    },
    package_dir={"": "src"},
    packages=setuptools.find_namespace_packages(where="./src"),
    entry_points=entry_points,
    # Dependencies.
    install_requires=list(base_requirements),
    extras_require={
        "ignore": [],  # This is a dummy extra to allow for trailing commas in the list.
        **{plugin: list(dependencies) for plugin, dependencies in plugins.items()},
        "dev": list(dev_requirements),
        "integration-tests": list(integration_test_requirements),
    },
)
