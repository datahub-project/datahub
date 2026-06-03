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
    # Airflow 3.0+. The only 3.0-vs-3.1 API gap we hit is BaseHook: it moved into
    # the Task SDK (airflow.sdk.bases.hook) in 3.1, but is still importable from
    # airflow.hooks.base on 3.0.x. hooks/datahub.py handles both via a fallback.
    "apache-airflow>=3.0.0,<4.0.0",
    # 2.1.0 added Airflow 3 listener interface support; we deliberately stay at
    # or below the version pinned by Airflow 3.1.0's constraints file (2.7.1) so
    # `pip install acryl-datahub-airflow-plugin` works under those constraints.
    "apache-airflow-providers-openlineage>=2.1.0",
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
    # Provider extras needed for loading test DAGs; airflow itself is already
    # pinned in base_requirements.
    "apache-airflow-providers-snowflake",
    "apache-airflow-providers-amazon",
    "apache-airflow-providers-google",
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
        # Backward-compatibility no-op: the plugin now always targets Airflow 3,
        # so `[airflow3]` adds nothing over the base install. Kept so existing
        # `pip install 'acryl-datahub-airflow-plugin[airflow3]'` commands (and any
        # pinned recipes/CI) keep working. (`[airflow2]` is intentionally not
        # restored — Airflow 2 is unsupported.)
        "airflow3": [],
        **{plugin: list(dependencies) for plugin, dependencies in plugins.items()},
        "dev": list(dev_requirements),
        "integration-tests": list(integration_test_requirements),
    },
)
