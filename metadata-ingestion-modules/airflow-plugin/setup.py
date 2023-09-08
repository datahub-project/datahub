import os
import pathlib

import setuptools

package_metadata: dict = {}
with open("./src/datahub_airflow_plugin/__init__.py") as fp:
    exec(fp.read(), package_metadata)


if package_metadata["__version__"] == "0.0.0.dev0":
    # This is a "development mode" install.
    # In this case, we use setuptools_scm to set the version number.

    datahub_version = None

    _setuptools_version_kwargs = dict(
        use_scm_version={
            "root": "../..",
        },
    )
else:
    # This is a "release mode" build.
    # The version number is already set in __init__.py, and we just need
    # to respect it.
    datahub_version = package_metadata["__version__"]

    _setuptools_version_kwargs = dict(
        version=datahub_version,
    )


def get_long_description():
    root = os.path.dirname(__file__)
    return pathlib.Path(os.path.join(root, "README.md")).read_text()


rest_common = {"requests", "requests_file"}

base_requirements = {
    # Compatibility.
    "dataclasses>=0.6; python_version < '3.7'",
    # Typing extension should be >=3.10.0.2 ideally but we can't restrict due to Airflow 2.0.2 dependency conflict
    "typing_extensions>=3.7.4.3 ;  python_version < '3.8'",
    "typing_extensions>=3.10.0.2,<4.6.0 ;  python_version >= '3.8'",
    "mypy_extensions>=0.4.3",
    # Actual dependencies.
    "typing-inspect",
    "pydantic>=1.5.1",
    "apache-airflow >= 2.0.2",
    *rest_common,
    ("acryl-datahub" + (f"=={datahub_version}" if datahub_version else "")),
}

acryl_kafka = {
    "acryl-datahub[datahub-kafka]"
    + (f"=={datahub_version}" if datahub_version else ""),
}


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
    # avrogen package requires this
    "types-pytz",
}

base_dev_requirements = {
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


entry_points = {
    "airflow.plugins": "acryl-datahub-airflow-plugin = datahub_airflow_plugin.datahub_plugin:DatahubPlugin"
}


setuptools.setup(
    name=package_metadata["__package_name__"],
    # Package metadata.
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
        "dev": list(dev_requirements),
        "datahub-kafka": [
            *acryl_kafka,
        ],
        "integration-tests": [
            *acryl_kafka,
            # Extra requirements for Airflow.
            "apache-airflow[snowflake]>=2.0.2",  # snowflake is used in example dags
            # Because of https://github.com/snowflakedb/snowflake-sqlalchemy/issues/350 we need to restrict SQLAlchemy's max version.
            "SQLAlchemy<1.4.42",
            "virtualenv",  # needed by PythonVirtualenvOperator
        ],
    },
    # Versioning.
    **_setuptools_version_kwargs,
)
