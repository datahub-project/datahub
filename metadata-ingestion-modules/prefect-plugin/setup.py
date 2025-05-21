import os
import pathlib

import setuptools

package_metadata: dict = {}
with open("./src/prefect_datahub/_version.py") as fp:
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


rest_common = {"requests", "requests_file"}

base_requirements = {
    # For python 3.7 and importlib-metadata>=5.0.0, build failed with attribute error
    "importlib-metadata>=4.4.0,<5.0.0; python_version < '3.8'",
    # Actual dependencies.
    # Temporary pinning to 2.0.0 until we can upgrade to 3.0.0
    "prefect >= 2.0.0,<3.0.0",
    *rest_common,
    f"acryl-datahub[datahub-rest]{_self_pin}",
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

dev_requirements = {
    *base_requirements,
    *mypy_stubs,
    "coverage>=5.1",
    "ruff==0.11.7",
    "mypy==1.14.1",
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

entry_points = {
    "prefect.block": "prefect-datahub = prefect_datahub.prefect_datahub:DatahubEmitter"
}


setuptools.setup(
    # Package metadata.
    name=package_metadata["__package_name__"],
    version=package_metadata["__version__"],
    url="https://docs.datahub.com/",
    project_urls={
        "Documentation": "https://docs.datahub.com/docs/",
        "Source": "https://github.com/datahub-project/datahub",
        "Changelog": "https://github.com/datahub-project/datahub/releases",
    },
    license="Apache License 2.0",
    description="Datahub prefect block to capture executions and send to Datahub",
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
        "dev": list(dev_requirements),
    },
)
