import os
import setuptools


def get_version():
    root = os.path.dirname(__file__)
    changelog = os.path.join(root, "CHANGELOG")
    with open(changelog) as f:
        return f.readline().strip()


def get_long_description():
    root = os.path.dirname(__file__)
    with open(os.path.join(root, "README.md")) as f:
        description = f.read()

    description += "\n\nChangelog\n=========\n\n"

    with open(os.path.join(root, "CHANGELOG")) as f:
        description += f.read()

    return description


setuptools.setup(
    name="gometa",
    version=get_version(),
    url="https://github.com/linkedin/datahub",
    author="DataHub Committers",
    license="Apache License 2.0",
    description="A CLI to work with DataHub metadata",
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
    python_requires=">=3.7",
    package_dir={"": "src"},
    packages=["gometa", "gometa.configuration"],#TODO: Are these the right components?
    include_package_data=True,
    package_data={"gometa": ["py.typed"]},
    entry_points={
        "console_scripts": [
            "gometa-ingest = gometa.entrypoints:gometa_ingest"
        ],
    },
    install_requires=[
        'dataclasses; python_version<="3.6"', #TODO: is this the right directive?
        "click>=7.1.1",
        "pyyaml>=5.4.1",
        "toml>=0.10.0",
        "pydantic>=1.5.1",
        "watchdog>=0.10.3", #TODO: Check if we want this
        "confluent_kafka>=1.5.0",
        "requests>=2.25.1",
        "fastavro>=1.3.0", #TODO: Do we need both avro-s?
        "avro-python3>=1.8.2",
        "sqlalchemy>=1.3.23", #Required for SQL sources
        "pymysql>=1.0.2", # Driver for MySQL
        "sqlalchemy-pytds>=0.3", # Driver for MS-SQL
        "avro_gen @ https://api.github.com/repos/hsheth2/avro_gen/tarball/master",
    ],
)
