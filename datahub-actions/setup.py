# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from typing import Dict, Set

import setuptools

package_metadata: dict = {}
with open("./src/datahub_actions/_version.py") as fp:
    exec(fp.read(), package_metadata)

_version: str = package_metadata["__version__"]
_self_pin = (
    f"=={_version}"
    if not (_version.endswith(("dev0", "dev1")) or "docker" in _version)
    else ""
)


def get_long_description():
    root = os.path.dirname(__file__)
    with open(os.path.join(root, "README.md")) as f:
        description = f.read()

    return description


lint_requirements = {
    # This is pinned only to avoid spurious errors in CI.
    # We should make an effort to keep it up to date.
    "ruff==0.11.7",
    "mypy==1.14.1",
}

base_requirements = {
    f"acryl-datahub[datahub-kafka]{_self_pin}",
    # Compatibility.
    "typing_extensions>=3.7.4; python_version < '3.8'",
    "mypy_extensions>=0.4.3",
    # Actual dependencies.
    "typing-inspect",
    "pydantic>=1.10.21",
    "ratelimit",
    # Lower bounds on httpcore and h11 due to CVE-2025-43859.
    "httpcore>=1.0.9",
    "azure-identity==1.21.0",
    "aws-msk-iam-sasl-signer-python==1.0.2",
    "h11>=0.16",
}

framework_common = {
    "click>=6.0.0",
    "click-default-group",
    "prometheus-client",
    "PyYAML",
    "toml>=0.10.0",
    "entrypoints",
    "python-dateutil>=2.8.0",
    "stackprinter",
    "progressbar2",
    "tenacity",
}

# Note: for all of these, framework_common will be added.
plugins: Dict[str, Set[str]] = {
    # Source Plugins
    "kafka": {
        "confluent-kafka[schemaregistry]",
    },
    # Action Plugins
    "executor": {
        "acryl-executor==0.1.2",
    },
    "slack": {
        "slack-bolt>=1.15.5",
    },
    "teams": {
        "pymsteams >=0.2.2",
    },
    "tag_propagation": set(),
    "term_propagation": set(),
    "snowflake_tag_propagation": {
        f"acryl-datahub[snowflake-slim]{_self_pin}",
    },
    "doc_propagation": set(),
    # Transformer Plugins (None yet)
}

mypy_stubs = {
    "types-pytz",
    "types-dataclasses",
    "sqlalchemy-stubs",
    "types-setuptools",
    "types-six",
    "types-python-dateutil",
    "types-requests",
    "types-toml",
    "types-PyMySQL",
    "types-PyYAML",
    "types-freezegun",
    "types-cachetools",
    # versions 0.1.13 and 0.1.14 seem to have issues
    "types-click==0.1.12",
}

base_dev_requirements = {
    *lint_requirements,
    *base_requirements,
    *framework_common,
    *mypy_stubs,
    "coverage>=5.1",
    "pytest>=6.2.2",
    "pytest-cov>=2.8.1",
    "pytest-dependency>=0.5.1",
    "pytest-docker>=0.10.3",
    "tox",
    "deepdiff",
    "requests-mock",
    "freezegun",
    "jsonpickle",
    "build",
    "twine",
    *list(
        dependency
        for plugin in [
            "kafka",
            "executor",
            "slack",
            "teams",
            "tag_propagation",
            "term_propagation",
            "snowflake_tag_propagation",
            "doc_propagation",
        ]
        for dependency in plugins[plugin]
    ),
}

dev_requirements = {
    *base_dev_requirements,
}

full_test_dev_requirements = {
    *list(
        dependency
        for plugin in [
            "kafka",
            "executor",
            "slack",
            "teams",
            "tag_propagation",
            "term_propagation",
            "snowflake_tag_propagation",
            "doc_propagation",
        ]
        for dependency in plugins[plugin]
    ),
    # In our tests, we want to always test against pydantic v2.
    # However, we maintain compatibility with pydantic v1 for now.
    "pydantic>2",
}

entry_points = {
    "console_scripts": ["datahub-actions = datahub_actions.entrypoints:main"],
    "datahub_actions.action.plugins": [
        "executor = datahub_actions.plugin.action.execution.executor_action:ExecutorAction",
        "slack = datahub_actions.plugin.action.slack.slack:SlackNotificationAction",
        "teams = datahub_actions.plugin.action.teams.teams:TeamsNotificationAction",
        "metadata_change_sync = datahub_actions.plugin.action.metadata_change_sync.metadata_change_sync:MetadataChangeSyncAction",
        "tag_propagation = datahub_actions.plugin.action.tag.tag_propagation_action:TagPropagationAction",
        "term_propagation = datahub_actions.plugin.action.term.term_propagation_action:TermPropagationAction",
        "snowflake_tag_propagation = datahub_actions.plugin.action.snowflake.tag_propagator:SnowflakeTagPropagatorAction",
        "doc_propagation = datahub_actions.plugin.action.propagation.docs.propagation_action:DocPropagationAction",
    ],
    "datahub_actions.transformer.plugins": [],
    "datahub_actions.source.plugins": [],
}


setuptools.setup(
    # Package metadata.
    name=package_metadata["__package_name__"],
    version=package_metadata["__version__"],
    url="https://docs.datahub.com/",
    project_urls={
        "Documentation": "https://docs.datahub.com/docs/actions",
        "Source": "https://github.com/acryldata/datahub-actions",
        "Changelog": "https://github.com/acryldata/datahub-actions/releases",
    },
    license="Apache License 2.0",
    description="An action framework to work with DataHub real time changes.",
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
    python_requires=">=3.8",
    package_dir={"": "src"},
    packages=setuptools.find_namespace_packages(where="./src"),
    package_data={
        "datahub_actions": ["py.typed"],
    },
    entry_points=entry_points,
    # Dependencies.
    install_requires=list(base_requirements | framework_common),
    extras_require={
        "base": list(framework_common),
        **{
            plugin: list(framework_common | dependencies)
            for (plugin, dependencies) in plugins.items()
        },
        "all": list(
            framework_common.union(
                *[requirements for plugin, requirements in plugins.items()]
            )
        ),
        "dev": list(dev_requirements),
        "integration-tests": list(full_test_dev_requirements),
    },
)
