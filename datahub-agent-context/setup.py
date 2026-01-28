# Copyright 2025 Acryl Data, Inc.
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
with open("./src/datahub_agent_context/_version.py") as fp:
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
    "mypy==1.17.1",
}

base_requirements = {
    f"acryl-datahub{_self_pin}",
    # Core dependencies for MCP tools
    "pydantic>=2.0.0,<3.0.0",
    "json-repair>=0.25.0,<1.0.0",
    "jmespath>=1.0.0,<2.0.0",
    "cachetools>=5.0.0,<7.0.0",
    "google-re2>=1.0,<2.0",  # Required for documents grep functionality
    # Lower bounds on httpcore and h11 due to CVE-2025-43859.
    "httpcore>=1.0.9,<2.0",
    "h11>=0.16,<1.0",
}

mypy_stubs = {
    "types-cachetools>=5.0.0,<7.0.0",
    "types-PyYAML>=6.0.0,<7.0.0",
    "types-requests>=2.0.0,<3.0.0",
    "types-toml>=0.10.0,<1.0.0",
    "types-jmespath>=1.0.0,<2.0.0",
}

langchain_requirements = {
    "langchain-core>=1.2.7,<2.0.0",
}

dev_requirements = {
    *lint_requirements,
    *mypy_stubs,
    "pytest>=8.3.4,<9.0.0",
    "pytest-cov>=2.8.0,<7.0.0",
    "tox>=4.0.0,<5.0.0",
}

setuptools.setup(
    name=package_metadata["__package_name__"],
    version=package_metadata["__version__"],
    url="https://datahub.io/",
    project_urls={
        "Documentation": "https://datahubproject.io/docs/",
        "Source": "https://github.com/datahub-project/datahub",
        "Changelog": "https://github.com/datahub-project/datahub/releases",
    },
    license="Apache License 2.0",
    description="DataHub Agent Context - MCP Tools for AI Agents",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
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
    packages=setuptools.find_namespace_packages(where="./src"),
    package_dir={"": "src"},
    package_data={
        "datahub_agent_context": ["py.typed"],
        "datahub_agent_context.mcp_tools": ["gql/*.gql"],
    },
    python_requires=">=3.9",
    zip_safe=False,
    install_requires=list(base_requirements),
    extras_require={
        "dev": list(dev_requirements),
        "langchain": list(langchain_requirements),
    },
)
