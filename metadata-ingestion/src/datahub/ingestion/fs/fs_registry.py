# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.ingestion.api.registry import PluginRegistry
from datahub.ingestion.fs.fs_base import FileSystem

fs_registry = PluginRegistry[FileSystem]()
fs_registry.register_from_entrypoint("datahub.fs.plugins")
