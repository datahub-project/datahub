# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import Dict, Type

from datahub.api.entities.assertion.compiler_interface import AssertionCompiler
from datahub.integrations.assertion.snowflake.compiler import SnowflakeAssertionCompiler

ASSERTION_PLATFORMS: Dict[str, Type[AssertionCompiler]] = {
    "snowflake": SnowflakeAssertionCompiler
}
