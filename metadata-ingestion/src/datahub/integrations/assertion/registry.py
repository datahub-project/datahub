from typing import Dict, Type

from datahub.api.entities.assertion.compiler_interface import AssertionCompiler
from datahub.integrations.assertion.snowflake.compiler import SnowflakeAssertionCompiler

ASSERTION_PLATFORMS: Dict[str, Type[AssertionCompiler]] = {
    "snowflake": SnowflakeAssertionCompiler
}
