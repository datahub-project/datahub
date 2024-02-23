from datahub_monitors.common.assertion.types import AssertionDatabaseParams
from datahub_monitors.common.types import Assertion


def get_database_parameters(assertion: Assertion) -> AssertionDatabaseParams:
    entity = assertion.entity
    return AssertionDatabaseParams(
        qualified_name=entity.qualified_name, table_name=entity.table_name
    )
