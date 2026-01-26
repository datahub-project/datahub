from typing import cast

import pytest

from datahub_executor.common.assertion.engine.evaluator.utils.time import (
    get_milliseconds_for_unit,
)
from datahub_executor.common.exceptions import InvalidParametersException
from datahub_executor.common.types import CalendarInterval


class TestTimeUtils:
    def test_get_milliseconds_for_unit_invalid_unit(self) -> None:
        with pytest.raises(InvalidParametersException) as excinfo:
            get_milliseconds_for_unit(cast(CalendarInterval, "INVALID_UNIT"))

        assert "Unrecognized unit received" in str(excinfo.value)
        assert "unsupported_calendar_interval" in excinfo.value.parameters
        assert "INVALID_UNIT" in excinfo.value.parameters
