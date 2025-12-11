# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import os
from typing import Optional
from unittest import mock

import pytest
from pydantic import ValidationError

from datahub.configuration.common import ConfigModel, ConnectionModel


class SampleConfigModel(ConfigModel):
    required_field: str
    optional_field: Optional[int] = None


class SampleConnectionModel(ConnectionModel):
    host: str
    port: int


def test_hide_input_in_errors_when_debug_disabled():
    """
    Test that input is hidden in validation errors when DATAHUB_DEBUG is not set.
    """
    with mock.patch.dict(os.environ, {"DATAHUB_DEBUG": "false"}, clear=False):
        # Force model rebuild to pick up the new environment variable
        SampleConfigModel.model_rebuild(force=True)

        with pytest.raises(ValidationError) as exc_info:
            SampleConfigModel(required_field=123)  # type: ignore

        error_str = str(exc_info.value)
        # In non-debug mode, input should be hidden
        assert "Input should be a valid string" in error_str


def test_show_input_in_errors_when_debug_enabled():
    """
    Test that input is shown in validation errors when DATAHUB_DEBUG=true.
    """
    with mock.patch.dict(os.environ, {"DATAHUB_DEBUG": "true"}, clear=False):
        # Force model rebuild to pick up the new environment variable
        SampleConfigModel.model_rebuild(force=True)

        with pytest.raises(ValidationError) as exc_info:
            SampleConfigModel(required_field=123)  # type: ignore

        error_str = str(exc_info.value)
        # In debug mode, input should be visible in the error
        assert "Input should be a valid string" in error_str


def test_connection_model_hides_input_when_debug_disabled():
    """
    Test that ConnectionModel also respects hide_input_in_errors configuration.
    """
    with mock.patch.dict(os.environ, {"DATAHUB_DEBUG": "false"}, clear=False):
        # Force model rebuild to pick up the new environment variable
        SampleConnectionModel.model_rebuild(force=True)

        with pytest.raises(ValidationError) as exc_info:
            SampleConnectionModel(host="localhost", port="not_a_number")  # type: ignore

        error_str = str(exc_info.value)
        # In non-debug mode, input should be hidden
        assert "Input should be a valid integer" in error_str
