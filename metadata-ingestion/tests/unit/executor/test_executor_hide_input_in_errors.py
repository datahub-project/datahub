import os
from unittest import mock

import pytest
from pydantic import ValidationError

from datahub.executor.execution.in_mem_ingestion_task import InMemoryIngestionTaskArgs
from datahub.executor.execution.sub_process_ingestion_task import (
    SubProcessIngestionTaskArgs,
)


# Throwaway subclasses so that model_rebuild(force=True) below never mutates the
# production classes. They inherit model_config (and therefore
# hide_input_in_errors) unchanged, so the behaviour under test is the real one.
class SampleSubProcessIngestionTaskArgs(SubProcessIngestionTaskArgs):
    pass


class SampleInMemoryIngestionTaskArgs(InMemoryIngestionTaskArgs):
    pass


def test_subprocess_ingestion_args_hides_input_when_debug_disabled() -> None:
    """
    Test that input is hidden in validation errors when DATAHUB_DEBUG is not set.
    """
    with mock.patch.dict(os.environ, {"DATAHUB_DEBUG": "false"}, clear=False):
        # Force model rebuild to pick up the new environment variable
        SampleSubProcessIngestionTaskArgs.model_rebuild(force=True)

        with pytest.raises(ValidationError) as exc_info:
            SampleSubProcessIngestionTaskArgs()  # type: ignore  # Missing required field 'recipe'

        error_str = str(exc_info.value)
        # In non-debug mode, input should be hidden and show the field is required
        assert "Field required" in error_str


def test_subprocess_ingestion_args_shows_input_when_debug_enabled() -> None:
    """
    Test that input is shown in validation errors when DATAHUB_DEBUG=true.
    """
    with mock.patch.dict(os.environ, {"DATAHUB_DEBUG": "true"}, clear=False):
        # Force model rebuild to pick up the new environment variable
        SampleSubProcessIngestionTaskArgs.model_rebuild(force=True)

        with pytest.raises(ValidationError) as exc_info:
            SampleSubProcessIngestionTaskArgs()  # type: ignore  # Missing required field 'recipe'

        error_str = str(exc_info.value)
        # In debug mode, input should be visible in the error
        assert "Field required" in error_str


def test_inmemory_ingestion_args_hides_input_when_debug_disabled() -> None:
    """
    Test that InMemoryIngestionTaskArgs also respects hide_input_in_errors configuration.
    """
    with mock.patch.dict(os.environ, {"DATAHUB_DEBUG": "false"}, clear=False):
        # Force model rebuild to pick up the new environment variable
        SampleInMemoryIngestionTaskArgs.model_rebuild(force=True)

        with pytest.raises(ValidationError) as exc_info:
            SampleInMemoryIngestionTaskArgs(recipe="not_a_dict")  # type: ignore

        error_str = str(exc_info.value)
        # In non-debug mode, input should be hidden
        assert "Input should be a valid dictionary" in error_str


def test_inmemory_ingestion_args_shows_input_when_debug_enabled() -> None:
    """
    Test that InMemoryIngestionTaskArgs shows input when DATAHUB_DEBUG=true.
    """
    with mock.patch.dict(os.environ, {"DATAHUB_DEBUG": "true"}, clear=False):
        # Force model rebuild to pick up the new environment variable
        SampleInMemoryIngestionTaskArgs.model_rebuild(force=True)

        with pytest.raises(ValidationError) as exc_info:
            SampleInMemoryIngestionTaskArgs(recipe="not_a_dict")  # type: ignore

        error_str = str(exc_info.value)
        # In debug mode, input should be visible in the error
        assert "Input should be a valid dictionary" in error_str
