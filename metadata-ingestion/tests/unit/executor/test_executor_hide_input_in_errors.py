"""Tests for hiding submitted values in task-argument validation errors.

Task arguments carry recipes and environment variables, which carry credentials. A
pydantic validation error normally echoes the offending input back, so these models
set ``hide_input_in_errors`` to keep secrets out of error text that reaches the UI
and the logs.

Note ``hide_input_in_errors=not get_debug()`` is evaluated when the class body runs,
so it is fixed at import time. Toggling ``DATAHUB_DEBUG`` afterwards -- with or
without ``model_rebuild(force=True)`` -- does not change it. These tests therefore
set the flag explicitly on throwaway subclasses rather than patching the
environment, which would assert nothing.
"""

from typing import Any, Dict, Type

import pydantic
import pytest
from pydantic import ValidationError

from datahub.executor.execution.in_mem_ingestion_task import InMemoryIngestionTaskArgs
from datahub.executor.execution.sub_process_ingestion_task import (
    SubProcessIngestionTaskArgs,
)

# A value distinctive enough that finding it in error text is unambiguous.
SENTINEL = "SENTINEL_SECRET_VALUE_a1b2c3"

# Each case feeds the sentinel to a field that rejects it, on a field that really does
# carry credentials in production: extra_env_vars is passed into the ingestion
# subprocess, and recipe holds the source config.
LEAK_CASES = [
    pytest.param(
        SubProcessIngestionTaskArgs,
        {"recipe": "recipe-body", "extra_env_vars": SENTINEL},
        "extra_env_vars",
        id="SubProcessIngestionTaskArgs-extra_env_vars",
    ),
    pytest.param(
        InMemoryIngestionTaskArgs,
        {"recipe": SENTINEL},
        "recipe",
        id="InMemoryIngestionTaskArgs-recipe",
    ),
]

ARG_MODELS = [SubProcessIngestionTaskArgs, InMemoryIngestionTaskArgs]


def _with_hiding(base: Type[Any], *, hide: bool) -> Type[Any]:
    """A throwaway subclass with hide_input_in_errors pinned.

    Subclassed rather than rebuilt in place so the production models are never
    mutated -- the suite runs in random order, so a mutated model would leak into
    unrelated tests.
    """
    return type(
        f"{base.__name__}Hide{hide}",
        (base,),
        {
            "model_config": pydantic.ConfigDict(
                populate_by_name=True, hide_input_in_errors=hide
            )
        },
    )


@pytest.mark.parametrize(("base", "kwargs", "field"), LEAK_CASES)
def test_submitted_value_is_absent_from_the_error_when_hiding_is_on(
    base: Type[Any], kwargs: Dict[str, Any], field: str
) -> None:
    model = _with_hiding(base, hide=True)

    with pytest.raises(ValidationError) as exc_info:
        model(**kwargs)

    error_str = str(exc_info.value)
    assert SENTINEL not in error_str
    assert "input_value" not in error_str
    # Hiding the value must not hide the problem: the operator still needs to know
    # which field was rejected.
    assert field in error_str


@pytest.mark.parametrize(("base", "kwargs", "field"), LEAK_CASES)
def test_submitted_value_is_present_when_hiding_is_off(
    base: Type[Any], kwargs: Dict[str, Any], field: str
) -> None:
    """The positive control.

    Without this, the test above would also pass if pydantic simply stopped echoing
    inputs, or if the error text changed shape -- i.e. it would pass for the wrong
    reason, which is what the previous version of this file did.
    """
    model = _with_hiding(base, hide=False)

    with pytest.raises(ValidationError) as exc_info:
        model(**kwargs)

    error_str = str(exc_info.value)
    assert SENTINEL in error_str
    assert "input_value" in error_str


@pytest.mark.parametrize("base", ARG_MODELS, ids=lambda b: b.__name__)
def test_production_models_configure_hiding_at_all(base: Type[Any]) -> None:
    """Guards the wiring, not the value.

    The value depends on DATAHUB_DEBUG at import time, so it is environment
    dependent and not worth asserting. That the key is present is not: dropping it
    would silently re-enable echoing submitted values into error text.
    """
    assert "hide_input_in_errors" in base.model_config
