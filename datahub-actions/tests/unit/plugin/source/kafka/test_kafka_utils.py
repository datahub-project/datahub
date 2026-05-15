# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from unittest.mock import patch

import pytest

from datahub_actions.plugin.source.kafka.utils import with_retry


def test_with_retry_reraises_after_exhaustion() -> None:
    # with_retry was returning None after exhausting attempts, so callers had
    # no way to distinguish "operation succeeded with None result" from
    # "operation failed permanently". The single caller in kafka_event_source
    # uses with_retry to commit offsets — silent commit failures meant the
    # pipeline incremented success_count instead of failed_ack_count, hiding
    # broken brokers from operator monitoring.
    def always_fails() -> None:
        raise ValueError("permanent failure")

    with patch("datahub_actions.plugin.source.kafka.utils.time.sleep"):
        with pytest.raises(ValueError, match="permanent failure"):
            with_retry(max_attempts=3, max_backoff=1.0, func=always_fails)


def test_with_retry_returns_value_on_success() -> None:
    # Confirm the success path still works: returns the function's return value.
    def succeeds() -> str:
        return "ok"

    result = with_retry(max_attempts=3, max_backoff=1.0, func=succeeds)
    assert result == "ok"


def test_with_retry_succeeds_after_transient_failure() -> None:
    # Confirm intermediate failures don't break the success path: function
    # raises once, then returns.
    attempts = {"n": 0}

    def fails_once_then_succeeds() -> str:
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise ValueError("transient")
        return "ok"

    with patch("datahub_actions.plugin.source.kafka.utils.time.sleep"):
        result = with_retry(
            max_attempts=3, max_backoff=1.0, func=fails_once_then_succeeds
        )
    assert result == "ok"
    assert attempts["n"] == 2
