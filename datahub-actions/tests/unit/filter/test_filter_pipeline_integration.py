# Copyright 2021 Acryl Data, Inc.
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

"""Integration tests for the pipeline filter layer."""

import warnings

from datahub_actions.pipeline.pipeline import Pipeline
from datahub_actions.plugin.transform.filter.filter_transformer import FilterTransformer

# ── helpers ───────────────────────────────────────────────────────────────────


def _base_opts() -> dict:
    return {
        "retry_count": 0,
        "failure_mode": "CONTINUE",
        "failed_events_dir": "/tmp/datahub/test",
    }


# ── set_filters is always called ─────────────────────────────────────────────


def test_set_filters_called_with_empty_list_when_no_filters_configured():
    """Pipeline must call set_filters even when the filters list is empty."""
    config = {
        "name": "test-pipeline",
        "source": {"type": "test_source"},
        "action": {"type": "test_action"},
        "options": _base_opts(),
    }
    pipeline = Pipeline.create(config)
    # set_filters no-op on TestEventSource; pipeline should build with empty filters list
    assert pipeline.filters == []


def test_filters_list_populated_from_config():
    config = {
        "name": "test-pipeline",
        "source": {"type": "test_source"},
        "filters": [
            {
                "type": "event_type",
                "config": {"filter": {"MetadataChangeLogEvent_v1": {}}},
            }
        ],
        "action": {"type": "test_action"},
        "options": _base_opts(),
    }
    pipeline = Pipeline.create(config)
    assert len(pipeline.filters) == 1


# ── filters run before transformers ──────────────────────────────────────────


def test_filter_blocks_event_transformer_not_called():
    """An event filtered out must never reach the transformer."""

    config = {
        "name": "test-pipeline",
        "source": {"type": "test_source"},
        "filters": [
            {
                "type": "event_type",
                # Only MCL events pass; source emits MCL + ECE + TestEvent
                "config": {"filter": {"MetadataChangeLogEvent_v1": {}}},
            }
        ],
        "transform": [{"type": "test_transformer"}],
        "action": {"type": "test_action"},
        "options": _base_opts(),
    }
    pipeline = Pipeline.create(config)
    pipeline.run()

    # Source emits 3 events: 1 MCL, 1 ECE, 1 TestEvent.
    # Only MCL passes the filter.
    assert pipeline.action.total_event_count == 1  # type: ignore

    # The filter processed all 3 events; 2 were filtered.
    f = pipeline.filters[0]
    filter_stats = pipeline.stats().get_filter_stats(f)
    assert filter_stats.processed_count == 3
    assert filter_stats.filtered_count == 2
    assert filter_stats.exception_count == 0


def test_filter_passes_all_when_no_spec():
    config = {
        "name": "test-pipeline",
        "source": {"type": "test_source"},
        "filters": [
            {
                "type": "event_type",
                "config": {
                    "filter": {
                        "MetadataChangeLogEvent_v1": {},
                        "EntityChangeLogEvent_v1": {},
                        "TestEvent": {},
                    }
                },
            }
        ],
        "transform": [{"type": "test_transformer"}],
        "action": {"type": "test_action"},
        "options": _base_opts(),
    }
    pipeline = Pipeline.create(config)
    pipeline.run()
    assert pipeline.action.total_event_count == 3  # type: ignore
    assert pipeline.stats().failed_event_count == 0


# ── deprecated filter section ─────────────────────────────────────────────────


def test_deprecated_filter_section_emits_warning():
    config = {
        "name": "test-pipeline",
        "source": {"type": "test_source"},
        "filter": {"event_type": "MetadataChangeLogEvent_v1"},
        "action": {"type": "test_action"},
        "options": _base_opts(),
    }
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        pipeline = Pipeline.create(config)
    assert any(issubclass(w.category, DeprecationWarning) for w in caught)
    # Legacy filter becomes the first transformer.
    assert len(pipeline.transforms) >= 1
    assert isinstance(pipeline.transforms[0], FilterTransformer)
