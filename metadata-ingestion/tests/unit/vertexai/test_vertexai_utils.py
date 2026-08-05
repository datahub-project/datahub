from contextlib import nullcontext
from typing import Any, List, cast
from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import ResourceExhausted, ServiceUnavailable

from datahub.ingestion.source.vertexai.vertexai_builder import (
    VertexAIExternalURLBuilder,
    VertexAINameFormatter,
)
from datahub.ingestion.source.vertexai.vertexai_utils import (
    create_vertex_retry_without_429,
    rate_limited_gapic_iter,
    rate_limited_gapic_list,
    rate_limited_paged_call,
)


def test_vertexai_multi_project_context_naming() -> None:
    name_formatter = VertexAINameFormatter(get_project_id_fn=lambda: "p2")
    url_builder = VertexAIExternalURLBuilder(
        base_url="https://console.cloud.google.com/vertex-ai",
        get_project_id_fn=lambda: "p2",
        get_region_fn=lambda: "r2",
    )

    assert name_formatter.format_model_group_name("m") == "p2.model_group.m"
    assert name_formatter.format_dataset_name("d") == "p2.dataset.d"

    assert (
        url_builder.make_model_url("xyz")
        == "https://console.cloud.google.com/vertex-ai/models/locations/r2/models/xyz?project=p2"
    )


# ---------------------------------------------------------------------------
# create_vertex_retry_without_429
# ---------------------------------------------------------------------------


def test_retry_does_not_retry_429() -> None:
    retry = create_vertex_retry_without_429()
    assert not retry._predicate(ResourceExhausted("quota exceeded"))


def test_retry_retries_transient_errors() -> None:
    retry = create_vertex_retry_without_429()
    assert retry._predicate(ServiceUnavailable("unavailable"))


# ---------------------------------------------------------------------------
# rate_limited_paged_call
# ---------------------------------------------------------------------------


def test_rate_limited_paged_call_acquires_token_on_first_page() -> None:
    pager = MagicMock()
    del pager._method  # single-page pager — no _method patching needed
    gapic_fn = MagicMock(return_value=pager)

    tokens = []

    class CountingLimiter:
        def __enter__(self):
            tokens.append(1)
            return self

        def __exit__(self, *_):
            return False

    result = rate_limited_paged_call(gapic_fn, "req", CountingLimiter())

    gapic_fn.assert_called_once_with(request="req")
    assert tokens == [1]
    assert result is pager


def test_rate_limited_paged_call_patches_method_for_subsequent_pages() -> None:
    """Each page fetch after the first should consume a rate-limit token."""
    tokens = []

    class CountingLimiter:
        def __enter__(self):
            tokens.append(1)
            return self

        def __exit__(self, *_):
            return False

    original_method = MagicMock(return_value="page2")
    pager = MagicMock()
    pager._method = original_method

    result = cast(
        MagicMock,
        rate_limited_paged_call(
            MagicMock(return_value=pager), "req", CountingLimiter()
        ),
    )

    assert tokens == [1]  # initial fetch
    assert result._method is not original_method  # patched

    result._method("arg")
    assert tokens == [1, 1]  # second page consumed another token
    original_method.assert_called_once_with("arg")


# ---------------------------------------------------------------------------
# rate_limited_gapic_iter (rate_limited_gapic_list is a thin list() wrapper, so
# behavior is asserted once here on the real generator implementation)
# ---------------------------------------------------------------------------


def _make_cls(*, supported_schemas=None, supported_uris=None):
    """Build a minimal mock SDK class for rate_limited_gapic_iter."""
    cls = MagicMock()
    cls.__name__ = "MockCls"
    cls._list_method = "list_things"
    cls._supported_training_schemas = supported_schemas
    cls._supported_metadata_schema_uris = supported_uris
    cls._empty_constructor.return_value.credentials = None
    cls._construct_sdk_resource_from_gapic.side_effect = lambda proto, **_: proto
    return cls


def test_rate_limited_gapic_iter_streams_lazily() -> None:
    """The streaming variant must pull at most one page item before the first
    yield, so peak memory does not hold the whole listing."""
    cls = _make_cls()
    pulled: List[int] = []

    def counting_pager():
        for i in range(5):
            pulled.append(i)
            yield i

    with (
        patch(
            "datahub.ingestion.source.vertexai.vertexai_utils.vertex_initializer"
        ) as mock_init,
        patch(
            "datahub.ingestion.source.vertexai.vertexai_utils.rate_limited_paged_call",
            return_value=counting_pager(),
        ),
    ):
        mock_init.global_config.common_location_path.return_value = (
            "projects/p/locations/l"
        )
        stream = rate_limited_gapic_iter(cls, nullcontext())
        first = next(stream)

    assert first == 0
    assert pulled == [0]  # only the first proto was materialized


def test_rate_limited_gapic_iter_matches_list() -> None:
    cls = _make_cls()
    with (
        patch(
            "datahub.ingestion.source.vertexai.vertexai_utils.vertex_initializer"
        ) as mock_init,
        patch(
            "datahub.ingestion.source.vertexai.vertexai_utils.rate_limited_paged_call",
            side_effect=lambda *a, **k: iter(["a", "b", "c"]),
        ),
    ):
        mock_init.global_config.common_location_path.return_value = (
            "projects/p/locations/l"
        )
        iter_result = list(rate_limited_gapic_iter(cls, nullcontext()))
        list_result = rate_limited_gapic_list(cls, nullcontext())

    assert iter_result == ["a", "b", "c"]
    assert list_result == iter_result


def test_rate_limited_gapic_iter_applies_proto_filter() -> None:
    wanted = MagicMock(training_task_definition="gs://schema/custom")
    unwanted = MagicMock(training_task_definition="gs://schema/other")

    cls = _make_cls(supported_schemas={"gs://schema/custom"})
    with (
        patch(
            "datahub.ingestion.source.vertexai.vertexai_utils.vertex_initializer"
        ) as mock_init,
        patch(
            "datahub.ingestion.source.vertexai.vertexai_utils.rate_limited_paged_call"
        ) as mock_paged,
    ):
        mock_init.global_config.common_location_path.return_value = (
            "projects/p/locations/l"
        )
        mock_paged.return_value = iter([wanted, unwanted])
        result = list(rate_limited_gapic_iter(cls, nullcontext()))

    assert result == [wanted]


def test_rate_limited_gapic_iter_no_list_method_falls_back() -> None:
    cls = MagicMock()
    del cls._list_method
    cls.list.return_value = ["a", "b"]

    result: List[Any] = list(rate_limited_gapic_iter(cls, nullcontext()))

    cls.list.assert_called_once()
    assert result == ["a", "b"]


def test_rate_limited_gapic_iter_attribute_error_falls_back() -> None:
    """Setup errors fall back to list(); the fallback must not fire mid-stream."""
    cls = _make_cls()
    cls._empty_constructor.side_effect = AttributeError("no constructor")
    cls.list.return_value = ["fallback"]

    result = list(rate_limited_gapic_iter(cls, nullcontext()))

    cls.list.assert_called_once()
    assert result == ["fallback"]


def test_rate_limited_gapic_iter_does_not_refetch_on_mid_stream_error() -> None:
    """A failure after streaming has started must propagate, not fall back to
    list() and re-emit already-yielded resources (the fallback is setup-only)."""
    cls = _make_cls()

    def pager_then_raise():
        yield "a"
        yield "b"
        raise AttributeError("boom mid-stream")

    emitted = []
    with (
        patch(
            "datahub.ingestion.source.vertexai.vertexai_utils.vertex_initializer"
        ) as mock_init,
        patch(
            "datahub.ingestion.source.vertexai.vertexai_utils.rate_limited_paged_call",
            return_value=pager_then_raise(),
        ),
    ):
        mock_init.global_config.common_location_path.return_value = (
            "projects/p/locations/l"
        )
        with pytest.raises(AttributeError):
            for item in rate_limited_gapic_iter(cls, nullcontext()):
                emitted.append(item)

    assert emitted == ["a", "b"]  # no duplicates from a re-run
    cls.list.assert_not_called()  # did not fall back to the high-level list()


def test_rate_limited_gapic_iter_passes_filter_str() -> None:
    cls = _make_cls()
    with (
        patch(
            "datahub.ingestion.source.vertexai.vertexai_utils.vertex_initializer"
        ) as mock_init,
        patch(
            "datahub.ingestion.source.vertexai.vertexai_utils.rate_limited_paged_call"
        ) as mock_paged,
    ):
        mock_init.global_config.common_location_path.return_value = (
            "projects/p/locations/l"
        )
        mock_paged.return_value = iter([])
        list(rate_limited_gapic_iter(cls, nullcontext(), filter_str="my_filter"))

    request = mock_paged.call_args[0][1]
    assert request["filter"] == "my_filter"


def test_rate_limited_gapic_iter_applies_metadata_schema_uri_filter() -> None:
    wanted = MagicMock(metadata_schema_uri="gs://meta/tabular")
    unwanted = MagicMock(metadata_schema_uri="gs://meta/image")

    cls = _make_cls(supported_uris={"gs://meta/tabular"})
    with (
        patch(
            "datahub.ingestion.source.vertexai.vertexai_utils.vertex_initializer"
        ) as mock_init,
        patch(
            "datahub.ingestion.source.vertexai.vertexai_utils.rate_limited_paged_call"
        ) as mock_paged,
    ):
        mock_init.global_config.common_location_path.return_value = (
            "projects/p/locations/l"
        )
        mock_paged.return_value = iter([wanted, unwanted])
        result = list(rate_limited_gapic_iter(cls, nullcontext()))

    assert result == [wanted]


def test_rate_limited_gapic_iter_order_by_value_error_retried_without_it() -> None:
    cls = _make_cls()
    call_count = [0]

    def paged_call_side_effect(gapic_fn, request, rate_limiter):
        call_count[0] += 1
        if call_count[0] == 1:
            raise ValueError("order_by not supported")
        pager = MagicMock()
        del pager._method
        pager.__iter__ = MagicMock(return_value=iter(["item"]))
        return pager

    with (
        patch(
            "datahub.ingestion.source.vertexai.vertexai_utils.vertex_initializer"
        ) as mock_init,
        patch(
            "datahub.ingestion.source.vertexai.vertexai_utils.rate_limited_paged_call",
            side_effect=paged_call_side_effect,
        ),
    ):
        mock_init.global_config.common_location_path.return_value = (
            "projects/p/locations/l"
        )
        result = list(
            rate_limited_gapic_iter(cls, nullcontext(), order_by="update_time")
        )

    assert call_count[0] == 2
    assert result == ["item"]
