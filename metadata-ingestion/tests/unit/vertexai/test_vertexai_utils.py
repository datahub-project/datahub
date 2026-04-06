from contextlib import nullcontext
from typing import Any, List, cast
from unittest.mock import MagicMock, patch

from google.api_core.exceptions import ResourceExhausted, ServiceUnavailable

from datahub.ingestion.source.vertexai.vertexai_builder import (
    VertexAIExternalURLBuilder,
    VertexAINameFormatter,
)
from datahub.ingestion.source.vertexai.vertexai_utils import (
    create_vertex_retry_without_429,
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
# rate_limited_gapic_list
# ---------------------------------------------------------------------------


def _make_cls(*, supported_schemas=None, supported_uris=None):
    """Build a minimal mock SDK class for rate_limited_gapic_list."""
    cls = MagicMock()
    cls.__name__ = "MockCls"
    cls._list_method = "list_things"
    cls._supported_training_schemas = supported_schemas
    cls._supported_metadata_schema_uris = supported_uris
    cls._empty_constructor.return_value.credentials = None
    cls._construct_sdk_resource_from_gapic.side_effect = lambda proto, **_: proto
    return cls


def test_rate_limited_gapic_list_no_list_method_falls_back() -> None:
    cls = MagicMock()
    del cls._list_method
    cls.list.return_value = ["a", "b"]

    result: List[Any] = rate_limited_gapic_list(cls, nullcontext())

    cls.list.assert_called_once()
    assert result == ["a", "b"]


def test_rate_limited_gapic_list_passes_filter_str() -> None:
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
        rate_limited_gapic_list(cls, nullcontext(), filter_str="my_filter")

    request = mock_paged.call_args[0][1]
    assert request["filter"] == "my_filter"


def test_rate_limited_gapic_list_training_schema_filter() -> None:
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
        result = rate_limited_gapic_list(cls, nullcontext())

    assert result == [wanted]


def test_rate_limited_gapic_list_metadata_schema_uri_filter() -> None:
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
        result = rate_limited_gapic_list(cls, nullcontext())

    assert result == [wanted]


def test_rate_limited_gapic_list_order_by_value_error_retried_without_it() -> None:
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
        result = rate_limited_gapic_list(cls, nullcontext(), order_by="update_time")

    assert call_count[0] == 2
    assert result == ["item"]


def test_rate_limited_gapic_list_attribute_error_falls_back() -> None:
    cls = _make_cls()
    cls._empty_constructor.side_effect = AttributeError("no constructor")
    cls.list.return_value = ["fallback"]

    result = rate_limited_gapic_list(cls, nullcontext())

    cls.list.assert_called_once()
    assert result == ["fallback"]
