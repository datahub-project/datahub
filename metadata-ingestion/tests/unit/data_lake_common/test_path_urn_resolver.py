from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.data_lake_common.path_urn_resolver import (
    DataLakePathResolver,
    dataset_path_is_rooted_at,
)


def _resolver(graph: MagicMock, env: str = "PROD") -> DataLakePathResolver:
    return DataLakePathResolver(graph=graph, env=env)


class TestDataLakePathResolver:
    def test_returns_datasets_rooted_at_path(self) -> None:
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = iter(
            [
                "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/folder/a,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/folder/b,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/folder,PROD)",
                # Sibling path sharing a character prefix — must be excluded.
                "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/folder_other,PROD)",
            ]
        )
        result = _resolver(graph).resolve_datasets_under_path(
            platform="s3", bucket="bucket", path="bucket/folder"
        )
        assert result.transient_error is None
        assert sorted(result.matched_urns) == [
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/folder,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/folder/a,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/folder/b,PROD)",
        ]

    def test_bucket_fetched_once_for_multiple_paths(self) -> None:
        # Two paths under the same bucket must share a single bulk fetch.
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = iter(
            [
                "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/x/t,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/y/t,PROD)",
            ]
        )
        resolver = _resolver(graph)
        r1 = resolver.resolve_datasets_under_path(
            platform="s3", bucket="bucket", path="bucket/x"
        )
        r2 = resolver.resolve_datasets_under_path(
            platform="s3", bucket="bucket", path="bucket/y"
        )
        assert graph.get_urns_by_filter.call_count == 1
        assert r1.matched_urns == (
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/x/t,PROD)",
        )
        assert r2.matched_urns == (
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/y/t,PROD)",
        )

    def test_different_buckets_fetched_separately(self) -> None:
        graph = MagicMock()
        graph.get_urns_by_filter.side_effect = [
            iter(["urn:li:dataset:(urn:li:dataPlatform:s3,b1/p/t,PROD)"]),
            iter(["urn:li:dataset:(urn:li:dataPlatform:s3,b2/p/t,PROD)"]),
        ]
        resolver = _resolver(graph)
        resolver.resolve_datasets_under_path(platform="s3", bucket="b1", path="b1/p")
        resolver.resolve_datasets_under_path(platform="s3", bucket="b2", path="b2/p")
        assert graph.get_urns_by_filter.call_count == 2

    def test_bulk_fetch_is_scoped_to_bucket_not_path(self) -> None:
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = iter([])
        _resolver(graph).resolve_datasets_under_path(
            platform="s3",
            bucket="bucket",
            path="bucket/deep/folder",
            platform_instance="prod",
        )
        kwargs = graph.get_urns_by_filter.call_args.kwargs
        assert kwargs["platform"] == "s3"
        assert kwargs["platform_instance"] == "prod"
        assert kwargs["extraFilters"][0]["values"] == [
            "urn:li:dataset:(urn:li:dataPlatform:s3,prod.bucket"
        ]

    def test_sibling_bucket_excluded_from_index(self) -> None:
        # A case-insensitive prefix wildcard can over-match `bucket-other`; the
        # bucket-boundary check must drop it before caching.
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = iter(
            [
                "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/p/t,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:s3,bucket-other/p/t,PROD)",
            ]
        )
        result = _resolver(graph).resolve_datasets_under_path(
            platform="s3", bucket="bucket", path="bucket/p"
        )
        assert result.matched_urns == (
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/p/t,PROD)",
        )

    def test_transient_error_returns_error_and_is_not_cached(self) -> None:
        graph = MagicMock()
        graph.get_urns_by_filter.side_effect = [
            RuntimeError("blip"),
            iter(["urn:li:dataset:(urn:li:dataPlatform:s3,bucket/p/t,PROD)"]),
        ]
        resolver = _resolver(graph)
        first = resolver.resolve_datasets_under_path(
            platform="s3", bucket="bucket", path="bucket/p"
        )
        assert first.transient_error is not None
        assert first.matched_urns == ()

        # The failed lookup must not be cached, so a retry hits the graph again.
        second = resolver.resolve_datasets_under_path(
            platform="s3", bucket="bucket", path="bucket/p"
        )
        assert second.transient_error is None
        assert second.matched_urns == (
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/p/t,PROD)",
        )
        assert graph.get_urns_by_filter.call_count == 2


_S3_BUCKET_FOLDER_PREFIX = "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/folder"
_GCS_BUCKET_FOLDER_PREFIX = "urn:li:dataset:(urn:li:dataPlatform:gcs,bucket/folder"


@pytest.mark.parametrize(
    "dataset_urn, urn_prefix, expected",
    [
        # Child folder under the path → match.
        (
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/folder/t,PROD)",
            _S3_BUCKET_FOLDER_PREFIX,
            True,
        ),
        # Exact-path dataset (next char is `,`) → match.
        (
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/folder,PROD)",
            _S3_BUCKET_FOLDER_PREFIX,
            True,
        ),
        # `bucket/folder_other` shares characters but is a sibling → reject.
        (
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/folder_other,PROD)",
            _S3_BUCKET_FOLDER_PREFIX,
            False,
        ),
        # GCS dataset under an s3 prefix → reject (different platform).
        (
            "urn:li:dataset:(urn:li:dataPlatform:gcs,bucket/folder/t,PROD)",
            _S3_BUCKET_FOLDER_PREFIX,
            False,
        ),
        # GCS dataset under a gcs prefix → match (covers all platforms).
        (
            "urn:li:dataset:(urn:li:dataPlatform:gcs,bucket/folder/t,PROD)",
            _GCS_BUCKET_FOLDER_PREFIX,
            True,
        ),
        # Malformed URN missing the dataPlatform segment → reject.
        ("urn:li:something-else:foo", _S3_BUCKET_FOLDER_PREFIX, False),
    ],
)
def test_dataset_path_is_rooted_at(
    dataset_urn: str, urn_prefix: str, expected: bool
) -> None:
    assert dataset_path_is_rooted_at(dataset_urn, urn_prefix) is expected
