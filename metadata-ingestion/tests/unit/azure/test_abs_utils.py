from datahub.ingestion.source.azure.abs_utils import make_abs_urn


class TestMakeAbsUrn:
    """
    make_abs_urn names an Azure Blob Storage dataset that the ABS source itself owns --
    Snowflake calls it to build external-stage lineage upstreams. Drift from what the ABS
    source emits for the same blob silently dangles the lineage edge instead of joining.
    """

    def test_urn_unchanged_without_platform_instance(self) -> None:
        assert (
            make_abs_urn(
                "https://myacct.blob.core.windows.net/my-container/data", "PROD"
            )
            == "urn:li:dataset:(urn:li:dataPlatform:abs,my-container/data,PROD)"
        )

    def test_urn_carries_platform_instance_prefix(self) -> None:
        assert make_abs_urn(
            "https://myacct.blob.core.windows.net/my-container/data",
            "PROD",
            platform_instance="product",
        ) == ("urn:li:dataset:(urn:li:dataPlatform:abs,product.my-container/data,PROD)")

    def test_urn_escapes_reserved_characters(self) -> None:
        # `(` `)` are legal in blob names but structural in a URN.
        assert make_abs_urn(
            "https://myacct.blob.core.windows.net/my-container/report (1).csv",
            "PROD",
        ) == (
            "urn:li:dataset:(urn:li:dataPlatform:abs,my-container/report %281%29.csv,PROD)"
        )

    def test_file_extension_is_not_mangled(self) -> None:
        # The ABS source (abs/source.py) names file-like datasets with the raw
        # `.strip("/")` path -- `data.parquet` stays `data.parquet`, it is never
        # rewritten to `data_parquet`. If this helper mangled it instead, every
        # file-like ABS stage lineage upstream would silently fail to join.
        assert (
            make_abs_urn(
                "https://myacct.blob.core.windows.net/my-container/data.parquet",
                "PROD",
            )
            == "urn:li:dataset:(urn:li:dataPlatform:abs,my-container/data.parquet,PROD)"
        )

    def test_strips_leading_and_trailing_slashes(self) -> None:
        assert make_abs_urn(
            "https://myacct.blob.core.windows.net/my-container/data/", "PROD"
        ) == make_abs_urn(
            "https://myacct.blob.core.windows.net/my-container/data", "PROD"
        )
