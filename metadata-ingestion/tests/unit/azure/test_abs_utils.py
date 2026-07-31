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
            "urn:li:dataset:(urn:li:dataPlatform:abs,my-container/report %281%29_csv,PROD)"
        )

    def test_extension_mangling_preserved(self) -> None:
        # Pre-existing behavior of this helper (unlike make_s3_urn_for_lineage, which
        # never mangles extensions) -- the platform-instance change did not touch this.
        assert (
            make_abs_urn(
                "https://myacct.blob.core.windows.net/my-container/data.parquet",
                "PROD",
            )
            == "urn:li:dataset:(urn:li:dataPlatform:abs,my-container/data_parquet,PROD)"
        )
