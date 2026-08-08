from datahub.ingestion.source.gcs.gcs_utils import make_gcs_urn


class TestMakeGcsUrn:
    """
    make_gcs_urn names a GCS dataset that the GCS source itself owns -- Snowflake calls it
    to build external storage lineage upstreams. Drift from what the GCS source emits for
    the same object silently dangles the lineage edge instead of joining.
    """

    def test_urn_unchanged_without_platform_instance(self) -> None:
        assert (
            make_gcs_urn("gs://my-bucket/data", "PROD")
            == "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/data,PROD)"
        )

    def test_urn_carries_platform_instance_prefix(self) -> None:
        assert make_gcs_urn(
            "gs://my-bucket/data", "PROD", platform_instance="product"
        ) == ("urn:li:dataset:(urn:li:dataPlatform:gcs,product.my-bucket/data,PROD)")

    def test_urn_escapes_reserved_characters(self) -> None:
        # `(` `)` are legal in object names but structural in a URN.
        assert make_gcs_urn("gs://my-bucket/report (1).csv", "PROD") == (
            "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/report %281%29.csv,PROD)"
        )

    def test_file_extension_is_not_mangled(self) -> None:
        # The GCS source names file-like datasets with the raw path -- `data.parquet`
        # stays `data.parquet`, it is never rewritten to `data_parquet`.
        assert (
            make_gcs_urn("gs://my-bucket/data.parquet", "PROD")
            == "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/data.parquet,PROD)"
        )

    def test_strips_leading_and_trailing_slashes(self) -> None:
        # The GCS source strips both ends; Snowflake stage URLs routinely carry a
        # trailing slash, and stripping only that end left the two sides misaligned.
        assert make_gcs_urn("gs://my-bucket/data/", "PROD") == make_gcs_urn(
            "gs://my-bucket/data", "PROD"
        )
