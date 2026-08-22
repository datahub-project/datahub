from datahub.ingestion.source.aws.s3_util import make_s3_urn, make_s3_urn_for_lineage


class TestMakeS3UrnForLineage:
    """
    make_s3_urn_for_lineage names an S3 dataset that the S3 source itself owns -- Glue,
    Unity Catalog, and Snowflake all call it to build lineage upstreams. If its output
    ever drifts from what the S3 source emits for the same object, the lineage edge
    silently dangles instead of joining. No error, no warning, no report counter.
    """

    def test_urn_unchanged_without_platform_instance(self) -> None:
        assert (
            make_s3_urn_for_lineage("s3://my-bucket/data", "PROD")
            == "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/data,PROD)"
        )

    def test_urn_carries_platform_instance_prefix(self) -> None:
        assert (
            make_s3_urn_for_lineage(
                "s3://my-bucket/data", "PROD", platform_instance="product"
            )
            == "urn:li:dataset:(urn:li:dataPlatform:s3,product.my-bucket/data,PROD)"
        )

    def test_urn_escapes_reserved_characters(self) -> None:
        # `,` and `(` `)` are legal in S3 object keys but structural in a URN. The S3
        # source escapes them; if this side stops, the two URNs stop joining. The comma
        # case also produced a malformed URN before this helper used the shared builder.
        assert (
            make_s3_urn_for_lineage("s3://my-bucket/report (1).csv", "PROD")
            == "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/report %281%29.csv,PROD)"
        )
        assert (
            make_s3_urn_for_lineage("s3://my-bucket/a,b", "PROD")
            == "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/a%2Cb,PROD)"
        )

    def test_strips_leading_and_trailing_slashes(self) -> None:
        # Stage/folder-style S3 paths routinely end (and sometimes start) with a slash;
        # if either side of the join strips differently, the URNs diverge silently.
        assert make_s3_urn_for_lineage(
            "s3://my-bucket/data/", "PROD"
        ) == make_s3_urn_for_lineage("s3://my-bucket/data", "PROD")


class TestMakeS3Urn:
    """
    make_s3_urn is the table-naming helper for Glue, Athena, and SageMaker. It is
    deliberately NOT routed through make_dataset_urn_with_platform_instance (unlike
    make_s3_urn_for_lineage) to avoid changing those connectors' existing URNs or
    extension-mangling behavior. These tests pin that it stays untouched.
    """

    def test_urn_uses_raw_fstring_construction(self) -> None:
        # No escaping here -- this is intentionally NOT the lineage helper's behavior.
        assert (
            make_s3_urn("s3://my-bucket/report (1).csv", "PROD")
            == "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/report (1)_csv,PROD)"
        )

    def test_extension_mangling_default_behavior(self) -> None:
        assert (
            make_s3_urn("s3://my-bucket/data.parquet", "PROD")
            == "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/data_parquet,PROD)"
        )

    def test_extension_preserved_when_remove_extension_false(self) -> None:
        assert (
            make_s3_urn("s3://my-bucket/data.parquet", "PROD", remove_extension=False)
            == "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/data.parquet,PROD)"
        )
