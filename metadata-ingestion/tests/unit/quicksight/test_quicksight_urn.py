from datahub.ingestion.source.quicksight.quicksight_urn import (
    chart_id_from_visual,
    id_from_arn,
    make_dashboard_urn,
    make_dataset_urn,
)


def test_id_from_arn_returns_trailing_resource_id():
    arn = "arn:aws:quicksight:us-east-1:064369473231:dataset/abc-123"
    assert id_from_arn(arn) == "abc-123"


def test_dataset_urn_matches_account_qualified_name():
    urn = make_dataset_urn("064369473231", "abc-123", None, "PROD")
    assert (
        urn
        == "urn:li:dataset:(urn:li:dataPlatform:quicksight,064369473231.abc-123,PROD)"
    )


def test_dataset_urn_includes_platform_instance():
    urn = make_dataset_urn("064369473231", "abc-123", "prod-account", "PROD")
    assert "prod-account.064369473231.abc-123" in urn


def test_dashboard_urn_uses_quicksight_platform():
    urn = make_dashboard_urn("dash-1", None)
    assert urn == "urn:li:dashboard:(quicksight,dash-1)"


def test_chart_id_prefixes_parent_for_bare_visual_ids():
    assert chart_id_from_visual("dash-1", "visual_1") == "dash-1_visual_1"


def test_chart_id_does_not_double_prefix_when_already_embedded():
    # QuickSight auto-generated VisualIds already embed the parent id.
    assert (
        chart_id_from_visual("dash-1", "dash-1_sheet-1_visual_1")
        == "dash-1_sheet-1_visual_1"
    )
