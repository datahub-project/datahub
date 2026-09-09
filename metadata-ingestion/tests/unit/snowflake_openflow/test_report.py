from datahub.ingestion.source.snowflake.snowflake_openflow_report import (
    SnowflakeOpenflowReport,
)


def test_empty_inventory_is_reported_as_a_possible_privilege_problem():
    # SHOW OPENFLOW ... is privilege-filtered and returns exit-0 with zero rows
    # when the role holds nothing on the objects. Presenting that as an empty
    # account is the single most misleading thing this connector could do.
    report = SnowflakeOpenflowReport()
    report.report_empty_inventory("runtimes")
    assert len(report.warnings) == 1
    warning_str = str(report.warnings)
    assert "MONITOR" in warning_str
    # Verify the object type is passed via context, not interpolated in the message
    assert "runtimes" in report.warnings[0].context


def test_filtered_objects_are_recorded():
    # Each object type keeps its own list, so a run can say which layer the
    # patterns actually filtered rather than only that something was dropped.
    report = SnowflakeOpenflowReport()
    report.report_dropped_deployment("skipped-deployment")
    report.report_dropped_runtime("skipped-runtime")
    report.report_dropped_connector("skipped-connector")

    assert "skipped-deployment" in report.filtered_deployments
    assert "skipped-runtime" in report.filtered_runtimes
    assert "skipped-connector" in report.filtered_connectors
    assert "skipped-connector" not in report.filtered_runtimes
