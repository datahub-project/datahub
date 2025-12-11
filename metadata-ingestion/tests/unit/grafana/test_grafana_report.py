# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.ingestion.source.grafana.report import GrafanaSourceReport


def test_grafana_report_initialization():
    report = GrafanaSourceReport()
    assert report.dashboards_scanned == 0
    assert report.charts_scanned == 0
    assert report.folders_scanned == 0
    assert report.datasets_scanned == 0


def test_report_dashboard_scanned():
    report = GrafanaSourceReport()
    report.report_dashboard_scanned()
    assert report.dashboards_scanned == 1
    report.report_dashboard_scanned()
    assert report.dashboards_scanned == 2


def test_report_chart_scanned():
    report = GrafanaSourceReport()
    report.report_chart_scanned()
    assert report.charts_scanned == 1
    report.report_chart_scanned()
    assert report.charts_scanned == 2


def test_report_folder_scanned():
    report = GrafanaSourceReport()
    report.report_folder_scanned()
    assert report.folders_scanned == 1
    report.report_folder_scanned()
    assert report.folders_scanned == 2


def test_report_dataset_scanned():
    report = GrafanaSourceReport()
    report.report_dataset_scanned()
    assert report.datasets_scanned == 1
    report.report_dataset_scanned()
    assert report.datasets_scanned == 2


def test_multiple_report_types():
    report = GrafanaSourceReport()

    report.report_dashboard_scanned()
    report.report_chart_scanned()
    report.report_folder_scanned()
    report.report_dataset_scanned()

    assert report.dashboards_scanned == 1
    assert report.charts_scanned == 1
    assert report.folders_scanned == 1
    assert report.datasets_scanned == 1
