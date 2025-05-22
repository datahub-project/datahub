from datahub.configuration.common import AllowDenyPattern

from datahub_integrations.propagation.bigquery.config import (
    BigqueryConnectionConfigPermissive,
)
from datahub_integrations.propagation.bigquery.util import is_urn_allowed


class TestBigqueryUtils:
    def test_bigquery_sync_allowed(self) -> None:
        urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.propagation_test.test_table_1,PROD)"
        assert is_urn_allowed(urn)

        urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.propagation_test.test_table_1,PROD),field_1)"
        assert is_urn_allowed(urn)

        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my-project.propagation_test.test_table_1,PROD)"
        assert not is_urn_allowed(urn)

    def test_bigquery_sync_allowed_with_project_ids(self) -> None:
        config = BigqueryConnectionConfigPermissive(project_ids=["my-project"])
        urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.propagation_test.test_table_1,PROD)"
        assert is_urn_allowed(urn, config)

        urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.propagation_test.test_table_1,PROD),field_1)"
        assert is_urn_allowed(urn)

        urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project2.propagation_test.test_table_1,PROD)"
        assert not is_urn_allowed(urn, config)

    def test_bigquery_sync_allowed_with_project_allow_deny(self) -> None:
        config = BigqueryConnectionConfigPermissive(
            project_id_pattern=AllowDenyPattern(allow=["my-project*"])
        )
        urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.propagation_test.test_table_1,PROD)"
        assert is_urn_allowed(urn, config)

        urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project2.propagation_test.test_table_1,PROD)"
        assert is_urn_allowed(urn, config)

        urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-other-project.propagation_test.test_table_1,PROD)"
        assert not is_urn_allowed(urn, config)

    def test_bigquery_sync_allowed_with_dataset_allow_deny(self) -> None:
        config = BigqueryConnectionConfigPermissive(
            dataset_pattern=AllowDenyPattern(allow=["my-project.propagation_test*"])
        )
        urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.propagation_test.test_table_1,PROD)"
        assert is_urn_allowed(urn, config)

        urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.propagation_test1.test_table_1,PROD)"
        assert is_urn_allowed(urn, config)

        urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.propagation_other_test.test_table_1,PROD)"
        assert not is_urn_allowed(urn, config)

    def test_bigquery_sync_project_id_overrides_project_pattern(self) -> None:
        config = BigqueryConnectionConfigPermissive(
            project_ids=["my-project"],
            project_id_pattern=AllowDenyPattern(allow=["my-other-project*"]),
        )
        urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.propagation_test.test_table_1,PROD)"
        assert is_urn_allowed(urn, config)

        urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-other-project.propagation_test1.test_table_1,PROD)"
        assert not is_urn_allowed(urn, config)
