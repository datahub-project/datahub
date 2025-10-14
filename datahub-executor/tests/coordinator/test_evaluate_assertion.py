from unittest.mock import Mock

from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.assertion.engine.engine import AssertionEngine
from datahub_executor.common.types import AssertionEvaluationResult, AssertionResultType
from datahub_executor.coordinator.assertion_handlers import (
    handle_post_evaluate_assertion,
)
from datahub_executor.coordinator.types import EvaluateAssertionInputSchema


class TestEvaluateAssertionHandler:
    def setup_method(self) -> None:
        self.input_data = EvaluateAssertionInputSchema.model_validate(
            {
                "type": "FRESHNESS",
                "connectionUrn": "urn:li:dataPlatform:snowflake",
                "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.dog_rates_twitter,PROD)",
                "assertion": {
                    "freshnessAssertion": {
                        "type": "DATASET_CHANGE",
                        "schedule": {
                            "type": "FIXED_INTERVAL",
                            "fixedInterval": {"unit": "HOUR", "multiple": 6},
                        },
                    }
                },
                "parameters": {
                    "type": "DATASET_FRESHNESS",
                    "datasetFreshnessParameters": {
                        "sourceType": "FIELD_VALUE",
                        "field": {
                            "path": "col_timestamp",
                            "type": "TIMESTAMP",
                            "nativeType": "TIMESTAMP_NTZ",
                            "kind": None,
                        },
                    },
                },
            }
        )

        # setup dependencies, some as mocks, for the handler
        self.engine_mock = Mock(spec=AssertionEngine)
        self.graph = Mock(spec=DataHubGraph)

    def test_evaluate_assertion_handler_success(self) -> None:
        self.engine_mock.evaluate.return_value = AssertionEvaluationResult(
            type=AssertionResultType.SUCCESS, parameters=None
        )
        resp = handle_post_evaluate_assertion(
            self.input_data,
            self.engine_mock,
            None,
            None,
            False,
        )
        if resp is not None:
            assert resp.type == "SUCCESS"

    def test_evaluate_assertion_handler_failure(self) -> None:
        self.engine_mock.evaluate.return_value = AssertionEvaluationResult(
            type=AssertionResultType.FAILURE, parameters=None
        )
        resp = handle_post_evaluate_assertion(
            self.input_data,
            self.engine_mock,
            None,
            None,
            False,
        )
        if resp is not None:
            assert resp.type == "FAILURE"
