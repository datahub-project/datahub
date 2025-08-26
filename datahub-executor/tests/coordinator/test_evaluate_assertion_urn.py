from unittest.mock import Mock

import fastapi
import pytest
from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.assertion.engine.engine import AssertionEngine
from datahub_executor.common.types import (
    Assertion,
    AssertionEvaluationResult,
    AssertionResultType,
    AssertionSourceType,
)
from datahub_executor.coordinator.assertion_handlers import (
    handle_post_evaluate_assertion_urn,
)
from datahub_executor.coordinator.types import (
    EvaluateAssertionUrnInputSchema,
    FreshnessAssertionParametersSchema,
)


class TestEvaluateAssertionUrnHandler:
    def setup_method(self) -> None:
        self.assertion = Assertion.parse_obj(
            {
                "urn": "urn:li:testAssertion",
                "type": "ASSERTION",
                "info": {
                    "type": "FRESHNESS",
                    "freshnessAssertion": {
                        "type": "DATASET_CHANGE",
                        "schedule": {
                            "type": "FIXED_INTERVAL",
                            "cron": None,
                            "fixedInterval": {"unit": "MINUTE", "multiple": 5},
                        },
                    },
                    "source": {"type": "NATIVE"},
                },
                "entity": {
                    "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.test_assertions_all_times,PROD)",
                    "table_name": "TEST_ASSERTIONS_ALL_TIMES",
                    "qualified_name": "test_db.public.test_assertions_all_times",
                    "platform_urn": "urn:li:dataPlatform:snowflake",
                    "sub_types": ["Table"],
                    "exists": True,
                },
            }
        )
        self.input_data = EvaluateAssertionUrnInputSchema.parse_obj(
            {
                "assertionUrn": "urn:li:testAssertion",
                "parameters": {
                    "type": "DATASET_FRESHNESS",
                    "datasetFreshnessParameters": FreshnessAssertionParametersSchema.parse_obj(
                        {
                            "sourceType": "FIELD_VALUE",
                            "field": {
                                "path": "col_timestamp",
                                "type": "TIMESTAMP",
                                "nativeType": "TIMESTAMP_NTZ",
                                "kind": "HIGH_WATERMARK",
                            },
                        }
                    ),
                },
            }
        )

        # setup dependencies,
        # some as mocks, for the handler
        self.engine_mock = Mock(spec=AssertionEngine)
        self.graph = Mock(spec=DataHubGraph)

    def test_evaluate_assertion_handler_invalid_source_type(self) -> None:
        self.assertion.source_type = AssertionSourceType.EXTERNAL

        with pytest.raises(fastapi.HTTPException):
            handle_post_evaluate_assertion_urn(
                self.assertion,
                self.input_data,
                self.engine_mock,
                False,
            )

    def test_evaluate_assertion_handler_success(self) -> None:
        self.engine_mock.evaluate.return_value = AssertionEvaluationResult(
            type=AssertionResultType.SUCCESS, parameters=None
        )
        resp = handle_post_evaluate_assertion_urn(
            self.assertion,
            self.input_data,
            self.engine_mock,
            False,
        )
        if resp is not None:
            assert resp.type == "SUCCESS"

    def test_evaluate_assertion_handler_failure(self) -> None:
        self.engine_mock.evaluate.return_value = AssertionEvaluationResult(
            type=AssertionResultType.FAILURE, parameters=None
        )
        resp = handle_post_evaluate_assertion_urn(
            self.assertion,
            self.input_data,
            self.engine_mock,
            False,
        )
        if resp is not None:
            assert resp.type == "FAILURE"
