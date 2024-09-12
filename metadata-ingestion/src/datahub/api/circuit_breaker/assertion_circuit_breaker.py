import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from pydantic import Field

from datahub.api.circuit_breaker.circuit_breaker import (
    AbstractCircuitBreaker,
    CircuitBreakerConfig,
)
from datahub.api.graphql import Assertion, Operation

logger: logging.Logger = logging.getLogger(__name__)


class AssertionCircuitBreakerConfig(CircuitBreakerConfig):
    verify_after_last_update: bool = Field(
        default=True,
        description="Whether to check if assertion happened after the dataset was last updated.",
    )
    time_delta: timedelta = Field(
        default=(timedelta(days=1)),
        description="In what timeframe should accept an assertion result if updated field is not available for the dataset",
    )


class AssertionCircuitBreaker(AbstractCircuitBreaker):
    r"""
    DataHub Assertion Circuit Breaker

    The circuit breaker checks if there are passing assertion on the Dataset.
    """

    config: AssertionCircuitBreakerConfig

    def __init__(self, config: AssertionCircuitBreakerConfig):
        super().__init__(config.datahub_host, config.datahub_token, config.timeout)
        self.config = config
        self.assertion_api = Assertion(
            datahub_host=config.datahub_host,
            datahub_token=config.datahub_token,
            timeout=config.timeout,
        )

    def get_last_updated(self, urn: str) -> Optional[datetime]:
        operation_api: Operation = Operation(transport=self.assertion_api.transport)
        operations = operation_api.query_operations(urn=urn)
        if not operations:
            return None
        else:
            return datetime.fromtimestamp(operations[0]["lastUpdatedTimestamp"] / 1000)

    def _check_if_assertion_failed(
        self, assertions: List[Dict[str, Any]], last_updated: Optional[datetime] = None
    ) -> bool:
        @dataclass
        class AssertionResult:
            time: int
            state: str
            run_event: Any

        # If last_updated is set we expect to have at least one successfull assertion
        if not assertions and last_updated:
            return True

        result: bool = True
        assertion_last_states: Dict[str, AssertionResult] = {}
        for assertion in assertions:
            if "runEvents" in assertion and "runEvents" in assertion["runEvents"]:
                for run_event in assertion["runEvents"]["runEvents"]:
                    assertion_time = run_event["timestampMillis"]
                    assertion_state = run_event["result"]["type"]
                    assertion_urn = run_event["assertionUrn"]
                    if (
                        assertion_urn not in assertion_last_states
                        or assertion_last_states[assertion_urn].time < assertion_time
                    ):
                        assertion_last_states[assertion_urn] = AssertionResult(
                            time=assertion_time,
                            state=assertion_state,
                            run_event=run_event,
                        )

        for assertion_urn, last_assertion in assertion_last_states.items():
            if last_assertion.state == "FAILURE":
                logger.debug(f"Runevent: {last_assertion.run_event}")
                logger.info(
                    f"Assertion {assertion_urn} is failed on dataset. Breaking the circuit"
                )
                return True
            elif last_assertion.state == "SUCCESS":
                logger.info(f"Found successful assertion: {assertion_urn}")
                result = False
            if last_updated is not None:
                last_run = datetime.fromtimestamp(last_assertion.time / 1000)
                if last_updated > last_run:
                    logger.error(
                        f"Missing assertion run for {assertion_urn}. The dataset was updated on {last_updated} but the last assertion run was at {last_run}"
                    )
                    return True
        return result

    def is_circuit_breaker_active(self, urn: str) -> bool:
        r"""
        Checks if the circuit breaker is active

        :param urn: The DataHub dataset unique identifier.
        """

        last_updated: Optional[datetime] = None

        if self.config.verify_after_last_update:
            last_updated = self.get_last_updated(urn)
            logger.info(
                f"The dataset {urn} was last updated at {last_updated}, using this as min assertion date."
            )

        if not last_updated:
            last_updated = datetime.now() - self.config.time_delta
            logger.info(
                f"Dataset {urn} doesn't have last updated or check_last_assertion_time is false, using calculated min assertion date {last_updated}"
            )

        assertions = self.assertion_api.query_assertion(
            urn,
            start_time_millis=int(last_updated.timestamp() * 1000),
            status="COMPLETE",
        )

        if self._check_if_assertion_failed(
            assertions,
            last_updated if self.config.verify_after_last_update is True else None,
        ):
            logger.info(f"Dataset {urn} has failed or missing assertion(s).")
            return True

        return False
