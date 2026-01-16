import logging
from typing import Callable, Dict, Iterable, List, Optional

from datahub.emitter import mce_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.hightouch.config import (
    HightouchSourceConfig,
    HightouchSourceReport,
)
from datahub.ingestion.source.hightouch.constants import (
    ASSERTION_TYPE_HIGHTOUCH_CONTRACT,
    HIGHTOUCH_PLATFORM,
)
from datahub.ingestion.source.hightouch.hightouch_api import HightouchAPIClient
from datahub.ingestion.source.hightouch.models import (
    HightouchContract,
    HightouchContractRun,
    HightouchModel,
    HightouchSourceConnection,
)
from datahub.ingestion.source.hightouch.urn_builder import HightouchUrnBuilder
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
    AssertionStdOperatorClass,
    AssertionTypeClass,
    DatasetAssertionInfoClass,
    DatasetAssertionScopeClass,
)

logger = logging.getLogger(__name__)


class HightouchAssertionsHandler:
    def __init__(
        self,
        config: HightouchSourceConfig,
        report: HightouchSourceReport,
        api_client: HightouchAPIClient,
        urn_builder: HightouchUrnBuilder,
        get_model: Callable[[str], Optional[HightouchModel]],
        get_source: Callable[[str], Optional[HightouchSourceConnection]],
    ) -> None:
        self.config = config
        self.report = report
        self.api_client = api_client
        self.urn_builder = urn_builder
        self.get_model = get_model
        self.get_source = get_source

    def get_assertion_workunits(
        self, contracts: List[HightouchContract]
    ) -> Iterable[MetadataWorkUnit]:
        for contract in contracts:
            if not self.config.contract_patterns.allowed(contract.name):
                continue

            try:
                yield from self._get_contract_workunits(contract)
            except Exception as e:
                self.report.warning(
                    title="Failed to process contract",
                    message=f"An error occurred while processing contract: {str(e)}",
                    context=f"contract: {contract.name} (contract_id: {contract.id})",
                    exc=e,
                )

    def _get_contract_workunits(
        self, contract: HightouchContract
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_contracts_scanned()

        yield from self._generate_assertion_from_contract(contract)

        if self.config.max_contract_runs_per_contract > 0:
            self.report.report_api_call()
            contract_runs = self.api_client.get_contract_runs(
                contract_id=contract.id,
                limit=self.config.max_contract_runs_per_contract,
            )
            yield from self._generate_assertion_results_from_contract_runs(
                contract=contract, runs=contract_runs
            )

    def _make_assertion_urn(self, contract: HightouchContract) -> str:
        # Deterministic GUID using platform, name, instance, env, and model_id
        guid_dict = {
            "platform": HIGHTOUCH_PLATFORM,
            "name": f"contract_{contract.id}_{contract.name}",
            "instance": self.config.platform_instance,
        }

        if self.config.env != mce_builder.DEFAULT_ENV:
            guid_dict["env"] = self.config.env

        if contract.model_id:
            guid_dict["model_id"] = contract.model_id

        guid_dict = {k: v for k, v in guid_dict.items() if v is not None}

        return mce_builder.make_assertion_urn(mce_builder.datahub_guid(guid_dict))

    def _get_assertion_dataset_urn(self, contract: HightouchContract) -> Optional[str]:
        if not contract.model_id:
            return None

        model = self.get_model(contract.model_id)
        if not model:
            logger.debug(
                f"Model {contract.model_id} not found for contract {contract.id}"
            )
            return None

        source = self.get_source(model.source_id)
        if not source:
            logger.debug(f"Source {model.source_id} not found for model {model.id}")
            return None

        urn = self.urn_builder.make_model_urn(model, source)
        return str(urn) if urn else None

    def _generate_assertion_from_contract(
        self, contract: HightouchContract
    ) -> Iterable[MetadataWorkUnit]:
        assertion_urn = self._make_assertion_urn(contract)
        dataset_urn = self._get_assertion_dataset_urn(contract)

        if not dataset_urn:
            logger.warning(
                f"Could not determine dataset for contract {contract.id} ({contract.name}), skipping assertion"
            )
            return

        assertion_info = AssertionInfoClass(
            type=AssertionTypeClass.DATA_SCHEMA,
            datasetAssertion=DatasetAssertionInfoClass(
                dataset=dataset_urn,
                scope=DatasetAssertionScopeClass.DATASET_ROWS,
                operator=AssertionStdOperatorClass._NATIVE_,
                nativeType=ASSERTION_TYPE_HIGHTOUCH_CONTRACT,
            ),
            description=contract.description
            or f"Hightouch Event Contract: {contract.name}",
            externalUrl=None,
            customProperties={
                "platform": HIGHTOUCH_PLATFORM,
                "contract_id": contract.id,
                "contract_name": contract.name,
                "enabled": str(contract.enabled),
                "severity": contract.severity or "unknown",
                "workspace_id": contract.workspace_id,
            },
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=assertion_info,
        ).as_workunit()

        self.report.report_contracts_emitted()

    def _generate_assertion_results_from_contract_runs(
        self, contract: HightouchContract, runs: List[HightouchContractRun]
    ) -> Iterable[MetadataWorkUnit]:
        assertion_urn = self._make_assertion_urn(contract)

        for run in runs:
            if run.status == "passed":
                result_type = AssertionResultTypeClass.SUCCESS
            elif run.status == "failed":
                result_type = AssertionResultTypeClass.FAILURE
            else:
                result_type = AssertionResultTypeClass.ERROR

            native_results: Dict[str, str] = {}

            if run.total_rows_checked is not None:
                native_results["total_rows_checked"] = str(run.total_rows_checked)
            if run.rows_passed is not None:
                native_results["rows_passed"] = str(run.rows_passed)
            if run.rows_failed is not None:
                native_results["rows_failed"] = str(run.rows_failed)

            if run.error:
                if isinstance(run.error, str):
                    native_results["error"] = run.error
                else:
                    native_results["error"] = str(run.error.get("message", run.error))

            assertion_result = AssertionRunEventClass(
                timestampMillis=int(run.created_at.timestamp() * 1000),
                assertionUrn=assertion_urn,
                asserteeUrn=self._get_assertion_dataset_urn(contract) or "",
                runId=run.id,
                status=AssertionRunStatusClass.COMPLETE,
                result=AssertionResultClass(
                    type=result_type,
                    nativeResults=native_results,
                ),
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=assertion_urn,
                aspect=assertion_result,
            ).as_workunit()

            self.report.report_contract_runs_scanned()
