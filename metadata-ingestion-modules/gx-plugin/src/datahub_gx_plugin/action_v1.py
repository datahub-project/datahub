import logging
import sys
from datahub.utilities._markupsafe_compat import MARKUPSAFE_PATCHED

# Load-bearing for its import side effect: it raises the "use action" ImportError
# under GX 0.x before the great_expectations imports below reach 1.x-only modules.
# The assert below keeps the name referenced so ruff won't flag it as unused.
from datahub_gx_plugin._compat_gx_1x import GX_1X_REQUIRED
from typing import Any, Dict, List, Literal, Optional, Union

from great_expectations.checkpoint.actions import (  # type: ignore[attr-defined]
    ActionContext,
    ValidationAction,
)
from great_expectations.checkpoint.checkpoint import (  # type: ignore[attr-defined]
    CheckpointResult,
)
from great_expectations.datasource.fluent.config_str import (  # type: ignore[attr-defined]
    ConfigStr,
)

import datahub.emitter.mce_builder as builder
from datahub.cli.env_utils import get_boolean_env_variable
from datahub.emitter.rest_emitter import DatahubRestEmitter, EmitMode
from datahub.ingestion.graph.config import ClientMode
from datahub.metadata.com.linkedin.pegasus2avro.assertion import BatchSpec
from datahub.metadata.schema_classes import PartitionSpecClass
from datahub_gx_plugin.common import (
    build_assertions_with_results,
    coerce_emit_mode,
    convert_to_string,
    emit_assertion_results,
    make_dataset_urn_from_sqlalchemy_uri,
    warn,
)

assert MARKUPSAFE_PATCHED and GX_1X_REQUIRED
logger = logging.getLogger(__name__)
if get_boolean_env_variable("DATAHUB_DEBUG", False):
    handler = logging.StreamHandler(stream=sys.stdout)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)


class DataHubValidationAction(ValidationAction):
    """GX Core 1.x ValidationAction that emits assertion results to DataHub.

    For GX 0.17/0.18, use ``datahub_gx_plugin.action.DataHubValidationAction`` instead.
    """

    type: Literal["datahub_validation_action"] = "datahub_validation_action"
    name: str = "DataHubValidationAction"
    server_url: str
    env: str = builder.DEFAULT_ENV
    platform_alias: Optional[str] = None
    platform_instance_map: Optional[Dict[str, str]] = None
    graceful_exceptions: bool = True
    # Prefer ``token="${DATAHUB_TOKEN}"`` so GX persists the placeholder, not a
    # cleartext secret, when writing checkpoints to disk.
    token: Optional[Union[ConfigStr, str]] = None
    timeout_sec: Optional[float] = None
    retry_status_codes: Optional[List[int]] = None
    retry_max_times: Optional[int] = None
    extra_headers: Optional[Dict[str, str]] = None
    exclude_dbname: Optional[bool] = None
    convert_urns_to_lowercase: bool = False
    emit_mode: Union[str, EmitMode] = EmitMode.ASYNC
    # Optional explicit identity when batch_spec cannot be inferred from meta.
    platform: Optional[str] = None
    dataset_name: Optional[str] = None
    platform_instance: Optional[str] = None

    def _resolve_token(self) -> Optional[str]:
        """Resolve GX ConfigStr placeholders; pass plain strings through.

        ``_substitute_config_str_if_needed`` always asks the active data context
        for a config provider, even for plain strings / None. Unit tests and
        callers that pass a literal token have no context, so only invoke it
        when the value is a ConfigStr (the form GX persists for ``${VAR}``).
        """
        token = self.token
        if isinstance(token, ConfigStr):
            return self._substitute_config_str_if_needed(token)
        return token

    def run(
        self,
        checkpoint_result: CheckpointResult,
        action_context: Union[ActionContext, None] = None,
    ) -> dict:
        try:
            emit_mode = coerce_emit_mode(self.emit_mode)
            emitter = DatahubRestEmitter(
                gms_server=self.server_url,
                token=self._resolve_token(),
                read_timeout_sec=self.timeout_sec,
                connect_timeout_sec=self.timeout_sec,
                retry_status_codes=self.retry_status_codes,
                retry_max_times=self.retry_max_times,
                extra_headers=self.extra_headers,
                client_mode=ClientMode.INGESTION,
                datahub_component="gx-plugin",
                default_emit_mode=emit_mode,
            )

            docs_link = self._docs_link_from_action_context(action_context)
            emitted_any = False
            checkpoint_name = getattr(checkpoint_result, "name", None) or getattr(
                getattr(checkpoint_result, "checkpoint_config", None), "name", None
            )
            checkpoint_id = getattr(
                getattr(checkpoint_result, "checkpoint_config", None), "id", None
            )

            for _, validation_result in checkpoint_result.run_results.items():
                datasets = self._datasets_from_validation_result(validation_result)
                if len(datasets) == 0 or datasets[0]["dataset_urn"] is None:
                    warn(
                        "Metadata not sent to datahub for a validation result. "
                        "No datasets found. Provide platform/dataset_name on the "
                        "action or ensure batch_spec is present in validation meta."
                    )
                    continue

                expectation_suite_name = getattr(validation_result, "suite_name", None)
                if expectation_suite_name is None and validation_result.meta:
                    expectation_suite_name = validation_result.meta.get(
                        "expectation_suite_name"
                    ) or validation_result.meta.get("suite_name")

                run_id = None
                if validation_result.meta:
                    meta_run_id = validation_result.meta.get("run_id")
                    # Prefer a real RunIdentifier from the checkpoint; GX 1.x may
                    # serialize run_id into meta as a JSON string.
                    if meta_run_id is not None and hasattr(meta_run_id, "run_time"):
                        run_id = meta_run_id
                if run_id is None:
                    run_id = checkpoint_result.run_id

                context_properties: Dict[str, str] = {}
                if checkpoint_name:
                    context_properties["checkpoint_name"] = str(checkpoint_name)
                if checkpoint_id:
                    context_properties["checkpoint_id"] = str(checkpoint_id)
                meta = validation_result.meta or {}
                if meta.get("validation_id"):
                    context_properties["validation_id"] = str(meta["validation_id"])
                validation_definition_name = self._validation_definition_name(
                    checkpoint_result, meta.get("validation_id")
                )
                if validation_definition_name:
                    context_properties["validation_definition_name"] = (
                        validation_definition_name
                    )

                assertions = build_assertions_with_results(
                    validation_result,
                    expectation_suite_name,
                    run_id,
                    datasets,
                    docs_link=docs_link,
                    context_properties=context_properties or None,
                )
                logger.info("Sending metadata to datahub ...")
                logger.info(
                    "Dataset URN - {urn}".format(urn=datasets[0]["dataset_urn"])
                )
                emit_assertion_results(emitter, assertions)
                emitted_any = True

            if not emitted_any:
                return {"datahub_notification_result": "none required"}

            logger.info("Metadata sent to datahub.")
            result = "DataHub notification succeeded"
        except Exception as e:
            result = "DataHub notification failed"
            if self.graceful_exceptions:
                logger.error(e)
                logger.info("Suppressing error because graceful_exceptions is set")
            else:
                raise

        return {"datahub_notification_result": result}

    def _validation_definition_name(
        self, checkpoint_result: CheckpointResult, validation_id: Optional[Any]
    ) -> Optional[str]:
        if not validation_id:
            return None
        checkpoint_config = getattr(checkpoint_result, "checkpoint_config", None)
        if checkpoint_config is None:
            return None
        for validation_definition in (
            getattr(checkpoint_config, "validation_definitions", None) or []
        ):
            if str(getattr(validation_definition, "id", None)) == str(validation_id):
                return getattr(validation_definition, "name", None)
        return None

    def _docs_link_from_action_context(
        self, action_context: Union[ActionContext, None]
    ) -> Optional[str]:
        if action_context is None:
            return None
        try:
            pages = self._get_data_docs_pages_from_prior_action(action_context)
            if not pages:
                return None
            for _validation_id, site_pages in pages.items():
                if not isinstance(site_pages, dict):
                    continue
                for _site_name, url in site_pages.items():
                    if isinstance(url, str) and "file://" not in url:
                        return url
        except Exception:
            logger.debug(
                "Unable to resolve Data Docs URL from action context",
                exc_info=True,
            )
        return None

    def _datasets_from_validation_result(
        self, validation_result: Any
    ) -> List[Dict[str, Any]]:
        meta = validation_result.meta or {}
        batch_spec = meta.get("batch_spec") or {}
        if not isinstance(batch_spec, dict):
            batch_spec = dict(batch_spec) if hasattr(batch_spec, "items") else {}
        batch_identifiers = meta.get("batch_identifiers") or {}
        active_batch_definition = meta.get("active_batch_definition") or {}
        if not isinstance(active_batch_definition, dict):
            active_batch_definition = {}

        # Prefer first-class GX 1.x asset_name over weak pandas batch_spec
        # (often just {"batch_data": "PandasDataFrame"}). Explicit action
        # dataset_name still wins in _resolve_dataset_urn when platform is set.
        data_asset_name = (
            self.dataset_name
            or getattr(validation_result, "asset_name", None)
            or batch_spec.get("data_asset_name")
            or active_batch_definition.get("data_asset_name")
        )
        datasource_name = (
            batch_spec.get("datasource_name")
            or active_batch_definition.get("datasource_name")
            or ""
        )
        batch_identifier = convert_to_string(
            batch_identifiers or batch_spec.get("batch_identifiers") or data_asset_name
        )
        batch_spec_properties = {
            "data_asset_name": str(data_asset_name) if data_asset_name else "",
            "datasource_name": str(datasource_name),
        }

        dataset_urn = self._resolve_dataset_urn(
            batch_spec=batch_spec,
            data_asset_name=data_asset_name,
            datasource_name=datasource_name,
        )
        if dataset_urn is None:
            return []

        partition_spec = None
        splitter_method = batch_spec.get("splitter_method")
        if splitter_method is not None and splitter_method != "_split_on_whole_table":
            partition_spec = PartitionSpecClass(
                partition=convert_to_string(
                    batch_spec.get("batch_identifiers", batch_identifiers)
                )
            )

        native_batch = BatchSpec(
            nativeBatchId=str(batch_identifier),
            customProperties=batch_spec_properties,
        )
        sampling_method = batch_spec.get("sampling_method", "")
        if sampling_method == "_sample_using_limit":
            sampling_kwargs = batch_spec.get("sampling_kwargs") or {}
            if "n" in sampling_kwargs:
                native_batch.limit = sampling_kwargs["n"]

        return [
            {
                "dataset_urn": dataset_urn,
                "partitionSpec": partition_spec,
                "batchSpec": native_batch,
            }
        ]

    def _resolve_dataset_urn(
        self,
        batch_spec: Dict[str, Any],
        data_asset_name: Optional[str],
        datasource_name: str,
    ) -> Optional[str]:
        if self.platform and (self.dataset_name or data_asset_name):
            name = self.dataset_name or data_asset_name
            assert name is not None
            if self.convert_urns_to_lowercase:
                name = name.lower()
            return builder.make_dataset_urn_with_platform_instance(
                platform=self.platform_alias or self.platform,
                name=name,
                platform_instance=self.platform_instance
                or self._platform_instance_for_datasource(datasource_name),
                env=self.env,
            )

        schema_name = batch_spec.get("schema_name")
        table_name = batch_spec.get("table_name") or data_asset_name
        sqlalchemy_uri = batch_spec.get("url") or batch_spec.get("connection_string")

        if sqlalchemy_uri and table_name:
            return make_dataset_urn_from_sqlalchemy_uri(
                sqlalchemy_uri,
                schema_name,
                table_name,
                self.env,
                self._platform_instance_for_datasource(datasource_name),
                self.exclude_dbname,
                self.platform_alias,
                self.convert_urns_to_lowercase,
            )

        if data_asset_name and (
            self.platform_alias or self.platform or datasource_name
        ):
            platform = self.platform_alias or self.platform or datasource_name
            name = data_asset_name
            if self.convert_urns_to_lowercase:
                name = name.lower()
            return builder.make_dataset_urn_with_platform_instance(
                platform=platform,
                name=name,
                platform_instance=self.platform_instance
                or self._platform_instance_for_datasource(datasource_name),
                env=self.env,
            )

        return None

    def _platform_instance_for_datasource(self, datasource_name: str) -> Optional[str]:
        if self.platform_instance is not None:
            return self.platform_instance
        if self.platform_instance_map and datasource_name in self.platform_instance_map:
            return self.platform_instance_map[datasource_name]
        if datasource_name:
            warn(
                f"Datasource {datasource_name} is not present in platform_instance_map"
            )
        return None
