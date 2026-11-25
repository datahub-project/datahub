import logging
import warnings
from typing import TYPE_CHECKING, Any, Optional, Sequence, Union

from airflow.sdk import BaseOperator
from avrogen.dict_wrapper import DictWrapper

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.errors import DataHubDeprecationWarning
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub_airflow_plugin.hooks.datahub import (
    DatahubGenericHook,
    DatahubKafkaHook,
    DatahubRestHook,
)

if TYPE_CHECKING:
    from airflow.sdk import Context
    from jinja2 import Environment

logger = logging.getLogger(__name__)

_MCES_DEPRECATION_MESSAGE = "DatahubEmitterOperator(mces=...) is deprecated; use mcps=."


class DatahubBaseOperator(BaseOperator):
    """
    The DatahubBaseOperator is used as a base operator all DataHub operators.
    """

    ui_color = "#4398c8"

    hook: Union[DatahubRestHook, DatahubKafkaHook]

    def __init__(
        self,
        *,
        datahub_conn_id: str,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)

        self.datahub_conn_id = datahub_conn_id
        self.generic_hook = DatahubGenericHook(datahub_conn_id)


class DatahubEmitterOperator(DatahubBaseOperator):
    """
    Emits metadata to DataHub using either a DataHub Rest or Kafka connection.

    :param mcps: MetadataChangeProposalWrapper objects to emit.
    :param mces: Deprecated alias of ``mcps``. A MetadataChangeEvent passed here
        is sent as one MCP per aspect.
    :param datahub_conn_id: Reference to the DataHub Rest or Kafka Connection.
    :type datahub_conn_id: str
    """

    template_fields = ["metadata"]

    def __init__(
        self,
        *,
        datahub_conn_id: str,
        mcps: Optional[Sequence[MetadataChangeProposalWrapper]] = None,
        mces: Optional[
            Sequence[Union[MetadataChangeEvent, MetadataChangeProposalWrapper]]
        ] = None,
        **kwargs: Any,
    ):
        super().__init__(
            datahub_conn_id=datahub_conn_id,
            **kwargs,
        )
        # Logged from execute() rather than here: __init__ also runs on every DAG
        # parse, and the DeprecationWarning is hidden by Python's default filter.
        self._mces_deprecated = mces is not None
        if mces is not None:
            if mcps is not None:
                raise ValueError("Pass either mcps= or mces=, not both")
            warnings.warn(
                _MCES_DEPRECATION_MESSAGE, DataHubDeprecationWarning, stacklevel=2
            )
        metadata: Optional[
            Sequence[Union[MetadataChangeEvent, MetadataChangeProposalWrapper]]
        ] = mces if mces is not None else mcps
        if metadata is None:
            raise ValueError("mcps= is required")
        self.metadata = metadata

    def _render_template_fields(
        self, field_value: Any, context: "Context", jinja_env: "Environment"
    ) -> Any:
        if isinstance(field_value, DictWrapper):
            for key, value in field_value.items():
                setattr(
                    field_value,
                    key,
                    self._render_template_fields(value, context, jinja_env),
                )
        elif isinstance(field_value, list):
            for item in field_value:
                self._render_template_fields(item, context, jinja_env)
        elif isinstance(field_value, str):
            return super().render_template(field_value, context, jinja_env)
        else:
            return super().render_template(field_value, context, jinja_env)
        return field_value

    def execute(self, context: "Context") -> None:
        if self._mces_deprecated:
            logger.warning(_MCES_DEPRECATION_MESSAGE)
        if context:
            jinja_env = self.get_template_env()

            """
            The `_render_template_fields` method is called in the `execute` method to ensure that all template fields
            are rendered with the current execution context, which includes runtime variables and other dynamic data,
            is only available during the execution of the task.

            The `render_template` method is not overridden because the `_render_template_fields` method is used to
            handle the rendering of template fields recursively.
            This approach allows for more granular control over how each field is rendered,
            especially when dealing with complex data structures like `DictWrapper` and lists.

            By not overriding `render_template`, the code leverages the existing functionality
            provided by the base class while adding custom logic for specific cases.
            """
            for item in self.metadata:
                if isinstance(item, MetadataChangeProposalWrapper):
                    for key in item.__dict__.keys():
                        value = getattr(item, key)
                        setattr(
                            item,
                            key,
                            self._render_template_fields(value, context, jinja_env),
                        )
                if isinstance(item, MetadataChangeEvent):
                    self._render_template_fields(item, context, jinja_env)

        self.generic_hook.get_underlying_hook().emit(self.metadata)
