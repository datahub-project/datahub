from typing import TYPE_CHECKING, Any, List, Union

from airflow.models import BaseOperator
from avrogen.dict_wrapper import DictWrapper

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub_airflow_plugin.hooks.datahub import (
    DatahubGenericHook,
    DatahubKafkaHook,
    DatahubRestHook,
)

if TYPE_CHECKING:
    from jinja2 import Environment

    # Import Context with version compatibility for type checking
    # Import to different names to avoid redefinition errors, then assign to Context
    Context: Any
    try:
        from airflow.utils.context import Context as _AirflowContext

        Context = _AirflowContext
    except ImportError:
        from airflow.sdk.definitions.context import Context as _Airflow3Context

        Context = _Airflow3Context  # type: ignore[no-redef]


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
    Emits a Metadata Change Event to DataHub using either a DataHub
    Rest or Kafka connection.

    :param datahub_conn_id: Reference to the DataHub Rest or Kafka Connection.
    :type datahub_conn_id: str
    """

    template_fields = ["metadata"]

    def __init__(
        self,
        mces: List[Union[MetadataChangeEvent, MetadataChangeProposalWrapper]],
        datahub_conn_id: str,
        **kwargs: Any,
    ):
        super().__init__(
            datahub_conn_id=datahub_conn_id,
            **kwargs,
        )
        self.metadata = mces

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
