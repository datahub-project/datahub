from typing import List, Union

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from avrogen.dict_wrapper import DictWrapper
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent

from datahub_airflow_plugin.hooks.datahub import (
    DatahubGenericHook,
    DatahubKafkaHook,
    DatahubRestHook,
)


class DatahubBaseOperator(BaseOperator):
    """
    The DatahubBaseOperator is used as a base operator all DataHub operators.
    """

    ui_color = "#4398c8"

    hook: Union[DatahubRestHook, DatahubKafkaHook]

    # mypy is not a fan of this. Newer versions of Airflow support proper typing for the decorator
    # using PEP 612. However, there is not yet a good way to inherit the types of the kwargs from
    # the superclass.
    @apply_defaults  # type: ignore[misc]
    def __init__(  # type: ignore[no-untyped-def]
        self,
        *,
        datahub_conn_id: str,
        **kwargs,
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

    # See above for why these mypy type issues are ignored here.
    @apply_defaults  # type: ignore[misc]
    def __init__(  # type: ignore[no-untyped-def]
        self,
        mces: List[Union[MetadataChangeEvent, MetadataChangeProposalWrapper]],
        datahub_conn_id: str,
        **kwargs,
    ):
        super().__init__(
            datahub_conn_id=datahub_conn_id,
            **kwargs,
        )
        self.metadata = mces

    def _render_template_fields(self, field_value, context, jinja_env):
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
            return self.render_template(field_value, context, jinja_env)
        else:
            return self.render_template(field_value, context, jinja_env)
        return field_value

    def execute(self, context):
        if context:
            jinja_env = self.get_template_env()
            for item in self.metadata:
                if isinstance(item, MetadataChangeProposalWrapper):
                    for key in item.__dict__.keys():
                        value = getattr(item, key)
                        self._render_template_fields(value, context, jinja_env)
                if isinstance(item, MetadataChangeEvent):
                    self._render_template_fields(item, context, jinja_env)

        self.generic_hook.get_underlying_hook().emit(self.metadata)
