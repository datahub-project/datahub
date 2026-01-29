# Don't import this file in the airflow DAG.
# This transformer is used to rewrite the input of the chartInfo aspect.
# It is used to convert the input of the chartInfo aspect from hive to trino.

import re
from typing import Iterable, List, Optional, Tuple, cast
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.base_transformer import BaseTransformer, MultipleAspectTransformer, SingleAspectTransformer
from datahub.metadata.schema_classes import InputFieldClass, InputFieldsClass, ChartQueryClass, SchemaFieldClass
from datahub.emitter.mce_builder import Aspect, make_schema_field_urn

    
class AddInputFieldsConfig(ConfigModel):
    """
    Configuration to parse rawQuery in chart.
    Example: 
        platform: trino
        platform_instance: hive
    """
    platform: str
    platform_instance: Optional[str]

class AddInputFieldsTransformer(BaseTransformer, MultipleAspectTransformer):
    ctx: PipelineContext
    config: AddInputFieldsConfig

    def __init__(self, config: AddInputFieldsConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddInputFieldsTransformer":
        config = AddInputFieldsConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def entity_types(self) -> List[str]:
        return ["chart"]

    def aspect_name(self) -> str:
        return "chartQuery"
    
    def transform_aspects(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Iterable[Tuple[str, Optional[Aspect]]]:
        if not aspect: 
            return []
        assert isinstance(aspect, ChartQueryClass)
        raw_query = aspect.rawQuery
        if not self.ctx.graph:
            return [("chartQuery", aspect)]
        sql_parsing_result = self.ctx.graph.parse_sql_lineage(
            sql=raw_query,
            platform=self.config.platform,
            platform_instance=self.config.platform_instance,
            )
        if not sql_parsing_result.column_lineage:
            return [("chartQuery", aspect)]
        fields = []
        for column_lineage in sql_parsing_result.column_lineage:
            if not column_lineage.downstream:
                return [("chartQuery", aspect)]
            if not column_lineage.downstream.column_type:
                return [("chartQuery", aspect)]
            if not column_lineage.downstream.native_column_type:
                return [("chartQuery", aspect)]
            
            schema_field = SchemaFieldClass(
                fieldPath=column_lineage.downstream.column,
                type=column_lineage.downstream.column_type,
                nativeDataType=column_lineage.downstream.native_column_type
            )
            
            for upstream in column_lineage.upstreams:
                schema_field_urn = make_schema_field_urn(upstream.table, upstream.column)
                fields.append(
                    InputFieldClass(
                        schemaFieldUrn=schema_field_urn,
                        schemaField=schema_field
                        )
                    )
        input_fields = cast(Optional[Aspect], InputFieldsClass(fields=fields))
        return [
            ("chartQuery", aspect),
            ("inputFields", input_fields)
        ]