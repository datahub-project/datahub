from .schema_classes import SCHEMA as get_schema_type
from .schema_classes import _json_converter as json_converter
from .schema_classes import KafkaAuditHeaderClass
from .schema_classes import ChartInfoClass
from .schema_classes import ChartQueryClass
from .schema_classes import ChartQueryTypeClass
from .schema_classes import ChartTypeClass
from .schema_classes import AccessLevelClass
from .schema_classes import AuditStampClass
from .schema_classes import ChangeAuditStampsClass
from .schema_classes import CostClass
from .schema_classes import CostCostClass
from .schema_classes import CostCostDiscriminatorClass
from .schema_classes import CostTypeClass
from .schema_classes import DeprecationClass
from .schema_classes import InstitutionalMemoryClass
from .schema_classes import InstitutionalMemoryMetadataClass
from .schema_classes import MLFeatureDataTypeClass
from .schema_classes import OwnerClass
from .schema_classes import OwnershipClass
from .schema_classes import OwnershipSourceClass
from .schema_classes import OwnershipSourceTypeClass
from .schema_classes import OwnershipTypeClass
from .schema_classes import StatusClass
from .schema_classes import VersionTagClass
from .schema_classes import TransformationTypeClass
from .schema_classes import UDFTransformerClass
from .schema_classes import DashboardInfoClass
from .schema_classes import DataProcessInfoClass
from .schema_classes import DatasetDeprecationClass
from .schema_classes import DatasetFieldMappingClass
from .schema_classes import DatasetLineageTypeClass
from .schema_classes import DatasetPropertiesClass
from .schema_classes import DatasetUpstreamLineageClass
from .schema_classes import UpstreamClass
from .schema_classes import UpstreamLineageClass
from .schema_classes import CorpGroupInfoClass
from .schema_classes import CorpUserEditableInfoClass
from .schema_classes import CorpUserInfoClass
from .schema_classes import ChartSnapshotClass
from .schema_classes import CorpGroupSnapshotClass
from .schema_classes import CorpUserSnapshotClass
from .schema_classes import DashboardSnapshotClass
from .schema_classes import DataProcessSnapshotClass
from .schema_classes import DatasetSnapshotClass
from .schema_classes import MLFeatureSnapshotClass
from .schema_classes import MLModelSnapshotClass
from .schema_classes import BaseDataClass
from .schema_classes import CaveatDetailsClass
from .schema_classes import CaveatsAndRecommendationsClass
from .schema_classes import EthicalConsiderationsClass
from .schema_classes import EvaluationDataClass
from .schema_classes import IntendedUseClass
from .schema_classes import IntendedUserTypeClass
from .schema_classes import MLFeaturePropertiesClass
from .schema_classes import MLModelFactorPromptsClass
from .schema_classes import MLModelFactorsClass
from .schema_classes import MLModelPropertiesClass
from .schema_classes import MetricsClass
from .schema_classes import QuantitativeAnalysesClass
from .schema_classes import SourceCodeClass
from .schema_classes import SourceCodeUrlClass
from .schema_classes import SourceCodeUrlTypeClass
from .schema_classes import TrainingDataClass
from .schema_classes import MetadataChangeEventClass
from .schema_classes import ArrayTypeClass
from .schema_classes import BinaryJsonSchemaClass
from .schema_classes import BooleanTypeClass
from .schema_classes import BytesTypeClass
from .schema_classes import DatasetFieldForeignKeyClass
from .schema_classes import EnumTypeClass
from .schema_classes import EspressoSchemaClass
from .schema_classes import FixedTypeClass
from .schema_classes import ForeignKeySpecClass
from .schema_classes import KafkaSchemaClass
from .schema_classes import KeyValueSchemaClass
from .schema_classes import MapTypeClass
from .schema_classes import MySqlDDLClass
from .schema_classes import NullTypeClass
from .schema_classes import NumberTypeClass
from .schema_classes import OracleDDLClass
from .schema_classes import OrcSchemaClass
from .schema_classes import OtherSchemaClass
from .schema_classes import PrestoDDLClass
from .schema_classes import RecordTypeClass
from .schema_classes import SchemaFieldClass
from .schema_classes import SchemaFieldDataTypeClass
from .schema_classes import SchemaMetadataClass
from .schema_classes import SchemalessClass
from .schema_classes import StringTypeClass
from .schema_classes import UnionTypeClass
from .schema_classes import UrnForeignKeyClass
from avro.io import DatumReader


class SpecificDatumReader(DatumReader):
    SCHEMA_TYPES = {
        "KafkaAuditHeader": KafkaAuditHeaderClass,
        ".KafkaAuditHeader": KafkaAuditHeaderClass,
        "com.linkedin.events.KafkaAuditHeader": KafkaAuditHeaderClass,
        "ChartInfo": ChartInfoClass,
        ".ChartInfo": ChartInfoClass,
        "com.linkedin.pegasus2avro.chart.ChartInfo": ChartInfoClass,
        "ChartQuery": ChartQueryClass,
        ".ChartQuery": ChartQueryClass,
        "com.linkedin.pegasus2avro.chart.ChartQuery": ChartQueryClass,
        "ChartQueryType": ChartQueryTypeClass,
        ".ChartQueryType": ChartQueryTypeClass,
        "com.linkedin.pegasus2avro.chart.ChartQueryType": ChartQueryTypeClass,
        "ChartType": ChartTypeClass,
        ".ChartType": ChartTypeClass,
        "com.linkedin.pegasus2avro.chart.ChartType": ChartTypeClass,
        "AccessLevel": AccessLevelClass,
        ".AccessLevel": AccessLevelClass,
        "com.linkedin.pegasus2avro.common.AccessLevel": AccessLevelClass,
        "AuditStamp": AuditStampClass,
        ".AuditStamp": AuditStampClass,
        "com.linkedin.pegasus2avro.common.AuditStamp": AuditStampClass,
        "ChangeAuditStamps": ChangeAuditStampsClass,
        ".ChangeAuditStamps": ChangeAuditStampsClass,
        "com.linkedin.pegasus2avro.common.ChangeAuditStamps": ChangeAuditStampsClass,
        "Cost": CostClass,
        ".Cost": CostClass,
        "com.linkedin.pegasus2avro.common.Cost": CostClass,
        "CostCost": CostCostClass,
        ".CostCost": CostCostClass,
        "com.linkedin.pegasus2avro.common.CostCost": CostCostClass,
        "CostCostDiscriminator": CostCostDiscriminatorClass,
        ".CostCostDiscriminator": CostCostDiscriminatorClass,
        "com.linkedin.pegasus2avro.common.CostCostDiscriminator": CostCostDiscriminatorClass,
        "CostType": CostTypeClass,
        ".CostType": CostTypeClass,
        "com.linkedin.pegasus2avro.common.CostType": CostTypeClass,
        "Deprecation": DeprecationClass,
        ".Deprecation": DeprecationClass,
        "com.linkedin.pegasus2avro.common.Deprecation": DeprecationClass,
        "InstitutionalMemory": InstitutionalMemoryClass,
        ".InstitutionalMemory": InstitutionalMemoryClass,
        "com.linkedin.pegasus2avro.common.InstitutionalMemory": InstitutionalMemoryClass,
        "InstitutionalMemoryMetadata": InstitutionalMemoryMetadataClass,
        ".InstitutionalMemoryMetadata": InstitutionalMemoryMetadataClass,
        "com.linkedin.pegasus2avro.common.InstitutionalMemoryMetadata": InstitutionalMemoryMetadataClass,
        "MLFeatureDataType": MLFeatureDataTypeClass,
        ".MLFeatureDataType": MLFeatureDataTypeClass,
        "com.linkedin.pegasus2avro.common.MLFeatureDataType": MLFeatureDataTypeClass,
        "Owner": OwnerClass,
        ".Owner": OwnerClass,
        "com.linkedin.pegasus2avro.common.Owner": OwnerClass,
        "Ownership": OwnershipClass,
        ".Ownership": OwnershipClass,
        "com.linkedin.pegasus2avro.common.Ownership": OwnershipClass,
        "OwnershipSource": OwnershipSourceClass,
        ".OwnershipSource": OwnershipSourceClass,
        "com.linkedin.pegasus2avro.common.OwnershipSource": OwnershipSourceClass,
        "OwnershipSourceType": OwnershipSourceTypeClass,
        ".OwnershipSourceType": OwnershipSourceTypeClass,
        "com.linkedin.pegasus2avro.common.OwnershipSourceType": OwnershipSourceTypeClass,
        "OwnershipType": OwnershipTypeClass,
        ".OwnershipType": OwnershipTypeClass,
        "com.linkedin.pegasus2avro.common.OwnershipType": OwnershipTypeClass,
        "Status": StatusClass,
        ".Status": StatusClass,
        "com.linkedin.pegasus2avro.common.Status": StatusClass,
        "VersionTag": VersionTagClass,
        ".VersionTag": VersionTagClass,
        "com.linkedin.pegasus2avro.common.VersionTag": VersionTagClass,
        "TransformationType": TransformationTypeClass,
        ".TransformationType": TransformationTypeClass,
        "com.linkedin.pegasus2avro.common.fieldtransformer.TransformationType": TransformationTypeClass,
        "UDFTransformer": UDFTransformerClass,
        ".UDFTransformer": UDFTransformerClass,
        "com.linkedin.pegasus2avro.common.fieldtransformer.UDFTransformer": UDFTransformerClass,
        "DashboardInfo": DashboardInfoClass,
        ".DashboardInfo": DashboardInfoClass,
        "com.linkedin.pegasus2avro.dashboard.DashboardInfo": DashboardInfoClass,
        "DataProcessInfo": DataProcessInfoClass,
        ".DataProcessInfo": DataProcessInfoClass,
        "com.linkedin.pegasus2avro.dataprocess.DataProcessInfo": DataProcessInfoClass,
        "DatasetDeprecation": DatasetDeprecationClass,
        ".DatasetDeprecation": DatasetDeprecationClass,
        "com.linkedin.pegasus2avro.dataset.DatasetDeprecation": DatasetDeprecationClass,
        "DatasetFieldMapping": DatasetFieldMappingClass,
        ".DatasetFieldMapping": DatasetFieldMappingClass,
        "com.linkedin.pegasus2avro.dataset.DatasetFieldMapping": DatasetFieldMappingClass,
        "DatasetLineageType": DatasetLineageTypeClass,
        ".DatasetLineageType": DatasetLineageTypeClass,
        "com.linkedin.pegasus2avro.dataset.DatasetLineageType": DatasetLineageTypeClass,
        "DatasetProperties": DatasetPropertiesClass,
        ".DatasetProperties": DatasetPropertiesClass,
        "com.linkedin.pegasus2avro.dataset.DatasetProperties": DatasetPropertiesClass,
        "DatasetUpstreamLineage": DatasetUpstreamLineageClass,
        ".DatasetUpstreamLineage": DatasetUpstreamLineageClass,
        "com.linkedin.pegasus2avro.dataset.DatasetUpstreamLineage": DatasetUpstreamLineageClass,
        "Upstream": UpstreamClass,
        ".Upstream": UpstreamClass,
        "com.linkedin.pegasus2avro.dataset.Upstream": UpstreamClass,
        "UpstreamLineage": UpstreamLineageClass,
        ".UpstreamLineage": UpstreamLineageClass,
        "com.linkedin.pegasus2avro.dataset.UpstreamLineage": UpstreamLineageClass,
        "CorpGroupInfo": CorpGroupInfoClass,
        ".CorpGroupInfo": CorpGroupInfoClass,
        "com.linkedin.pegasus2avro.identity.CorpGroupInfo": CorpGroupInfoClass,
        "CorpUserEditableInfo": CorpUserEditableInfoClass,
        ".CorpUserEditableInfo": CorpUserEditableInfoClass,
        "com.linkedin.pegasus2avro.identity.CorpUserEditableInfo": CorpUserEditableInfoClass,
        "CorpUserInfo": CorpUserInfoClass,
        ".CorpUserInfo": CorpUserInfoClass,
        "com.linkedin.pegasus2avro.identity.CorpUserInfo": CorpUserInfoClass,
        "ChartSnapshot": ChartSnapshotClass,
        ".ChartSnapshot": ChartSnapshotClass,
        "com.linkedin.pegasus2avro.metadata.snapshot.ChartSnapshot": ChartSnapshotClass,
        "CorpGroupSnapshot": CorpGroupSnapshotClass,
        ".CorpGroupSnapshot": CorpGroupSnapshotClass,
        "com.linkedin.pegasus2avro.metadata.snapshot.CorpGroupSnapshot": CorpGroupSnapshotClass,
        "CorpUserSnapshot": CorpUserSnapshotClass,
        ".CorpUserSnapshot": CorpUserSnapshotClass,
        "com.linkedin.pegasus2avro.metadata.snapshot.CorpUserSnapshot": CorpUserSnapshotClass,
        "DashboardSnapshot": DashboardSnapshotClass,
        ".DashboardSnapshot": DashboardSnapshotClass,
        "com.linkedin.pegasus2avro.metadata.snapshot.DashboardSnapshot": DashboardSnapshotClass,
        "DataProcessSnapshot": DataProcessSnapshotClass,
        ".DataProcessSnapshot": DataProcessSnapshotClass,
        "com.linkedin.pegasus2avro.metadata.snapshot.DataProcessSnapshot": DataProcessSnapshotClass,
        "DatasetSnapshot": DatasetSnapshotClass,
        ".DatasetSnapshot": DatasetSnapshotClass,
        "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot": DatasetSnapshotClass,
        "MLFeatureSnapshot": MLFeatureSnapshotClass,
        ".MLFeatureSnapshot": MLFeatureSnapshotClass,
        "com.linkedin.pegasus2avro.metadata.snapshot.MLFeatureSnapshot": MLFeatureSnapshotClass,
        "MLModelSnapshot": MLModelSnapshotClass,
        ".MLModelSnapshot": MLModelSnapshotClass,
        "com.linkedin.pegasus2avro.metadata.snapshot.MLModelSnapshot": MLModelSnapshotClass,
        "BaseData": BaseDataClass,
        ".BaseData": BaseDataClass,
        "com.linkedin.pegasus2avro.ml.metadata.BaseData": BaseDataClass,
        "CaveatDetails": CaveatDetailsClass,
        ".CaveatDetails": CaveatDetailsClass,
        "com.linkedin.pegasus2avro.ml.metadata.CaveatDetails": CaveatDetailsClass,
        "CaveatsAndRecommendations": CaveatsAndRecommendationsClass,
        ".CaveatsAndRecommendations": CaveatsAndRecommendationsClass,
        "com.linkedin.pegasus2avro.ml.metadata.CaveatsAndRecommendations": CaveatsAndRecommendationsClass,
        "EthicalConsiderations": EthicalConsiderationsClass,
        ".EthicalConsiderations": EthicalConsiderationsClass,
        "com.linkedin.pegasus2avro.ml.metadata.EthicalConsiderations": EthicalConsiderationsClass,
        "EvaluationData": EvaluationDataClass,
        ".EvaluationData": EvaluationDataClass,
        "com.linkedin.pegasus2avro.ml.metadata.EvaluationData": EvaluationDataClass,
        "IntendedUse": IntendedUseClass,
        ".IntendedUse": IntendedUseClass,
        "com.linkedin.pegasus2avro.ml.metadata.IntendedUse": IntendedUseClass,
        "IntendedUserType": IntendedUserTypeClass,
        ".IntendedUserType": IntendedUserTypeClass,
        "com.linkedin.pegasus2avro.ml.metadata.IntendedUserType": IntendedUserTypeClass,
        "MLFeatureProperties": MLFeaturePropertiesClass,
        ".MLFeatureProperties": MLFeaturePropertiesClass,
        "com.linkedin.pegasus2avro.ml.metadata.MLFeatureProperties": MLFeaturePropertiesClass,
        "MLModelFactorPrompts": MLModelFactorPromptsClass,
        ".MLModelFactorPrompts": MLModelFactorPromptsClass,
        "com.linkedin.pegasus2avro.ml.metadata.MLModelFactorPrompts": MLModelFactorPromptsClass,
        "MLModelFactors": MLModelFactorsClass,
        ".MLModelFactors": MLModelFactorsClass,
        "com.linkedin.pegasus2avro.ml.metadata.MLModelFactors": MLModelFactorsClass,
        "MLModelProperties": MLModelPropertiesClass,
        ".MLModelProperties": MLModelPropertiesClass,
        "com.linkedin.pegasus2avro.ml.metadata.MLModelProperties": MLModelPropertiesClass,
        "Metrics": MetricsClass,
        ".Metrics": MetricsClass,
        "com.linkedin.pegasus2avro.ml.metadata.Metrics": MetricsClass,
        "QuantitativeAnalyses": QuantitativeAnalysesClass,
        ".QuantitativeAnalyses": QuantitativeAnalysesClass,
        "com.linkedin.pegasus2avro.ml.metadata.QuantitativeAnalyses": QuantitativeAnalysesClass,
        "SourceCode": SourceCodeClass,
        ".SourceCode": SourceCodeClass,
        "com.linkedin.pegasus2avro.ml.metadata.SourceCode": SourceCodeClass,
        "SourceCodeUrl": SourceCodeUrlClass,
        ".SourceCodeUrl": SourceCodeUrlClass,
        "com.linkedin.pegasus2avro.ml.metadata.SourceCodeUrl": SourceCodeUrlClass,
        "SourceCodeUrlType": SourceCodeUrlTypeClass,
        ".SourceCodeUrlType": SourceCodeUrlTypeClass,
        "com.linkedin.pegasus2avro.ml.metadata.SourceCodeUrlType": SourceCodeUrlTypeClass,
        "TrainingData": TrainingDataClass,
        ".TrainingData": TrainingDataClass,
        "com.linkedin.pegasus2avro.ml.metadata.TrainingData": TrainingDataClass,
        "MetadataChangeEvent": MetadataChangeEventClass,
        ".MetadataChangeEvent": MetadataChangeEventClass,
        "com.linkedin.pegasus2avro.mxe.MetadataChangeEvent": MetadataChangeEventClass,
        "ArrayType": ArrayTypeClass,
        ".ArrayType": ArrayTypeClass,
        "com.linkedin.pegasus2avro.schema.ArrayType": ArrayTypeClass,
        "BinaryJsonSchema": BinaryJsonSchemaClass,
        ".BinaryJsonSchema": BinaryJsonSchemaClass,
        "com.linkedin.pegasus2avro.schema.BinaryJsonSchema": BinaryJsonSchemaClass,
        "BooleanType": BooleanTypeClass,
        ".BooleanType": BooleanTypeClass,
        "com.linkedin.pegasus2avro.schema.BooleanType": BooleanTypeClass,
        "BytesType": BytesTypeClass,
        ".BytesType": BytesTypeClass,
        "com.linkedin.pegasus2avro.schema.BytesType": BytesTypeClass,
        "DatasetFieldForeignKey": DatasetFieldForeignKeyClass,
        ".DatasetFieldForeignKey": DatasetFieldForeignKeyClass,
        "com.linkedin.pegasus2avro.schema.DatasetFieldForeignKey": DatasetFieldForeignKeyClass,
        "EnumType": EnumTypeClass,
        ".EnumType": EnumTypeClass,
        "com.linkedin.pegasus2avro.schema.EnumType": EnumTypeClass,
        "EspressoSchema": EspressoSchemaClass,
        ".EspressoSchema": EspressoSchemaClass,
        "com.linkedin.pegasus2avro.schema.EspressoSchema": EspressoSchemaClass,
        "FixedType": FixedTypeClass,
        ".FixedType": FixedTypeClass,
        "com.linkedin.pegasus2avro.schema.FixedType": FixedTypeClass,
        "ForeignKeySpec": ForeignKeySpecClass,
        ".ForeignKeySpec": ForeignKeySpecClass,
        "com.linkedin.pegasus2avro.schema.ForeignKeySpec": ForeignKeySpecClass,
        "KafkaSchema": KafkaSchemaClass,
        ".KafkaSchema": KafkaSchemaClass,
        "com.linkedin.pegasus2avro.schema.KafkaSchema": KafkaSchemaClass,
        "KeyValueSchema": KeyValueSchemaClass,
        ".KeyValueSchema": KeyValueSchemaClass,
        "com.linkedin.pegasus2avro.schema.KeyValueSchema": KeyValueSchemaClass,
        "MapType": MapTypeClass,
        ".MapType": MapTypeClass,
        "com.linkedin.pegasus2avro.schema.MapType": MapTypeClass,
        "MySqlDDL": MySqlDDLClass,
        ".MySqlDDL": MySqlDDLClass,
        "com.linkedin.pegasus2avro.schema.MySqlDDL": MySqlDDLClass,
        "NullType": NullTypeClass,
        ".NullType": NullTypeClass,
        "com.linkedin.pegasus2avro.schema.NullType": NullTypeClass,
        "NumberType": NumberTypeClass,
        ".NumberType": NumberTypeClass,
        "com.linkedin.pegasus2avro.schema.NumberType": NumberTypeClass,
        "OracleDDL": OracleDDLClass,
        ".OracleDDL": OracleDDLClass,
        "com.linkedin.pegasus2avro.schema.OracleDDL": OracleDDLClass,
        "OrcSchema": OrcSchemaClass,
        ".OrcSchema": OrcSchemaClass,
        "com.linkedin.pegasus2avro.schema.OrcSchema": OrcSchemaClass,
        "OtherSchema": OtherSchemaClass,
        ".OtherSchema": OtherSchemaClass,
        "com.linkedin.pegasus2avro.schema.OtherSchema": OtherSchemaClass,
        "PrestoDDL": PrestoDDLClass,
        ".PrestoDDL": PrestoDDLClass,
        "com.linkedin.pegasus2avro.schema.PrestoDDL": PrestoDDLClass,
        "RecordType": RecordTypeClass,
        ".RecordType": RecordTypeClass,
        "com.linkedin.pegasus2avro.schema.RecordType": RecordTypeClass,
        "SchemaField": SchemaFieldClass,
        ".SchemaField": SchemaFieldClass,
        "com.linkedin.pegasus2avro.schema.SchemaField": SchemaFieldClass,
        "SchemaFieldDataType": SchemaFieldDataTypeClass,
        ".SchemaFieldDataType": SchemaFieldDataTypeClass,
        "com.linkedin.pegasus2avro.schema.SchemaFieldDataType": SchemaFieldDataTypeClass,
        "SchemaMetadata": SchemaMetadataClass,
        ".SchemaMetadata": SchemaMetadataClass,
        "com.linkedin.pegasus2avro.schema.SchemaMetadata": SchemaMetadataClass,
        "Schemaless": SchemalessClass,
        ".Schemaless": SchemalessClass,
        "com.linkedin.pegasus2avro.schema.Schemaless": SchemalessClass,
        "StringType": StringTypeClass,
        ".StringType": StringTypeClass,
        "com.linkedin.pegasus2avro.schema.StringType": StringTypeClass,
        "UnionType": UnionTypeClass,
        ".UnionType": UnionTypeClass,
        "com.linkedin.pegasus2avro.schema.UnionType": UnionTypeClass,
        "UrnForeignKey": UrnForeignKeyClass,
        ".UrnForeignKey": UrnForeignKeyClass,
        "com.linkedin.pegasus2avro.schema.UrnForeignKey": UrnForeignKeyClass,
    }
    
    
    def __init__(self, readers_schema=None, **kwargs):
        writers_schema = kwargs.pop("writers_schema", readers_schema)
        writers_schema = kwargs.pop("writer_schema", writers_schema)
        super(SpecificDatumReader, self).__init__(writers_schema, readers_schema, **kwargs)
    
    
    def read_record(self, writers_schema, readers_schema, decoder):
        result = super(SpecificDatumReader, self).read_record(writers_schema, readers_schema, decoder)
        
        if readers_schema.fullname in SpecificDatumReader.SCHEMA_TYPES:
            result = SpecificDatumReader.SCHEMA_TYPES[readers_schema.fullname](result)
        
        return result