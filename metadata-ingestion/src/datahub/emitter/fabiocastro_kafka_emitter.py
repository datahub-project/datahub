import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import ChangeTypeClass, DatasetPropertiesClass, SchemaFieldClass, SchemaFieldDataTypeClass
from datahub.ingestion.api.source import SourceReport
from datahub.emitter.kafka_emitter import DatahubKafkaEmitter, KafkaEmitterConfig

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.sql_common import SQLAlchemyConfig, SQLAlchemySource
from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    make_data_platform_urn,
    make_dataset_urn,
)

from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    EnumTypeClass,
    ForeignKeyConstraint,
    MySqlDDL,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
)

# Criando um emitter para Kafka
emitter = DatahubKafkaEmitter(
    KafkaEmitterConfig.parse_obj(
        {
            "connection": {
                "bootstrap": "localhost:9092",
                "schema_registry_url": "http://localhost:8081",
                "schema_registry_config": {}, # schema_registry configuracao para schema registry client
                "producer_config": {}, # extra producer configuracao para kafka producer
            }
        }
    )
)



# Construcao dataset propriedades do objeto
schema_metadata = SchemaMetadata(
    schemaName="dataset_name",
    platform="urn:li:dataPlatform:mysql",
    version=0,
    hash="",
    platformSchema=MySqlDDL(tableSchema=""),
    fields=[
        SchemaFieldClass(fieldPath="idproduto", type=SchemaFieldDataTypeClass(type=StringTypeClass()), nativeDataType="varchar(100)"),
        SchemaFieldClass(fieldPath="idvenda", type=SchemaFieldDataTypeClass(type=StringTypeClass()), nativeDataType="varchar(100)"),
        SchemaFieldClass(fieldPath="idcompra", type=SchemaFieldDataTypeClass(type=NumberTypeClass()), nativeDataType="int(10)"),
        SchemaFieldClass(fieldPath="idtelemovel", type=SchemaFieldDataTypeClass(type=NumberTypeClass()), nativeDataType="int(10)"),
        SchemaFieldClass(fieldPath="idaddress", type=SchemaFieldDataTypeClass(type=NumberTypeClass()), nativeDataType="int(10)")
    ]
)


# Construcao do MetadataChangeProposalWrapper objeto 
metadata_event = MetadataChangeProposalWrapper(
    entityType="dataset",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=builder.make_dataset_urn("mysql", "fabio-mysql.fabio-dataset.user-table"),
    aspectName="schemaMetadata",
    aspect=schema_metadata,
)


# Envia o metadata!
emitter.emit(
    metadata_event,
    callback=lambda exc, message: print(f"Message sent to topic:{message.topic()}, partition:{message.partition()}, offset:{message.offset()}") if message else print(f"Failed to send with: {exc}")
)

#Envia todos todos eventos
emitter.flush()