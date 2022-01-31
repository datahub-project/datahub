import datahub.emitter.mce_builder as builder
from datahub.emitter.kafka_emitter import DatahubKafkaEmitter, KafkaEmitterConfig

# Construcao lineage objeto.
lineage_mce = builder.make_lineage_mce(
    [
        builder.make_dataset_urn("bigquery", "upstream1"),
        builder.make_dataset_urn("bigquery", "upstream2"),
    ],
    builder.make_dataset_urn("bigquery", "downstream2"),
)

# Criando um emitter para Datahub Kafka broker
emitter = DatahubKafkaEmitter(
    KafkaEmitterConfig.parse_obj(
        # Mesma configuracao padrao do Kafka sink YAML.
        {
            "connection": {
                "bootstrap": "localhost:9092",
                "producer_config": {},
                "schema_registry_url": "http://localhost:8081",
            }
        }
    )
)


# Envia metadata!
def callback(err, msg):
    if err:
        # Verifica metadata emissao de erro.
        print("error:", err)


emitter.emit_mce_async(lineage_mce, callback)
emitter.flush()
