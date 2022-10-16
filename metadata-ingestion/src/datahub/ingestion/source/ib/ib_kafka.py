from datahub.ingestion.api.decorators import config_class, platform_name

from datahub.ingestion.source.ib.ib_common import *


class IBKafkaSourceConfig(IBRedashSourceConfig):
    pass


@platform_name("IBKafka")
@config_class(IBKafkaSourceConfig)
class IBKafkaSource(IBRedashDatasetSource):
    parent_subtypes = ["DataCenter", "Kafka Cluster"]
    object_subtype = "Kafka Topic"
    platform = "kafka"

    def __init__(self, config: IBKafkaSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config: IBKafkaSourceConfig = config

    def get_default_ingestion_job_id(self) -> JobId:
        return JobId("ingest_kafka_from_redash_source")
